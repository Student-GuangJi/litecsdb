package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.RocksDBGlobalResourceManager;
import org.example.utils.StorageUtils;

import org.rocksdb.*;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 原生 RocksDB 基准 — 32 ColumnFamily 并行写入 + 查询支持
 */
public class NativeRocksDBWrapper implements AutoCloseable {
    private static final int WRITE_THREADS = 16;
    private static final int BATCH_SIZE = 10_000;
    private static final int NUM_CF = 32;

    private final String dbPath;
    private RocksDB db;
    private ColumnFamilyHandle[] cfHandles;
    private final ExecutorService writePool;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;

    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private long initialDirSize = 0;

    public NativeRocksDBWrapper(String dbPath) {
        this.dbPath = dbPath;
        RocksDB.loadLibrary();

        try {
            new File(dbPath).mkdirs();

            RocksDBGlobalResourceManager globalRes = RocksDBGlobalResourceManager.getInstance();

            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setBlockCache(globalRes.getBlockCache());

            DBOptions dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setEnv(globalRes.getEnv())
                    .setWriteBufferManager(globalRes.getWriteBufferManager())
                    .setMaxBackgroundJobs(16)
                    .setIncreaseParallelism(Math.max(8, Runtime.getRuntime().availableProcessors()))
                    .setBytesPerSync(4L * 1024 * 1024);

            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setTableFormatConfig(tableConfig)
                    .setWriteBufferSize(256L * 1024 * 1024)
                    .setMaxWriteBufferNumber(4)
                    .setMinWriteBufferNumberToMerge(2)
                    .setLevel0FileNumCompactionTrigger(8)
                    .setLevel0SlowdownWritesTrigger(32)
                    .setLevel0StopWritesTrigger(48);

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
            for (int i = 0; i < NUM_CF; i++) {
                cfDescriptors.add(new ColumnFamilyDescriptor(("cf_" + i).getBytes(), cfOptions));
            }

            List<ColumnFamilyHandle> handles = new ArrayList<>();
            this.db = RocksDB.open(dbOptions, dbPath, cfDescriptors, handles);

            this.cfHandles = new ColumnFamilyHandle[NUM_CF];
            for (int i = 0; i < NUM_CF; i++) {
                cfHandles[i] = handles.get(i + 1);
            }

            this.writeOptions = new WriteOptions().setDisableWAL(false).setSync(false);
            this.readOptions = new ReadOptions();

            this.initialDirSize = StorageUtils.getDirectorySize(dbPath);

        } catch (RocksDBException e) {
            throw new RuntimeException("NativeRocksDB init failed", e);
        }

        this.writePool = Executors.newFixedThreadPool(WRITE_THREADS, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("NativeRocksDB-Writer-" + t.getId());
            return t;
        });
    }

    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) return;

        logicalBytesWritten.addAndGet(points.size() * 90L);

        Map<Integer, List<LightCurvePoint>> cfBuckets = new HashMap<>();
        for (LightCurvePoint p : points) {
            int cf = (int) (Math.abs(p.sourceId) % NUM_CF);
            cfBuckets.computeIfAbsent(cf, k -> new ArrayList<>()).add(p);
        }

        int chunkSize = Math.max(1, (cfBuckets.size() + WRITE_THREADS - 1) / WRITE_THREADS);
        List<List<Map.Entry<Integer, List<LightCurvePoint>>>> chunks = new ArrayList<>();
        List<Map.Entry<Integer, List<LightCurvePoint>>> entries = new ArrayList<>(cfBuckets.entrySet());

        for (int i = 0; i < entries.size(); i += chunkSize) {
            chunks.add(entries.subList(i, Math.min(i + chunkSize, entries.size())));
        }

        CountDownLatch latch = new CountDownLatch(chunks.size());
        for (List<Map.Entry<Integer, List<LightCurvePoint>>> chunk : chunks) {
            writePool.submit(() -> {
                try {
                    for (Map.Entry<Integer, List<LightCurvePoint>> entry : chunk) {
                        int cfIdx = entry.getKey();
                        List<LightCurvePoint> cfPoints = entry.getValue();
                        writeCFBatch(cfIdx, cfPoints);
                    }
                } catch (RocksDBException e) {
                    System.err.println("NativeRocksDB write failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void writeCFBatch(int cfIdx, List<LightCurvePoint> points) throws RocksDBException {
        ColumnFamilyHandle cfh = cfHandles[cfIdx];

        for (int start = 0; start < points.size(); start += BATCH_SIZE) {
            int end = Math.min(start + BATCH_SIZE, points.size());

            try (WriteBatch batch = new WriteBatch()) {
                for (int i = start; i < end; i++) {
                    LightCurvePoint p = points.get(i);
                    batch.put(cfh, buildKey(p), serializePoint(p));
                }
                db.write(writeOptions, batch);
            }
        }
    }

    /**
     * 简单查询：按sourceId前缀扫描，返回行数
     */
    public int queryBySourceId(long sourceId) {
        return queryBySourceIdFull(sourceId).size();
    }

    /**
     * 简单查询：按sourceId前缀扫描，返回完整数据
     */
    public List<LightCurvePoint> queryBySourceIdFull(long sourceId) {
        int cfIdx = (int) (Math.abs(sourceId) % NUM_CF);
        ColumnFamilyHandle cfh = cfHandles[cfIdx];

        ByteBuffer prefixBuf = ByteBuffer.allocate(8);
        prefixBuf.putLong(sourceId);
        byte[] prefix = prefixBuf.array();

        List<LightCurvePoint> results = new ArrayList<>();
        try (RocksIterator it = db.newIterator(cfh, readOptions)) {
            it.seek(prefix);
            while (it.isValid()) {
                byte[] key = it.key();
                if (key.length >= 8) {
                    ByteBuffer keyBuf = ByteBuffer.wrap(key);
                    long keySid = keyBuf.getLong();
                    if (keySid != sourceId) break;
                    LightCurvePoint p = deserializePoint(it.value());
                    if (p != null) results.add(p);
                }
                it.next();
            }
        }
        return results;
    }

    /**
     * STMK查询：全表扫描 + 过滤 + 分组聚合
     */
    public int executeSTMKQuery(double startTime, double endTime, String band,
                                double magMin, double magMax,
                                double ra, double dec, double radius, int minObs) {
        Map<Long, int[]> sourceStats = new HashMap<>();  // sourceId -> [count, hasCoords, ra*1e6, dec*1e6]

        // 并行扫描所有CF
        AtomicInteger totalMatches = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(NUM_CF);

        for (int cf = 0; cf < NUM_CF; cf++) {
            final int cfIdx = cf;
            writePool.submit(() -> {
                try {
                    Map<Long, int[]> localStats = new HashMap<>();
                    ColumnFamilyHandle cfh = cfHandles[cfIdx];

                    try (RocksIterator it = db.newIterator(cfh, readOptions)) {
                        for (it.seekToFirst(); it.isValid(); it.next()) {
                            LightCurvePoint p = deserializePoint(it.value());
                            if (p == null) continue;

                            // 时间+波段+星等过滤
                            if (p.time >= startTime && p.time <= endTime &&
                                    p.band.equals(band) &&
                                    p.mag >= magMin && p.mag <= magMax) {

                                int[] stats = localStats.computeIfAbsent(p.sourceId,
                                        k -> new int[]{0, 0, (int)(p.ra * 1e6), (int)(p.dec * 1e6)});
                                stats[0]++;
                                stats[1] = 1;
                            }
                        }
                    }

                    // 合并到全局统计
                    synchronized (sourceStats) {
                        for (Map.Entry<Long, int[]> e : localStats.entrySet()) {
                            sourceStats.merge(e.getKey(), e.getValue(), (old, nw) -> {
                                old[0] += nw[0];
                                return old;
                            });
                        }
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // 应用空间过滤和minObs约束
        int count = 0;
        for (Map.Entry<Long, int[]> entry : sourceStats.entrySet()) {
            int[] stats = entry.getValue();
            if (stats[0] >= minObs) {
                double pRa = stats[2] / 1e6;
                double pDec = stats[3] / 1e6;
                if (isInCone(pRa, pDec, ra, dec, radius)) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * 时间范围查询：按sourceId前缀扫描 + 时间过滤
     */
    public List<LightCurvePoint> queryBySourceIdTimeRange(long sourceId, double startTime, double endTime) {
        int cfIdx = (int) (Math.abs(sourceId) % NUM_CF);
        ColumnFamilyHandle cfh = cfHandles[cfIdx];
        ByteBuffer prefixBuf = ByteBuffer.allocate(8);
        prefixBuf.putLong(sourceId);
        byte[] prefix = prefixBuf.array();
        List<LightCurvePoint> results = new ArrayList<>();
        try (RocksIterator it = db.newIterator(cfh, readOptions)) {
            it.seek(prefix);
            while (it.isValid()) {
                byte[] key = it.key();
                if (key.length >= 8) {
                    ByteBuffer keyBuf = ByteBuffer.wrap(key);
                    long keySid = keyBuf.getLong();
                    if (keySid != sourceId) break;
                    LightCurvePoint p = deserializePoint(it.value());
                    if (p != null && p.time >= startTime && p.time <= endTime) results.add(p);
                }
                it.next();
            }
        }
        return results;
    }

    /**
     * 聚合统计查询：按sourceId前缀扫描，返回 double[]{count, avgMag, minMag, maxMag}
     */
    public double[] queryAggregation(long sourceId) {
        int cfIdx = (int) (Math.abs(sourceId) % NUM_CF);
        ColumnFamilyHandle cfh = cfHandles[cfIdx];
        ByteBuffer prefixBuf = ByteBuffer.allocate(8);
        prefixBuf.putLong(sourceId);
        byte[] prefix = prefixBuf.array();
        int count = 0; double sum = 0, min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        try (RocksIterator it = db.newIterator(cfh, readOptions)) {
            it.seek(prefix);
            while (it.isValid()) {
                byte[] key = it.key();
                if (key.length >= 8) {
                    ByteBuffer keyBuf = ByteBuffer.wrap(key);
                    if (keyBuf.getLong() != sourceId) break;
                    LightCurvePoint p = deserializePoint(it.value());
                    if (p != null) { count++; sum += p.mag; if (p.mag < min) min = p.mag; if (p.mag > max) max = p.mag; }
                }
                it.next();
            }
        }
        return new double[]{count, count > 0 ? sum / count : 0, count > 0 ? min : 0, count > 0 ? max : 0};
    }

    /**
     * 单波段查询：按sourceId前缀扫描 + band过滤
     */
    public List<LightCurvePoint> queryBySourceIdBand(long sourceId, String band) {
        int cfIdx = (int) (Math.abs(sourceId) % NUM_CF);
        ColumnFamilyHandle cfh = cfHandles[cfIdx];
        ByteBuffer prefixBuf = ByteBuffer.allocate(8);
        prefixBuf.putLong(sourceId);
        byte[] prefix = prefixBuf.array();
        List<LightCurvePoint> results = new ArrayList<>();
        try (RocksIterator it = db.newIterator(cfh, readOptions)) {
            it.seek(prefix);
            while (it.isValid()) {
                byte[] key = it.key();
                if (key.length >= 8) {
                    ByteBuffer keyBuf = ByteBuffer.wrap(key);
                    if (keyBuf.getLong() != sourceId) break;
                    LightCurvePoint p = deserializePoint(it.value());
                    if (p != null && p.band.equals(band)) results.add(p);
                }
                it.next();
            }
        }
        return results;
    }

    private boolean isInCone(double ra1, double dec1, double ra2, double dec2, double radius) {
        double ra1R = Math.toRadians(ra1), dec1R = Math.toRadians(dec1);
        double ra2R = Math.toRadians(ra2), dec2R = Math.toRadians(dec2);
        double a = Math.sin((dec2R - dec1R) / 2) * Math.sin((dec2R - dec1R) / 2) +
                Math.cos(dec1R) * Math.cos(dec2R) *
                        Math.sin((ra2R - ra1R) / 2) * Math.sin((ra2R - ra1R) / 2);
        return Math.toDegrees(2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))) <= radius;
    }

    private byte[] buildKey(LightCurvePoint p) {
        byte[] bandBytes = p.band.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(17 + bandBytes.length);
        buf.putLong(p.sourceId);
        buf.put((byte) bandBytes.length);
        buf.put(bandBytes);
        buf.putDouble(p.time);
        return buf.array();
    }

    private byte[] serializePoint(LightCurvePoint p) {
        byte[] bandBytes = p.band.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(90 + bandBytes.length);
        buf.putLong(p.sourceId);
        buf.putDouble(p.ra);
        buf.putDouble(p.dec);
        buf.putLong(p.transitId);
        buf.putInt(bandBytes.length);
        buf.put(bandBytes);
        buf.putDouble(p.time);
        buf.putDouble(p.mag);
        buf.putDouble(p.flux);
        buf.putDouble(p.fluxError);
        buf.putDouble(p.fluxOverError);
        buf.put((byte) (p.rejectedByPhotometry ? 1 : 0));
        buf.put((byte) (p.rejectedByVariability ? 1 : 0));
        buf.putInt(p.otherFlags);
        buf.putLong(p.solutionId);
        return buf.array();
    }

    private LightCurvePoint deserializePoint(byte[] data) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(data);
            long sourceId = buf.getLong();
            double ra = buf.getDouble();
            double dec = buf.getDouble();
            long transitId = buf.getLong();
            int bandLen = buf.getInt();
            byte[] bandBytes = new byte[bandLen];
            buf.get(bandBytes);
            String band = new String(bandBytes, StandardCharsets.UTF_8);
            double time = buf.getDouble();
            double mag = buf.getDouble();
            double flux = buf.getDouble();
            double fluxError = buf.getDouble();
            double fluxOverError = buf.getDouble();
            boolean rejectedByPhotometry = buf.get() == 1;
            boolean rejectedByVariability = buf.get() == 1;
            int otherFlags = buf.getInt();
            long solutionId = buf.getLong();

            return new LightCurvePoint(sourceId, ra, dec, transitId, band, time, mag,
                    flux, fluxError, fluxOverError, rejectedByPhotometry,
                    rejectedByVariability, otherFlags, solutionId);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 清除查询缓存 — 关闭并重新打开 DB，使用独立 block cache
     * 确保查询不使用任何缓存数据，保证公平对比
     */
    public void clearQueryCache() {
        try {
            // 1. 关闭当前 DB 和 handles
            if (readOptions != null) readOptions.close();
            if (writeOptions != null) writeOptions.close();
            for (ColumnFamilyHandle cfh : cfHandles) {
                if (cfh != null) cfh.close();
            }
            db.close();

            // 2. 使用独立的新 block cache 重新打开（不共享全局缓存）
            Cache freshCache = new LRUCache(64 * 1024 * 1024); // 64MB 独立 cache
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setBlockCache(freshCache)
                    .setNoBlockCache(false);

            DBOptions dbOptions = new DBOptions()
                    .setCreateIfMissing(false)
                    .setCreateMissingColumnFamilies(false)
                    .setMaxBackgroundJobs(4);

            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setTableFormatConfig(tableConfig);

            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
            for (int i = 0; i < NUM_CF; i++) {
                cfDescriptors.add(new ColumnFamilyDescriptor(("cf_" + i).getBytes(), cfOptions));
            }

            List<ColumnFamilyHandle> handles = new ArrayList<>();
            this.db = RocksDB.open(dbOptions, dbPath, cfDescriptors, handles);
            for (int i = 0; i < NUM_CF; i++) {
                cfHandles[i] = handles.get(i + 1);
            }
            this.writeOptions = new WriteOptions().setDisableWAL(false).setSync(false);
            this.readOptions = new ReadOptions();

        } catch (RocksDBException e) {
            System.err.println("NativeRocksDB cache clear failed: " + e.getMessage());
        }
    }

    public void forceFlush() {
        try {
            for (ColumnFamilyHandle cfh : cfHandles) {
                db.flush(new FlushOptions().setWaitForFlush(true), cfh);
            }
            for (ColumnFamilyHandle cfh : cfHandles) {
                db.compactRange(cfh);
            }
        } catch (RocksDBException e) {
            System.err.println("NativeRocksDB flush/compact failed: " + e.getMessage());
        }
    }

    public double getWriteAmplification() {
        long logical = logicalBytesWritten.get();
        if (logical == 0) return 1.0;
        long currentDirSize = StorageUtils.getDirectorySize(dbPath);
        long physical = currentDirSize - initialDirSize;
        return physical > 0 ? (double) physical / logical : 1.0;
    }

    @Override
    public void close() {
        writePool.shutdown();
        try { writePool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        if (writeOptions != null) writeOptions.close();
        if (readOptions != null) readOptions.close();
        if (cfHandles != null) {
            for (ColumnFamilyHandle cfh : cfHandles) {
                if (cfh != null) cfh.close();
            }
        }
        if (db != null) db.close();
    }
}