package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.RocksDBGlobalResourceManager;
import org.example.utils.StorageUtils;

import org.rocksdb.*;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 原生 RocksDB 基准 — 32 ColumnFamily 并行写入
 *
 * 并行度对齐：
 *   - TDengine:  32 个 vgroup 并行
 *   - LitecsDB:  ~48 个 HEALPix RocksDB 实例并行
 *   - InfluxDB:  32 个 HTTP 客户端连接并行
 *   - NativeRocksDB: 32 个 ColumnFamily 并行 ← 本文件
 *
 * 16 个写入线程，每线程按 sourceId hash 分发到 32 个 CF，
 * 每个 CF 独立 WriteBatch → db.write()
 */
public class NativeRocksDBWrapper implements AutoCloseable {
    private static final int WRITE_THREADS = 16;
    private static final int BATCH_SIZE = 10_000;
    private static final int NUM_CF = 32;

    private final String dbPath;
    private RocksDB db;
    private final ColumnFamilyHandle[] cfHandles;
    private final ExecutorService writePool;
    private final WriteOptions writeOptions;

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

            // DB 级选项
            DBOptions dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setEnv(globalRes.getEnv())
                    .setWriteBufferManager(globalRes.getWriteBufferManager())
                    .setMaxBackgroundJobs(16)
                    .setIncreaseParallelism(Math.max(8, Runtime.getRuntime().availableProcessors()))
                    .setBytesPerSync(4L * 1024 * 1024);

            // CF 级选项
            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setTableFormatConfig(tableConfig)
                    .setWriteBufferSize(256L * 1024 * 1024)
                    .setMaxWriteBufferNumber(4)
                    .setMinWriteBufferNumberToMerge(2)
                    .setLevel0FileNumCompactionTrigger(8)
                    .setLevel0SlowdownWritesTrigger(32)
                    .setLevel0StopWritesTrigger(48);

            // 创建 32 个 CF
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(), cfOptions));
            for (int i = 0; i < NUM_CF; i++) {
                cfDescriptors.add(new ColumnFamilyDescriptor(
                        ("cf_" + i).getBytes(), cfOptions));
            }

            List<ColumnFamilyHandle> handles = new ArrayList<>();
            this.db = RocksDB.open(dbOptions, dbPath, cfDescriptors, handles);

            this.cfHandles = new ColumnFamilyHandle[NUM_CF];
            for (int i = 0; i < NUM_CF; i++) {
                cfHandles[i] = handles.get(i + 1); // skip default
            }

            // WAL=on: 写入返回时数据在 WAL，与 InfluxDB/TDengine 对齐
            this.writeOptions = new WriteOptions()
                    .setDisableWAL(false)
                    .setSync(false);

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

        System.out.println("[Phase 1] RocksDB initialized");
        System.out.printf("  %d column_families, write_buffer=256MB, write_threads=%d, batch_size=%d, WAL=on%n",
                NUM_CF, WRITE_THREADS, BATCH_SIZE);
    }

    /**
     * 16 线程并行写入，按 sourceId hash 分发到 32 个 CF
     */
    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) return;

        logicalBytesWritten.addAndGet(points.size() * 90L);

        // 按 CF 分组
        Map<Integer, List<LightCurvePoint>> cfBuckets = new HashMap<>();
        for (LightCurvePoint p : points) {
            int cf = (int) (Math.abs(p.sourceId) % NUM_CF);
            cfBuckets.computeIfAbsent(cf, k -> new ArrayList<>()).add(p);
        }

        // 分成 WRITE_THREADS 份
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

    /**
     * 每个 CF 独立 WriteBatch
     */
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
        if (cfHandles != null) {
            for (ColumnFamilyHandle cfh : cfHandles) {
                if (cfh != null) cfh.close();
            }
        }
        if (db != null) db.close();
    }
}