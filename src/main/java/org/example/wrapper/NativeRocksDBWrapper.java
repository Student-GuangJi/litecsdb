package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RocksDB 客户端包装器 — 仿照 TDengine C++ 两阶段（Two-Phase）策略
 *
 * <h2>与 TDengine 配置的等价对应关系</h2>
 * <pre>
 * ┌──────────────────────────┬──────────────────────────────────────────────────────┐
 * │ TDengine 配置            │ RocksDB 等价配置                                     │
 * ├──────────────────────────┼──────────────────────────────────────────────────────┤
 * │ NUM_THREADS = 16         │ WRITE_THREADS = 16（客户端并发写入线程）              │
 * │ BATCH_SIZE = 10000       │ BATCH_SIZE = 10000（每批 WriteBatch 行数）           │
 * │ NUM_VGROUPS = 32         │ CF_COUNT = 32 个 ColumnFamily                       │
 * │                          │ （每个CF独立 memtable/flush，等价于独立vgroup）       │
 * │ BUFFER_SIZE = 256MB/vg   │ writeBufferSize = 256MB per CF                      │
 * │                          │ （每个CF独立256MB写缓冲，等价于每vgroup 256MB）       │
 * └──────────────────────────┴──────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>两阶段策略</h2>
 * <ul>
 *   <li>Phase 1（构造函数）: 预创建 32 个 ColumnFamily，每个配置 256MB 写缓冲（不计入写入时间）</li>
 *   <li>Phase 2（putBatch）: 按 sourceId 哈希分片到 32 个 CF → 轮询分配到 16 个线程 → 多线程并行 WriteBatch 写入</li>
 * </ul>
 */
public class NativeRocksDBWrapper implements AutoCloseable {

    // ==================== 配置常量（对齐 TDengine C++ 版本）====================

    /** 并行写入线程数（对应 TDengine NUM_THREADS = 16） */
    private static final int WRITE_THREADS = 16;

    /** 每批 WriteBatch 行数（对应 TDengine BATCH_SIZE = 10000） */
    private static final int BATCH_SIZE = 10_000;

    /**
     * ColumnFamily 数量（对应 TDengine NUM_VGROUPS = 32）。
     * 每个 CF 拥有独立的 memtable 和 flush 通道，实现与 vgroup 等价的并行写入隔离。
     */
    private static final int CF_COUNT = 32;

    /**
     * 每个 ColumnFamily 的写缓冲大小（对应 TDengine BUFFER_SIZE = 256 MB/vgroup）。
     * 每个 CF 独占 256MB memtable，总内存预算 = CF_COUNT × WRITE_BUFFER_SIZE_PER_CF
     *                                          = 32 × 256MB = 8GB
     */
    private static final long WRITE_BUFFER_SIZE_PER_CF = 256L * 1024 * 1024;

    /** 每个 CF 最大 memtable 数量（允许后台 flush 时前台继续写入） */
    private static final int MAX_WRITE_BUFFER_NUMBER = 4;

    /** 合并前最少 memtable 数量 */
    private static final int MIN_WRITE_BUFFER_NUMBER_TO_MERGE = 2;

    private static final String DEFAULT_DB_PATH = "/mnt/nvme/home/wangxc/litecsdb/nativeRocksDB/";
    private static final long LOGICAL_BYTES_PER_POINT = 90L;

    // ==================== 实例字段 ====================

    private final String dbPath;
    private final DBOptions dbOptions;
    private final WriteOptions writeOptions;
    private final FlushOptions flushOptions;
    private final Statistics statistics;

    private RocksDB db;

    /** 32 个 ColumnFamily 句柄（对应 32 个 vgroup） */
    private final ColumnFamilyHandle[] cfHandles;

    /** 32 个 CF 各自的 ColumnFamilyOptions（每个独立配置 256MB 写缓冲） */
    private final ColumnFamilyOptions[] cfOptionsList;

    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private final AtomicLong failedRows = new AtomicLong(0);
    private final AtomicLong insertedRows = new AtomicLong(0);
    private long initialDirSize = 0;

    // ==================== 构造函数（Phase 1：预创建 ColumnFamily，不计入写入时间）====================

    public NativeRocksDBWrapper() {
        this(DEFAULT_DB_PATH);
    }

    public NativeRocksDBWrapper(String dbPath) {
        this.dbPath = normalizePath(dbPath);
        ensureDirectoryExists(this.dbPath);
        RocksDB.loadLibrary();

        this.statistics = new Statistics();

        // ========== DBOptions（全局配置）==========
        this.dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                // 后台 flush/compaction 线程数 = WRITE_THREADS（对齐 C++ NUM_THREADS）
                .setMaxBackgroundJobs(WRITE_THREADS)
                .setIncreaseParallelism(Math.max(WRITE_THREADS, Runtime.getRuntime().availableProcessors()))
                .setBytesPerSync(4L * 1024 * 1024)
                .setStatistics(statistics);

        // ========== WriteOptions（写入选项）==========
        this.writeOptions = new WriteOptions()
                .setDisableWAL(true)
                .setSync(false)
                .setNoSlowdown(false);

        this.flushOptions = new FlushOptions().setWaitForFlush(true);

        // ========== Phase 1: 预创建 32 个 ColumnFamily ==========
        this.cfOptionsList = new ColumnFamilyOptions[CF_COUNT];
        this.cfHandles = new ColumnFamilyHandle[CF_COUNT];

        try {
            // 构建 CF 描述列表
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>(CF_COUNT + 1);

            // 默认 CF（RocksDB 强制要求）
            ColumnFamilyOptions defaultCfOptions = createCfOptions();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultCfOptions));

            // 32 个数据 CF（对应 32 个 vgroup）
            for (int i = 0; i < CF_COUNT; i++) {
                cfOptionsList[i] = createCfOptions();
                String cfName = "vg_" + i;
                cfDescriptors.add(new ColumnFamilyDescriptor(
                        cfName.getBytes(StandardCharsets.UTF_8), cfOptionsList[i]));
            }

            // 打开数据库（含所有 CF）
            List<ColumnFamilyHandle> handleList = new ArrayList<>(CF_COUNT + 1);
            this.db = RocksDB.open(dbOptions, this.dbPath, cfDescriptors, handleList);

            // handleList[0] = default CF, handleList[1..32] = vg_0..vg_31
            ColumnFamilyHandle defaultHandle = handleList.get(0);
            for (int i = 0; i < CF_COUNT; i++) {
                cfHandles[i] = handleList.get(i + 1);
            }

            this.initialDirSize = StorageUtils.getDirectorySize(this.dbPath);

            System.out.printf("[Phase 1] RocksDB 初始化完成%n");
            System.out.printf("  column_families=%d, write_buffer_per_cf=%dMB, write_threads=%d, batch_size=%d%n",
                    CF_COUNT, WRITE_BUFFER_SIZE_PER_CF / (1024 * 1024), WRITE_THREADS, BATCH_SIZE);
            System.out.printf("  总内存预算: %d MB (= %d CF × %d MB)%n",
                    CF_COUNT * (WRITE_BUFFER_SIZE_PER_CF / (1024 * 1024)),
                    CF_COUNT, WRITE_BUFFER_SIZE_PER_CF / (1024 * 1024));

            // 关闭默认CF的options（已传给RocksDB，但我们不单独追踪它）
            // defaultCfOptions 的生命周期由 RocksDB 管理

        } catch (RocksDBException e) {
            close();
            throw new RuntimeException("初始化 RocksDB 失败", e);
        }
    }

    /**
     * 创建单个 ColumnFamily 的配置（对应 TDengine 单个 vgroup 的配置）。
     * 每个 CF 独立 256MB 写缓冲，独立 flush 通道。
     */
    private static ColumnFamilyOptions createCfOptions() {
        return new ColumnFamilyOptions()
                // 每个 CF 256MB 写缓冲（对应 TDengine BUFFER = 256 MB/vgroup）
                .setWriteBufferSize(WRITE_BUFFER_SIZE_PER_CF)
                // 最多 4 个 memtable（允许后台 flush 时不阻塞写入）
                .setMaxWriteBufferNumber(MAX_WRITE_BUFFER_NUMBER)
                // 至少 2 个 memtable 才合并 flush
                .setMinWriteBufferNumberToMerge(MIN_WRITE_BUFFER_NUMBER_TO_MERGE)
                // compaction 触发阈值
                .setLevel0FileNumCompactionTrigger(8)
                .setLevel0SlowdownWritesTrigger(32)
                .setLevel0StopWritesTrigger(48);
    }

    // ==================== Phase 2: 多线程并行 WriteBatch 写入 ====================

    /**
     * 对外暴露的批量写入入口。
     * 仿照 C++ 两阶段策略 Phase 2：
     *   1. 按 sourceId 哈希分片到 CF_COUNT=32 个 ColumnFamily（对应 C++ 按 SubTable/vgroup 分组）
     *   2. 将 32 个分片轮询分配到 WRITE_THREADS=16 个线程
     *   3. 每线程按 BATCH_SIZE=10000 分批创建 WriteBatch 并写入
     */
    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }

        // Step 1: 按 ColumnFamily（vgroup）分组
        @SuppressWarnings("unchecked")
        List<LightCurvePoint>[] cfBuckets = new List[CF_COUNT];
        for (int i = 0; i < CF_COUNT; i++) {
            cfBuckets[i] = new ArrayList<>();
        }

        for (LightCurvePoint p : points) {
            int cf = (int) Math.floorMod(p.sourceId, CF_COUNT);
            cfBuckets[cf].add(p);
        }

        // Step 2: 将 32 个 CF 分片轮询分配到 16 个线程
        //  每个线程持有若干 (cfIndex, points) 对
        @SuppressWarnings("unchecked")
        List<int[]>[] threadAssignments = new List[WRITE_THREADS]; // int[] = {cfIndex}
        @SuppressWarnings("unchecked")
        List<List<LightCurvePoint>>[] threadData = new List[WRITE_THREADS];

        for (int i = 0; i < WRITE_THREADS; i++) {
            threadAssignments[i] = new ArrayList<>();
            threadData[i] = new ArrayList<>();
        }

        for (int cf = 0; cf < CF_COUNT; cf++) {
            if (!cfBuckets[cf].isEmpty()) {
                int tid = cf % WRITE_THREADS;
                threadAssignments[tid].add(new int[]{cf});
                threadData[tid].add(cfBuckets[cf]);
            }
        }

        // Step 3: 多线程并行写入
        ExecutorService executor = Executors.newFixedThreadPool(WRITE_THREADS);
        CountDownLatch latch = new CountDownLatch(WRITE_THREADS);

        for (int threadId = 0; threadId < WRITE_THREADS; threadId++) {
            final int tid = threadId;
            final List<int[]> myCfIndices = threadAssignments[tid];
            final List<List<LightCurvePoint>> myData = threadData[tid];

            executor.submit(() -> {
                try {
                    insertWorker(tid, myCfIndices, myData);
                } catch (Exception e) {
                    System.err.printf("[ERROR] Thread %d 写入异常: %s%n", tid, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdown();
    }

    /**
     * 单个写入线程的工作函数（对应 C++ 的 insert_worker / direct_worker_thread）。
     *
     * 每线程处理分配给自己的 CF 分片：
     *   - 对每个 CF，按 BATCH_SIZE 分批创建 WriteBatch
     *   - WriteBatch.put 到指定的 ColumnFamilyHandle
     *   - db.write(writeOptions, batch)
     *
     * RocksDB 的 db.write() 是线程安全的，多线程可并发调用。
     * 不同 CF 有独立的 memtable，写入不会相互阻塞。
     */
    private void insertWorker(int threadId, List<int[]> cfIndices, List<List<LightCurvePoint>> dataList) {
        StringBuilder keyBuilder = new StringBuilder(48);
        StringBuilder valueBuilder = new StringBuilder(160);

        for (int g = 0; g < cfIndices.size(); g++) {
            int cfIndex = cfIndices.get(g)[0];
            List<LightCurvePoint> rows = dataList.get(g);
            ColumnFamilyHandle cfHandle = cfHandles[cfIndex];

            if (rows.isEmpty()) {
                continue;
            }

            // 按 BATCH_SIZE 分批（对应 C++ for (batch_start += BATCH_SIZE)）
            for (int batchStart = 0; batchStart < rows.size(); batchStart += BATCH_SIZE) {
                int batchEnd = Math.min(batchStart + BATCH_SIZE, rows.size());
                int batchCount = batchEnd - batchStart;

                try (WriteBatch batch = new WriteBatch()) {
                    // 填充 WriteBatch（对应 C++ 填充 ts_buf/mag_buf 等缓冲区）
                    for (int i = batchStart; i < batchEnd; i++) {
                        LightCurvePoint p = rows.get(i);
                        byte[] key = serializeKey(p, keyBuilder);
                        byte[] value = serializeValue(p, valueBuilder);
                        batch.put(cfHandle, key, value);
                    }

                    // 执行写入（对应 C++ taos_stmt_execute）
                    db.write(writeOptions, batch);

                    insertedRows.addAndGet(batchCount);
                    logicalBytesWritten.addAndGet(batchCount * LOGICAL_BYTES_PER_POINT);

                } catch (RocksDBException e) {
                    failedRows.addAndGet(batchCount);
                    System.err.printf("[ERROR] Thread %d CF vg_%d 批量写入失败: %s%n",
                            threadId, cfIndex, e.getMessage());
                }
            }
        }
    }

    // ==================== 序列化方法 ====================

    private static byte[] serializeKey(LightCurvePoint p, StringBuilder sb) {
        sb.setLength(0);
        sb.append(p.sourceId).append('|').append(p.band).append('|').append(p.time);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] serializeValue(LightCurvePoint p, StringBuilder sb) {
        sb.setLength(0);
        sb.append(p.sourceId).append(',')
                .append(p.ra).append(',')
                .append(p.dec).append(',')
                .append(p.transitId).append(',')
                .append(p.band).append(',')
                .append(p.time).append(',')
                .append(p.mag).append(',')
                .append(p.flux).append(',')
                .append(p.fluxError).append(',')
                .append(p.fluxOverError).append(',')
                .append(p.rejectedByPhotometry ? 1 : 0).append(',')
                .append(p.rejectedByVariability ? 1 : 0).append(',')
                .append(p.otherFlags).append(',')
                .append(p.solutionId);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    // ==================== 辅助方法 ====================

    private static void ensureDirectoryExists(String path) {
        File dir = new File(path);
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new RuntimeException("RocksDB 路径不是目录: " + path);
            }
            return;
        }
        if (!dir.mkdirs()) {
            throw new RuntimeException("无法创建 RocksDB 目录: " + path);
        }
    }

    private static String normalizePath(String path) {
        if (path == null || path.trim().isEmpty()) {
            return DEFAULT_DB_PATH;
        }
        return path.trim();
    }

    // ==================== 管理操作 ====================

    public void forceFlush() {
        try {
            // 逐个 flush 所有 CF（对应 TDengine FLUSH DATABASE）
            for (int i = 0; i < CF_COUNT; i++) {
                if (cfHandles[i] != null) {
                    db.flush(flushOptions, cfHandles[i]);
                }
            }
        } catch (RocksDBException e) {
            System.err.println("RocksDB 刷盘失败: " + e.getMessage());
        }
    }

    public double getWriteAmplification() {
        long logical = logicalBytesWritten.get();
        if (logical == 0) {
            return 1.0;
        }
        long currentDirSize = StorageUtils.getDirectorySize(dbPath);
        long physical = currentDirSize - initialDirSize;
        return physical > 0 ? (double) physical / logical : 1.0;
    }

    public String getFailureSummary() {
        return String.format("insertedRows=%d, failedRows=%d",
                insertedRows.get(), failedRows.get());
    }

    @Override
    public void close() {
        // 先关闭 CF handles
        if (cfHandles != null) {
            for (ColumnFamilyHandle h : cfHandles) {
                if (h != null) {
                    h.close();
                }
            }
        }
        // 关闭 DB
        if (db != null) {
            db.close();
            db = null;
        }
        // 关闭选项
        if (flushOptions != null) {
            flushOptions.close();
        }
        if (writeOptions != null) {
            writeOptions.close();
        }
        if (cfOptionsList != null) {
            for (ColumnFamilyOptions opt : cfOptionsList) {
                if (opt != null) {
                    opt.close();
                }
            }
        }
        if (dbOptions != null) {
            dbOptions.close();
        }
        if (statistics != null) {
            statistics.close();
        }
    }
}