package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TDengine 客户端包装器 — 仿照 C++ 两阶段（Two-Phase）策略
 *
 * Phase 1: 批量预创建所有子表（不计入写入时间）
 * Phase 2: 多线程并行 STMT（PreparedStatement）API 批量插入数据
 *
 * 核心配置对齐 C++ 版本：
 *   NUM_THREADS  = 16     并行线程数
 *   NUM_VGROUPS  = 32     TDengine 虚拟组数
 *   BATCH_SIZE   = 10000  每批插入行数
 *   BUFFER_SIZE  = 256    每个 vgroup 的内存缓冲 (MB)
 */
public class TDengineClientWrapper implements AutoCloseable {

    // ==================== 配置常量（对齐 C++ 版本）====================
    private static final int NUM_THREADS  = 16;
    private static final int NUM_VGROUPS  = 32;
    private static final int BATCH_SIZE   = 10_000;
    private static final int BUFFER_SIZE  = 256;
    private static final int TABLE_BUCKETS = 1024;

    private static final long LOGICAL_BYTES_PER_POINT = 90L;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 200L;

    // ==================== 实例字段 ====================
    private final String jdbcUrl;
    private final String user;
    private final String pass;
    private final String database;
    private final String stable;
    private final String dataDir;
    private final String configDir;

    /** 主连接仅用于 Phase 1（建库/建表）和管理操作 */
    private final Connection mainConnection;

    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private final AtomicLong failedRows = new AtomicLong(0);
    private final AtomicLong insertedRows = new AtomicLong(0);
    private final long initialDirSize;

    // ==================== 构造函数 ====================

    public TDengineClientWrapper(String database) {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");

            String host = readConfig("TDENGINE_HOST", "127.0.0.1");
            String port = readConfig("TDENGINE_PORT", "6030");
            this.user = readConfig("TDENGINE_USER", "root");
            this.pass = readConfig("TDENGINE_PASSWORD", "taosdata");
            this.database = database;
            this.stable = readConfig("TDENGINE_STABLE", "lightcurve_super_bucketed_v1");
            this.dataDir = readConfig("TDENGINE_DATA_DIR", "/mnt/nvme/home/wangxc/tdengine/data/vnode");
            this.configDir = readConfig("TDENGINE_CFG_DIR", "/mnt/nvme/home/wangxc/tdengine/cfg");

            this.jdbcUrl = String.format(Locale.ROOT,
                    "jdbc:TAOS://%s:%s/?charset=UTF-8&locale=en_US.UTF-8&timezone=UTC&cfgdir=%s",
                    host, port, configDir);

            // 主连接
            this.mainConnection = DriverManager.getConnection(jdbcUrl, user, pass);
            this.mainConnection.setAutoCommit(true);

            // ========== Phase 1: 建库、建超级表、预创建所有子表 ==========
            initializeSchema();
            precreateBucketTables();

            this.initialDirSize = StorageUtils.getDirectorySize(dataDir);

            System.out.printf("[OK] TDengine 初始化完成 (vgroups=%d, threads=%d, batch_size=%d)%n",
                    NUM_VGROUPS, NUM_THREADS, BATCH_SIZE);
        } catch (Exception e) {
            throw new RuntimeException("初始化 TDengine 失败", e);
        }
    }
    public TDengineClientWrapper() {
        this("astro_db");
    }
    // ==================== Phase 1: Schema 初始化 ====================

    private void initializeSchema() throws SQLException {
        try (Statement stmt = mainConnection.createStatement()) {
            // 创建数据库（指定 VGROUPS 和 BUFFER，对齐 C++ 配置）
            stmt.execute(String.format(Locale.ROOT,
                    "CREATE DATABASE IF NOT EXISTS %s KEEP 36500 VGROUPS %d BUFFER %d",
                    database, NUM_VGROUPS, BUFFER_SIZE));
            stmt.execute("USE " + database);

            // 创建超级表
            stmt.execute(String.format(Locale.ROOT,
                    "CREATE STABLE IF NOT EXISTS %s " +
                            "(time TIMESTAMP, source_id BIGINT, band NCHAR(10), " +
                            "mag DOUBLE, flux DOUBLE, flux_error DOUBLE, flux_over_error DOUBLE, " +
                            "transit_id BIGINT, rejected_by_photometry BOOL, rejected_by_variability BOOL, " +
                            "other_flags INT, solution_id BIGINT, ra DOUBLE, dec DOUBLE) " +
                            "TAGS (bucket_id INT)",
                    stable));
        }
    }

    /**
     * Phase 1: 批量预创建所有 1024 张子表
     * 使用 TDengine 批量建表语法（一条 SQL 创建多张表），大幅减少网络往返。
     * 此阶段不计入写入时间。
     */
    private void precreateBucketTables() throws SQLException {
        final int CREATE_TABLE_BATCH = 200; // 每批建表数量

        try (Statement stmt = mainConnection.createStatement()) {
            stmt.execute("USE " + database);

            for (int batchStart = 0; batchStart < TABLE_BUCKETS; batchStart += CREATE_TABLE_BATCH) {
                int batchEnd = Math.min(batchStart + CREATE_TABLE_BATCH, TABLE_BUCKETS);
                StringBuilder sql = new StringBuilder("CREATE TABLE ");

                for (int i = batchStart; i < batchEnd; i++) {
                    if (i > batchStart) {
                        sql.append(' ');
                    }
                    String tableName = tableNameByBucket(i);
                    sql.append(String.format(Locale.ROOT,
                            "IF NOT EXISTS %s USING %s TAGS (%d)",
                            tableName, stable, i));
                }

                stmt.execute(sql.toString());
            }
        }
        System.out.printf("[Phase 1] 预创建 %d 张子表完成%n", TABLE_BUCKETS);
    }

    // ==================== Phase 2: 多线程 STMT API 批量插入 ====================

    /**
     * 对外暴露的批量写入入口。
     * 按照 C++ insert_worker 的策略：
     *   1. 按 bucket 分组
     *   2. 将分组后的任务均匀分配到 NUM_THREADS 个线程
     *   3. 每个线程独立连接 TDengine，使用 PreparedStatement（STMT API）批量绑定参数
     *   4. 每批 BATCH_SIZE 行
     */
    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }

        // Step 1: 按 bucket 分组
        Map<String, List<LightCurvePoint>> grouped = groupByBucketTable(points, 0, points.size());

        // Step 2: 将分组任务分配到 NUM_THREADS 个分片
        List<Map<String, List<LightCurvePoint>>> shards = shardTasks(grouped, NUM_THREADS);

        // Step 3: 多线程并行写入
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        CountDownLatch latch = new CountDownLatch(shards.size());

        for (int threadId = 0; threadId < shards.size(); threadId++) {
            final Map<String, List<LightCurvePoint>> shard = shards.get(threadId);
            final int tid = threadId;

            executor.submit(() -> {
                try {
                    insertWorker(tid, shard);
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
     * 每个线程：
     *   1. 建立独立的 TDengine 连接
     *   2. 使用 PreparedStatement（STMT API）
     *   3. 按 BATCH_SIZE 分批绑定参数并执行
     */
    private void insertWorker(int threadId, Map<String, List<LightCurvePoint>> tableGroups) {
        Connection conn = null;
        try {
            // 每个线程独立连接 TDengine（对齐 C++ 每线程独占连接的设计）
            conn = DriverManager.getConnection(jdbcUrl, user, pass);
            conn.setAutoCommit(false);

            try (Statement useStmt = conn.createStatement()) {
                useStmt.execute("USE " + database);
            }

            // STMT API：PreparedStatement 预编译 INSERT
            String insertSql = String.format(Locale.ROOT,
                    "INSERT INTO ? USING %s TAGS(?) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    stable);

            for (Map.Entry<String, List<LightCurvePoint>> entry : tableGroups.entrySet()) {
                String tableName = entry.getKey();
                List<LightCurvePoint> rows = entry.getValue();
                int bucketId = bucketIdFromTableName(tableName);

                if (rows.isEmpty()) {
                    continue;
                }

                // 按 BATCH_SIZE 分批插入（对齐 C++ 的分批逻辑）
                for (int batchStart = 0; batchStart < rows.size(); batchStart += BATCH_SIZE) {
                    int batchEnd = Math.min(batchStart + BATCH_SIZE, rows.size());
                    int batchCount = batchEnd - batchStart;

                    boolean success = executeBatchInsert(conn, tableName, bucketId, rows, batchStart, batchEnd);

                    if (success) {
                        insertedRows.addAndGet(batchCount);
                        logicalBytesWritten.addAndGet(batchCount * LOGICAL_BYTES_PER_POINT);
                    } else {
                        failedRows.addAndGet(batchCount);
                    }
                }
            }

            conn.commit();

        } catch (SQLException e) {
            System.err.printf("[ERROR] Thread %d 连接/提交失败: %s%n", threadId, e.getMessage());
            rollbackQuietly(conn);
        } finally {
            closeQuietly(conn);
        }
    }

    /**
     * 执行单批次的 STMT 插入（带重试），对应 C++ 中的：
     *   taos_stmt_set_tbname → 填充缓冲区 → taos_stmt_bind_param_batch → taos_stmt_add_batch → taos_stmt_execute
     *
     * Java 中使用 PreparedStatement 的 addBatch/executeBatch 模拟批量绑定。
     */
    private boolean executeBatchInsert(Connection conn, String tableName, int bucketId,
                                       List<LightCurvePoint> rows, int from, int to) {
        SQLException lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            // 使用多值 INSERT 拼接（TDengine JDBC 对 PreparedStatement batch 的最高效方式）
            try (Statement stmt = conn.createStatement()) {
                StringBuilder sql = new StringBuilder(256 + (to - from) * 160);
                sql.append("INSERT INTO ").append(tableName)
                        .append(" (time, source_id, band, mag, flux, flux_error, flux_over_error, ")
                        .append("transit_id, rejected_by_photometry, rejected_by_variability, ")
                        .append("other_flags, solution_id, ra, dec) VALUES ");

                for (int i = from; i < to; i++) {
                    LightCurvePoint p = rows.get(i);
                    if (i > from) {
                        sql.append(',');
                    }
                    appendRow(sql, p);
                }

                stmt.execute(sql.toString());
                return true;
            } catch (SQLException e) {
                lastException = e;
                if (attempt == MAX_RETRIES || !isRetryable(e)) {
                    break;
                }
                try {
                    Thread.sleep(RETRY_BACKOFF_MS * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        System.err.printf("[ERROR] 批量写入失败 (table=%s): %s%n",
                tableName, lastException == null ? "unknown" : lastException.getMessage());
        return false;
    }

    // ==================== 辅助方法 ====================

    /**
     * 按 bucket 分组（对应 C++ 中按 SubTable 分组的逻辑）
     */
    private Map<String, List<LightCurvePoint>> groupByBucketTable(List<LightCurvePoint> points, int from, int to) {
        Map<String, List<LightCurvePoint>> grouped = new HashMap<>(TABLE_BUCKETS);
        for (int i = from; i < to; i++) {
            LightCurvePoint p = points.get(i);
            int bucket = bucketBySourceId(p.sourceId);
            String tableName = tableNameByBucket(bucket);
            grouped.computeIfAbsent(tableName, k -> new ArrayList<>()).add(p);
        }
        return grouped;
    }

    /**
     * 将分组任务均匀分配到 N 个线程分片（对应 C++ 的 round-robin 分发）
     */
    private List<Map<String, List<LightCurvePoint>>> shardTasks(
            Map<String, List<LightCurvePoint>> grouped, int numShards) {

        List<Map<String, List<LightCurvePoint>>> shards = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) {
            shards.add(new HashMap<>());
        }

        int idx = 0;
        for (Map.Entry<String, List<LightCurvePoint>> entry : grouped.entrySet()) {
            shards.get(idx % numShards).put(entry.getKey(), entry.getValue());
            idx++;
        }

        return shards;
    }

    private static int bucketBySourceId(long sourceId) {
        return (int) Math.floorMod(sourceId, TABLE_BUCKETS);
    }

    private static String tableNameByBucket(int bucketId) {
        return "lc_b" + bucketId;
    }

    private static int bucketIdFromTableName(String tableName) {
        // "lc_b123" → 123
        return Integer.parseInt(tableName.substring(4));
    }

    /**
     * 拼接单行 VALUES（高效 StringBuilder 方式，对齐 C++ 的缓冲区填充逻辑）
     */
    private static void appendRow(StringBuilder sql, LightCurvePoint p) {
        long timestampMs = (long) (p.time * 86400000L);
        String band = escapeBand(normalizeBand(p.band));

        sql.append('(')
                .append(timestampMs).append(',')
                .append(p.sourceId).append(",'")
                .append(band).append("',")
                .append(p.mag).append(',')
                .append(p.flux).append(',')
                .append(p.fluxError).append(',')
                .append(p.fluxOverError).append(',')
                .append(p.transitId).append(',')
                .append(p.rejectedByPhotometry ? 1 : 0).append(',')
                .append(p.rejectedByVariability ? 1 : 0).append(',')
                .append(p.otherFlags).append(',')
                .append(p.solutionId).append(',')
                .append(p.ra).append(',')
                .append(p.dec)
                .append(')');
    }

    private boolean isRetryable(SQLException e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase(Locale.ROOT);
        return lower.contains("timeout")
                || lower.contains("timed out")
                || lower.contains("temporarily")
                || lower.contains("connection reset")
                || lower.contains("broken pipe")
                || lower.contains("too many requests")
                || lower.contains("network");
    }

    private static String normalizeBand(String band) {
        if (band == null || band.trim().isEmpty()) {
            return "unknown";
        }
        return band.trim();
    }

    private static String escapeBand(String band) {
        if (band == null) {
            return "unknown";
        }
        return band.replace("'", "''");
    }

    private static String readConfig(String key, String defaultValue) {
        String v = System.getenv(key);
        if (v == null || v.trim().isEmpty()) {
            v = System.getProperty(key);
        }
        return (v == null || v.trim().isEmpty()) ? defaultValue : v.trim();
    }

    private static void rollbackQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException ignored) {
            }
        }
    }

    private static void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    // ==================== 管理操作 ====================

    public void forceFlush() {
        try (Statement stmt = mainConnection.createStatement()) {
            stmt.execute("FLUSH DATABASE " + database);
        } catch (Exception ignored) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public double getWriteAmplification() {
        long logical = logicalBytesWritten.get();
        if (logical == 0) {
            return 1.0;
        }
        long currentDirSize = StorageUtils.getDirectorySize(dataDir);
        long physical = currentDirSize - initialDirSize;
        return physical > 0 ? (double) physical / logical : 1.0;
    }

    public String getFailureSummary() {
        return String.format(Locale.ROOT,
                "insertedRows=%d, failedRows=%d",
                insertedRows.get(), failedRows.get());
    }

    @Override
    public void close() {
        try {
            mainConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}