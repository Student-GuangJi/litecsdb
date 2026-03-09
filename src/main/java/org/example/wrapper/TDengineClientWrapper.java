package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TDengine 客户端包装器
 *
 * 优化点（对齐其他系统公平性）：
 * 1. 16 个写入线程，每线程独立 JDBC 连接（避免锁竞争）
 * 2. 按子表 hash 分桶后批量 INSERT（TDengine 官方推荐写法）
 * 3. 多表写入语法：INSERT INTO t_0 VALUES (...) (...) t_1 VALUES (...) (...)
 *    一条 SQL 写入多张子表（减少 JDBC 往返）
 * 4. 每批 10000 行（对齐 BATCH_SIZE）
 * 5. ts 用原子自增微秒时间戳（避免时间转换问题）
 */
public class TDengineClientWrapper implements AutoCloseable {

    private static final int WRITE_THREADS = 16;
    private static final int BATCH_SIZE = 10_000;
    private static final int VGROUPS = 32;
    private static final int BUCKET_COUNT = 1024;

    private final String database;
    private final String host;
    private final int port;
    private final String stableName = "lightcurve";

    // 每个写入线程独立的 JDBC 连接
    private final Connection[] threadConnections;
    private final ExecutorService writePool;

    // 全局自增时间戳（微秒），保证每行唯一
    private final AtomicLong tsCounter = new AtomicLong(
            System.currentTimeMillis() * 1000L);

    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private long initialDataDirSize;
    private String dataDirPath;

    private final AtomicLong insertedRows = new AtomicLong(0);
    private final AtomicLong failedRows = new AtomicLong(0);

    public TDengineClientWrapper() {
        this("astro_db");
    }

    public TDengineClientWrapper(String database) {
        this.database = database;
        this.host = System.getenv().getOrDefault("TDENGINE_HOST", "127.0.0.1");
        this.port = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_PORT", "6030"));

        // 创建 16 个独立的 JDBC 连接
        this.threadConnections = new Connection[WRITE_THREADS];
        try {
            // 先用一个连接初始化 schema
            initializeSchema();

            // 然后为每个写入线程创建独立连接
            for (int i = 0; i < WRITE_THREADS; i++) {
                threadConnections[i] = createConnection();
                threadConnections[i].createStatement().execute("USE " + database);
            }
        } catch (Exception e) {
            throw new RuntimeException("TDengine init failed", e);
        }

        this.writePool = Executors.newFixedThreadPool(WRITE_THREADS, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("TDengine-Writer-" + t.getId());
            return t;
        });

        this.dataDirPath = findTDengineDataDir();
        this.initialDataDirSize = getDataDirSize();

        System.out.printf("[OK] TDengine initialized (db=%s, vgroups=%d, threads=%d, batch_size=%d)%n",
                database, VGROUPS, WRITE_THREADS, BATCH_SIZE);
    }

    private void initializeSchema() throws SQLException {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            // 先清理同名数据库，确保 vnodes 被释放
            stmt.execute("DROP DATABASE IF EXISTS " + database);
            System.out.printf("[清理] 已删除残留数据库: %s%n", database);

            stmt.execute(String.format(
                    "CREATE DATABASE IF NOT EXISTS %s " +
                            "VGROUPS %d " +
                            "BUFFER 256 " +
                            "WAL_LEVEL 1 " +
                            "WAL_FSYNC_PERIOD 3000 " +
                            "PAGES 256 " +
                            "PRECISION 'us'",
                    database, VGROUPS));

            stmt.execute("USE " + database);

            stmt.execute(
                    "CREATE STABLE IF NOT EXISTS " + stableName + " (" +
                            "  ts TIMESTAMP, " +
                            "  transit_id BIGINT, " +
                            "  band NCHAR(4), " +
                            "  obs_time DOUBLE, " +
                            "  mag DOUBLE, " +
                            "  flux DOUBLE, " +
                            "  flux_error DOUBLE, " +
                            "  flux_over_error DOUBLE, " +
                            "  rejected_by_photometry BOOL, " +
                            "  rejected_by_variability BOOL, " +
                            "  other_flags INT, " +
                            "  solution_id BIGINT" +
                            ") TAGS (" +
                            "  source_id BIGINT, " +
                            "  ra DOUBLE, " +
                            "  dec_val DOUBLE" +
                            ")");

            // 逐条建表
            for (int i = 0; i < BUCKET_COUNT; i++) {
                stmt.execute(String.format(
                        "CREATE TABLE IF NOT EXISTS %s.t_%d USING %s.%s TAGS(%d, 0.0, 0.0)",
                        database, i, database, stableName, i));
            }
            System.out.printf("[Phase 1] 预创建 %d 张子表完成%n", BUCKET_COUNT);
        }
    }

    /**
     * 16 线程并行写入
     * 将数据按 hash 分成 WRITE_THREADS 组，每组用独立连接写入
     */
    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) return;

        logicalBytesWritten.addAndGet(points.size() * 90L);

        // 按写入线程分组（不是按子表，减少 latch 数量）
        int chunkSize = Math.max(1, (points.size() + WRITE_THREADS - 1) / WRITE_THREADS);
        List<List<LightCurvePoint>> chunks = new ArrayList<>();
        for (int i = 0; i < points.size(); i += chunkSize) {
            chunks.add(points.subList(i, Math.min(i + chunkSize, points.size())));
        }

        CountDownLatch latch = new CountDownLatch(chunks.size());
        for (int idx = 0; idx < chunks.size(); idx++) {
            final int threadIdx = idx;
            final List<LightCurvePoint> chunk = chunks.get(idx);

            writePool.submit(() -> {
                try {
                    writeChunkMultiTable(threadIdx, chunk);
                    insertedRows.addAndGet(chunk.size());
                } catch (Exception e) {
                    failedRows.addAndGet(chunk.size());
                    System.err.println("TDengine write thread " + threadIdx + " failed: " + e.getMessage());
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
    }

    // 类成员变量区域，删除 tsCounter，加上：
    private static final long GAIA_EPOCH_UNIX_US = 1420070400000000L; // 2015-01-01T00:00:00 UTC (微秒)
    private static final long US_PER_DAY = 86400000000L;              // 一天的微秒数

    private void writeChunkMultiTable(int threadIdx, List<LightCurvePoint> points) throws SQLException {
        Connection conn = threadConnections[threadIdx % WRITE_THREADS];

        // 按子表分组
        Map<Integer, List<LightCurvePoint>> buckets = new HashMap<>();
        for (LightCurvePoint p : points) {
            int bucket = (int) (Math.abs(p.sourceId) % BUCKET_COUNT);
            buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(p);
        }

        // 每个子表单独一条 INSERT SQL
        for (Map.Entry<Integer, List<LightCurvePoint>> entry : buckets.entrySet()) {
            int bucket = entry.getKey();
            List<LightCurvePoint> bucketPoints = entry.getValue();

            // 每 BATCH_SIZE 条一批
            for (int start = 0; start < bucketPoints.size(); start += BATCH_SIZE) {
                int end = Math.min(start + BATCH_SIZE, bucketPoints.size());

                StringBuilder sql = new StringBuilder();
                sql.append("INSERT INTO ").append(database).append(".t_").append(bucket).append(" VALUES ");

                for (int i = start; i < end; i++) {
                    LightCurvePoint p = bucketPoints.get(i);

                    long tsUs = GAIA_EPOCH_UNIX_US + (long) (p.time * US_PER_DAY)
                            + Math.abs(p.transitId % 999_000);

                    if (i > start) sql.append(" ");
                    sql.append(String.format(Locale.US,
                            "(%d,%d,'%s',%.6f,%.6f,%.6f,%.6f,%.6f,%s,%s,%d,%d)",
                            tsUs, p.transitId, p.band, p.time, p.mag,
                            p.flux, p.fluxError, p.fluxOverError,
                            p.rejectedByPhotometry ? "true" : "false",
                            p.rejectedByVariability ? "true" : "false",
                            p.otherFlags, p.solutionId));
                }

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(sql.toString());
                }
            }
        }
    }

    private Connection createConnection() throws SQLException {
        String url = String.format("jdbc:TAOS://%s:%d/?charset=UTF-8", host, port);
        Properties props = new Properties();
        props.setProperty("user", System.getenv().getOrDefault("TDENGINE_USER", "root"));
        props.setProperty("password", System.getenv().getOrDefault("TDENGINE_PASSWORD", "taosdata"));
        return DriverManager.getConnection(url, props);
    }

    public void forceFlush() {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("FLUSH DATABASE " + database);
        } catch (Exception e) {
            // ignore
        }
    }

    public double getWriteAmplification() {
        long logical = logicalBytesWritten.get();
        if (logical <= 0) return 1.0;
        long physical = getDataDirSize() - initialDataDirSize;
        return physical > 0 ? (double) physical / logical : 1.0;
    }

    private long getDataDirSize() {
        try {
            File dataDir = new File(dataDirPath);
            if (!dataDir.exists()) return 0;
            return StorageUtils.getDirectorySize(dataDirPath);
        } catch (Exception e) {
            return 0;
        }
    }

    private String findTDengineDataDir() {
        String taosDir = System.getenv().getOrDefault("TAOS_DIR", System.getProperty("user.home") + "/tdengine");
        return taosDir + "/data/vnode";
    }

    public String getFailureSummary() {
        return String.format("insertedRows=%d, failedRows=%d", insertedRows.get(), failedRows.get());
    }

    public boolean hasWriteFailures() {
        return failedRows.get() > 0;
    }

    @Override
    public void close() {
        writePool.shutdown();
        try { writePool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}

        for (Connection conn : threadConnections) {
            try { if (conn != null) conn.close(); } catch (Exception e) {}
        }

        // DROP DATABASE 清理数据
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + database);
        } catch (Exception e) {}
    }
}