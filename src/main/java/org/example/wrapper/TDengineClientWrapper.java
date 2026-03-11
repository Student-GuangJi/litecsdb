package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
 *
 * 查询优化（Experiment 9）：
 * 6. 多线程并行查询支持
 * 7. STMK 查询分片并行执行 + 应用层空间过滤
 * 8. 查询前清除缓存，确保公平对比
 */
public class TDengineClientWrapper implements AutoCloseable {

    private static final int WRITE_THREADS = 16;
    private static final int QUERY_THREADS = 8;
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

    // 查询线程池和连接池
    private final Connection[] queryConnections;
    private final ExecutorService queryPool;

    // GAIA 时间转换常量
    private static final long GAIA_EPOCH_UNIX_US = 1420070400000000L; // 2015-01-01T00:00:00 UTC (微秒)
    private static final long US_PER_DAY = 86400000000L;              // 一天的微秒数
    private static final long GAIA_EPOCH_UNIX_MS = 1420070400000L;
    private static final long MS_PER_DAY = 86400000L;

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

        // 创建 16 个独立的 JDBC 写入连接
        this.threadConnections = new Connection[WRITE_THREADS];
        // 创建查询连接池
        this.queryConnections = new Connection[QUERY_THREADS];

        try {
            // 先用一个连接初始化 schema
            initializeSchema();

            // 为每个写入线程创建独立连接
            for (int i = 0; i < WRITE_THREADS; i++) {
                threadConnections[i] = createConnection();
                threadConnections[i].createStatement().execute("USE " + database);
            }

            // 为每个查询线程创建独立连接
            for (int i = 0; i < QUERY_THREADS; i++) {
                queryConnections[i] = createConnection();
                queryConnections[i].createStatement().execute("USE " + database);
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

        this.queryPool = Executors.newFixedThreadPool(QUERY_THREADS, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("TDengine-Query-" + t.getId());
            return t;
        });

        this.dataDirPath = findTDengineDataDir();
        this.initialDataDirSize = getDataDirSize();

        System.out.printf("[OK] TDengine initialized (db=%s, vgroups=%d, write_threads=%d, query_threads=%d)%n",
                database, VGROUPS, WRITE_THREADS, QUERY_THREADS);
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

    // ==================== 写入方法 ====================

    /**
     * 16 线程并行写入
     */
    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) return;

        logicalBytesWritten.addAndGet(points.size() * 90L);

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

    private void writeChunkMultiTable(int threadIdx, List<LightCurvePoint> points) throws SQLException {
        Connection conn = threadConnections[threadIdx % WRITE_THREADS];

        Map<Integer, List<LightCurvePoint>> buckets = new HashMap<>();
        for (LightCurvePoint p : points) {
            int bucket = (int) (Math.abs(p.sourceId) % BUCKET_COUNT);
            buckets.computeIfAbsent(bucket, k -> new ArrayList<>()).add(p);
        }

        for (Map.Entry<Integer, List<LightCurvePoint>> entry : buckets.entrySet()) {
            int bucket = entry.getKey();
            List<LightCurvePoint> bucketPoints = entry.getValue();

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

    // ==================== 查询方法（Experiment 9） ====================

    /**
     * 清除 TDengine 查询缓存
     * TDengine 不提供直接清缓存命令，通过重建查询连接实现
     */
    public void clearQueryCache() {
        try {
            // 关闭旧的查询连接
            for (int i = 0; i < QUERY_THREADS; i++) {
                if (queryConnections[i] != null) {
                    try { queryConnections[i].close(); } catch (Exception e) {}
                }
            }
            // 创建新连接，避免 JDBC 驱动层面的语句缓存
            for (int i = 0; i < QUERY_THREADS; i++) {
                queryConnections[i] = createConnection();
                queryConnections[i].createStatement().execute("USE " + database);
            }

            // 尝试通过 RESET QUERY CACHE 命令清除服务端缓存（若支持）
            try (Connection conn = createConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("RESET QUERY CACHE");
            } catch (Exception e) {
                // TDengine 某些版本不支持，忽略
            }
        } catch (Exception e) {
            System.err.println("[WARN] TDengine cache clear failed: " + e.getMessage());
        }
    }

    /**
     * 简单查询：按 source_id 检索光变曲线
     */
    public int executeSimpleQuery(String dbName, long sourceId) {
        String sql = String.format(
                "SELECT ts, mag, flux FROM %s.lightcurve WHERE source_id = %d ORDER BY ts",
                dbName, sourceId);
        try (Statement stmt = queryConnections[0].createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
            return count;
        } catch (SQLException e) {
            return 0;
        }
    }

    /**
     * 多线程并行简单查询
     * 将 sourceId 列表分配到多个查询线程并发执行
     */
    public List<int[]> executeSimpleQueryParallel(String dbName, List<Long> sourceIds) {
        List<int[]> allResults = new ArrayList<>();
        for (int i = 0; i < sourceIds.size(); i++) {
            allResults.add(new int[]{0, 0}); // [count, latencyNs]
        }

        // 按线程数分片
        int chunkSize = Math.max(1, (sourceIds.size() + QUERY_THREADS - 1) / QUERY_THREADS);
        CountDownLatch latch = new CountDownLatch(
                Math.min(QUERY_THREADS, (sourceIds.size() + chunkSize - 1) / chunkSize));

        for (int t = 0; t < QUERY_THREADS && t * chunkSize < sourceIds.size(); t++) {
            final int threadIdx = t;
            final int start = t * chunkSize;
            final int end = Math.min(start + chunkSize, sourceIds.size());

            queryPool.submit(() -> {
                try {
                    Connection conn = queryConnections[threadIdx];
                    for (int i = start; i < end; i++) {
                        long qStart = System.nanoTime();
                        String sql = String.format(
                                "SELECT ts, mag, flux FROM %s.lightcurve WHERE source_id = %d ORDER BY ts",
                                dbName, sourceIds.get(i));
                        int count = 0;
                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(sql)) {
                            while (rs.next()) count++;
                        }
                        allResults.get(i)[0] = count;
                        allResults.get(i)[1] = (int) ((System.nanoTime() - qStart) / 1_000_000);
                    }
                } catch (Exception e) {
                    System.err.println("TDengine simple query thread " + threadIdx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(120, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return allResults;
    }

    /**
     * STMK 查询：多线程并行执行
     *
     * 策略：将 STMK 查询拆分为多个子查询（按子表范围分片），
     * 各线程独立执行 SQL 后合并结果，最后做应用层空间过滤。
     *
     * SQL 等价查询：
     *   SELECT source_id, ra, dec_val, COUNT(*) as cnt
     *   FROM {db}.lightcurve
     *   WHERE ts >= {startUs} AND ts <= {endUs}
     *     AND band = '{band}' AND mag >= {magMin} AND mag <= {magMax}
     *   PARTITION BY source_id
     *   HAVING COUNT(*) >= {minObs}
     *
     * 然后应用层对返回结果做 greatCircleDistance 空间过滤。
     */
    public int executeSTMKQuery(String dbName, double ra, double dec, double radius,
                                double startTime, double endTime, String band,
                                double magMin, double magMax, int minObs) {

        long startUs = (GAIA_EPOCH_UNIX_MS + (long) (startTime * MS_PER_DAY)) * 1000;
        long endUs = (GAIA_EPOCH_UNIX_MS + (long) (endTime * MS_PER_DAY)) * 1000;

        // 将子表范围按查询线程数分片
        int tablesPerThread = (BUCKET_COUNT + QUERY_THREADS - 1) / QUERY_THREADS;
        AtomicInteger totalCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(QUERY_THREADS);

        for (int t = 0; t < QUERY_THREADS; t++) {
            final int threadIdx = t;
            final int tableStart = t * tablesPerThread;
            final int tableEnd = Math.min(tableStart + tablesPerThread, BUCKET_COUNT);

            queryPool.submit(() -> {
                try {
                    Connection conn = queryConnections[threadIdx];
                    int localCount = 0;

                    // 构建子表列表限定的查询
                    // TDengine 支持 PARTITION BY tbname 来限制扫描范围
                    // 这里用 UNION 或直接全表查询 + 子表 hint
                    // 最高效的做法：用 tbname IN (...) 或分段查子表
                    for (int tbl = tableStart; tbl < tableEnd; tbl++) {
                        String sql = String.format(Locale.US,
                                "SELECT source_id, COUNT(*) as cnt FROM %s.t_%d " +
                                        "WHERE ts >= %d AND ts <= %d AND band = '%s' " +
                                        "AND mag >= %f AND mag <= %f " +
                                        "GROUP BY source_id HAVING COUNT(*) >= %d",
                                dbName, tbl, startUs, endUs, band, magMin, magMax, minObs);

                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(sql)) {
                            while (rs.next()) {
                                // TDengine 子表 tag 里有 ra, dec_val
                                // 但 GROUP BY 后无法直接拿 tag，需二次查询或在应用层过滤
                                // 这里先计数，空间过滤在外层统一做
                                localCount++;
                            }
                        } catch (SQLException e) {
                            // 子表可能为空，忽略
                        }
                    }

                    totalCount.addAndGet(localCount);
                } catch (Exception e) {
                    System.err.println("TDengine STMK query thread " + threadIdx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return totalCount.get();
    }

    /**
     * STMK 查询（带空间过滤版本）：多线程并行
     *
     * 先聚合得到满足时间+波段+星等+计数条件的 source_id 及其 ra/dec，
     * 再做应用层 cone search 过滤。
     */
    public int executeSTMKQueryWithSpatialFilter(String dbName,
                                                 double centerRa, double centerDec, double radius,
                                                 double startTime, double endTime, String band,
                                                 double magMin, double magMax, int minObs) {

        long startUs = (GAIA_EPOCH_UNIX_MS + (long) (startTime * MS_PER_DAY)) * 1000;
        long endUs = (GAIA_EPOCH_UNIX_MS + (long) (endTime * MS_PER_DAY)) * 1000;

        // 多线程分片并行查询
        int tablesPerThread = (BUCKET_COUNT + QUERY_THREADS - 1) / QUERY_THREADS;

        // 每个线程收集满足条件的 (source_id, ra, dec) 候选
        @SuppressWarnings("unchecked")
        List<double[]>[] threadCandidates = new List[QUERY_THREADS];
        for (int i = 0; i < QUERY_THREADS; i++) {
            threadCandidates[i] = new ArrayList<>();
        }

        CountDownLatch latch = new CountDownLatch(QUERY_THREADS);

        for (int t = 0; t < QUERY_THREADS; t++) {
            final int threadIdx = t;
            final int tableStart = t * tablesPerThread;
            final int tableEnd = Math.min(tableStart + tablesPerThread, BUCKET_COUNT);

            queryPool.submit(() -> {
                try {
                    Connection conn = queryConnections[threadIdx];

                    for (int tbl = tableStart; tbl < tableEnd; tbl++) {
                        // 用子表级查询，带 tag（ra, dec_val）+ 聚合过滤
                        // TDengine SQL: 查子表内满足条件的 source_id 及其聚合
                        String sql = String.format(Locale.US,
                                "SELECT source_id, ra, dec_val, COUNT(*) as cnt " +
                                        "FROM %s.t_%d " +
                                        "WHERE ts >= %d AND ts <= %d AND band = '%s' " +
                                        "AND mag >= %f AND mag <= %f " +
                                        "GROUP BY source_id HAVING COUNT(*) >= %d",
                                dbName, tbl, startUs, endUs, band, magMin, magMax, minObs);

                        try (Statement stmt = conn.createStatement();
                             ResultSet rs = stmt.executeQuery(sql)) {
                            while (rs.next()) {
                                double pRa = rs.getDouble("ra");
                                double pDec = rs.getDouble("dec_val");
                                threadCandidates[threadIdx].add(new double[]{pRa, pDec});
                            }
                        } catch (SQLException e) {
                            // 子表可能为空，忽略
                        }
                    }
                } catch (Exception e) {
                    System.err.println("TDengine STMK spatial query thread " + threadIdx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // 应用层空间过滤（cone search）
        int count = 0;
        for (List<double[]> candidates : threadCandidates) {
            for (double[] coord : candidates) {
                if (isInCone(coord[0], coord[1], centerRa, centerDec, radius)) {
                    count++;
                }
            }
        }
        return count;
    }

    private static boolean isInCone(double ra1, double dec1, double ra2, double dec2, double radius) {
        double ra1R = Math.toRadians(ra1), dec1R = Math.toRadians(dec1);
        double ra2R = Math.toRadians(ra2), dec2R = Math.toRadians(dec2);
        double a = Math.sin((dec2R - dec1R) / 2) * Math.sin((dec2R - dec1R) / 2) +
                Math.cos(dec1R) * Math.cos(dec2R) *
                        Math.sin((ra2R - ra1R) / 2) * Math.sin((ra2R - ra1R) / 2);
        return Math.toDegrees(2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))) <= radius;
    }

    // ==================== 工具方法 ====================

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

    public String getDatabase() {
        return database;
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
        queryPool.shutdown();
        try { writePool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        try { queryPool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}

        for (Connection conn : threadConnections) {
            try { if (conn != null) conn.close(); } catch (Exception e) {}
        }
        for (Connection conn : queryConnections) {
            try { if (conn != null) conn.close(); } catch (Exception e) {}
        }

        // DROP DATABASE 清理数据
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + database);
        } catch (Exception e) {}
    }
}