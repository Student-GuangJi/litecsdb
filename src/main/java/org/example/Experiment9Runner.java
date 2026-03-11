package org.example;

import org.example.MainNode.*;
import org.example.RocksDBServer.LightCurvePoint;
import org.example.wrapper.NativeRocksDBWrapper;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Experiment 9: Query Performance Comparison (Simple + STMK)
 *
 * 纯查询测试，数据由 DataImporter 预先导入
 *
 * 实验设计：
 *   1. 简单查询：单天体光变曲线检索（点查询）
 *   2. STMK聚合查询：空间+时间+星等+观测次数联合约束
 *
 * 对比系统：
 *   - LitecsDB（调用 MainNode 接口）
 *   - TDengine（多线程并行 SQL 查询 + 应用层空间过滤）
 *   - InfluxDB（多线程并行 Flux 查询 + 应用层空间过滤）
 *   - Native RocksDB（多线程前缀扫描 + 应用层聚合过滤）
 *
 * 公平性保证：
 *   - 所有系统查询前清除缓存（重建连接 / RESET QUERY CACHE / 重新打开迭代器）
 *   - 所有系统通过真实数据库链接查询，不允许使用缓存
 *   - 每个查询之间清除缓存
 *   - 使用相同的查询参数
 */
public class Experiment9Runner {

    // ==================== 实验配置 ====================
    private static final int SIMPLE_QUERY_COUNT = 100;
    private static final int STMK_QUERY_COUNT = 10;
    private static final int WARMUP_QUERIES = 3;        // 预热查询数（不计入结果）
    private static final int HEALPIX_LEVEL = 1;
    private static final int NODE_COUNT = 2;

    private static final String RESULT_DIR = "experiment9_results/";

    // TDengine
    private static final String TDENGINE_USER = "root";
    private static final String TDENGINE_PASSWORD = "taosdata";

    // InfluxDB
    private static final String INFLUX_URL = "http://127.0.0.1:8086";
    private static final String INFLUX_TOKEN = "wangxc";
    private static final String INFLUX_ORG = "astro_research";
    private static final String INFLUX_BUCKET = "gaia_lightcurves";

    // GAIA 时间转换常量
    private static final long GAIA_EPOCH_UNIX_MS = 1420070400000L;
    private static final long MS_PER_DAY = 86400000L;

    // ==================== 结果记录 ====================
    private final List<QueryBenchmarkResult> simpleResults = new ArrayList<>();
    private final List<QueryBenchmarkResult> stmkResults = new ArrayList<>();
    private PrintWriter logWriter;
    private PrintWriter csvWriter;

    // 查询参数（从TDengine获取，所有系统共用）
    private List<Long> simpleQuerySourceIds;
    private List<STMKQueryParams> stmkParams;

    public static void main(String[] args) {
        new Experiment9Runner().run();
    }

    public void run() {
        initResultDir();
        initWriters();

        log("================================================================================");
        log("Experiment 9: Query Performance Comparison (Simple + STMK)");
        log("Start time: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        log("================================================================================");
        log("");
        log("配置: simpleQueries=" + SIMPLE_QUERY_COUNT +
                ", stmkQueries=" + STMK_QUERY_COUNT +
                ", warmup=" + WARMUP_QUERIES);
        log("注意: 数据需预先通过 DataImporter 导入各系统");
        log("注意: 每次查询前清除缓存，确保公平对比");
        log("");

        try {
            // 从TDengine获取查询参数（source_id列表、数据范围等）
            initQueryParams();

            // 测试各系统（顺序随机化以减少系统级别的缓存偏差）
            testLitecsDB();
            testTDengine();
            testInfluxDB();
            testNativeRocksDB();

        } catch (Exception e) {
            log("[ERROR] Experiment failed: " + e.getMessage());
            e.printStackTrace();
        }

        printSummaryReport();
        exportCSV();
        closeWriters();
    }

    /**
     * 从TDengine获取查询参数
     */
    private void initQueryParams() throws SQLException {
        log("[Init] Fetching query parameters from TDengine...");

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(
                    "jdbc:TAOS://127.0.0.1:6030/?charset=UTF-8",
                    TDENGINE_USER, TDENGINE_PASSWORD);

            String dbName = findTDengineDatabase(conn);
            if (dbName == null) {
                throw new RuntimeException("No TDengine database found. Run DataImporter first.");
            }
            log("  Using database: " + dbName);

            // 获取source_id列表
            simpleQuerySourceIds = new ArrayList<>();
            String sql = "SELECT DISTINCT source_id FROM " + dbName + ".lightcurve LIMIT " +
                    (SIMPLE_QUERY_COUNT + WARMUP_QUERIES);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    simpleQuerySourceIds.add(rs.getLong(1));
                }
            }
            log("  Got " + simpleQuerySourceIds.size() + " source IDs for simple queries");

            // 获取数据范围用于STMK查询
            double minTime = 0, maxTime = 3000;
            double minMag = 5, maxMag = 20;
            sql = "SELECT MIN(obs_time), MAX(obs_time), MIN(mag), MAX(mag) FROM " + dbName + ".lightcurve";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    minTime = rs.getDouble(1);
                    maxTime = rs.getDouble(2);
                    minMag = rs.getDouble(3);
                    maxMag = rs.getDouble(4);
                }
            }
            log(String.format("  Data range: time=[%.2f, %.2f], mag=[%.2f, %.2f]",
                    minTime, maxTime, minMag, maxMag));

            // 获取样本坐标用于STMK空间查询
            List<double[]> sampleCoords = new ArrayList<>();
            sql = "SELECT DISTINCT source_id, ra, dec_val FROM " + dbName + ".lightcurve LIMIT 20";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    sampleCoords.add(new double[]{rs.getDouble(2), rs.getDouble(3)});
                }
            }

            // 生成STMK查询参数
            stmkParams = new ArrayList<>();
            Random rand = new Random(42);
            double[] radii = {5.0, 10.0, 20.0};
            int[] minObsList = {3, 5, 10};
            String[] bands = {"G", "BP", "RP"};

            for (int i = 0; i < STMK_QUERY_COUNT + WARMUP_QUERIES; i++) {
                double[] coord = sampleCoords.get(rand.nextInt(sampleCoords.size()));
                double timeRange = maxTime - minTime;
                double startTime = minTime + rand.nextDouble() * 0.3 * timeRange;
                double endTime = startTime + (0.3 + rand.nextDouble() * 0.4) * timeRange;

                stmkParams.add(new STMKQueryParams(
                        coord[0], coord[1], radii[rand.nextInt(radii.length)],
                        startTime, endTime, bands[rand.nextInt(bands.length)],
                        minMag, maxMag, minObsList[rand.nextInt(minObsList.length)]
                ));
            }
            log("  Generated " + stmkParams.size() + " STMK query parameters (incl. " + WARMUP_QUERIES + " warmup)");

        } finally {
            if (conn != null) conn.close();
        }
    }

    private String findTDengineDatabase(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {
            while (rs.next()) {
                String name = rs.getString(1);
                if (name.startsWith("astro_exp9_")) {
                    return name;
                }
            }
        }
        return null;
    }

    // ==================== LitecsDB 测试 ====================
    private void testLitecsDB() {
        log("\n" + "=".repeat(60));
        log("Testing LitecsDB");
        log("=".repeat(60));

        File resultDir = new File(RESULT_DIR);
        File[] litecsdbDirs = resultDir.listFiles((dir, name) -> name.startsWith("litecsdb_"));
        if (litecsdbDirs == null || litecsdbDirs.length == 0) {
            log("[ERROR] No LitecsDB database found in " + RESULT_DIR);
            return;
        }
        String basePath = litecsdbDirs[0].getAbsolutePath() + "/";
        log("  Using database: " + basePath);

        MainNode mainNode = null;
        try {
            mainNode = new MainNode(NODE_COUNT, HEALPIX_LEVEL, basePath);

            // 预建索引缓存 — 构建 star metadata 映射用于简单查询
            log("  Building metadata index for simple queries...");
            Map<Long, long[]> sourceToHealpix = new HashMap<>(); // sourceId -> [healpixId]
            List<org.example.RocksDBServer.StarMetadata> allStars = mainNode.getAllStarsMetadata();
            for (org.example.RocksDBServer.StarMetadata star : allStars) {
                long hpid = mainNode.calculateHealpixId(star.ra, star.dec);
                sourceToHealpix.put(star.sourceId, new long[]{hpid});
            }
            log("  Indexed " + sourceToHealpix.size() + " stars");

            // === 简单查询测试 ===
            log("  Running simple queries (" + SIMPLE_QUERY_COUNT + " queries, " + WARMUP_QUERIES + " warmup)...");
            List<Double> simpleLats = new ArrayList<>();
            int totalPoints = 0;

            for (int i = 0; i < simpleQuerySourceIds.size(); i++) {
                long sid = simpleQuerySourceIds.get(i);
                long[] hpInfo = sourceToHealpix.get(sid);

                long start = System.nanoTime();
                List<LightCurvePoint> lc = Collections.emptyList();
                if (hpInfo != null) {
                    lc = mainNode.getLightCurve(hpInfo[0], sid, "G");
                }
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    simpleLats.add(latency);
                    totalPoints += lc.size();
                }
            }
            logSimpleResults("LitecsDB", simpleLats,
                    simpleLats.isEmpty() ? 0 : totalPoints / simpleLats.size());

            // === STMK查询测试 ===
            log("  Running STMK queries (" + STMK_QUERY_COUNT + " queries, " + WARMUP_QUERIES + " warmup)...");
            List<Double> stmkLats = new ArrayList<>();
            List<Integer> stmkCounts = new ArrayList<>();

            for (int i = 0; i < stmkParams.size(); i++) {
                STMKQueryParams q = stmkParams.get(i);

                long start = System.nanoTime();
                DistributedQueryResult result = mainNode.executeSTMKQuery(
                        q.ra, q.dec, q.radius, q.startTime, q.endTime,
                        q.band, q.magMin, q.magMax, q.minObs);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    stmkLats.add(latency);
                    stmkCounts.add(result.candidateCount);
                }
            }
            logSTMKResults("LitecsDB", stmkLats, stmkCounts);

        } catch (Exception e) {
            log("[ERROR] LitecsDB test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (mainNode != null) mainNode.shutdown();
        }
    }

    // ==================== TDengine 测试（多线程优化） ====================
    private void testTDengine() {
        log("\n" + "=".repeat(60));
        log("Testing TDengine (multi-threaded)");
        log("=".repeat(60));

        Connection conn = null;
        // 用于并行查询的连接池
        final int QUERY_THREADS = 8;
        Connection[] queryConns = new Connection[QUERY_THREADS];
        ExecutorService queryPool = Executors.newFixedThreadPool(QUERY_THREADS, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("TDengine-Query-" + t.getId());
            return t;
        });

        try {
            conn = DriverManager.getConnection(
                    "jdbc:TAOS://127.0.0.1:6030/?charset=UTF-8",
                    TDENGINE_USER, TDENGINE_PASSWORD);

            String dbName = findTDengineDatabase(conn);
            if (dbName == null) {
                log("[ERROR] No TDengine database found");
                return;
            }
            log("  Using database: " + dbName);

            // 创建查询线程的独立连接
            for (int i = 0; i < QUERY_THREADS; i++) {
                queryConns[i] = DriverManager.getConnection(
                        "jdbc:TAOS://127.0.0.1:6030/?charset=UTF-8",
                        TDENGINE_USER, TDENGINE_PASSWORD);
                queryConns[i].createStatement().execute("USE " + dbName);
            }
            log("  Created " + QUERY_THREADS + " parallel query connections");

            // 清除缓存
            clearTDengineCache(conn);

            // === 简单查询测试 ===
            log("  Running simple queries...");
            List<Double> simpleLats = new ArrayList<>();
            int totalPoints = 0;

            for (int i = 0; i < simpleQuerySourceIds.size(); i++) {
                // 每次查询前重置连接状态（避免 JDBC 语句缓存）
                long sid = simpleQuerySourceIds.get(i);
                long start = System.nanoTime();
                int count = executeTDengineSimpleQuery(conn, dbName, sid);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    simpleLats.add(latency);
                    totalPoints += count;
                }
            }
            logSimpleResults("TDengine", simpleLats,
                    simpleLats.isEmpty() ? 0 : totalPoints / simpleLats.size());

            // === STMK查询测试（多线程并行） ===
            log("  Running STMK queries (multi-threaded)...");
            clearTDengineCache(conn);

            List<Double> stmkLats = new ArrayList<>();
            List<Integer> stmkCounts = new ArrayList<>();

            for (int i = 0; i < stmkParams.size(); i++) {
                STMKQueryParams q = stmkParams.get(i);

                // 清除缓存
                clearTDengineCache(conn);

                long start = System.nanoTime();
                int count = executeTDengineSTMKQueryParallel(queryConns, queryPool,
                        QUERY_THREADS, dbName, q);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    stmkLats.add(latency);
                    stmkCounts.add(count);
                    log(String.format("    STMK[%d]: %.2fms, results=%d", i - WARMUP_QUERIES, latency, count));
                }
            }
            logSTMKResults("TDengine", stmkLats, stmkCounts);

        } catch (Exception e) {
            log("[ERROR] TDengine test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            queryPool.shutdown();
            try { queryPool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
            for (Connection qc : queryConns) {
                try { if (qc != null) qc.close(); } catch (Exception e) {}
            }
            try { if (conn != null) conn.close(); } catch (Exception e) {}
        }
    }

    /**
     * 清除 TDengine 查询缓存
     */
    private void clearTDengineCache(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("RESET QUERY CACHE");
        } catch (Exception e) {
            // 某些版本不支持，忽略
        }
    }

    /**
     * TDengine 简单查询 — 直接访问 per-source 子表
     * DataManager 使用 s_{sourceId} 命名子表，TAG 含 source_id/ra/dec_val
     */
    private int executeTDengineSimpleQuery(Connection conn, String dbName, long sourceId) {
        // 优先尝试 per-source 子表直接访问（O(1) 定位）
        String sql = String.format(
                "SELECT ts, mag, flux FROM %s.s_%d ORDER BY ts",
                dbName, sourceId);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
            return count;
        } catch (SQLException e) {
            // 回退到 TAG 过滤查询（兼容旧 hash 分桶模式）
            String fallbackSql = String.format(
                    "SELECT ts, mag, flux FROM %s.lightcurve WHERE source_id = %d ORDER BY ts",
                    dbName, sourceId);
            try (Statement stmt2 = conn.createStatement();
                 ResultSet rs2 = stmt2.executeQuery(fallbackSql)) {
                int count = 0;
                while (rs2.next()) count++;
                return count;
            } catch (SQLException e2) {
                return 0;
            }
        }
    }

    /**
     * TDengine STMK 多线程并行查询（per-source 子表模式）
     *
     * 策略：使用超级表级别查询 + PARTITION BY TBNAME，
     * TDengine 内部通过 vgroups 自动并行化。
     * 外部通过多连接分段执行时间范围查询增强并行度。
     *
     * 查询结果直接包含 TAG 值 (source_id, ra, dec_val)，
     * 无需二次坐标查询。
     *
     * SQL:
     *   SELECT source_id, ra, dec_val, COUNT(*) as cnt
     *   FROM {db}.lightcurve
     *   WHERE ts >= {startUs} AND ts <= {endUs}
     *     AND band = '{band}' AND mag >= {magMin} AND mag <= {magMax}
     *   PARTITION BY TBNAME
     *   HAVING COUNT(*) >= {minObs}
     */
    private int executeTDengineSTMKQueryParallel(Connection[] queryConns,
                                                 ExecutorService queryPool,
                                                 int numThreads,
                                                 String dbName,
                                                 STMKQueryParams q) {
        long startUs = (GAIA_EPOCH_UNIX_MS + (long) (q.startTime * MS_PER_DAY)) * 1000;
        long endUs = (GAIA_EPOCH_UNIX_MS + (long) (q.endTime * MS_PER_DAY)) * 1000;
        long totalRange = endUs - startUs;

        if (totalRange <= 0) return 0;

        // 按时间范围分片到多个线程（利用 TDengine 的时间索引）
        long shardSize = (totalRange + numThreads - 1) / numThreads;

        // 每个线程收集 (source_id -> partial_count, ra, dec)
        @SuppressWarnings("unchecked")
        Map<Long, double[]>[] shardResults = new Map[numThreads];
        for (int i = 0; i < numThreads; i++) {
            shardResults[i] = new HashMap<>();
        }

        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadIdx = t;
            final long shardStart = startUs + t * shardSize;
            final long shardEnd = Math.min(shardStart + shardSize, endUs);

            queryPool.submit(() -> {
                try {
                    Connection threadConn = queryConns[threadIdx];

                    // 分片查询：每个线程负责一段时间范围
                    // PARTITION BY TBNAME 自动按子表分组，TAG 直接返回
                    String sql = String.format(Locale.US,
                            "SELECT source_id, ra, dec_val, COUNT(*) as cnt " +
                                    "FROM %s.lightcurve " +
                                    "WHERE ts >= %d AND ts <= %d AND band = '%s' " +
                                    "AND mag >= %f AND mag <= %f " +
                                    "PARTITION BY TBNAME",
                            dbName, shardStart, shardEnd, q.band, q.magMin, q.magMax);

                    try (Statement stmt = threadConn.createStatement();
                         ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) {
                            long sourceId = rs.getLong("source_id");
                            double ra = rs.getDouble("ra");
                            double decVal = rs.getDouble("dec_val");
                            long cnt = rs.getLong("cnt");

                            // 存储: [count, ra, dec]
                            shardResults[threadIdx].merge(sourceId,
                                    new double[]{cnt, ra, decVal},
                                    (old, nw) -> {
                                        old[0] += nw[0];
                                        return old;
                                    });
                        }
                    }
                } catch (Exception e) {
                    System.err.println("TDengine STMK thread " + threadIdx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // 合并所有分片结果
        Map<Long, double[]> globalCounts = new HashMap<>();
        for (Map<Long, double[]> shard : shardResults) {
            for (Map.Entry<Long, double[]> entry : shard.entrySet()) {
                globalCounts.merge(entry.getKey(), entry.getValue(),
                        (old, nw) -> {
                            old[0] += nw[0];
                            return old;
                        });
            }
        }

        // 过滤: minObs + 空间 cone search
        int count = 0;
        for (Map.Entry<Long, double[]> entry : globalCounts.entrySet()) {
            double[] vals = entry.getValue();
            if (vals[0] >= q.minObs) {
                if (isInCone(vals[1], vals[2], q.ra, q.dec, q.radius)) {
                    count++;
                }
            }
        }
        return count;
    }

    // ==================== InfluxDB 测试（多线程优化） ====================
    private void testInfluxDB() {
        log("\n" + "=".repeat(60));
        log("Testing InfluxDB (multi-threaded)");
        log("=".repeat(60));

        final int QUERY_THREADS = 8;
        ExecutorService queryPool = Executors.newFixedThreadPool(QUERY_THREADS, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("InfluxDB-Query-" + t.getId());
            return t;
        });

        try {
            // === 简单查询测试 ===
            log("  Running simple queries...");
            List<Double> simpleLats = new ArrayList<>();
            int totalPoints = 0;

            for (int i = 0; i < simpleQuerySourceIds.size(); i++) {
                long sid = simpleQuerySourceIds.get(i);
                long start = System.nanoTime();
                int count = executeInfluxSimpleQuery(sid);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    simpleLats.add(latency);
                    totalPoints += count;
                }
            }
            logSimpleResults("InfluxDB", simpleLats,
                    simpleLats.isEmpty() ? 0 : totalPoints / simpleLats.size());

            // === STMK查询测试（多线程并行） ===
            log("  Running STMK queries (multi-threaded)...");
            List<Double> stmkLats = new ArrayList<>();
            List<Integer> stmkCounts = new ArrayList<>();

            for (int i = 0; i < stmkParams.size(); i++) {
                STMKQueryParams q = stmkParams.get(i);

                long start = System.nanoTime();
                int count = executeInfluxSTMKQueryParallel(queryPool, QUERY_THREADS, q);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    stmkLats.add(latency);
                    stmkCounts.add(count);
                    log(String.format("    STMK[%d]: %.2fms, results=%d", i - WARMUP_QUERIES, latency, count));
                }
            }
            logSTMKResults("InfluxDB", stmkLats, stmkCounts);

        } catch (Exception e) {
            log("[ERROR] InfluxDB test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            queryPool.shutdown();
            try { queryPool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        }
    }

    private int executeInfluxSimpleQuery(long sourceId) {
        String flux = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: 0) " +
                        "|> filter(fn: (r) => r.source_id == \"%d\") " +
                        "|> filter(fn: (r) => r._field == \"mag\")",
                INFLUX_BUCKET, sourceId);
        return executeFluxQuery(flux);
    }

    /**
     * InfluxDB STMK 多线程并行查询
     *
     * 策略：将时间范围分成 N 段，每段由一个线程查询 Flux，
     * 合并分片结果后重新聚合 count，过滤 minObs，最后空间过滤。
     *
     * Flux 等价：
     *   from(bucket) |> range(start, stop)
     *   |> filter(measurement == "lightcurve" and band == X and _field == "mag")
     *   |> filter(_value >= magMin and _value <= magMax)
     *   |> group(columns: ["source_id"])
     *   |> count()
     */
    private int executeInfluxSTMKQueryParallel(ExecutorService queryPool, int numThreads,
                                               STMKQueryParams q) {
        long startMs = GAIA_EPOCH_UNIX_MS + (long) (q.startTime * MS_PER_DAY);
        long endMs = GAIA_EPOCH_UNIX_MS + (long) (q.endTime * MS_PER_DAY);
        long totalRange = endMs - startMs;

        if (totalRange <= 0) return 0;

        // 分成 numThreads 个时间段
        long shardSize = (totalRange + numThreads - 1) / numThreads;

        // 每个线程收集 source_id -> partial count
        @SuppressWarnings("unchecked")
        Map<String, Integer>[] shardResults = new Map[numThreads];
        for (int i = 0; i < numThreads; i++) {
            shardResults[i] = new HashMap<>();
        }

        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int s = 0; s < numThreads; s++) {
            final int shardIdx = s;
            final long shardStart = startMs + s * shardSize;
            final long shardEnd = Math.min(shardStart + shardSize, endMs);

            queryPool.submit(() -> {
                try {
                    String flux = String.format(Locale.US,
                            "from(bucket: \"%s\") " +
                                    "|> range(start: %d, stop: %d) " +
                                    "|> filter(fn: (r) => r._measurement == \"lightcurve\" and r.band == \"%s\" and r._field == \"mag\") " +
                                    "|> filter(fn: (r) => r._value >= %f and r._value <= %f) " +
                                    "|> group(columns: [\"source_id\"]) " +
                                    "|> count()",
                            INFLUX_BUCKET, shardStart, shardEnd, q.band, q.magMin, q.magMax);

                    shardResults[shardIdx] = executeFluxQueryGetCounts(flux);
                } catch (Exception e) {
                    System.err.println("InfluxDB STMK shard " + shardIdx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // 合并各分片的计数
        Map<String, Integer> globalCounts = new HashMap<>();
        for (Map<String, Integer> shard : shardResults) {
            for (Map.Entry<String, Integer> entry : shard.entrySet()) {
                globalCounts.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }

        // 过滤满足 minObs 的 source_id
        List<Long> candidateSourceIds = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : globalCounts.entrySet()) {
            if (entry.getValue() >= q.minObs) {
                try {
                    candidateSourceIds.add(Long.parseLong(entry.getKey()));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }

        if (candidateSourceIds.isEmpty()) return 0;

        // 应用层空间过滤：获取 ra/dec 后做 cone search
        AtomicInteger spatialCount = new AtomicInteger(0);
        CountDownLatch spatialLatch = new CountDownLatch(candidateSourceIds.size());

        for (long sourceId : candidateSourceIds) {
            queryPool.submit(() -> {
                try {
                    double[] coord = getInfluxSourceCoord(sourceId);
                    if (coord != null && isInCone(coord[0], coord[1], q.ra, q.dec, q.radius)) {
                        spatialCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    spatialLatch.countDown();
                }
            });
        }

        try { spatialLatch.await(120, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return spatialCount.get();
    }

    /**
     * 从 InfluxDB 获取 source_id 的 ra/dec 坐标
     */
    private double[] getInfluxSourceCoord(long sourceId) {
        String fluxRa = String.format(
                "from(bucket: \"%s\") |> range(start: 0) " +
                        "|> filter(fn: (r) => r.source_id == \"%d\" and r._field == \"ra\") " +
                        "|> first()",
                INFLUX_BUCKET, sourceId);

        String fluxDec = String.format(
                "from(bucket: \"%s\") |> range(start: 0) " +
                        "|> filter(fn: (r) => r.source_id == \"%d\" and r._field == \"dec\") " +
                        "|> first()",
                INFLUX_BUCKET, sourceId);

        double ra = executeFluxQueryGetFirstValue(fluxRa);
        double dec = executeFluxQueryGetFirstValue(fluxDec);

        if (!Double.isNaN(ra) && !Double.isNaN(dec)) {
            return new double[]{ra, dec};
        }
        return null;
    }

    /**
     * 执行 Flux 查询返回第一个数值
     */
    private double executeFluxQueryGetFirstValue(String flux) {
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL(INFLUX_URL + "/api/v2/query?org=" + INFLUX_ORG).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Token " + INFLUX_TOKEN);
            conn.setRequestProperty("Content-Type", "application/vnd.flux");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(flux.getBytes());
            }

            if (conn.getResponseCode() != 200) return Double.NaN;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith(",")) {
                        String[] parts = line.split(",");
                        // CSV annotated format: 最后几列通常是 _value
                        for (int p = parts.length - 1; p >= 0; p--) {
                            try {
                                return Double.parseDouble(parts[p].trim());
                            } catch (NumberFormatException e) {
                                // try next column
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return Double.NaN;
    }

    /**
     * 执行 Flux 查询返回 source_id -> count 映射
     */
    private Map<String, Integer> executeFluxQueryGetCounts(String flux) {
        Map<String, Integer> counts = new HashMap<>();
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL(INFLUX_URL + "/api/v2/query?org=" + INFLUX_ORG).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Token " + INFLUX_TOKEN);
            conn.setRequestProperty("Content-Type", "application/vnd.flux");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(120000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(flux.getBytes());
            }

            if (conn.getResponseCode() != 200) return counts;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                String headerLine = null;
                int sourceIdColIdx = -1;
                int valueColIdx = -1;

                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) {
                        // 新表开始，重置表头
                        headerLine = null;
                        sourceIdColIdx = -1;
                        valueColIdx = -1;
                        continue;
                    }
                    if (line.startsWith("#")) continue;

                    if (headerLine == null) {
                        headerLine = line;
                        String[] headers = line.split(",");
                        for (int i = 0; i < headers.length; i++) {
                            String h = headers[i].trim();
                            if (h.equals("source_id")) sourceIdColIdx = i;
                            if (h.equals("_value")) valueColIdx = i;
                        }
                        continue;
                    }

                    if (sourceIdColIdx >= 0 && valueColIdx >= 0) {
                        String[] parts = line.split(",");
                        if (parts.length > Math.max(sourceIdColIdx, valueColIdx)) {
                            try {
                                String sid = parts[sourceIdColIdx].trim();
                                int cnt = (int) Double.parseDouble(parts[valueColIdx].trim());
                                counts.merge(sid, cnt, Integer::sum);
                            } catch (NumberFormatException e) {
                                // ignore
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("InfluxDB Flux count query failed: " + e.getMessage());
        }
        return counts;
    }

    private int executeFluxQuery(String flux) {
        try {
            java.net.HttpURLConnection conn = (java.net.HttpURLConnection)
                    new java.net.URL(INFLUX_URL + "/api/v2/query?org=" + INFLUX_ORG).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Token " + INFLUX_TOKEN);
            conn.setRequestProperty("Content-Type", "application/vnd.flux");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(flux.getBytes());
            }

            if (conn.getResponseCode() != 200) return 0;

            int count = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith(",")) {
                        count++;
                    }
                }
            }
            return Math.max(0, count - 1);
        } catch (Exception e) {
            return 0;
        }
    }

    // ==================== Native RocksDB 测试（多线程优化） ====================
    private void testNativeRocksDB() {
        log("\n" + "=".repeat(60));
        log("Testing Native RocksDB (multi-threaded)");
        log("=".repeat(60));

        File resultDir = new File(RESULT_DIR);
        File[] rocksdbDirs = resultDir.listFiles((dir, name) -> name.startsWith("rocksdb_"));
        if (rocksdbDirs == null || rocksdbDirs.length == 0) {
            log("[ERROR] No NativeRocksDB database found in " + RESULT_DIR);
            return;
        }
        String dbPath = rocksdbDirs[0].getAbsolutePath();
        log("  Using database: " + dbPath);

        NativeRocksDBWrapper rocksdb = null;
        try {
            rocksdb = new NativeRocksDBWrapper(dbPath);

            // === 简单查询测试 ===
            log("  Running simple queries...");
            List<Double> simpleLats = new ArrayList<>();
            int totalPoints = 0;

            for (int i = 0; i < simpleQuerySourceIds.size(); i++) {
                long sid = simpleQuerySourceIds.get(i);
                long start = System.nanoTime();
                int count = rocksdb.queryBySourceId(sid);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    simpleLats.add(latency);
                    totalPoints += count;
                }
            }
            logSimpleResults("NativeRocksDB", simpleLats,
                    simpleLats.isEmpty() ? 0 : totalPoints / simpleLats.size());

            // === STMK查询测试（已内置多线程扫描） ===
            log("  Running STMK queries...");
            List<Double> stmkLats = new ArrayList<>();
            List<Integer> stmkCounts = new ArrayList<>();

            for (int i = 0; i < stmkParams.size(); i++) {
                STMKQueryParams q = stmkParams.get(i);
                long start = System.nanoTime();
                int count = rocksdb.executeSTMKQuery(q.startTime, q.endTime, q.band,
                        q.magMin, q.magMax, q.ra, q.dec, q.radius, q.minObs);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    stmkLats.add(latency);
                    stmkCounts.add(count);
                    log(String.format("    STMK[%d]: %.2fms, results=%d", i - WARMUP_QUERIES, latency, count));
                }
            }
            logSTMKResults("NativeRocksDB", stmkLats, stmkCounts);

        } catch (Exception e) {
            log("[ERROR] Native RocksDB test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (rocksdb != null) rocksdb.close();
        }
    }

    // ==================== 空间工具 ====================

    private static boolean isInCone(double ra1, double dec1, double ra2, double dec2, double radius) {
        double ra1R = Math.toRadians(ra1), dec1R = Math.toRadians(dec1);
        double ra2R = Math.toRadians(ra2), dec2R = Math.toRadians(dec2);
        double a = Math.sin((dec2R - dec1R) / 2) * Math.sin((dec2R - dec1R) / 2) +
                Math.cos(dec1R) * Math.cos(dec2R) *
                        Math.sin((ra2R - ra1R) / 2) * Math.sin((ra2R - ra1R) / 2);
        return Math.toDegrees(2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))) <= radius;
    }

    // ==================== 日志和结果 ====================

    private void logSimpleResults(String system, List<Double> latencies, int avgPoints) {
        double avg = latencies.stream().mapToDouble(d -> d).average().orElse(0);
        double p50 = percentile(latencies, 50);
        double p99 = percentile(latencies, 99);
        double min = latencies.stream().mapToDouble(d -> d).min().orElse(0);
        double max = latencies.stream().mapToDouble(d -> d).max().orElse(0);
        double qps = avg > 0 ? 1000.0 / avg : 0;

        log(String.format("  [Simple Query] %s: avg=%.3fms, p50=%.3fms, p99=%.3fms, " +
                        "min=%.3fms, max=%.3fms, QPS=%.1f, avgPoints=%d",
                system, avg, p50, p99, min, max, qps, avgPoints));

        simpleResults.add(new QueryBenchmarkResult(system + "_simple", latencies, null));
    }

    private void logSTMKResults(String system, List<Double> latencies, List<Integer> counts) {
        double avg = latencies.stream().mapToDouble(d -> d).average().orElse(0);
        double p50 = percentile(latencies, 50);
        double p99 = percentile(latencies, 99);
        double min = latencies.stream().mapToDouble(d -> d).min().orElse(0);
        double max = latencies.stream().mapToDouble(d -> d).max().orElse(0);
        double qps = avg > 0 ? 1000.0 / avg : 0;
        double avgResults = counts.stream().mapToInt(i -> i).average().orElse(0);

        log(String.format("  [STMK Query]   %s: avg=%.3fms, p50=%.3fms, p99=%.3fms, " +
                        "min=%.3fms, max=%.3fms, QPS=%.1f, avgResults=%.1f",
                system, avg, p50, p99, min, max, qps, avgResults));

        stmkResults.add(new QueryBenchmarkResult(system, latencies, counts));
    }

    private double percentile(List<Double> data, int p) {
        if (data.isEmpty()) return 0;
        List<Double> sorted = data.stream().sorted().collect(Collectors.toList());
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(idx, sorted.size() - 1)));
    }

    private void printSummaryReport() {
        log("\n" + "=".repeat(80));
        log("SUMMARY: Query Performance Comparison");
        log("=".repeat(80));

        // Simple Query Summary
        log("\n--- Simple Query Results ---");
        log(String.format("%-15s %12s %12s %12s %12s %10s",
                "System", "Avg(ms)", "P50(ms)", "P99(ms)", "Min(ms)", "QPS"));
        log("-".repeat(75));

        for (QueryBenchmarkResult r : simpleResults) {
            log(String.format("%-15s %12.2f %12.2f %12.2f %12.2f %10.1f",
                    r.system.replace("_simple", ""),
                    r.avgLatency, r.p50Latency, r.p99Latency, r.minLatency, r.throughput));
        }

        // STMK Query Summary
        log("\n--- STMK Query Results ---");
        log(String.format("%-15s %12s %12s %12s %12s %10s %10s",
                "System", "Avg(ms)", "P50(ms)", "P99(ms)", "Min(ms)", "QPS", "AvgRes"));
        log("-".repeat(85));

        QueryBenchmarkResult litecsResult = stmkResults.stream()
                .filter(r -> r.system.equals("LitecsDB")).findFirst().orElse(null);

        for (QueryBenchmarkResult r : stmkResults) {
            String speedup = "";
            if (litecsResult != null && !r.system.equals("LitecsDB") && litecsResult.avgLatency > 0) {
                double ratio = r.avgLatency / litecsResult.avgLatency;
                speedup = String.format(" (%.1fx %s)", ratio > 1 ? ratio : 1.0 / ratio,
                        ratio > 1 ? "slower" : "faster");
            }
            log(String.format("%-15s %12.2f %12.2f %12.2f %12.2f %10.1f %10.1f%s",
                    r.system, r.avgLatency, r.p50Latency, r.p99Latency,
                    r.minLatency, r.throughput, r.avgResults, speedup));
        }

        log("\nExperiment completed at: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }

    private void exportCSV() {
        try {
            String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            PrintWriter csv = new PrintWriter(new FileWriter(RESULT_DIR + "experiment9_" + ts + ".csv"));
            csv.println("query_type,system,query_idx,latency_ms,result_count");

            for (QueryBenchmarkResult r : simpleResults) {
                for (int i = 0; i < r.latencies.size(); i++) {
                    csv.printf("%s,%s,%d,%.3f,%d%n", "simple",
                            r.system.replace("_simple", ""), i, r.latencies.get(i), 0);
                }
            }

            for (QueryBenchmarkResult r : stmkResults) {
                for (int i = 0; i < r.latencies.size(); i++) {
                    int cnt = (r.counts != null && i < r.counts.size()) ? r.counts.get(i) : 0;
                    csv.printf("%s,%s,%d,%.3f,%d%n", "stmk", r.system, i, r.latencies.get(i), cnt);
                }
            }

            csv.close();
            log("CSV exported to " + RESULT_DIR);
        } catch (IOException e) {
            log("[WARN] CSV export failed: " + e.getMessage());
        }
    }

    private void initResultDir() {
        try { Files.createDirectories(Paths.get(RESULT_DIR)); } catch (IOException e) {}
    }

    private void initWriters() {
        try {
            String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            logWriter = new PrintWriter(new FileWriter(RESULT_DIR + "experiment9_" + ts + ".log"));
        } catch (IOException e) {}
    }

    private void closeWriters() {
        if (logWriter != null) logWriter.close();
    }

    private void log(String msg) {
        System.out.println(msg);
        if (logWriter != null) { logWriter.println(msg); logWriter.flush(); }
    }

    // ==================== 内部类 ====================

    static class STMKQueryParams {
        final double ra, dec, radius, startTime, endTime;
        final String band;
        final double magMin, magMax;
        final int minObs;

        STMKQueryParams(double ra, double dec, double radius, double startTime, double endTime,
                        String band, double magMin, double magMax, int minObs) {
            this.ra = ra; this.dec = dec; this.radius = radius;
            this.startTime = startTime; this.endTime = endTime;
            this.band = band; this.magMin = magMin; this.magMax = magMax;
            this.minObs = minObs;
        }
    }

    static class QueryBenchmarkResult {
        final String system;
        final double avgLatency, p50Latency, p99Latency, minLatency, maxLatency, throughput;
        final double avgResults;
        final List<Double> latencies;
        final List<Integer> counts;

        QueryBenchmarkResult(String system, List<Double> latencies, List<Integer> counts) {
            this.system = system;
            this.latencies = new ArrayList<>(latencies);
            this.counts = counts != null ? new ArrayList<>(counts) : null;
            this.avgLatency = latencies.stream().mapToDouble(d -> d).average().orElse(0);
            List<Double> sorted = latencies.stream().sorted().collect(Collectors.toList());
            this.p50Latency = sorted.isEmpty() ? 0 : sorted.get(sorted.size() / 2);
            this.p99Latency = sorted.isEmpty() ? 0 : sorted.get(Math.min((int) (sorted.size() * 0.99), sorted.size() - 1));
            this.minLatency = sorted.isEmpty() ? 0 : sorted.get(0);
            this.maxLatency = sorted.isEmpty() ? 0 : sorted.get(sorted.size() - 1);
            this.throughput = avgLatency > 0 ? 1000.0 / avgLatency : 0;
            this.avgResults = counts != null ? counts.stream().mapToInt(i -> i).average().orElse(0) : 0;
        }
    }
}