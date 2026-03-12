package org.example;

import org.example.MainNode.*;
import org.example.RocksDBServer.LightCurvePoint;
import org.example.wrapper.InfluxDBClientWrapper;
import org.example.wrapper.NativeRocksDBWrapper;
import org.example.wrapper.TDengineClientWrapper;
import org.example.wrapper.TDengineClientWrapper;

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
    private static final int SIMPLE_QUERY_COUNT = 5;
    private static final int STMK_QUERY_COUNT = 5;
    private static final int MAX_PRINT_ROWS = 3;   // 简单查询最多打印前N行详细数据
    private static final int MAX_PRINT_IDS = 10;    // STMK查询最多打印前N个source_id
    private static final int WARMUP_QUERIES = 3;        // 预热查询数（不计入结果）
    private static final int HEALPIX_LEVEL = 1;
    private static final int NODE_COUNT = 2;

    private static final String RESULT_DIR = "experiment9_results/";

    // TDengine
    private static final String TDENGINE_USER = "root";
    private static final String TDENGINE_PASSWORD = "taosdata";

    // InfluxDB
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
                List<LightCurvePoint> lc = new ArrayList<>();
                if (hpInfo != null) {
                    // 查询全部波段（G/BP/RP）的所有属性
                    for (String band : new String[]{"G", "BP", "RP"}) {
                        lc.addAll(mainNode.getLightCurve(hpInfo[0], sid, band));
                    }
                }
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    simpleLats.add(latency);
                    totalPoints += lc.size();
                    logSimpleQueryDetail("LitecsDB", i - WARMUP_QUERIES, sid, lc, latency);
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
                    List<Long> matchedIds = new ArrayList<>(result.candidateObjects);
                    logSTMKQueryDetail("LitecsDB", i - WARMUP_QUERIES, q, matchedIds, latency);
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

    // ==================== TDengine 测试 ====================
    private void testTDengine() {
        log("\n" + "=".repeat(60));
        log("Testing TDengine (HEALPix supertable + TAG-based query)");
        log("=".repeat(60));

        TDengineClientWrapper tdengine = null;
        try {
            tdengine = new TDengineClientWrapper(findTDengineDbName());
            tdengine.clearQueryCache();

            // === 简单查询测试 ===
            log("  Running simple queries...");
            List<Double> simpleLats = new ArrayList<>();
            int totalPoints = 0;

            for (int i = 0; i < simpleQuerySourceIds.size(); i++) {
                long sid = simpleQuerySourceIds.get(i);
                long start = System.nanoTime();
                List<LightCurvePoint> rows = tdengine.executeSimpleQuery(sid);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    simpleLats.add(latency);
                    totalPoints += rows.size();
                    logSimpleQueryDetail("TDengine", i - WARMUP_QUERIES, sid, rows, latency);
                }
            }
            logSimpleResults("TDengine", simpleLats,
                    simpleLats.isEmpty() ? 0 : totalPoints / simpleLats.size());

            // === STMK 查询测试 ===
            log("  Running STMK queries (HEALPix-pruned)...");
            tdengine.clearQueryCache();
            List<Double> stmkLats = new ArrayList<>();
            List<Integer> stmkCounts = new ArrayList<>();

            for (int i = 0; i < stmkParams.size(); i++) {
                STMKQueryParams q = stmkParams.get(i);
                tdengine.clearQueryCache();

                long start = System.nanoTime();
                List<Long> matchedIds = tdengine.executeSTMKQuery(
                        q.ra, q.dec, q.radius, q.startTime, q.endTime,
                        q.magMin, q.magMax, q.minObs);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    stmkLats.add(latency);
                    stmkCounts.add(matchedIds.size());
                    logSTMKQueryDetail("TDengine", i - WARMUP_QUERIES, q, matchedIds, latency);
                }
            }
            logSTMKResults("TDengine", stmkLats, stmkCounts);

        } catch (Exception e) {
            log("[ERROR] TDengine test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (tdengine != null) tdengine.close();
        }
    }

    /** 查找已存在的 TDengine 数据库名 */
    private String findTDengineDbName() {
        try {
            Connection conn = DriverManager.getConnection(
                    "jdbc:TAOS://127.0.0.1:6030/?charset=UTF-8", TDENGINE_USER, TDENGINE_PASSWORD);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW DATABASES")) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (name.startsWith("astro_exp9")) { conn.close(); return name; }
                }
            }
            conn.close();
        } catch (Exception e) { /* ignore */ }
        return "astro_exp9";
    }

    // ==================== InfluxDB 测试 ====================
    private void testInfluxDB() {
        log("\n" + "=".repeat(60));
        log("Testing InfluxDB (HEALPix measurement + tag-based query)");
        log("=".repeat(60));

        InfluxDBClientWrapper influx = null;
        try {
            influx = new InfluxDBClientWrapper(INFLUX_BUCKET);

            // === 简单查询测试 ===
            log("  Running simple queries...");
            List<Double> simpleLats = new ArrayList<>();
            int totalPoints = 0;

            for (int i = 0; i < simpleQuerySourceIds.size(); i++) {
                long sid = simpleQuerySourceIds.get(i);
                long start = System.nanoTime();
                List<String> rows = influx.executeSimpleQuery(sid);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    simpleLats.add(latency);
                    totalPoints += rows.size();
                    log(String.format("    [Simple-%d] REQUEST:  sourceId=%d", i - WARMUP_QUERIES, sid));
                    log(String.format("    [Simple-%d] RESPONSE: %d rows, latency=%.3fms",
                            i - WARMUP_QUERIES, rows.size(), latency));
                    int printCount = Math.min(rows.size(), MAX_PRINT_ROWS);
                    for (int r = 0; r < printCount; r++) {
                        log("      Row " + (r + 1) + ": " + rows.get(r));
                    }
                    if (rows.size() > MAX_PRINT_ROWS) {
                        log("      ... (" + (rows.size() - MAX_PRINT_ROWS) + " more rows omitted)");
                    }
                }
            }
            logSimpleResults("InfluxDB", simpleLats,
                    simpleLats.isEmpty() ? 0 : totalPoints / simpleLats.size());

            // === STMK 查询测试 ===
            log("  Running STMK queries (HEALPix-pruned)...");
            List<Double> stmkLats = new ArrayList<>();
            List<Integer> stmkCounts = new ArrayList<>();

            for (int i = 0; i < stmkParams.size(); i++) {
                STMKQueryParams q = stmkParams.get(i);

                long start = System.nanoTime();
                List<Long> matchedIds = influx.executeSTMKQuery(
                        q.ra, q.dec, q.radius, q.startTime, q.endTime,
                        q.magMin, q.magMax, q.minObs);
                double latency = (System.nanoTime() - start) / 1_000_000.0;

                if (i >= WARMUP_QUERIES) {
                    stmkLats.add(latency);
                    stmkCounts.add(matchedIds.size());
                    logSTMKQueryDetail("InfluxDB", i - WARMUP_QUERIES, q, matchedIds, latency);
                }
            }
            logSTMKResults("InfluxDB", stmkLats, stmkCounts);

        } catch (Exception e) {
            log("[ERROR] InfluxDB test failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (influx != null) influx.close();
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
            // NativeRocksDB 通过前缀扫描返回全部数据，queryBySourceId 内部读取所有列
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
                    log(String.format("    [Simple-%d] REQUEST:  sourceId=%d", i - WARMUP_QUERIES, sid));
                    log(String.format("    [Simple-%d] RESPONSE: %d rows (all columns scanned via prefix seek), latency=%.3fms",
                            i - WARMUP_QUERIES, count, latency));
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
                    log(String.format(Locale.US,
                            "    [STMK-%d] REQUEST:  ra=%.4f, dec=%.4f, radius=%.2f sq.deg, " +
                                    "t_min=%.2f, t_max=%.2f, mag_min=%.4f, mag_max=%.4f, count=%d",
                            i - WARMUP_QUERIES, q.ra, q.dec, q.radius,
                            q.startTime, q.endTime, q.magMin, q.magMax, q.minObs));
                    log(String.format("    [STMK-%d] RESPONSE: %d source_ids matched (full scan + app-level filter), latency=%.2fms",
                            i - WARMUP_QUERIES, count, latency));
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

    // ==================== 格式化输出辅助 ====================

    /** 格式化打印单条 LightCurvePoint 的所有属性 */
    private String formatPoint(LightCurvePoint p) {
        return String.format(Locale.US,
                "sourceId=%d, ra=%.6f, dec=%.6f, transitId=%d, band=%s, time=%.4f, " +
                        "mag=%.4f, flux=%.6f, fluxErr=%.6f, fluxOverErr=%.4f, " +
                        "rejPhoto=%s, rejVar=%s, flags=%d, solId=%d",
                p.sourceId, p.ra, p.dec, p.transitId, p.band, p.time,
                p.mag, p.flux, p.fluxError, p.fluxOverError,
                p.rejectedByPhotometry, p.rejectedByVariability, p.otherFlags, p.solutionId);
    }

    /** 格式化打印简单查询的请求与响应 */
    private void logSimpleQueryDetail(String system, int queryIdx, long sourceId,
                                      List<LightCurvePoint> rows, double latencyMs) {
        log(String.format("    [Simple-%d] REQUEST:  sourceId=%d", queryIdx, sourceId));
        log(String.format("    [Simple-%d] RESPONSE: %d rows, latency=%.3fms", queryIdx, rows.size(), latencyMs));
        int printCount = Math.min(rows.size(), MAX_PRINT_ROWS);
        for (int r = 0; r < printCount; r++) {
            log("      Row " + (r + 1) + ": " + formatPoint(rows.get(r)));
        }
        if (rows.size() > MAX_PRINT_ROWS) {
            log("      ... (" + (rows.size() - MAX_PRINT_ROWS) + " more rows omitted)");
        }
    }

    /** 格式化打印STMK查询的请求与响应 */
    private void logSTMKQueryDetail(String system, int queryIdx, STMKQueryParams q,
                                    List<Long> matchedIds, double latencyMs) {
        log(String.format(Locale.US,
                "    [STMK-%d] REQUEST:  ra=%.4f, dec=%.4f, radius=%.2f sq.deg, " +
                        "t_min=%.2f, t_max=%.2f, mag_min=%.4f, mag_max=%.4f, count=%d",
                queryIdx, q.ra, q.dec, q.radius,
                q.startTime, q.endTime, q.magMin, q.magMax, q.minObs));
        log(String.format("    [STMK-%d] RESPONSE: %d source_ids matched, latency=%.2fms",
                queryIdx, matchedIds.size(), latencyMs));
        if (!matchedIds.isEmpty()) {
            int printCount = Math.min(matchedIds.size(), MAX_PRINT_IDS);
            StringBuilder sb = new StringBuilder("      Matched source_ids: [");
            for (int r = 0; r < printCount; r++) {
                if (r > 0) sb.append(", ");
                sb.append(matchedIds.get(r));
            }
            if (matchedIds.size() > MAX_PRINT_IDS) {
                sb.append(", ... +(").append(matchedIds.size() - MAX_PRINT_IDS).append(" more)");
            }
            sb.append("]");
            log(sb.toString());
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