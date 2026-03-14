package org.example.exp;

import org.example.MainNode;
import org.example.RocksDBServer.LightCurvePoint;
import org.example.wrapper.InfluxDBClientWrapper;
import org.example.wrapper.NativeRocksDBWrapper;
import org.example.wrapper.TDengineClientWrapper;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Experiment 9: Incremental Multi-Group Query Performance Comparison
 *
 * 分组递增实验：8组不同数据规模（10→100K天体），增量写入，每组跑完整查询基准测试
 * 5种查询类型 × 4个系统，观察查询性能随数据规模的变化趋势
 */
public class Experiment9Runner {

    // ==================== 分组配置 ====================
    private static final int[] GROUP_SIZES = {10, 100, 500, 1_000, 5_000, 10_000, 50_000, 100_000};
    private static final String DATA_DIR = "generated_datasets/batch_2000000/individual_lightcurves/";
    private static final String DATA_BASE_DIR = "generated_datasets/";

    // ==================== 实验配置 ====================
    private static final int SIMPLE_QUERY_COUNT = 100;
    private static final int STMK_QUERY_COUNT = 5;
    private static final double QUERY_RADIUS = 0.5;
    private static final int QUERY_MIN_K = 3;
    private static final String[] BANDS = {"G", "BP", "RP"};
    private static final int HEALPIX_LEVEL = 1;
    private static final int NODE_COUNT = 2;
    private static final String RESULT_DIR = "experiment9_results/";
    private static final String LITECSDB_PATH = RESULT_DIR + "litecsdb_exp9/";
    private static final String ROCKSDB_PATH = RESULT_DIR + "rocksdb_exp9/";
    private static final String INFLUX_BUCKET = "gaia_lightcurves";
    private static final String TDENGINE_DB = "astro_exp9";

    // ==================== 系统句柄（跨组复用） ====================
    private MainNode mainNode;
    private TDengineClientWrapper tdengine;
    private InfluxDBClientWrapper influxDB;
    private NativeRocksDBWrapper nativeRocksDB;

    // ==================== 轻量级数据追踪 ====================
    private final Set<Long> loadedSourceIds = new LinkedHashSet<>();
    private final Map<Long, double[]> sourceCoordCache = new HashMap<>();
    private double globalMinTime = Double.MAX_VALUE, globalMaxTime = Double.MIN_VALUE;
    private double globalMinMag = Double.MAX_VALUE, globalMaxMag = Double.MIN_VALUE;
    private boolean phase1Done = false;

    // ==================== 查询参数 ====================
    private List<Long> simpleQuerySourceIds;
    private List<STMKQueryParams> stmkParams;
    private List<double[]> timeRangeWindows;
    private List<String> bandQueryBands;

    // ==================== 结果记录（按组） ====================
    // groupIdx -> list of QueryBenchmarkResult (4 systems per group)
    private final Map<Integer, List<QueryBenchmarkResult>> simpleResultsByGroup = new LinkedHashMap<>();
    private final Map<Integer, List<QueryBenchmarkResult>> stmkResultsByGroup = new LinkedHashMap<>();
    private final Map<Integer, List<QueryBenchmarkResult>> timeRangeResultsByGroup = new LinkedHashMap<>();
    private final Map<Integer, List<QueryBenchmarkResult>> aggResultsByGroup = new LinkedHashMap<>();
    private final Map<Integer, List<QueryBenchmarkResult>> bandResultsByGroup = new LinkedHashMap<>();
    private PrintWriter logWriter;

    // ==================== Main ====================
    public static void main(String[] args) {
        new Experiment9Runner().run();
    }

    public void run() {
        initResultDir();
        initWriters();

        log("================================================================================");
        log("Experiment 9: Incremental Multi-Group Query Performance Comparison");
        log("Start time: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        log("Groups: " + Arrays.toString(GROUP_SIZES));
        log("================================================================================");

        try {
            // Step 1: 加载坐标映射
            Map<Long, String> coordMap = loadCoordinateMap();
            log("[Init] 加载了 " + coordMap.size() + " 个天体坐标");

            // Step 2: 列出并排序所有CSV文件
            File[] allFiles = listAndSortCsvFiles();
            log("[Init] 找到 " + allFiles.length + " 个CSV文件（天体）");
            if (allFiles.length < GROUP_SIZES[GROUP_SIZES.length - 1]) {
                log("[WARN] 文件数 " + allFiles.length + " 不足最大组 " + GROUP_SIZES[GROUP_SIZES.length - 1]);
            }

            // Step 3: 初始化所有数据库系统
            initAllSystems();

            // Step 4: 主循环 — 逐组递增
            int filesLoadedSoFar = 0;

            for (int g = 0; g < GROUP_SIZES.length; g++) {
                int targetCount = Math.min(GROUP_SIZES[g], allFiles.length);
                int delta = targetCount - filesLoadedSoFar;
                if (delta <= 0) {
                    log("[SKIP] Group " + (g + 1) + ": 无新文件可加载");
                    continue;
                }

                log("\n" + "#".repeat(80));
                log(String.format("GROUP %d/%d: 加载天体 %d -> %d (增量: %d 个天体)",
                        g + 1, GROUP_SIZES.length, filesLoadedSoFar + 1, targetCount, delta));
                log("#".repeat(80));

                // 4a: 解析增量CSV文件
                ParseResult parseResult = parseDeltaFiles(allFiles, filesLoadedSoFar, targetCount, coordMap);
                log(String.format("  解析了 %d 个点，来自 %d 个新文件", parseResult.points.size(), delta));

                // 4b: 写入所有系统
                long insertStart = System.currentTimeMillis();
                insertIntoAllSystems(parseResult.points, parseResult.csvLines);
                log(String.format("  写入完成，耗时 %d ms", System.currentTimeMillis() - insertStart));

                // 4c: 更新统计
                updateStats(parseResult.points);
                filesLoadedSoFar = targetCount;

                // 4d: Flush + 构建索引
                flushAndBuildIndexes();

                // 4e: 生成查询参数
                deriveQueryParams();

                // 4f: 运行查询基准测试
                runGroupBenchmark(g, targetCount);
            }

            // Step 5: 汇总报告
            printGroupedSummaryReport();
            exportGroupedCSV();

        } catch (Exception e) {
            log("[ERROR] Experiment failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            shutdownAllSystems();
            closeWriters();
        }
    }

    // ==================== 内部类 ====================

    private static class ParseResult {
        List<LightCurvePoint> points = new ArrayList<>();
        List<String> csvLines = new ArrayList<>();
    }

    private static class STMKQueryParams {
        double ra, dec, radius, startTime, endTime, magMin, magMax;
        String band;
        int minObs;

        STMKQueryParams(double ra, double dec, double radius, double st, double et,
                        String band, double magMin, double magMax, int minObs) {
            this.ra = ra;
            this.dec = dec;
            this.radius = radius;
            this.startTime = st;
            this.endTime = et;
            this.band = band;
            this.magMin = magMin;
            this.magMax = magMax;
            this.minObs = minObs;
        }
    }

    private static class QueryBenchmarkResult {
        String system;
        String queryType;
        int groupSize;
        double avgLatencyMs;
        int queryCount;
        double totalMs;
        double qps;

        QueryBenchmarkResult(String system, String queryType, int groupSize,
                             double totalMs, int queryCount) {
            this.system = system;
            this.queryType = queryType;
            this.groupSize = groupSize;
            this.totalMs = totalMs;
            this.queryCount = queryCount;
            this.avgLatencyMs = queryCount > 0 ? totalMs / queryCount : 0;
            this.qps = totalMs > 0 ? queryCount / (totalMs / 1000.0) : 0;
        }

    }

    // ==================== 初始化方法 ====================

    private void initResultDir() {
        new File(RESULT_DIR).mkdirs();
    }

    private void initWriters() {
        try {
            logWriter = new PrintWriter(new BufferedWriter(
                    new FileWriter(RESULT_DIR + "experiment9_log.txt", false)));
        } catch (IOException e) {
            throw new RuntimeException("Cannot create log writer", e);
        }
    }

    private void closeWriters() {
        if (logWriter != null) {
            logWriter.flush();
            logWriter.close();
        }
    }

    private void log(String msg) {
        System.out.println(msg);
        if (logWriter != null) {
            logWriter.println(msg);
            logWriter.flush();
        }
    }

    // ==================== 数据加载 ====================

    private Map<Long, String> loadCoordinateMap() throws IOException {
        Map<Long, String> coordMap = new HashMap<>();
        String coordFile = DATA_BASE_DIR + "batch_2000000/source_coordinates.csv";
        File f = new File(coordFile);
        if (!f.exists()) {
            coordFile = "gaiadr2/source_coordinates.csv";
            f = new File(coordFile);
        }
        if (!f.exists()) {
            log("[WARN] 坐标文件不存在: " + coordFile);
            return coordMap;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals("source_id")) continue;
                try {
                    long sid = Long.parseLong(parts[0].trim());
                    coordMap.put(sid, parts[1].trim() + "," + parts[2].trim());
                    double ra = Double.parseDouble(parts[1].trim());
                    double dec = Double.parseDouble(parts[2].trim());
                    sourceCoordCache.put(sid, new double[]{ra, dec});
                } catch (Exception e) {
                }
            }
        }
        return coordMap;
    }

    private File[] listAndSortCsvFiles() {
        File dir = new File(DATA_DIR);
        if (!dir.exists() || !dir.isDirectory()) {
            log("[ERROR] 数据目录不存在: " + DATA_DIR);
            return new File[0];
        }
        File[] files = dir.listFiles((d, name) -> name.endsWith(".csv"));
        if (files == null) return new File[0];
        Arrays.sort(files, Comparator.comparing(File::getName));
        return files;
    }

    // ==================== 系统初始化/关闭 ====================

    private void initAllSystems() {
        log("[Init] 初始化 LitecsDB...");
        new File(LITECSDB_PATH).mkdirs();
        mainNode = new MainNode(NODE_COUNT, HEALPIX_LEVEL, LITECSDB_PATH);

        log("[Init] 初始化 NativeRocksDB...");
        new File(ROCKSDB_PATH).mkdirs();
        nativeRocksDB = new NativeRocksDBWrapper(ROCKSDB_PATH);

        log("[Init] 初始化 TDengine...");
        try {
            tdengine = new TDengineClientWrapper(TDENGINE_DB);
        } catch (Exception e) {
            log("[WARN] TDengine 初始化失败: " + e.getMessage());
            tdengine = null;
        }

        log("[Init] 初始化 InfluxDB...");
        try {
            influxDB = new InfluxDBClientWrapper(INFLUX_BUCKET);
        } catch (Exception e) {
            log("[WARN] InfluxDB 初始化失败: " + e.getMessage());
            influxDB = null;
        }
        log("[Init] 所有系统初始化完成");
    }

    private void shutdownAllSystems() {
        log("[Shutdown] 关闭所有系统...");
        try {
            if (mainNode != null) mainNode.shutdown();
        } catch (Exception e) {
        }
        try {
            if (nativeRocksDB != null) nativeRocksDB.close();
        } catch (Exception e) {
        }
        try {
            if (tdengine != null) tdengine.close();
        } catch (Exception e) {
        }
        try {
            if (influxDB != null) influxDB.close();
        } catch (Exception e) {
        }
    }

    // ==================== 数据解析 ====================

    private ParseResult parseDeltaFiles(File[] allFiles, int from, int to, Map<Long, String> coordMap)
            throws IOException {
        ParseResult result = new ParseResult();
        for (int i = from; i < to && i < allFiles.length; i++) {
            try (BufferedReader br = new BufferedReader(new FileReader(allFiles[i]))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts[0].equals("source_id")) continue;
                    try {
                        long sourceId = Long.parseLong(parts[0].trim());
                        String coords = coordMap.getOrDefault(sourceId, "0.0,0.0");
                        String csvLine = parts[0] + "," + coords + "," +
                                String.join(",", Arrays.copyOfRange(parts, 1, parts.length));
                        result.csvLines.add(csvLine);

                        String[] fullParts = csvLine.split(",", -1);
                        if (fullParts.length >= 14) {
                            double ra = Double.parseDouble(fullParts[1].trim());
                            double dec = Double.parseDouble(fullParts[2].trim());
                            LightCurvePoint point = new LightCurvePoint(
                                    (long) Double.parseDouble(fullParts[0].trim()),
                                    ra, dec,
                                    (long) Double.parseDouble(fullParts[3].trim()),
                                    fullParts[4].trim(),
                                    Double.parseDouble(fullParts[5].trim()),
                                    Double.parseDouble(fullParts[6].trim()),
                                    Double.parseDouble(fullParts[7].trim()),
                                    Double.parseDouble(fullParts[8].trim()),
                                    Double.parseDouble(fullParts[9].trim()),
                                    Boolean.parseBoolean(fullParts[10].trim()),
                                    Boolean.parseBoolean(fullParts[11].trim()),
                                    (int) Double.parseDouble(fullParts[12].trim()),
                                    (long) Double.parseDouble(fullParts[13].trim())
                            );
                            result.points.add(point);
                            loadedSourceIds.add(sourceId);
                            sourceCoordCache.putIfAbsent(sourceId, new double[]{ra, dec});
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }
        return result;
    }

    // ==================== 写入所有系统 ====================

    private void insertIntoAllSystems(List<LightCurvePoint> points, List<String> csvLines) {
        // LitecsDB: Phase 1 预创建 + Phase 2 写入
        if (!phase1Done) {
            mainNode.preCreateHealpixDatabases(csvLines);
            phase1Done = true;
        } else {
            mainNode.preCreateHealpixDatabases(csvLines);
        }
        Map<Long, List<LightCurvePoint>> healpixDataMap = mainNode.preParseData(csvLines);
        mainNode.distributePreParsedData(healpixDataMap);

        // NativeRocksDB
        nativeRocksDB.putBatch(points);

        // TDengine
        if (tdengine != null) {
            try {
                tdengine.putBatch(points);
            } catch (Exception e) {
                log("[WARN] TDengine 写入失败: " + e.getMessage());
            }
        }

        // InfluxDB
        if (influxDB != null) {
            try {
                influxDB.putBatch(points);
            } catch (Exception e) {
                log("[WARN] InfluxDB 写入失败: " + e.getMessage());
            }
        }
    }

    private void updateStats(List<LightCurvePoint> points) {
        for (LightCurvePoint p : points) {
            if (p.time < globalMinTime) globalMinTime = p.time;
            if (p.time > globalMaxTime) globalMaxTime = p.time;
            if (p.mag < globalMinMag) globalMinMag = p.mag;
            if (p.mag > globalMaxMag) globalMaxMag = p.mag;
        }
    }

    private void flushAndBuildIndexes() {
        log("  [Flush] LitecsDB flush + 构建时间桶索引...");
        mainNode.forceFlushAllNoCompaction();
        mainNode.buildAllTimeBucketsOffline();

        log("  [Flush] NativeRocksDB flush...");
        nativeRocksDB.forceFlush();

        if (tdengine != null) {
            log("  [Flush] TDengine flush...");
            tdengine.forceFlush();
        }
        if (influxDB != null) {
            log("  [Flush] InfluxDB flush...");
            influxDB.forceFlush();
        }
    }

    // ==================== 查询生成 ====================

    private List<STMKQueryParams> generateSTMKQueries(int count) {
        List<STMKQueryParams> queries = new ArrayList<>();
        List<Long> sids = new ArrayList<>(loadedSourceIds);
        if (sids.isEmpty()) return queries;
        Random rng = new Random(42);
        double timeSpan = globalMaxTime - globalMinTime;
        for (int i = 0; i < count; i++) {
            long sid = sids.get(rng.nextInt(sids.size()));
            double[] coord = sourceCoordCache.get(sid);
            if (coord == null) continue;
            double windowSize = timeSpan * (0.05 + rng.nextDouble() * 0.3);
            double st = globalMinTime + rng.nextDouble() * (timeSpan - windowSize);
            double et = st + windowSize;
            String band = BANDS[rng.nextInt(BANDS.length)];
            queries.add(new STMKQueryParams(coord[0], coord[1], QUERY_RADIUS,
                    st, et, band, globalMinMag, globalMaxMag, QUERY_MIN_K));
        }
        return queries;
    }

    private List<long[]> generateSimpleQueryTargets(int count) {
        List<long[]> targets = new ArrayList<>();
        List<Long> sids = new ArrayList<>(loadedSourceIds);
        if (sids.isEmpty()) return targets;
        Random rng = new Random(123);
        for (int i = 0; i < count; i++) {
            long sid = sids.get(rng.nextInt(sids.size()));
            double[] coord = sourceCoordCache.getOrDefault(sid, new double[]{0, 0});
            targets.add(new long[]{sid, Double.doubleToLongBits(coord[0]),
                    Double.doubleToLongBits(coord[1])});
        }
        return targets;
    }

    private List<double[]> generateTimeWindows(int count) {
        List<double[]> windows = new ArrayList<>();
        Random rng = new Random(456);
        double timeSpan = globalMaxTime - globalMinTime;
        for (int i = 0; i < count; i++) {
            double windowSize = timeSpan * (0.05 + rng.nextDouble() * 0.2);
            double st = globalMinTime + rng.nextDouble() * (timeSpan - windowSize);
            windows.add(new double[]{st, st + windowSize});
        }
        return windows;
    }

    private List<String> generateBandList(int count) {
        List<String> bands = new ArrayList<>();
        Random rng = new Random(789);
        for (int i = 0; i < count; i++) {
            bands.add(BANDS[rng.nextInt(BANDS.length)]);
        }
        return bands;
    }

    // ==================== 查询参数派生 ====================

    private void deriveQueryParams() {
        simpleQuerySourceIds = new ArrayList<>();
        List<long[]> targets = generateSimpleQueryTargets(SIMPLE_QUERY_COUNT);
        for (long[] t : targets) simpleQuerySourceIds.add(t[0]);

        stmkParams = generateSTMKQueries(STMK_QUERY_COUNT);
        timeRangeWindows = generateTimeWindows(SIMPLE_QUERY_COUNT);
        bandQueryBands = generateBandList(SIMPLE_QUERY_COUNT);
    }

    // ==================== 分组基准测试 ====================

    private void runGroupBenchmark(int groupIdx, int groupSize) {
        log(String.format("\n  [Benchmark] Group %d (size=%d): 开始查询基准测试...", groupIdx + 1, groupSize));

        // 1. 单天体查询
        simpleResultsByGroup.put(groupIdx, runSimpleQueryBenchmark(groupSize));

        // 2. STMK 查询
        stmkResultsByGroup.put(groupIdx, runSTMKQueryBenchmark(groupSize));

        // 3. 时间范围查询
        timeRangeResultsByGroup.put(groupIdx, runTimeRangeQueryBenchmark(groupSize));

        // 4. 聚合查询
        aggResultsByGroup.put(groupIdx, runAggQueryBenchmark(groupSize));

        // 5. 波段查询
        bandResultsByGroup.put(groupIdx, runBandQueryBenchmark(groupSize));

        log(String.format("  [Benchmark] Group %d 查询基准测试完成", groupIdx + 1));
    }

    // ==================== 5种查询基准测试 ====================

    private List<QueryBenchmarkResult> runSimpleQueryBenchmark(int groupSize) {
        List<QueryBenchmarkResult> results = new ArrayList<>();
        List<long[]> targets = generateSimpleQueryTargets(Math.min(SIMPLE_QUERY_COUNT, loadedSourceIds.size()));
        log("    [Simple] 单天体查询 x " + targets.size());

        // LitecsDB
        {
            long t0 = System.nanoTime();
            for (long[] t : targets) {
                long sid = t[0];
                double ra = Double.longBitsToDouble(t[1]);
                double dec = Double.longBitsToDouble(t[2]);
                long hpid = mainNode.calculateHealpixId(ra, dec);
                mainNode.getLightCurve(hpid, sid, "G");
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("LitecsDB", "simple", groupSize, totalMs, targets.size()));
        }

        // NativeRocksDB
        {
            long t0 = System.nanoTime();
            for (long[] t : targets) {
                nativeRocksDB.queryBySourceId(t[0]);
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("NativeRocksDB", "simple", groupSize, totalMs, targets.size()));
        }

        // TDengine — 批量并行查询
        if (tdengine != null) {
            long t0 = System.nanoTime();
            tdengine.executeBatchSimpleQueries(targets);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("TDengine", "simple", groupSize, totalMs, targets.size()));
        }

        // InfluxDB — 批量并行查询
        if (influxDB != null) {
            long t0 = System.nanoTime();
            influxDB.executeBatchSimpleQueries(targets);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("InfluxDB", "simple", groupSize, totalMs, targets.size()));
        }

        return results;
    }

    private List<QueryBenchmarkResult> runSTMKQueryBenchmark(int groupSize) {
        List<QueryBenchmarkResult> results = new ArrayList<>();
        List<STMKQueryParams> queries = stmkParams;
        if (queries == null || queries.isEmpty()) return results;
        log("    [STMK] STMK查询 x " + queries.size());

        List<double[]> batchParams = new ArrayList<>();
        for (STMKQueryParams q : queries) {
            double bandCode = q.band.equals("G") ? 0 : q.band.equals("BP") ? 1 : 2;
            batchParams.add(new double[]{q.ra, q.dec, q.radius, q.startTime, q.endTime,
                    q.magMin, q.magMax, q.minObs, 0, bandCode});
        }

        // LitecsDB
        {
            long t0 = System.nanoTime();
            for (STMKQueryParams q : queries) {
                mainNode.executeSTMKQuery(q.ra, q.dec, q.radius, q.startTime, q.endTime,
                        q.band, q.magMin, q.magMax, q.minObs);
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("LitecsDB", "STMK", groupSize,
                    totalMs, queries.size()));
        }

        // NativeRocksDB
        {
            long t0 = System.nanoTime();
            for (STMKQueryParams q : queries) {
                nativeRocksDB.executeSTMKQuery(q.startTime, q.endTime, q.band,
                        q.magMin, q.magMax, q.ra, q.dec, q.radius, q.minObs);
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("NativeRocksDB", "STMK", groupSize,
                    totalMs, queries.size()));
        }

        // TDengine — 批量并行查询
        if (tdengine != null) {
            long t0 = System.nanoTime();
            tdengine.executeBatchSTMKQueries(batchParams);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("TDengine", "STMK", groupSize, totalMs, queries.size()));
        }

        // InfluxDB — 批量并行查询
        if (influxDB != null) {
            long t0 = System.nanoTime();
            influxDB.executeBatchSTMKQueries(batchParams);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("InfluxDB", "STMK", groupSize, totalMs, queries.size()));
        }

        return results;
    }

    private List<QueryBenchmarkResult> runTimeRangeQueryBenchmark(int groupSize) {
        List<QueryBenchmarkResult> results = new ArrayList<>();
        List<long[]> targets = generateSimpleQueryTargets(Math.min(SIMPLE_QUERY_COUNT, loadedSourceIds.size()));
        List<double[]> windows = timeRangeWindows;
        int count = Math.min(targets.size(), windows.size());
        log("    [TimeRange] 时间范围查询 x " + count);

        // LitecsDB
        {
            long t0 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                long sid = targets.get(i)[0];
                double ra = Double.longBitsToDouble(targets.get(i)[1]);
                double dec = Double.longBitsToDouble(targets.get(i)[2]);
                long hpid = mainNode.calculateHealpixId(ra, dec);
                mainNode.getLightCurve(hpid, sid, "G");
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("LitecsDB", "timeRange", groupSize, totalMs, count));
        }

        // NativeRocksDB
        {
            long t0 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                nativeRocksDB.queryBySourceIdTimeRange(targets.get(i)[0], windows.get(i)[0], windows.get(i)[1]);
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("NativeRocksDB", "timeRange", groupSize, totalMs, count));
        }

        // TDengine — 批量并行查询
        if (tdengine != null) {
            long t0 = System.nanoTime();
            tdengine.executeBatchTimeRangeQueries(targets, windows);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("TDengine", "timeRange", groupSize, totalMs, count));
        }

        // InfluxDB — 批量并行查询
        if (influxDB != null) {
            long t0 = System.nanoTime();
            influxDB.executeBatchTimeRangeQueries(targets, windows);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("InfluxDB", "timeRange", groupSize, totalMs, count));
        }

        return results;
    }

    private List<QueryBenchmarkResult> runAggQueryBenchmark(int groupSize) {
        List<QueryBenchmarkResult> results = new ArrayList<>();
        List<long[]> targets = generateSimpleQueryTargets(Math.min(SIMPLE_QUERY_COUNT, loadedSourceIds.size()));
        log("    [Agg] 聚合查询 x " + targets.size());

        // LitecsDB — 获取光变曲线后本地聚合
        {
            long t0 = System.nanoTime();
            for (long[] t : targets) {
                long sid = t[0];
                double ra = Double.longBitsToDouble(t[1]);
                double dec = Double.longBitsToDouble(t[2]);
                long hpid = mainNode.calculateHealpixId(ra, dec);
                List<LightCurvePoint> pts = mainNode.getLightCurve(hpid, sid, "G");
                if (pts != null && !pts.isEmpty()) {
                    double sum = 0, min = Double.MAX_VALUE, max = Double.MIN_VALUE;
                    for (LightCurvePoint p : pts) {
                        sum += p.mag; min = Math.min(min, p.mag); max = Math.max(max, p.mag);
                    }
                }
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("LitecsDB", "aggregation", groupSize, totalMs, targets.size()));
        }

        // NativeRocksDB
        {
            long t0 = System.nanoTime();
            for (long[] t : targets) {
                List<LightCurvePoint> pts = nativeRocksDB.queryBySourceIdFull(t[0]);
                if (pts != null && !pts.isEmpty()) {
                    double sum = 0, min = Double.MAX_VALUE, max = Double.MIN_VALUE;
                    for (LightCurvePoint p : pts) {
                        sum += p.mag; min = Math.min(min, p.mag); max = Math.max(max, p.mag);
                    }
                }
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("NativeRocksDB", "aggregation", groupSize, totalMs, targets.size()));
        }

        // TDengine — 批量并行查询
        if (tdengine != null) {
            long t0 = System.nanoTime();
            tdengine.executeBatchAggregationQueries(targets);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("TDengine", "aggregation", groupSize, totalMs, targets.size()));
        }

        // InfluxDB — 批量并行查询
        if (influxDB != null) {
            long t0 = System.nanoTime();
            influxDB.executeBatchAggregationQueries(targets);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("InfluxDB", "aggregation", groupSize, totalMs, targets.size()));
        }

        return results;
    }

    private List<QueryBenchmarkResult> runBandQueryBenchmark(int groupSize) {
        List<QueryBenchmarkResult> results = new ArrayList<>();
        List<long[]> targets = generateSimpleQueryTargets(Math.min(SIMPLE_QUERY_COUNT, loadedSourceIds.size()));
        List<String> bands = bandQueryBands;
        int count = Math.min(targets.size(), bands.size());
        log("    [Band] 波段查询 x " + count);

        // LitecsDB
        {
            long t0 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                long sid = targets.get(i)[0];
                double ra = Double.longBitsToDouble(targets.get(i)[1]);
                double dec = Double.longBitsToDouble(targets.get(i)[2]);
                long hpid = mainNode.calculateHealpixId(ra, dec);
                mainNode.getLightCurve(hpid, sid, bands.get(i));
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("LitecsDB", "band", groupSize, totalMs, count));
        }

        // NativeRocksDB
        {
            long t0 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                nativeRocksDB.queryBySourceIdBand(targets.get(i)[0], bands.get(i));
            }
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("NativeRocksDB", "band", groupSize, totalMs, count));
        }

        // TDengine — 批量并行查询
        if (tdengine != null) {
            long t0 = System.nanoTime();
            tdengine.executeBatchBandQueries(targets, bands);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("TDengine", "band", groupSize, totalMs, count));
        }

        // InfluxDB — 批量并行查询
        if (influxDB != null) {
            long t0 = System.nanoTime();
            influxDB.executeBatchBandQueries(targets, bands);
            double totalMs = (System.nanoTime() - t0) / 1e6;
            results.add(new QueryBenchmarkResult("InfluxDB", "band", groupSize, totalMs, count));
        }

        return results;
    }

    // ==================== 报告输出 ====================

    private void printGroupedSummaryReport() {
        log("\n========== Experiment 9 综合查询性能报告 ==========\n");
        String[] queryTypes = {"simple", "STMK", "timeRange", "aggregation", "band"};
        Map<String, Map<Integer, List<QueryBenchmarkResult>>> allMaps = new LinkedHashMap<>();
        allMaps.put("simple", simpleResultsByGroup);
        allMaps.put("STMK", stmkResultsByGroup);
        allMaps.put("timeRange", timeRangeResultsByGroup);
        allMaps.put("aggregation", aggResultsByGroup);
        allMaps.put("band", bandResultsByGroup);

        for (String qt : queryTypes) {
            Map<Integer, List<QueryBenchmarkResult>> groupMap = allMaps.get(qt);
            if (groupMap == null || groupMap.isEmpty()) continue;
            log(String.format("--- 查询类型: %s ---", qt));
            log(String.format("%-14s %-10s %-12s %-10s %-10s",
                    "System", "GroupSize", "AvgLatency", "TotalMs", "QPS"));
            for (Map.Entry<Integer, List<QueryBenchmarkResult>> entry : groupMap.entrySet()) {
                for (QueryBenchmarkResult r : entry.getValue()) {
                    log(String.format("%-14s %-10d %-12.2f %-10.2f %-10.2f",
                            r.system, r.groupSize, r.avgLatencyMs,
                            r.totalMs, r.qps));
                }
            }
            log("");
        }
    }

    private void exportGroupedCSV() {
        String csvFile = RESULT_DIR + "experiment9_query_results.csv";
        log("[Export] 导出CSV: " + csvFile);
        try (PrintWriter pw = new PrintWriter(new BufferedWriter(
                new FileWriter(csvFile)))) {
            pw.println("system,queryType,groupSize,avgLatencyMs,queryCount,totalMs,qps");
            Map<String, Map<Integer, List<QueryBenchmarkResult>>> allMaps
                    = new LinkedHashMap<>();
            allMaps.put("simple", simpleResultsByGroup);
            allMaps.put("STMK", stmkResultsByGroup);
            allMaps.put("timeRange", timeRangeResultsByGroup);
            allMaps.put("aggregation", aggResultsByGroup);
            allMaps.put("band", bandResultsByGroup);
            for (Map.Entry<String, Map<Integer, List<QueryBenchmarkResult>>> e
                    : allMaps.entrySet()) {
                if (e.getValue() == null) continue;
                for (Map.Entry<Integer, List<QueryBenchmarkResult>> ge
                        : e.getValue().entrySet()) {
                    for (QueryBenchmarkResult r : ge.getValue()) {
                        pw.printf("%s,%s,%d,%.2f,%d,%.2f,%.2f%n",
                                r.system, r.queryType, r.groupSize,
                                r.avgLatencyMs, r.queryCount, r.totalMs, r.qps);
                    }
                }
            }
        } catch (IOException e) {
            log("[ERROR] CSV导出失败: " + e.getMessage());
        }
    }
}