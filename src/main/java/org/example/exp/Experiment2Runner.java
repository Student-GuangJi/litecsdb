package org.example.exp;

import org.example.MainNode;
import org.example.RocksDBServer.*;
import org.example.MainNode.*;

import java.io.*;
import java.util.*;

/**
 * 实验2：λ 参数敏感性分析
 *
 * 实验设计：
 *   自变量: λ ∈ {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9}
 *   因变量:
 *     - 桶数量、每对象平均桶数
 *     - 桶内星等方差
 *     - STMK 查询延迟
 *     - 剪枝率 = (CONFIRMED + PRUNED) / 总查询对象数
 *     - 精炼率 = AMBIGUOUS / 总查询对象数
 *
 * 修改说明：
 *   对三种查询类型 Q-Narrow、Q-Medium、Q-Wide，各生成 10 个测试用例，
 *   每个测试用例使用不同的随机参数（查询中心、时间窗口等），最后取平均值。
 *
 * 全链路调用 MainNode → StorageNode → RocksDBServer，确保实验真实性。
 */
public class Experiment2Runner {

    // ==================== 实验配置 ====================
    private static final int WARMUP_ROUNDS = 0;
    private static final int REPEAT_TIMES = 1;

    // 数据集
    private static final String DEFAULT_COORD_FILE = "gaiadr2/source_coordinates.csv";
    private static final String DEFAULT_BATCH_DIR = "gaiadr2/individual_lightcurves/";

    // λ 候选值
    private static final double[] LAMBDA_CANDIDATES = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};

    // 固定 storeCost（可通过命令行覆盖）
    private static double STORE_COST = 40;

    // 每种查询类型生成的测试用例数量
    private static final int TEST_CASES_PER_TYPE = 10;

    // 每个测试用例使用的查询中心点数量
    private static final int QUERY_CENTERS_PER_CASE = 5;

    // 系统配置
    private static final int NODES_COUNT = 2;
    private static final int HEALPIX_LEVEL = 1;
    private static final String DB_BASE_PATH = "LitecsDB_Exp2";

    // 随机数生成器（固定种子确保可重复性）
    private static final Random RNG = new Random(42);

    // 查询类型基础配置
    private static class QueryTypeConfig {
        String name;
        // 半径范围 [min, max]
        double radiusMin, radiusMax;
        // 时间窗口长度范围 [min, max]（天）
        double timeWindowMin, timeWindowMax;
        // 星等范围
        double magMin, magMax;
        // minK 范围 [min, max]
        int minKMin, minKMax;

        QueryTypeConfig(String name,
                        double radiusMin, double radiusMax,
                        double timeWindowMin, double timeWindowMax,
                        double magMin, double magMax,
                        int minKMin, int minKMax) {
            this.name = name;
            this.radiusMin = radiusMin;
            this.radiusMax = radiusMax;
            this.timeWindowMin = timeWindowMin;
            this.timeWindowMax = timeWindowMax;
            this.magMin = magMin;
            this.magMax = magMax;
            this.minKMin = minKMin;
            this.minKMax = minKMax;
        }
    }

    // 三种查询类型的参数范围配置
    private static final QueryTypeConfig[] QUERY_TYPE_CONFIGS = {
            // Q-Narrow: 小半径、短时间窗口、高minK（严格查询）
            new QueryTypeConfig("Q-Narrow",
                    0.5, 2.0,      // radius: 0.5° ~ 2°
                    3.0, 7.0,      // timeWindow: 3 ~ 7 天
                    10.0, 18.0,    // mag: 10 ~ 18
                    3, 5),         // minK: 3 ~ 5

            // Q-Medium: 中等半径、中等时间窗口、中等minK
            new QueryTypeConfig("Q-Medium",
                    3.0, 7.0,      // radius: 3° ~ 7°
                    7.0, 15.0,     // timeWindow: 7 ~ 15 天
                    10.0, 18.0,    // mag: 10 ~ 18
                    2, 4),         // minK: 2 ~ 4

            // Q-Wide: 大半径、长时间窗口、低minK（宽松查询）
            new QueryTypeConfig("Q-Wide",
                    8.0, 15.0,     // radius: 8° ~ 15°
                    15.0, 30.0,    // timeWindow: 15 ~ 30 天
                    10.0, 18.0,    // mag: 10 ~ 18
                    1, 3),         // minK: 1 ~ 3
    };

    // 数据的时间范围（不要修改）
    private static final double DATA_TIME_START = 1800.0;
    private static final double DATA_TIME_END = 1900.0;

    // 单个测试用例的具体参数
    private static class QueryTestCase {
        String typeName;
        int caseId;
        double radius;
        double timeStart, timeEnd;
        double magMin, magMax;
        int minK;
        List<double[]> queryCenters;  // 查询中心点列表

        QueryTestCase(String typeName, int caseId) {
            this.typeName = typeName;
            this.caseId = caseId;
            this.queryCenters = new ArrayList<>();
        }

        @Override
        public String toString() {
            return String.format("%s-Case%d(r=%.1f, T=[%.0f,%.0f], K=%d)",
                    typeName, caseId, radius, timeStart, timeEnd, minK);
        }
    }

    // ==================== 实验结果 ====================
    private static class Exp2Result {
        double lambda;
        long totalBuckets;
        double bucketsPerObject;
        double avgBucketSize;
        double avgIntraBucketVariance;

        // 每种查询类型的平均结果（Q-Narrow, Q-Medium, Q-Wide）
        double[] avgQueryTimeMs;      // 10个测试用例的平均查询时间
        double[] stdQueryTimeMs;      // 10个测试用例的标准差
        double[] avgPruningRates;     // 平均剪枝率
        double[] avgRefinementRates;  // 平均精炼率
        double[] avgResultCounts;     // 平均结果数

        // 每个测试用例的详细结果（用于分析）
        double[][] caseQueryTimeMs;   // [queryType][caseId]
        double[][] casePruningRates;
        double[][] caseRefinementRates;
        int[][] caseResultCounts;
    }

    // ==================== 查询统计 ====================
    private static class QueryStats {
        int totalObjects = 0;
        int confirmedCount = 0;
        int prunedCount = 0;
        int ambiguousCount = 0;
        int resultCount = 0;
        double queryTimeMs = 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("  实验2：λ 参数敏感性分析（多测试用例版本）");
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.printf("  每种查询类型生成 %d 个测试用例，取平均值%n", TEST_CASES_PER_TYPE);

        // 解析命令行参数
        String coordFile = DEFAULT_COORD_FILE;
        String batchDir = DEFAULT_BATCH_DIR;
        String datasetSize = null;
        String baseDir = "generated_datasets";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--coordFile": case "-c": coordFile = args[++i]; break;
                case "--batchDir": case "-b": batchDir = args[++i]; break;
                case "--size": case "-s": datasetSize = args[++i]; break;
                case "--baseDir": case "-d": baseDir = args[++i]; break;
                case "--storeCost": STORE_COST = Double.parseDouble(args[++i]); break;
            }
        }

        if (datasetSize != null) {
            String batchRoot = baseDir + "/batch_" + datasetSize;
            batchDir = batchRoot + "/individual_lightcurves/";
            File coordCandidate = new File(batchRoot + "/source_coordinates.csv");
            if (coordCandidate.exists()) coordFile = coordCandidate.getPath();
        }

        // 预加载数据
        System.out.println("\n>>> 预加载数据集...");
        List<String> allCsvLines = preloadData(coordFile, batchDir);
        System.out.printf("  数据加载完成: %d 条记录%n", allCsvLines.size());

        // 加载所有坐标点（用于生成查询中心）
        List<double[]> allCoords = loadAllCoordinates(coordFile);
        System.out.printf("  坐标点数量: %d%n", allCoords.size());

        // 为每种查询类型生成 10 个测试用例
        Map<String, List<QueryTestCase>> testCasesByType = generateAllTestCases(allCoords);

        // 打印生成的测试用例概览
        printTestCasesOverview(testCasesByType);

        String timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String csvFile = "exp2_lambda_sensitivity_" + timestamp + ".csv";
        String detailCsvFile = "exp2_lambda_sensitivity_detail_" + timestamp + ".csv";
        String summaryFile = "exp2_lambda_sensitivity_summary_" + timestamp + ".txt";

        List<Exp2Result> allResults = new ArrayList<>();

        // ==================== 遍历 λ ====================
        for (double lambda : LAMBDA_CANDIDATES) {
            System.out.printf("%n━━━━━━━━━━━ λ=%.1f (storeCost=%.1f) ━━━━━━━━━━━%n", lambda, STORE_COST);

            Exp2Result result = runSingleLambda(allCsvLines, testCasesByType, lambda);
            allResults.add(result);

            System.out.printf("  桶数=%d | 桶/对象=%.1f | 桶均大小=%.1f | 桶内方差=%.4f%n",
                    result.totalBuckets, result.bucketsPerObject,
                    result.avgBucketSize, result.avgIntraBucketVariance);

            for (int q = 0; q < QUERY_TYPE_CONFIGS.length; q++) {
                System.out.printf("  %s (avg of %d cases): 查询=%.2f±%.2fms | 剪枝率=%.1f%% | 精炼率=%.1f%% | 结果数=%.1f%n",
                        QUERY_TYPE_CONFIGS[q].name,
                        TEST_CASES_PER_TYPE,
                        result.avgQueryTimeMs[q],
                        result.stdQueryTimeMs[q],
                        result.avgPruningRates[q] * 100,
                        result.avgRefinementRates[q] * 100,
                        result.avgResultCounts[q]);
            }
        }

        // ==================== 输出 ====================
        writeCSV(csvFile, allResults);
        writeDetailCSV(detailCsvFile, allResults);
        writeSummary(summaryFile, allResults, testCasesByType);

        System.out.printf("%n══════════════════════════════════════════════════════════%n");
        System.out.printf("  实验2完成！storeCost=%.1f%n", STORE_COST);
        System.out.printf("  汇总CSV: %s%n", csvFile);
        System.out.printf("  详细CSV: %s%n", detailCsvFile);
        System.out.printf("  摘要: %s%n", summaryFile);
        System.out.printf("══════════════════════════════════════════════════════════%n");
    }

    // ==================== 生成测试用例 ====================

    /**
     * 为每种查询类型生成 TEST_CASES_PER_TYPE 个测试用例
     */
    private static Map<String, List<QueryTestCase>> generateAllTestCases(List<double[]> allCoords) {
        Map<String, List<QueryTestCase>> testCasesByType = new LinkedHashMap<>();

        for (QueryTypeConfig config : QUERY_TYPE_CONFIGS) {
            List<QueryTestCase> cases = new ArrayList<>();

            for (int caseId = 0; caseId < TEST_CASES_PER_TYPE; caseId++) {
                QueryTestCase tc = new QueryTestCase(config.name, caseId);

                // 随机生成查询参数
                tc.radius = randomInRange(config.radiusMin, config.radiusMax);

                // 随机时间窗口
                double timeWindow = randomInRange(config.timeWindowMin, config.timeWindowMax);
                tc.timeStart = randomInRange(DATA_TIME_START, DATA_TIME_END - timeWindow);
                tc.timeEnd = tc.timeStart + timeWindow;

                // 星等范围（固定）
                tc.magMin = config.magMin;
                tc.magMax = config.magMax;

                // 随机 minK
                tc.minK = randomIntInRange(config.minKMin, config.minKMax);

                // 随机选择查询中心点
                for (int i = 0; i < QUERY_CENTERS_PER_CASE; i++) {
                    int idx = RNG.nextInt(allCoords.size());
                    tc.queryCenters.add(allCoords.get(idx));
                }

                cases.add(tc);
            }

            testCasesByType.put(config.name, cases);
        }

        return testCasesByType;
    }

    private static double randomInRange(double min, double max) {
        return min + RNG.nextDouble() * (max - min);
    }

    private static int randomIntInRange(int min, int max) {
        return min + RNG.nextInt(max - min + 1);
    }

    private static void printTestCasesOverview(Map<String, List<QueryTestCase>> testCasesByType) {
        System.out.println("\n>>> 生成的测试用例概览:");
        for (Map.Entry<String, List<QueryTestCase>> entry : testCasesByType.entrySet()) {
            System.out.printf("  %s: %d 个测试用例%n", entry.getKey(), entry.getValue().size());
            // 打印前3个作为示例
            for (int i = 0; i < Math.min(3, entry.getValue().size()); i++) {
                QueryTestCase tc = entry.getValue().get(i);
                System.out.printf("    Case%d: r=%.2f°, T=[%.1f,%.1f], K=%d, centers=%d%n",
                        i, tc.radius, tc.timeStart, tc.timeEnd, tc.minK, tc.queryCenters.size());
            }
            if (entry.getValue().size() > 3) {
                System.out.printf("    ... 还有 %d 个测试用例%n", entry.getValue().size() - 3);
            }
        }
    }

    // ==================== 核心实验方法 ====================

    private static Exp2Result runSingleLambda(
            List<String> allCsvLines,
            Map<String, List<QueryTestCase>> testCasesByType,
            double lambda) throws Exception {

        Exp2Result result = new Exp2Result();
        result.lambda = lambda;

        int numTypes = QUERY_TYPE_CONFIGS.length;
        result.avgQueryTimeMs = new double[numTypes];
        result.stdQueryTimeMs = new double[numTypes];
        result.avgPruningRates = new double[numTypes];
        result.avgRefinementRates = new double[numTypes];
        result.avgResultCounts = new double[numTypes];

        result.caseQueryTimeMs = new double[numTypes][TEST_CASES_PER_TYPE];
        result.casePruningRates = new double[numTypes][TEST_CASES_PER_TYPE];
        result.caseRefinementRates = new double[numTypes][TEST_CASES_PER_TYPE];
        result.caseResultCounts = new int[numTypes][TEST_CASES_PER_TYPE];

        String dbPath = String.format("%s/lambda_%.1f", DB_BASE_PATH, lambda);
        MainNode system = new MainNode(NODES_COUNT, HEALPIX_LEVEL, dbPath);

        try {
            // 1. 写入数据
            system.preCreateHealpixDatabases(allCsvLines);
            Map<Long, List<LightCurvePoint>> healpixDataMap = system.preParseData(allCsvLines);
            system.distributePreParsedData(healpixDataMap);

            // 2. 设置 DP 参数并构建时间桶索引
            system.setTimeBucketDPParams(lambda, STORE_COST);
            system.buildAllTimeBucketsOffline();

            // 3. 收集桶统计
            DetailedBucketStats bucketStats = system.collectDetailedTimeBucketStats();
            result.totalBuckets = bucketStats.totalBuckets;
            result.bucketsPerObject = bucketStats.bucketsPerObject;
            result.avgBucketSize = bucketStats.avgBucketSize;
            result.avgIntraBucketVariance = bucketStats.avgIntraBucketVariance;

            // 4. 对每种查询类型的每个测试用例执行查询
            int typeIdx = 0;
            for (QueryTypeConfig config : QUERY_TYPE_CONFIGS) {
                List<QueryTestCase> cases = testCasesByType.get(config.name);

                List<Double> allCaseQueryTimes = new ArrayList<>();
                List<Double> allCasePruningRates = new ArrayList<>();
                List<Double> allCaseRefinementRates = new ArrayList<>();
                List<Integer> allCaseResultCounts = new ArrayList<>();

                for (int caseId = 0; caseId < cases.size(); caseId++) {
                    QueryTestCase tc = cases.get(caseId);

                    // 执行该测试用例的所有查询
                    QueryStats caseStats = runTestCase(system, tc);

                    // 记录该测试用例的结果
                    double caseQueryTime = caseStats.queryTimeMs;
                    double casePruningRate = caseStats.totalObjects > 0
                            ? (double)(caseStats.confirmedCount + caseStats.prunedCount) / caseStats.totalObjects
                            : 0;
                    double caseRefinementRate = caseStats.totalObjects > 0
                            ? (double) caseStats.ambiguousCount / caseStats.totalObjects
                            : 0;
                    int caseResultCount = caseStats.resultCount;

                    result.caseQueryTimeMs[typeIdx][caseId] = caseQueryTime;
                    result.casePruningRates[typeIdx][caseId] = casePruningRate;
                    result.caseRefinementRates[typeIdx][caseId] = caseRefinementRate;
                    result.caseResultCounts[typeIdx][caseId] = caseResultCount;

                    allCaseQueryTimes.add(caseQueryTime);
                    allCasePruningRates.add(casePruningRate);
                    allCaseRefinementRates.add(caseRefinementRate);
                    allCaseResultCounts.add(caseResultCount);
                }

                // 计算该查询类型的平均值和标准差
                result.avgQueryTimeMs[typeIdx] = average(allCaseQueryTimes);
                result.stdQueryTimeMs[typeIdx] = stdDev(allCaseQueryTimes);
                result.avgPruningRates[typeIdx] = average(allCasePruningRates);
                result.avgRefinementRates[typeIdx] = average(allCaseRefinementRates);
                result.avgResultCounts[typeIdx] = allCaseResultCounts.stream()
                        .mapToInt(Integer::intValue).average().orElse(0);

                typeIdx++;
            }

        } finally {
            system.shutdown();
            deleteDir(new File(dbPath));
        }

        return result;
    }

    /**
     * 执行单个测试用例的所有查询
     */
    private static QueryStats runTestCase(MainNode system, QueryTestCase tc) {
        QueryStats stats = new QueryStats();

        int totalRounds = WARMUP_ROUNDS + REPEAT_TIMES;
        List<Double> queryTimes = new ArrayList<>();

        // 执行查询（warmup + repeat）
        for (int round = 0; round < totalRounds; round++) {
            boolean warmup = round < WARMUP_ROUNDS;
            long roundStart = System.currentTimeMillis();

            for (double[] center : tc.queryCenters) {
                system.executeSTMKQuery(
                        center[0], center[1], tc.radius,
                        tc.timeStart, tc.timeEnd,
                        "G", tc.magMin, tc.magMax, tc.minK);
            }

            long roundEnd = System.currentTimeMillis();
            if (!warmup) {
                queryTimes.add((double)(roundEnd - roundStart));
            }
        }

        stats.queryTimeMs = queryTimes.stream().mapToDouble(d -> d).average().orElse(0);

        // 剪枝统计
        for (double[] center : tc.queryCenters) {
            DetailedSTMKResult detailed = system.executeSTMKQueryDetailed(
                    center[0], center[1], tc.radius,
                    tc.timeStart, tc.timeEnd,
                    "G", tc.magMin, tc.magMax, tc.minK);
            stats.totalObjects += detailed.totalObjectsEvaluated;
            stats.confirmedCount += detailed.confirmedCount;
            stats.prunedCount += detailed.prunedCount;
            stats.ambiguousCount += detailed.ambiguousCount;
            stats.resultCount += detailed.resultCount;
        }

        return stats;
    }

    // ==================== 统计辅助方法 ====================

    private static double average(List<Double> values) {
        return values.stream().mapToDouble(d -> d).average().orElse(0);
    }

    private static double stdDev(List<Double> values) {
        if (values.size() <= 1) return 0;
        double mean = average(values);
        double variance = values.stream()
                .mapToDouble(d -> (d - mean) * (d - mean))
                .sum() / (values.size() - 1);
        return Math.sqrt(variance);
    }

    // ==================== 数据预加载 ====================

    private static List<String> preloadData(String coordFile, String lcDir) throws Exception {
        Map<Long, String> coordsMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(coordFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals("source_id")) continue;
                coordsMap.put(Long.parseLong(parts[0]), parts[1] + "," + parts[2]);
            }
        }

        List<String> allLines = new ArrayList<>();
        File dir = new File(lcDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".csv"));
        if (files != null) {
            Arrays.sort(files, Comparator.comparing(File::getName));
            for (File file : files) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split(",");
                        if (parts[0].equals("source_id")) continue;
                        long sourceId = Long.parseLong(parts[0]);
                        String coords = coordsMap.getOrDefault(sourceId, "0.0,0.0");
                        allLines.add(parts[0] + "," + coords + "," +
                                String.join(",", Arrays.copyOfRange(parts, 1, parts.length)));
                    }
                }
            }
        }
        return allLines;
    }

    private static List<double[]> loadAllCoordinates(String coordFile) throws Exception {
        List<double[]> allCoords = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(coordFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals("source_id")) continue;
                allCoords.add(new double[]{Double.parseDouble(parts[1]), Double.parseDouble(parts[2])});
            }
        }
        return allCoords;
    }

    // ==================== 输出 ====================

    /**
     * 输出汇总CSV（每种查询类型的平均值）
     */
    private static void writeCSV(String path, List<Exp2Result> results) throws Exception {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            // 表头
            StringBuilder header = new StringBuilder();
            header.append("lambda,total_buckets,buckets_per_object,avg_bucket_size,avg_intra_bucket_variance");
            for (QueryTypeConfig config : QUERY_TYPE_CONFIGS) {
                String tag = config.name;
                header.append(",").append(tag).append("_avg_query_ms");
                header.append(",").append(tag).append("_std_query_ms");
                header.append(",").append(tag).append("_avg_pruning_rate");
                header.append(",").append(tag).append("_avg_refinement_rate");
                header.append(",").append(tag).append("_avg_result_count");
            }
            pw.println(header);

            // 数据行
            for (Exp2Result r : results) {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format(Locale.US, "%.1f,%d,%.2f,%.2f,%.6f",
                        r.lambda, r.totalBuckets, r.bucketsPerObject,
                        r.avgBucketSize, r.avgIntraBucketVariance));
                for (int q = 0; q < QUERY_TYPE_CONFIGS.length; q++) {
                    sb.append(String.format(Locale.US, ",%.2f,%.2f,%.4f,%.4f,%.1f",
                            r.avgQueryTimeMs[q], r.stdQueryTimeMs[q],
                            r.avgPruningRates[q], r.avgRefinementRates[q],
                            r.avgResultCounts[q]));
                }
                pw.println(sb);
            }
        }
    }

    /**
     * 输出详细CSV（每个测试用例的结果）
     */
    private static void writeDetailCSV(String path, List<Exp2Result> results) throws Exception {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            // 表头
            pw.println("lambda,query_type,case_id,query_ms,pruning_rate,refinement_rate,result_count");

            // 数据行
            for (Exp2Result r : results) {
                for (int q = 0; q < QUERY_TYPE_CONFIGS.length; q++) {
                    for (int c = 0; c < TEST_CASES_PER_TYPE; c++) {
                        pw.printf(Locale.US, "%.1f,%s,%d,%.2f,%.4f,%.4f,%d%n",
                                r.lambda,
                                QUERY_TYPE_CONFIGS[q].name,
                                c,
                                r.caseQueryTimeMs[q][c],
                                r.casePruningRates[q][c],
                                r.caseRefinementRates[q][c],
                                r.caseResultCounts[q][c]);
                    }
                }
            }
        }
    }

    private static void writeSummary(String path, List<Exp2Result> results,
                                     Map<String, List<QueryTestCase>> testCasesByType) throws Exception {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            pw.println("══════════════════════════════════════════════════════════════════════════════════════════════════════");
            pw.printf("  实验2：λ 参数敏感性分析 — 摘要报告 (storeCost=%.1f)%n", STORE_COST);
            pw.printf("  每种查询类型 %d 个测试用例，每个用例 %d 个查询中心点%n",
                    TEST_CASES_PER_TYPE, QUERY_CENTERS_PER_CASE);
            pw.println("══════════════════════════════════════════════════════════════════════════════════════════════════════");

            // 测试用例参数概览
            pw.println("\n── 测试用例参数范围 ──");
            for (QueryTypeConfig config : QUERY_TYPE_CONFIGS) {
                pw.printf("  %s: r∈[%.1f,%.1f]°, ΔT∈[%.0f,%.0f]d, K∈[%d,%d]%n",
                        config.name,
                        config.radiusMin, config.radiusMax,
                        config.timeWindowMin, config.timeWindowMax,
                        config.minKMin, config.minKMax);
            }

            // 桶统计表
            pw.println("\n── 索引质量指标 ──");
            pw.printf("%-6s %10s %10s %10s %12s%n", "λ", "桶数", "桶/对象", "桶均大小", "桶内方差");
            pw.println("─".repeat(56));
            for (Exp2Result r : results) {
                pw.printf(Locale.US, "%-6.1f %10d %10.1f %10.1f %12.4f%n",
                        r.lambda, r.totalBuckets, r.bucketsPerObject,
                        r.avgBucketSize, r.avgIntraBucketVariance);
            }

            // 每种查询类型的性能表
            for (int q = 0; q < QUERY_TYPE_CONFIGS.length; q++) {
                QueryTypeConfig config = QUERY_TYPE_CONFIGS[q];
                pw.printf("%n── %s (r∈[%.1f,%.1f]°, ΔT∈[%.0f,%.0f]d, K∈[%d,%d]) ── [%d 测试用例平均]%n",
                        config.name,
                        config.radiusMin, config.radiusMax,
                        config.timeWindowMin, config.timeWindowMax,
                        config.minKMin, config.minKMax,
                        TEST_CASES_PER_TYPE);
                pw.printf("%-6s %14s %10s %10s %12s%n",
                        "λ", "查询(ms)", "剪枝率", "精炼率", "平均结果数");
                pw.println("─".repeat(60));
                for (Exp2Result r : results) {
                    pw.printf(Locale.US, "%-6.1f %8.2f±%-5.2f %9.1f%% %9.1f%% %12.1f%n",
                            r.lambda,
                            r.avgQueryTimeMs[q], r.stdQueryTimeMs[q],
                            r.avgPruningRates[q] * 100,
                            r.avgRefinementRates[q] * 100,
                            r.avgResultCounts[q]);
                }
            }

            // 找最优 λ
            pw.println("\n── 最优 λ 分析 ──");
            for (int q = 0; q < QUERY_TYPE_CONFIGS.length; q++) {
                double bestLambda = 0;
                double bestTime = Double.MAX_VALUE;
                for (Exp2Result r : results) {
                    if (r.avgQueryTimeMs[q] < bestTime) {
                        bestTime = r.avgQueryTimeMs[q];
                        bestLambda = r.lambda;
                    }
                }
                double finalBestLambda = bestLambda;
                int finalQ = q;
                pw.printf("  %s 最优 λ = %.1f (平均查询延迟 = %.2f±%.2f ms)%n",
                        QUERY_TYPE_CONFIGS[q].name, bestLambda, bestTime,
                        results.stream()
                                .filter(r -> r.lambda == finalBestLambda)
                                .findFirst()
                                .map(r -> r.stdQueryTimeMs[finalQ])
                                .orElse(0.0));
            }
        }
    }

    private static void deleteDir(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) deleteDir(child);
            }
        }
        dir.delete();
    }
}