package org.example.exp;

import org.example.MainNode;
import org.example.RocksDBServer.*;
import org.example.MainNode.*;

import java.io.*;
import java.util.*;

/**
 * 实验3：自适应时间桶参数敏感性分析
 *
 * 实验设计：
 *   自变量1: storeCost ∈ {0.1, 1, 10, 100, 1000}  — 先扫描找到 λ 敏感的 storeCost
 *   自变量2: λ ∈ {0.1, 0.2, ..., 0.9}
 *   因变量:
 *     - 桶数量、桶/对象、桶均大小
 *     - 桶内方差（衡量桶内信息混杂度）
 *     - STMK 查询时间
 *     - 剪枝率 = (CONFIRMED + PRUNED) / 总查询对象数
 *     - 精炼率 = AMBIGUOUS / 总查询对象数
 *
 * 全链路调用 MainNode → StorageNode → RocksDBServer，确保实验真实性。
 */
public class Experiment3Runner {

    // ==================== 实验配置 ====================
    private static final int WARMUP_ROUNDS = 0;
    private static final int REPEAT_TIMES = 1;

    // 数据集
    private static final String DEFAULT_COORD_FILE = "gaiadr2/source_coordinates.csv";
    private static final String DEFAULT_BATCH_DIR = "gaiadr2/individual_lightcurves/";
    // storeCost 候选值（先扫描）
    private static final double[] STORE_COST_CANDIDATES = {36, 37, 38, 39, 40, 41, 42};
    // λ 候选值
    private static final double[] LAMBDA_CANDIDATES = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};

    // STMK 查询参数
    private static final int NUM_QUERIES = 50;
    private static final double QUERY_RADIUS = 1.0;
    private static final double QUERY_TIME_START = 1800.0;
    private static final double QUERY_TIME_END = 1900.0;
    private static final String QUERY_BAND = "G";
    private static final double QUERY_MAG_MIN = 10.0;
    private static final double QUERY_MAG_MAX = 18.0;
    private static final int QUERY_MIN_K = 3;

    // 系统配置
    private static final int NODES_COUNT = 2;
    private static final int HEALPIX_LEVEL = 1;
    private static final String DB_BASE_PATH = "LitecsDB_Exp3";

    // ==================== 查询统计 ====================
    private static class QueryStats {
        int totalObjects = 0;      // 总查询对象数
        int confirmedCount = 0;    // 桶级直接确认（不回表）
        int prunedCount = 0;       // 桶级直接剪枝（不回表）
        int ambiguousCount = 0;    // 需要精炼（回表）
        int resultCount = 0;       // 最终结果数
        double queryTimeMs = 0;
    }

    // ==================== 实验结果 ====================
    private static class ExpResult {
        double storeCost;
        double lambda;
        long totalBuckets;
        double bucketsPerObject;
        double avgBucketSize;
        double avgIntraBucketVariance;
        double avgQueryTimeMs;
        double stdQueryTimeMs;
        double pruningRate;     // (confirmed + pruned) / total
        double refinementRate;  // ambiguous / total
        int avgResultCount;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("  实验3：自适应时间桶参数敏感性分析");
        System.out.println("  Phase 1: 扫描 storeCost 找到 λ 敏感区间");
        System.out.println("  Phase 2: 在最优 storeCost 下精细扫描 λ");
        System.out.println("═══════════════════════════════════════════════════════════");

        // 解析命令行参数
        String coordFile = DEFAULT_COORD_FILE;
        String batchDir = DEFAULT_BATCH_DIR;
        String datasetSize = "8000000";
        String baseDir = "generated_datasets";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--coordFile": case "-c": coordFile = args[++i]; break;
                case "--batchDir": case "-b": batchDir = args[++i]; break;
                case "--size": case "-s": datasetSize = args[++i]; break;
                case "--baseDir": case "-d": baseDir = args[++i]; break;
            }
        }

        if (datasetSize != null) {
            String batchRoot = baseDir + "/batch_" + datasetSize;
            batchDir = batchRoot + "/individual_lightcurves/";
            File coordCandidate = new File(batchRoot + "/source_coordinates.csv");
            if (coordCandidate.exists()) coordFile = coordCandidate.getPath();
        }

        // 预加载数据到内存（只解析一次，后续所有实验复用）
        System.out.println("\n>>> 预加载数据集...");
        List<String> allCsvLines = preloadData(coordFile, batchDir);
        System.out.printf("  数据加载完成: %d 条记录%n", allCsvLines.size());

        // 生成查询点（基于实际数据分布）
        List<double[]> queryPoints = generateQueryPoints(coordFile, NUM_QUERIES);

        String timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String csvFile = "exp3_sensitivity_" + timestamp + ".csv";
        String summaryFile = "exp3_sensitivity_summary_" + timestamp + ".txt";

        List<ExpResult> allResults = new ArrayList<>();

        // ==================== Phase 1: storeCost 扫描 ====================
        System.out.println("\n══════ Phase 1: 扫描 storeCost (λ 固定为 0.5) ══════");
        double bestStoreCost = 10.0;
        double maxLambdaSensitivity = 0;

        for (double sc : STORE_COST_CANDIDATES) {
            // 用 λ=0.3 和 λ=0.7 测量敏感性差异
            ExpResult r03 = runSingleExperiment(allCsvLines, queryPoints, sc, 0.3, "sc_scan");
            ExpResult r07 = runSingleExperiment(allCsvLines, queryPoints, sc, 0.7, "sc_scan");

            double bucketDiff = Math.abs(r03.totalBuckets - r07.totalBuckets);
            double pruningDiff = Math.abs(r03.pruningRate - r07.pruningRate);
            double queryDiff = Math.abs(r03.avgQueryTimeMs - r07.avgQueryTimeMs);

            // 综合敏感性指标：桶数变化率 + 剪枝率变化 + 查询时间变化率
            double sensitivity = bucketDiff / Math.max(1, (r03.totalBuckets + r07.totalBuckets) / 2.0)
                    + pruningDiff * 10
                    + queryDiff / Math.max(1, (r03.avgQueryTimeMs + r07.avgQueryTimeMs) / 2.0);

            System.out.printf("  storeCost=%7.2f | λ=0.3: 桶=%d, 剪枝=%.1f%% | λ=0.7: 桶=%d, 剪枝=%.1f%% | 敏感性=%.4f%n",
                    sc, r03.totalBuckets, r03.pruningRate * 100, r07.totalBuckets, r07.pruningRate * 100, sensitivity);

            if (sensitivity > maxLambdaSensitivity) {
                maxLambdaSensitivity = sensitivity;
                bestStoreCost = sc;
            }
        }
        System.out.printf("\n  >>> 最优 storeCost = %.2f (敏感性=%.4f)%n", bestStoreCost, maxLambdaSensitivity);

        // ==================== Phase 2: λ 精细扫描 ====================
        System.out.printf("\n══════ Phase 2: storeCost=%.2f, 精细扫描 λ ══════%n", bestStoreCost);

        for (double lambda : LAMBDA_CANDIDATES) {
            System.out.printf("%n━━━━━━━━━━━ storeCost=%.2f, λ=%.1f ━━━━━━━━━━━%n", bestStoreCost, lambda);
            ExpResult result = runSingleExperiment(allCsvLines, queryPoints, bestStoreCost, lambda, "main");
            allResults.add(result);

            System.out.printf("  λ=%.1f | 桶数=%d | 桶/对象=%.1f | 桶均大小=%.1f | 桶内方差=%.4f%n",
                    lambda, result.totalBuckets, result.bucketsPerObject, result.avgBucketSize, result.avgIntraBucketVariance);
            System.out.printf("       | 查询=%.2f±%.2f ms | 剪枝率=%.1f%% | 精炼率=%.1f%% | 结果数=%d%n",
                    result.avgQueryTimeMs, result.stdQueryTimeMs,
                    result.pruningRate * 100, result.refinementRate * 100, result.avgResultCount);
        }

        // ==================== 输出 CSV ====================
        writeCSV(csvFile, allResults);
        writeSummary(summaryFile, allResults, bestStoreCost);

        System.out.printf("%n══════════════════════════════════════════════════════════%n");
        System.out.printf("  实验3完成！最优 storeCost=%.2f%n", bestStoreCost);
        System.out.printf("  CSV: %s%n", csvFile);
        System.out.printf("  摘要: %s%n", summaryFile);
        System.out.printf("══════════════════════════════════════════════════════════%n");
    }

    // ==================== 核心实验方法 ====================

    private static ExpResult runSingleExperiment(
            List<String> allCsvLines, List<double[]> queryPoints,
            double storeCost, double lambda, String tag) throws Exception {

        ExpResult result = new ExpResult();
        result.storeCost = storeCost;
        result.lambda = lambda;

        // 1. 创建系统实例，注入 storeCost 和 λ
        String dbPath = String.format("%s/%s_sc%.2f_l%.1f", DB_BASE_PATH, tag, storeCost, lambda);
        MainNode system = new MainNode(NODES_COUNT, HEALPIX_LEVEL, dbPath);

        try {
            // 2. Phase 1: 预创建 + 预解析
            system.preCreateHealpixDatabases(allCsvLines);
            Map<Long, List<LightCurvePoint>> healpixDataMap = system.preParseData(allCsvLines);

            // 3. Phase 2: 写入数据
            system.distributePreParsedData(healpixDataMap);

            // 4. 设置 DP 参数并构建时间桶索引
            //    需要通过 Config 注入 lambda 和 storeCost
            setDPParameters(system, lambda, storeCost);
            system.buildAllTimeBucketsOffline();

            // 5. 统计桶信息
            BucketStats bucketStats = collectBucketStats(system);
            result.totalBuckets = bucketStats.totalBuckets;
            result.bucketsPerObject = bucketStats.bucketsPerObject;
            result.avgBucketSize = bucketStats.avgBucketSize;
            result.avgIntraBucketVariance = bucketStats.avgIntraBucketVariance;

            // 6. 执行 STMK 查询 (warmup + repeat)
            List<Double> queryTimes = new ArrayList<>();
            List<QueryStats> allQueryStats = new ArrayList<>();

            int totalRounds = WARMUP_ROUNDS + REPEAT_TIMES;
            for (int round = 0; round < totalRounds; round++) {
                boolean warmup = round < WARMUP_ROUNDS;
                if (warmup) {
                    System.out.printf("  [预热轮 %d/%d]%n", round + 1, WARMUP_ROUNDS);
                } else {
                    System.out.printf("  [正式轮 %d/%d]%n", round - WARMUP_ROUNDS + 1, REPEAT_TIMES);
                }

                QueryStats roundStats = new QueryStats();
                long roundStart = System.currentTimeMillis();

                for (double[] qp : queryPoints) {
                    double qRa = qp[0], qDec = qp[1];

                    // ★★★ 调用 MainNode 的真正 STMK 查询接口 ★★★
                    DistributedQueryResult dqr = system.executeSTMKQuery(
                            qRa, qDec, QUERY_RADIUS,
                            QUERY_TIME_START, QUERY_TIME_END,
                            QUERY_BAND, QUERY_MAG_MIN, QUERY_MAG_MAX, QUERY_MIN_K);

                    roundStats.resultCount += dqr.candidateCount;
                }

                long roundEnd = System.currentTimeMillis();
                double roundTime = roundEnd - roundStart;

                if (!warmup) {
                    queryTimes.add(roundTime);
                }
            }

            // 7. 单独统计剪枝率和精炼率（用一轮详细统计）
            QueryStats detailedStats = runDetailedQueryStats(system, queryPoints);
            result.pruningRate = detailedStats.totalObjects > 0
                    ? (double)(detailedStats.confirmedCount + detailedStats.prunedCount) / detailedStats.totalObjects
                    : 0;
            result.refinementRate = detailedStats.totalObjects > 0
                    ? (double) detailedStats.ambiguousCount / detailedStats.totalObjects
                    : 0;
            result.avgResultCount = detailedStats.resultCount / Math.max(1, queryPoints.size());

            // 8. 计算查询时间统计量
            double avgTime = queryTimes.stream().mapToDouble(d -> d).average().orElse(0);
            double stdTime = 0;
            if (queryTimes.size() > 1) {
                double mean = avgTime;
                stdTime = Math.sqrt(queryTimes.stream().mapToDouble(d -> (d - mean) * (d - mean)).sum() / (queryTimes.size() - 1));
            }
            result.avgQueryTimeMs = avgTime;
            result.stdQueryTimeMs = stdTime;

        } finally {
            system.shutdown();
            // 清理本轮 DB 目录
            deleteDir(new File(dbPath));
        }

        return result;
    }

    /**
     * 通过反射或直接调用设置 RocksDBServer.Config 的 DP 参数
     * 这需要 StorageNode 暴露一个接口来更新所有 healpix DB 的 Config
     */
    private static void setDPParameters(MainNode system, double lambda, double storeCost) {
        // 方案：在 MainNode 中增加方法 setTimeBucketDPParams(lambda, storeCost)
        // 该方法遍历所有 StorageNode，再遍历每个 healpix DB 的 Config，设置参数
        system.setTimeBucketDPParams(lambda, storeCost);
    }

    /**
     * 详细的剪枝/精炼统计
     *
     * 直接调用 StorageNode 层的 STMK 查询，获取每个天体的判定状态
     * 剪枝率 = (CONFIRMED + PRUNED) / 总天体数
     * 精炼率 = AMBIGUOUS / 总天体数
     */
    private static QueryStats runDetailedQueryStats(MainNode system, List<double[]> queryPoints) {
        QueryStats stats = new QueryStats();

        for (double[] qp : queryPoints) {
            double qRa = qp[0], qDec = qp[1];

            // 调用 MainNode 的详细统计查询
            MainNode.DetailedSTMKResult detailed = system.executeSTMKQueryDetailed(
                    qRa, qDec, QUERY_RADIUS,
                    QUERY_TIME_START, QUERY_TIME_END,
                    QUERY_BAND, QUERY_MAG_MIN, QUERY_MAG_MAX, QUERY_MIN_K);

            stats.totalObjects += detailed.totalObjectsEvaluated;
            stats.confirmedCount += detailed.confirmedCount;
            stats.prunedCount += detailed.prunedCount;
            stats.ambiguousCount += detailed.ambiguousCount;
            stats.resultCount += detailed.resultCount;
        }

        return stats;
    }

    // ==================== 桶统计 ====================

    public static class BucketStats {
        public long totalBuckets;
        public long totalObjects;
        public double bucketsPerObject;
        public double avgBucketSize;
        public double avgIntraBucketVariance;
    }

    private static BucketStats collectBucketStats(MainNode system) {
        // 调用 MainNode 的统计接口
        return system.collectTimeBucketStats();
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

    private static List<double[]> generateQueryPoints(String coordFile, int count) throws Exception {
        List<double[]> allCoords = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(coordFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals("source_id")) continue;
                allCoords.add(new double[]{Double.parseDouble(parts[1]), Double.parseDouble(parts[2])});
            }
        }
        // 均匀采样
        List<double[]> selected = new ArrayList<>();
        Random rng = new Random(42);
        for (int i = 0; i < count && !allCoords.isEmpty(); i++) {
            selected.add(allCoords.get(rng.nextInt(allCoords.size())));
        }
        return selected;
    }

    // ==================== 输出 ====================

    private static void writeCSV(String path, List<ExpResult> results) throws Exception {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            pw.println("store_cost,lambda,total_buckets,buckets_per_object,avg_bucket_size," +
                    "avg_intra_bucket_variance,avg_query_time_ms,std_query_time_ms," +
                    "pruning_rate,refinement_rate,avg_result_count");
            for (ExpResult r : results) {
                pw.printf(Locale.US, "%.2f,%.1f,%d,%.2f,%.2f,%.4f,%.2f,%.2f,%.4f,%.4f,%d%n",
                        r.storeCost, r.lambda, r.totalBuckets, r.bucketsPerObject,
                        r.avgBucketSize, r.avgIntraBucketVariance,
                        r.avgQueryTimeMs, r.stdQueryTimeMs,
                        r.pruningRate, r.refinementRate, r.avgResultCount);
            }
        }
    }

    private static void writeSummary(String path, List<ExpResult> results, double bestStoreCost) throws Exception {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            pw.println("══════════════════════════════════════════════════════════════");
            pw.println("  实验3：自适应时间桶参数敏感性分析 — 摘要报告");
            pw.printf("  最优 storeCost: %.2f%n", bestStoreCost);
            pw.println("══════════════════════════════════════════════════════════════");
            pw.println();
            pw.printf("%-6s %10s %10s %10s %10s %12s %10s %10s%n",
                    "λ", "桶数", "桶/对象", "桶均大小", "桶内方差", "查询(ms)", "剪枝率", "精炼率");
            pw.println("─".repeat(86));
            for (ExpResult r : results) {
                pw.printf(Locale.US, "%-6.1f %10d %10.1f %10.1f %10.4f %8.2f±%4.2f %9.1f%% %9.1f%%%n",
                        r.lambda, r.totalBuckets, r.bucketsPerObject, r.avgBucketSize,
                        r.avgIntraBucketVariance, r.avgQueryTimeMs, r.stdQueryTimeMs,
                        r.pruningRate * 100, r.refinementRate * 100);
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