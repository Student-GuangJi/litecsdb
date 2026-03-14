package org.example.exp;

import org.example.MainNode;
import org.example.RocksDBServer.*;
import org.example.MainNode.*;

import java.io.*;
import java.util.*;

/**
 * 实验1：自适应分桶 vs 固定分桶 对比实验
 *
 * 实验设计：
 *   自变量: 分桶策略 ∈ {DP-Adaptive, Fixed-1d, Fixed-7d, Fixed-30d, Fixed-90d}
 *   因变量:
 *     - 桶数量（总/每对象平均）
 *     - 桶内星等方差（衡量桶内一致性）
 *     - 空桶率（仅固定分桶有此问题）
 *     - 桶大小标准差（衡量负载均衡性）
 *     - 索引存储开销（字节）
 *     - STMK 查询延迟与剪枝率（衡量索引对查询的帮助）
 *
 * 全链路调用 MainNode → StorageNode → RocksDBServer，确保实验真实性。
 */
public class Experiment4Runner {

    // ==================== 实验配置 ====================
    private static final int WARMUP_ROUNDS = 0;
    private static final int REPEAT_TIMES = 1;

    // 数据集
    private static final String DEFAULT_COORD_FILE = "gaiadr2/source_coordinates.csv";
    private static final String DEFAULT_BATCH_DIR = "gaiadr2/individual_lightcurves/";

    // 分桶策略：deltaDays = -1 表示 DP 自适应
    private static final double[] FIXED_DELTA_DAYS = {0.5, 1.0, 3.0, 7.0};
    private static final String[] STRATEGY_NAMES = {"DP-Adaptive", "Fixed-0.5d", "Fixed-1d", "Fixed-3d", "Fixed-7d"};

    // DP 默认参数
    private static final double DEFAULT_LAMBDA = 0.3;
    private static final double DEFAULT_STORE_COST = 40.0;

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
    private static final String DB_BASE_PATH = "LitecsDB_Exp4";

    // ==================== 实验结果 ====================
    private static class Exp4Result {
        String strategy;
        double deltaDays;
        long totalBuckets;
        long totalObjects;
        double bucketsPerObject;
        double avgBucketSize;
        double bucketSizeStdDev;
        double avgIntraBucketVariance;
        double emptyBucketRatio;
        long indexStorageBytes;
        double avgQueryTimeMs;
        double stdQueryTimeMs;
        double pruningRate;
        double refinementRate;
        int avgResultCount;
    }

    // ==================== 查询统计 ====================
    private static class QueryStats {
        int totalObjects = 0;
        int confirmedCount = 0;
        int prunedCount = 0;
        int ambiguousCount = 0;
        int resultCount = 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("  实验4：自适应分桶 vs 固定分桶 对比实验");
        System.out.println("═══════════════════════════════════════════════════════════");

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

        // 生成查询点
        List<double[]> queryPoints = generateQueryPoints(coordFile, NUM_QUERIES);

        String timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        String csvFile = "exp4_adaptive_vs_fixed_" + timestamp + ".csv";
        String summaryFile = "exp4_adaptive_vs_fixed_summary_" + timestamp + ".txt";

        List<Exp4Result> allResults = new ArrayList<>();

        // ==================== 运行每种策略 ====================
        for (int si = 0; si < STRATEGY_NAMES.length; si++) {
            String strategy = STRATEGY_NAMES[si];
            double deltaDays = si == 0 ? -1 : FIXED_DELTA_DAYS[si - 1];

            System.out.printf("%n━━━━━━━━━━━ 策略: %s ━━━━━━━━━━━%n", strategy);

            Exp4Result result = runSingleStrategy(allCsvLines, queryPoints, strategy, deltaDays);
            allResults.add(result);

            System.out.printf("  桶数=%d | 桶/对象=%.1f | 桶均大小=%.1f | 桶大小StdDev=%.1f%n",
                    result.totalBuckets, result.bucketsPerObject, result.avgBucketSize, result.bucketSizeStdDev);
            System.out.printf("  桶内方差=%.4f | 空桶率=%.1f%% | 索引大小=%dKB%n",
                    result.avgIntraBucketVariance, result.emptyBucketRatio * 100, result.indexStorageBytes / 1024);
            System.out.printf("  查询=%.2f±%.2f ms | 剪枝率=%.1f%% | 精炼率=%.1f%% | 结果数=%d%n",
                    result.avgQueryTimeMs, result.stdQueryTimeMs,
                    result.pruningRate * 100, result.refinementRate * 100, result.avgResultCount);
        }

        // ==================== 输出 ====================
        writeCSV(csvFile, allResults);
        writeSummary(summaryFile, allResults);

        System.out.printf("%n══════════════════════════════════════════════════════════%n");
        System.out.printf("  实验1完成！%n");
        System.out.printf("  CSV: %s%n", csvFile);
        System.out.printf("  摘要: %s%n", summaryFile);
        System.out.printf("══════════════════════════════════════════════════════════%n");
    }

    // ==================== 核心实验方法 ====================

    private static Exp4Result runSingleStrategy(
            List<String> allCsvLines, List<double[]> queryPoints,
            String strategy, double deltaDays) throws Exception {

        Exp4Result result = new Exp4Result();
        result.strategy = strategy;
        result.deltaDays = deltaDays;

        String dbPath = String.format("%s/%s", DB_BASE_PATH, strategy.replace("-", "_"));
        MainNode system = new MainNode(NODES_COUNT, HEALPIX_LEVEL, dbPath);

        try {
            // 1. 写入数据
            system.preCreateHealpixDatabases(allCsvLines);
            Map<Long, List<LightCurvePoint>> healpixDataMap = system.preParseData(allCsvLines);
            system.distributePreParsedData(healpixDataMap);

            // 2. 构建时间桶索引
            long buildStart = System.currentTimeMillis();
            if (deltaDays < 0) {
                // DP 自适应分桶
                system.setTimeBucketDPParams(DEFAULT_LAMBDA, DEFAULT_STORE_COST);
                system.buildAllTimeBucketsOffline();
            } else {
                // 固定窗口分桶
                system.buildAllTimeBucketsOfflineFixed(deltaDays);
            }
            long buildTime = System.currentTimeMillis() - buildStart;
            System.out.printf("  时间桶构建耗时: %d ms%n", buildTime);

            // 3. 收集详细桶统计
            DetailedBucketStats bucketStats = system.collectDetailedTimeBucketStats();
            result.totalBuckets = bucketStats.totalBuckets;
            result.totalObjects = bucketStats.totalObjects;
            result.bucketsPerObject = bucketStats.bucketsPerObject;
            result.avgBucketSize = bucketStats.avgBucketSize;
            result.bucketSizeStdDev = bucketStats.bucketSizeStdDev;
            result.avgIntraBucketVariance = bucketStats.avgIntraBucketVariance;

            // 4. 计算空桶率（仅固定分桶有意义）
            if (deltaDays > 0) {
                result.emptyBucketRatio = computeEmptyBucketRatio(system, deltaDays, bucketStats.totalBuckets);
            } else {
                result.emptyBucketRatio = 0;
            }

            // 5. 索引存储大小
            system.forceFlushAllNoCompaction();
            result.indexStorageBytes = system.getTimeBucketStorageSize();

            // 6. 执行 STMK 查询（warmup + repeat）
            List<Double> queryTimes = new ArrayList<>();
            int totalRounds = WARMUP_ROUNDS + REPEAT_TIMES;

            for (int round = 0; round < totalRounds; round++) {
                boolean warmup = round < WARMUP_ROUNDS;
                long roundStart = System.currentTimeMillis();

                for (double[] qp : queryPoints) {
                    system.executeSTMKQuery(
                            qp[0], qp[1], QUERY_RADIUS,
                            QUERY_TIME_START, QUERY_TIME_END,
                            QUERY_BAND, QUERY_MAG_MIN, QUERY_MAG_MAX, QUERY_MIN_K);
                }

                long roundEnd = System.currentTimeMillis();
                if (!warmup) {
                    queryTimes.add((double)(roundEnd - roundStart));
                }
            }

            // 7. 剪枝统计
            QueryStats detailedStats = runDetailedQueryStats(system, queryPoints);
            result.pruningRate = detailedStats.totalObjects > 0
                    ? (double)(detailedStats.confirmedCount + detailedStats.prunedCount) / detailedStats.totalObjects
                    : 0;
            result.refinementRate = detailedStats.totalObjects > 0
                    ? (double) detailedStats.ambiguousCount / detailedStats.totalObjects
                    : 0;
            result.avgResultCount = detailedStats.resultCount / Math.max(1, queryPoints.size());

            // 8. 查询时间统计
            double avgTime = queryTimes.stream().mapToDouble(d -> d).average().orElse(0);
            double stdTime = 0;
            if (queryTimes.size() > 1) {
                double mean = avgTime;
                stdTime = Math.sqrt(queryTimes.stream()
                        .mapToDouble(d -> (d - mean) * (d - mean)).sum() / (queryTimes.size() - 1));
            }
            result.avgQueryTimeMs = avgTime;
            result.stdQueryTimeMs = stdTime;

        } finally {
            system.shutdown();
            deleteDir(new File(dbPath));
        }

        return result;
    }

    /**
     * 计算固定分桶的空桶率
     *
     * 对每个天体，理论桶数 = max(1, ceil((lastObs - firstObs) / deltaDays))
     * 空桶率 = 1 - 实际非空桶总数 / 理论桶总数
     */
    private static double computeEmptyBucketRatio(MainNode system, double deltaDays, long actualBuckets) {
        List<StarMetadata> allStars = system.getAllStarsMetadata();
        long theoreticalTotal = 0;
        for (StarMetadata star : allStars) {
            double timeSpan = star.lastObsTime - star.firstObsTime;
            if (timeSpan <= 0) {
                theoreticalTotal += 1;
            } else {
                theoreticalTotal += Math.max(1, (long) Math.ceil(timeSpan / deltaDays));
            }
        }

        if (theoreticalTotal <= 0) return 0;
        return Math.max(0, 1.0 - (double) actualBuckets / theoreticalTotal);
    }

    private static QueryStats runDetailedQueryStats(MainNode system, List<double[]> queryPoints) {
        QueryStats stats = new QueryStats();
        for (double[] qp : queryPoints) {
            DetailedSTMKResult detailed = system.executeSTMKQueryDetailed(
                    qp[0], qp[1], QUERY_RADIUS,
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
        List<double[]> selected = new ArrayList<>();
        Random rng = new Random(42);
        for (int i = 0; i < count && !allCoords.isEmpty(); i++) {
            selected.add(allCoords.get(rng.nextInt(allCoords.size())));
        }
        return selected;
    }

    // ==================== 输出 ====================

    private static void writeCSV(String path, List<Exp4Result> results) throws Exception {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            pw.println("strategy,delta_days,total_buckets,total_objects,buckets_per_object," +
                    "avg_bucket_size,bucket_size_std_dev,avg_intra_bucket_variance," +
                    "empty_bucket_ratio,index_storage_bytes," +
                    "avg_query_time_ms,std_query_time_ms," +
                    "pruning_rate,refinement_rate,avg_result_count");
            for (Exp4Result r : results) {
                pw.printf(Locale.US, "%s,%.1f,%d,%d,%.2f,%.2f,%.2f,%.6f,%.4f,%d,%.2f,%.2f,%.4f,%.4f,%d%n",
                        r.strategy, r.deltaDays, r.totalBuckets, r.totalObjects,
                        r.bucketsPerObject, r.avgBucketSize, r.bucketSizeStdDev,
                        r.avgIntraBucketVariance, r.emptyBucketRatio, r.indexStorageBytes,
                        r.avgQueryTimeMs, r.stdQueryTimeMs,
                        r.pruningRate, r.refinementRate, r.avgResultCount);
            }
        }
    }

    private static void writeSummary(String path, List<Exp4Result> results) throws Exception {
        try (PrintWriter pw = new PrintWriter(new FileWriter(path))) {
            pw.println("══════════════════════════════════════════════════════════════════════════════════════");
            pw.println("  实验1：自适应分桶 vs 固定分桶 对比实验 — 摘要报告");
            pw.println("══════════════════════════════════════════════════════════════════════════════════════");
            pw.println();
            pw.printf("%-14s %8s %8s %8s %10s %8s %10s %8s %10s%n",
                    "策略", "桶数", "桶/对象", "桶均大小", "大小StdDev", "桶内方差", "空桶率", "查询(ms)", "剪枝率");
            pw.println("─".repeat(98));
            for (Exp4Result r : results) {
                pw.printf(Locale.US, "%-14s %8d %8.1f %8.1f %10.1f %8.4f %9.1f%% %8.2f %9.1f%%%n",
                        r.strategy, r.totalBuckets, r.bucketsPerObject,
                        r.avgBucketSize, r.bucketSizeStdDev,
                        r.avgIntraBucketVariance, r.emptyBucketRatio * 100,
                        r.avgQueryTimeMs, r.pruningRate * 100);
            }
            pw.println();
            pw.println("说明:");
            pw.println("  桶内方差: 桶内星等方差的全局均值，越小说明桶内数据越一致");
            pw.println("  空桶率: 仅固定分桶存在此问题，DP 自适应分桶无空桶");
            pw.println("  剪枝率: (直接确认 + 直接剪枝) / 总评估天体数");
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
