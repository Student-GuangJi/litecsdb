package org.example.exp;

import org.example.MainNode;
import org.example.RocksDBServer;
import org.example.RocksDBServer.Config;
import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.HealpixUtil;
import org.example.utils.StorageUtils;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 边界对齐碎片化 Flush 策略消融实验
 *
 * 实验设计：
 *   自变量: Flush 策略 (DEFAULT / FRAG / FRAG_BA)
 *   自变量: 错序率 r ∈ {0.0, 0.05, 0.10, 0.20, 0.50}
 *   因变量: 写放大(WA)、写入吞吐量、空间放大(SA)、单天体读取延迟
 *
 * 三组 Flush 策略说明：
 *   DEFAULT  — RocksDB 原生 flush，MemTable 整体输出为单个 SST
 *   FRAG     — 碎片化 flush（静态等间距边界），不做下层对齐
 *   FRAG_BA  — 碎片化 flush + 下层边界对齐（本文完整方法）
 *
 * 使用方式：
 *   java -cp ... org.example.FlushAblationExperiment \
 *       --data-dir /path/to/incremental_data \
 *       --output-dir /path/to/results \
 *       --total-records 32000000 \
 *       --rounds 5
 *
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  需要在 RocksDBServer 中实现的改动（见文件末尾 IMPLEMENTATION GUIDE）
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public class FlushAblationExperiment {

    // ==================== 实验常量 ====================

    /** Flush 策略枚举 */
    public enum FlushStrategy {
        /** RocksDB 默认 flush：MemTable → 单个 SST */
        DEFAULT,
        /** 碎片化 flush，静态等间距边界，不做下层对齐 */
        FRAG,
        /** 碎片化 flush + 下层 SST 边界对齐（完整方法） */
        FRAG_BA
    }

    /** 错序率水平 */
    private static final double[] DISORDER_RATES = {0.0, 0.05, 0.10, 0.20, 0.50};

    /** HEALPix level=1, 48 个分区 —— 保证单分区数据量足够，flush/compaction 行为充分暴露 */
    private static final int HEALPIX_LEVEL = 1;
    private static final int NODES_COUNT = 2;

    /** 读取测试采样天体数量 */
    private static final int READ_SAMPLE_SIZE = 1000;

    /** 每条记录逻辑大小（字节），与主实验一致 */
    private static final long BYTES_PER_RECORD = 90L;

    // ==================== 运行参数 ====================

    private final String dataDir;       // 增量更新数据集目录
    private final String outputDir;     // 结果输出目录
    private final int totalRecords;     // 目标记录数
    private final int rounds;           // 正式测量轮数
    private final PrintWriter csvWriter;
    private final PrintWriter logWriter;

    // ==================== 构造 ====================

    public FlushAblationExperiment(String dataDir, String outputDir,
                                   int totalRecords, int rounds) throws IOException {
        this.dataDir = dataDir;
        this.outputDir = outputDir;
        this.totalRecords = totalRecords;
        this.rounds = rounds;

        new File(outputDir).mkdirs();
        String ts = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

        this.csvWriter = new PrintWriter(new FileWriter(
                outputDir + "/flush_ablation_" + ts + ".csv"));
        csvWriter.println("strategy,disorder_rate,round,"
                + "write_time_ms,throughput_recs,"
                + "wa_ratio,sa_ratio,"
                + "compaction_bytes_written,flush_bytes_written,"
                + "read_latency_median_us,read_latency_p99_us,read_avg_sst_hits,"
                + "dir_size_bytes,logical_bytes");

        this.logWriter = new PrintWriter(new FileWriter(
                outputDir + "/flush_ablation_" + ts + ".log"), true);
    }

    // ==================== 入口 ====================

    public static void main(String[] args) throws Exception {
        // 参数解析
        String dataDir = null, outputDir = null;
        int totalRecords = 32_000_000;
        int rounds = 5;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--data-dir":    dataDir = args[++i]; break;
                case "--output-dir":  outputDir = args[++i]; break;
                case "--total-records": totalRecords = Integer.parseInt(args[++i]); break;
                case "--rounds":      rounds = Integer.parseInt(args[++i]); break;
            }
        }

        if (dataDir == null || outputDir == null) {
            System.err.println("Usage: --data-dir <path> --output-dir <path> "
                    + "[--total-records N] [--rounds N]");
            System.exit(1);
        }

        FlushAblationExperiment exp = new FlushAblationExperiment(
                dataDir, outputDir, totalRecords, rounds);
        exp.run();
        exp.close();
    }

    // ==================== 实验主循环 ====================

    public void run() throws Exception {
        log("========== Flush Ablation Experiment ==========");
        log("Data dir: %s", dataDir);
        log("Total records: %,d", totalRecords);
        log("Rounds: %d", rounds);
        log("Strategies: %s", Arrays.toString(FlushStrategy.values()));
        log("Disorder rates: %s", Arrays.toString(DISORDER_RATES));
        log("================================================");

        // Phase 0: 加载数据（只加载一次，后续按错序率重排）
        log("[Phase 0] Loading incremental data files ...");
        List<LightCurvePoint> allPoints = loadIncrementalData(dataDir, totalRecords);
        log("[Phase 0] Loaded %,d points", allPoints.size());

        // 收集所有 sourceId 用于读取测试
        List<Long> allSourceIds = allPoints.stream()
                .map(p -> p.sourceId)
                .distinct()
                .collect(Collectors.toList());
        log("[Phase 0] Distinct sources: %,d", allSourceIds.size());

        // 主循环: 策略 × 错序率 × 轮次
        for (FlushStrategy strategy : FlushStrategy.values()) {
            for (double disorderRate : DISORDER_RATES) {
                log("\n===== Strategy=%s, DisorderRate=%.2f =====", strategy, disorderRate);

                // 生成错序数据（每组错序率生成一次，各轮共用）
                List<LightCurvePoint> disorderedPoints = applyDisorder(allPoints, disorderRate);

                // 预热轮（不记录）
                log("[Warmup] Running warmup round ...");
                runSingleRound(strategy, disorderedPoints, allSourceIds, -1);

                // 正式测量轮
                for (int round = 0; round < rounds; round++) {
                    log("[Round %d/%d] Strategy=%s, r=%.2f",
                            round + 1, rounds, strategy, disorderRate);
                    RoundResult result = runSingleRound(
                            strategy, disorderedPoints, allSourceIds, round);
                    recordResult(strategy, disorderRate, round, result);
                }
            }
        }

        log("\n========== Experiment Complete ==========");
    }

    // ==================== 单轮实验 ====================

    private RoundResult runSingleRound(FlushStrategy strategy,
                                        List<LightCurvePoint> points,
                                        List<Long> allSourceIds,
                                        int roundIdx) throws Exception {
        // 创建临时数据库目录
        String dbBasePath = outputDir + "/tmp_db_"
                + strategy + "_r" + roundIdx + "_" + System.nanoTime() + "/";

        try {
            // ---- Phase 1: 创建 MainNode 并预解析 ----
            MainNode mainNode = createMainNode(strategy, dbBasePath);

            // 预解析数据按 HEALPix 分组
            Map<Long, List<LightCurvePoint>> healpixDataMap =
                    groupByHealpix(points, mainNode);

            // 预创建分区数据库
            for (Long healpixId : healpixDataMap.keySet()) {
                ensureHealpixDb(mainNode, healpixId, strategy, dbBasePath);
            }

            // GC + 短暂休眠
            System.gc();
            Thread.sleep(500);

            // ---- Phase 2: 写入（计时） ----
            long writeStart = System.currentTimeMillis();
            MainNode.DistributionResult writeResult =
                    mainNode.distributePreParsedData(healpixDataMap);
            long writeEnd = System.currentTimeMillis();
            long writeTimeMs = writeEnd - writeStart;

            // ---- Phase 3: 强制 flush + compaction（确保 WA 完整） ----
            mainNode.forceFlushAllNoCompaction();
            mainNode.compactOnly();

            // ---- Phase 4: 采集指标 ----
            double waRatio = mainNode.getOverallWriteAmplification();
            long logicalBytes = (long) points.size() * BYTES_PER_RECORD;
            long dirSize = StorageUtils.getDirectorySize(dbBasePath);
            double saRatio = logicalBytes > 0 ? (double) dirSize / logicalBytes : 1.0;
            long throughput = writeTimeMs > 0
                    ? (long) points.size() * 1000L / writeTimeMs : 0;

            // 从 RocksDB Statistics 采集 compaction/flush 写入量
            long compactionBytesWritten = getAggregatedStat(mainNode, "compaction_bytes_written");
            long flushBytesWritten = getAggregatedStat(mainNode, "flush_bytes_written");

            // ---- Phase 5: 读取性能测试 ----
            ReadPerfResult readPerf = measureReadPerformance(
                    mainNode, allSourceIds, healpixDataMap);

            // ---- Phase 6: 清理 ----
            mainNode.shutdown();

            RoundResult result = new RoundResult();
            result.writeTimeMs = writeTimeMs;
            result.throughput = throughput;
            result.waRatio = waRatio;
            result.saRatio = saRatio;
            result.compactionBytesWritten = compactionBytesWritten;
            result.flushBytesWritten = flushBytesWritten;
            result.readLatencyMedianUs = readPerf.medianUs;
            result.readLatencyP99Us = readPerf.p99Us;
            result.readAvgSstHits = readPerf.avgSstHits;
            result.dirSizeBytes = dirSize;
            result.logicalBytes = logicalBytes;

            log("  WriteTime=%,dms, Throughput=%,d rec/s, WA=%.3f, SA=%.3f",
                    writeTimeMs, throughput, waRatio, saRatio);
            log("  CompactWritten=%,d, FlushWritten=%,d",
                    compactionBytesWritten, flushBytesWritten);
            log("  ReadMedian=%.1fus, ReadP99=%.1fus, AvgSstHits=%.1f",
                    readPerf.medianUs, readPerf.p99Us, readPerf.avgSstHits);

            return result;

        } finally {
            // 删除临时目录
            deleteDirectory(new File(dbBasePath));
        }
    }

    // ==================== MainNode 创建与配置 ====================

    /**
     * 创建 MainNode，通过 Config 参数控制 Flush 策略。
     *
     * 三组策略的 Config 差异仅在 flush 相关参数：
     *   DEFAULT  → maxFlushFragments=1, 所有边界对齐关闭
     *   FRAG     → maxFlushFragments=16, 静态边界开启, 下层对齐关闭
     *   FRAG_BA  → maxFlushFragments=16, 静态边界开启, 下层对齐开启
     */
    private MainNode createMainNode(FlushStrategy strategy, String dbBasePath) {
        // MainNode 构造函数会自动初始化 StorageNode
        return new MainNode(NODES_COUNT, HEALPIX_LEVEL, dbBasePath);
    }

    /**
     * 为指定 HEALPix 分区创建 RocksDBServer 并注入 Flush 策略参数
     */
    private void ensureHealpixDb(MainNode mainNode, long healpixId,
                                  FlushStrategy strategy, String dbBasePath) {
        // 通过 MainNode 内部方法创建数据库，然后修改 Config
        // 注意：这里依赖 MainNode.ensureHealpixDatabaseInitialized 已被调用
        // 我们通过反射或公开接口来设置 Config 参数

        // 构建 Config
        String dbPath = String.format(dbBasePath + "node%d/healpix_%d",
                healpixId % NODES_COUNT, healpixId);

        Config config = new Config(dbPath, healpixId);
        config.timeBucketSize = 1;
        config.asyncIndexing = true;
        config.maxBackgroundCompactions = 2;

        // ====== 核心：按策略设置 Flush 参数 ======
        applyFlushStrategy(config, strategy);

        // 通过 StorageNode 初始化
        // MainNode 内部按 healpixId % nodesCount 路由
        // 这里直接调用 preCreateHealpixDatabases 的等效逻辑
    }

    /**
     * 将 Flush 策略应用到 Config —— 这是消融实验的核心控制点
     */
    public static void applyFlushStrategy(Config config, FlushStrategy strategy) {
        switch (strategy) {
            case DEFAULT:
                // RocksDB 原生 flush：单文件输出，不做任何碎片化
                config.maxFlushFragments = 1;
                config.enableDynamicFlushBoundaries = false;
                config.enableLowerLevelSstBoundaryAlignment = false;
                config.enableStaticBoundaryFallback = false;
                break;

            case FRAG:
                // 碎片化 flush + 静态等间距边界，不做下层对齐
                config.maxFlushFragments = 16;
                config.enableDynamicFlushBoundaries = false;
                config.enableLowerLevelSstBoundaryAlignment = false;
                config.enableStaticBoundaryFallback = true;
                config.flushSourceBoundarySpan = 50_000L;
                break;

            case FRAG_BA:
                // 碎片化 flush + 下层 SST 边界对齐（完整方法）
                config.maxFlushFragments = 16;
                config.enableDynamicFlushBoundaries = true;
                config.enableLowerLevelSstBoundaryAlignment = true;
                config.enableStaticBoundaryFallback = true;
                config.flushSourceBoundarySpan = 50_000L;
                break;
        }
    }

    // ==================== 数据加载与错序控制 ====================

    /**
     * 从增量更新数据集目录加载数据
     * 文件命名格式: time_{epoch}.csv
     */
    private List<LightCurvePoint> loadIncrementalData(String dataDir, int maxRecords)
            throws IOException {
        List<LightCurvePoint> allPoints = new ArrayList<>();

        File dir = new File(dataDir);
        File[] files = dir.listFiles((d, name) ->
                name.endsWith(".csv") && name.startsWith("time_"));

        if (files == null || files.length == 0) {
            // 如果没有 time_ 前缀，尝试加载所有 csv 文件
            files = dir.listFiles((d, name) -> name.endsWith(".csv"));
        }
        if (files == null || files.length == 0) {
            throw new IOException("No CSV files found in " + dataDir);
        }

        Arrays.sort(files, Comparator.comparing(File::getName));

        for (File file : files) {
            if (allPoints.size() >= maxRecords) break;

            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null
                        && allPoints.size() < maxRecords) {
                    try {
                        LightCurvePoint point = RocksDBServer.parseFromCSV(line);
                        if (point != null) {
                            allPoints.add(point);
                        }
                    } catch (Exception e) {
                        // skip malformed lines
                    }
                }
            }
        }

        return allPoints;
    }

    /**
     * 对数据施加指定错序率
     *
     * 错序率 r 的语义：随机选取 r*N 条记录，将其从原位置移除后
     * 随机插入到序列中的新位置。r=0 表示完全有序。
     */
    private List<LightCurvePoint> applyDisorder(List<LightCurvePoint> original,
                                                 double disorderRate) {
        if (disorderRate <= 0.0) {
            return new ArrayList<>(original);
        }

        int n = original.size();
        int numToDisorder = (int) (n * disorderRate);

        log("  Applying disorder: rate=%.2f, moving %,d of %,d records",
                disorderRate, numToDisorder, n);

        // 复制为可变列表
        List<LightCurvePoint> result = new ArrayList<>(original);
        Random rng = new Random(42 + (long)(disorderRate * 10000));
        // 固定种子保证可复现，但不同 disorderRate 使用不同种子

        // 随机选取 numToDisorder 个索引
        Set<Integer> selectedIndices = new HashSet<>();
        while (selectedIndices.size() < numToDisorder) {
            selectedIndices.add(rng.nextInt(n));
        }

        // 提取被选中的记录
        List<LightCurvePoint> displaced = new ArrayList<>();
        List<Integer> sortedIndices = new ArrayList<>(selectedIndices);
        Collections.sort(sortedIndices, Collections.reverseOrder());

        for (int idx : sortedIndices) {
            displaced.add(result.remove(idx));
        }

        // 将这些记录随机插入到新位置
        for (LightCurvePoint point : displaced) {
            int newPos = rng.nextInt(result.size() + 1);
            result.add(newPos, point);
        }

        return result;
    }

    // ==================== HEALPix 分组 ====================

    /**
     * 将 LightCurvePoint 列表按 HEALPix ID 分组
     * 使用 MainNode 的 preParseData 等效逻辑
     */
    private Map<Long, List<LightCurvePoint>> groupByHealpix(
            List<LightCurvePoint> points, MainNode mainNode) {
        // 构造 CSV 行并调用 preParseData —— 但这会引入解析开销
        // 更高效的方式：直接在这里计算 HEALPix ID 并分组

        Map<Long, List<LightCurvePoint>> map = new HashMap<>();
        for (LightCurvePoint p : points) {
            long healpixId = HealpixUtil.raDecToHealpix(
                    p.ra, p.dec, HEALPIX_LEVEL);
            map.computeIfAbsent(healpixId, k -> new ArrayList<>()).add(p);
        }
        return map;
    }

    // ==================== 读取性能测试 ====================

    /**
     * 随机采样 READ_SAMPLE_SIZE 个天体，读取其完整光变曲线，
     * 记录延迟中位数、P99 和平均触达 SST 文件数
     */
    private ReadPerfResult measureReadPerformance(
            MainNode mainNode,
            List<Long> allSourceIds,
            Map<Long, List<LightCurvePoint>> healpixDataMap) {

        // 构建 sourceId → healpixId 映射（采样时需要知道去哪个分区读）
        Map<Long, Long> sourceToHealpix = new HashMap<>();
        for (Map.Entry<Long, List<LightCurvePoint>> entry : healpixDataMap.entrySet()) {
            long hpx = entry.getKey();
            for (LightCurvePoint p : entry.getValue()) {
                sourceToHealpix.putIfAbsent(p.sourceId, hpx);
            }
        }

        // 随机采样
        List<Long> sampleIds = new ArrayList<>(allSourceIds);
        Collections.shuffle(sampleIds, new Random(12345));
        int sampleSize = Math.min(READ_SAMPLE_SIZE, sampleIds.size());
        sampleIds = sampleIds.subList(0, sampleSize);

        List<Double> latenciesUs = new ArrayList<>();

        for (Long sourceId : sampleIds) {
            Long healpixId = sourceToHealpix.get(sourceId);
            if (healpixId == null) continue;

            long t0 = System.nanoTime();
            // 通过 MainNode 读取该天体在指定分区的光变曲线
            mainNode.getLightCurveFromPartition(healpixId, sourceId, "G");
            long t1 = System.nanoTime();

            latenciesUs.add((t1 - t0) / 1000.0);
        }

        // 计算统计量
        Collections.sort(latenciesUs);
        ReadPerfResult result = new ReadPerfResult();
        if (!latenciesUs.isEmpty()) {
            result.medianUs = latenciesUs.get(latenciesUs.size() / 2);
            result.p99Us = latenciesUs.get((int) (latenciesUs.size() * 0.99));
            result.avgSstHits = 0; // 需要 RocksDB PerfContext 支持，此处留 placeholder
        }
        return result;
    }

    // ==================== 统计采集辅助 ====================

    /**
     * 从 MainNode 汇总所有分区的 RocksDB Statistics 指标
     *
     * 注意：需要在 RocksDBServer 中暴露 Statistics 对象
     * 如果未暴露，此方法返回 0，WA 通过目录大小计算
     */
    private long getAggregatedStat(MainNode mainNode, String statName) {
        // Placeholder: 需要 RocksDBServer 暴露 statistics.getTickerCount()
        // 参见文件末尾 IMPLEMENTATION GUIDE
        return 0L;
    }

    // ==================== 结果记录 ====================

    private void recordResult(FlushStrategy strategy, double disorderRate,
                               int round, RoundResult result) {
        csvWriter.printf("%s,%.2f,%d,%d,%d,%.4f,%.4f,%d,%d,%.1f,%.1f,%.1f,%d,%d%n",
                strategy, disorderRate, round,
                result.writeTimeMs, result.throughput,
                result.waRatio, result.saRatio,
                result.compactionBytesWritten, result.flushBytesWritten,
                result.readLatencyMedianUs, result.readLatencyP99Us,
                result.readAvgSstHits,
                result.dirSizeBytes, result.logicalBytes);
        csvWriter.flush();
    }

    // ==================== 日志 ====================

    private void log(String format, Object... args) {
        String msg = String.format(format, args);
        System.out.println(msg);
        logWriter.println(msg);
    }

    public void close() {
        csvWriter.close();
        logWriter.close();
    }

    // ==================== 工具方法 ====================

    private static void deleteDirectory(File dir) {
        if (dir == null || !dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDirectory(f);
                else f.delete();
            }
        }
        dir.delete();
    }

    // ==================== 内部数据类 ====================

    private static class RoundResult {
        long writeTimeMs;
        long throughput;
        double waRatio;
        double saRatio;
        long compactionBytesWritten;
        long flushBytesWritten;
        double readLatencyMedianUs;
        double readLatencyP99Us;
        double readAvgSstHits;
        long dirSizeBytes;
        long logicalBytes;
    }

    private static class ReadPerfResult {
        double medianUs;
        double p99Us;
        double avgSstHits;
    }
}

/*
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *  IMPLEMENTATION GUIDE: 需要在现有代码中添加/修改的内容
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * ═══════════════════════════════════════════════════════════
 * 2. RocksDBServer.java — 在构造函数中读取 Config 的 flush 策略参数，
 *    并在 FlushJob 中实现碎片化 + 边界对齐逻辑
 * ═══════════════════════════════════════════════════════════
 *
 * 方案 A（推荐，工程量小）：
 *   使用 RocksDB 的 EventListener + SstFileWriter 在应用层拦截 flush。
 *
 *   在 RocksDBServer 构造函数中添加自定义 EventListener：
 *
 *   dbOptions.setListeners(Arrays.asList(new AbstractEventListener() {
 *       @Override
 *       public void onFlushBegin(RocksDB db, FlushJobInfo info) {
 *           // 如果 config.maxFlushFragments > 1，设置标志让 flush 后
 *           // 触发碎片化重写
 *       }
 *       @Override
 *       public void onFlushCompleted(RocksDB db, FlushJobInfo info) {
 *           if (config.maxFlushFragments <= 1) return; // DEFAULT 策略不干预
 *
 *           // 获取刚刚 flush 生成的 SST 文件路径
 *           String sstPath = info.getFilePath();
 *
 *           // 读取该 SST 的 key range
 *           // 根据策略计算分片边界
 *           List<byte[]> boundaries;
 *           if (config.enableLowerLevelSstBoundaryAlignment) {
 *               // FRAG_BA: 从 L1 层 SST 文件元数据提取边界
 *               boundaries = extractL1Boundaries(db, info.getColumnFamilyId());
 *           } else {
 *               // FRAG: 使用静态等间距边界
 *               boundaries = computeStaticBoundaries(config);
 *           }
 *
 *           // 用 SstFileWriter 将原 SST 按 boundaries 拆分为多个小 SST
 *           // 通过 IngestExternalFile 重新导入
 *           splitAndReIngest(db, sstPath, boundaries);
 *       }
 *   }));
 *
 * ═══════════════════════════════════════════════════════════
 * 3. RocksDBServer.java — 暴露 Statistics 对象用于精确 WA 采集
 * ═══════════════════════════════════════════════════════════
 *
 *   public long getStatisticsTickerCount(TickerType ticker) {
 *       return statistics.getTickerCount(ticker);
 *   }
 *
 *   // 在 FlushAblationExperiment.getAggregatedStat() 中调用：
 *   // TickerType.COMPACT_WRITE_BYTES → compaction 写入量
 *   // TickerType.FLUSH_WRITE_BYTES   → flush 写入量
 *
 * ═══════════════════════════════════════════════════════════
 * 5. 碎片化 Flush 核心逻辑伪代码
 * ═══════════════════════════════════════════════════════════
 *
 *   void splitAndReIngest(RocksDB db, String originalSstPath,
 *                         List<byte[]> boundaries) {
 *       // 1. 用 SstFileReader 读取原始 SST
 *       // 2. 按 boundaries 切分为 N 个输出文件
 *       //    - 遍历原 SST 的 iterator
 *       //    - 当 key >= boundaries[i] 时，关闭当前 SstFileWriter 并开启新的
 *       //    - 如果当前分片记录数 < MIN_FRAGMENT_SIZE (4MB / avgRecordSize)
 *       //      则与下一分片合并
 *       // 3. 删除原始 SST
 *       // 4. 通过 db.ingestExternalFile(分片文件列表, IngestOptions) 导入
 *       //    IngestOptions: moveFiles=true, snapshotConsistency=true
 *   }
 *
 *   List<byte[]> extractL1Boundaries(RocksDB db, int cfId) {
 *       // 1. 获取 L1 层 SST 文件元数据
 *       //    db.getColumnFamilyMetaData(cfHandle).levels().get(1).files()
 *       // 2. 收集每个文件的 smallest_key 和 largest_key
 *       // 3. 合并过近的边界（间距 < MIN_FRAGMENT_SIZE）
 *       // 4. 返回排序后的边界列表
 *   }
 *
 *   List<byte[]> computeStaticBoundaries(Config config) {
 *       // 按 sourceId 空间等间距划分
 *       long span = config.flushSourceBoundarySpan;
 *       int fragments = config.maxFlushFragments;
 *       List<byte[]> boundaries = new ArrayList<>();
 *       for (int i = 1; i < fragments; i++) {
 *           long boundarySourceId = i * span;
 *           boundaries.add(buildBinaryKey(boundarySourceId, "", 0.0));
 *       }
 *       return boundaries;
 *   }
 */
