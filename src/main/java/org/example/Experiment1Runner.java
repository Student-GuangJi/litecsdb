package org.example;

import org.example.wrapper.*;
import org.example.RocksDBServer.LightCurvePoint;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Experiment1Runner {

    private static final int BATCH_SIZE = 2000000;
    private static final long MAX_RECORDS = 128000000;
    private static final int REPEAT_TIMES = 10;
    private static final int WARMUP_ROUNDS = 1;
    private static final String DEFAULT_COORD_FILE = "gaiadr2/source_coordinates.csv";
    private static final String DEFAULT_BATCH_DIR = "gaiadr2/individual_lightcurves/";
    private static final String DEFAULT_INCREMENTAL_DIR = "gaiadr2/observation_records_by_time/";
    private static final Pattern TRAILING_NUMBER_PATTERN = Pattern.compile(".*_(\\d+)\\.csv$");

    private enum RunMode {
        BATCH, INCREMENTAL, BOTH;
        static RunMode fromString(String value) {
            if (value == null) return BOTH;
            switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "batch": return BATCH;
                case "incremental": return INCREMENTAL;
                case "both": return BOTH;
                default: throw new IllegalArgumentException("无效 --mode 参数: " + value);
            }
        }
    }

    private static class RunnerConfig {
        RunMode mode = RunMode.BOTH;
        String datasetSize;
        String baseDir = "generated_datasets";
        String coordFileOverride;
        String myDbBasePath = "LitecsDB_Data";
        String nativeDbPath = "nativeRocksDB";
        boolean isolateDbPerRound = true;
        String runTag;
        String influxBucket = "gaia_lightcurves";
        String tdengineDb = "astro_db";
        boolean help;
        boolean useGeneratedDataset() { return datasetSize != null && !datasetSize.trim().isEmpty(); }
    }

    private static class DatasetPaths {
        String coordFile;
        String batchDir;
        String incrementalDir;
    }

    private static class RunStats {
        long totalRecords;
        long mySystemWriteTimeMs;
        long nativeRocksWriteTimeMs;
        long influxWriteTimeMs;
        long tdengineWriteTimeMs;
        double mySystemThroughput;
        double nativeThroughput;
        double influxThroughput;
        double tdengineThroughput;
        double mySystemSA;
        double nativeSA;
        double influxSA;
        double tdengineSA;
    }

    static MainNode mySystem;
    static NativeRocksDBWrapper nativeRocksDB;
    static InfluxDBClientWrapper influxDB;
    static TDengineClientWrapper tdengine;

    static long mySystemWriteTimeMs = 0;
    static long nativeRocksWriteTimeMs = 0;
    static long influxDBWriteTimeMs = 0;
    static long tdengineWriteTimeMs = 0;
    static long phase1LitecsDBMs = 0;
    static long phase1TDengineMs = 0;

    // ========== Phase 1 预解析的缓存 ==========
    static Map<Long, List<LightCurvePoint>> preParsedHealpixData = null;

    public static void main(String[] args) throws Exception {
        RunnerConfig config = parseArgs(args);
        if (config.help) { printUsage(); return; }
        DatasetPaths paths = resolvePaths(config);

        System.out.println("=== 实验一：时域光变曲线写入性能评估 ===");
        System.out.println("=== 策略：仿照 TDengine 两阶段（Two-Phase）策略 ===");
        System.out.printf("  Phase 1: 预创建分区/子表 + 预解析CSV（不计入写入时间）%n");
        System.out.printf("  Phase 2: 多线程并行批量写入（16线程, 每批10000行）%n%n");
        System.out.printf("模式: %s | 数据集: %s%n", config.mode,
                config.useGeneratedDataset() ? "generated_datasets (size=" + config.datasetSize + ")" : "gaiadr2 (默认)");
        System.out.printf("RocksDB基路径: mySystem=%s | native=%s | round隔离=%s%n",
                config.myDbBasePath, config.nativeDbPath, config.isolateDbPerRound);
        System.out.printf("统计轮次: 正式%d轮 + 预热%d轮(不计入平均)%n%n", REPEAT_TIMES, WARMUP_ROUNDS);

        if (config.mode == RunMode.BATCH || config.mode == RunMode.BOTH) {
            requireFile(paths.coordFile, "Batch 坐标文件不存在");
            requireDir(paths.batchDir, "Batch 光变文件目录不存在");
            List<RunStats> statsList = new ArrayList<>();
            int totalRounds = WARMUP_ROUNDS + REPEAT_TIMES;
            for (int i = 0; i < totalRounds; i++) {
                boolean warmup = i < WARMUP_ROUNDS;
                int logicalRound = warmup ? i : (i - WARMUP_ROUNDS + 1);
                if (warmup) {
                    System.out.printf("%n===== [Batch] 预热轮 %d/%d (不计入平均) =====%n", i + 1, WARMUP_ROUNDS);
                } else {
                    System.out.printf("%n===== [Batch] 第 %d/%d 轮 =====%n", logicalRound, REPEAT_TIMES);
                }
                initSystems(config, "batch", i);
                RunStats roundStats = runBatchTest(paths.coordFile, paths.batchDir, i, warmup);
                if (!warmup) statsList.add(roundStats);
                closeSystems();
            }
            printAverageStats("Batch", statsList);
        }

        if (config.mode == RunMode.INCREMENTAL || config.mode == RunMode.BOTH) {
            requireDir(paths.incrementalDir, "Incremental 时间切片目录不存在");
            List<RunStats> statsList = new ArrayList<>();
            int totalRounds = WARMUP_ROUNDS + REPEAT_TIMES;
            for (int i = 0; i < totalRounds; i++) {
                boolean warmup = i < WARMUP_ROUNDS;
                int logicalRound = warmup ? i : (i - WARMUP_ROUNDS + 1);
                if (warmup) {
                    System.out.printf("%n===== [Incremental] 预热轮 %d/%d (不计入平均) =====%n", i + 1, WARMUP_ROUNDS);
                } else {
                    System.out.printf("%n===== [Incremental] 第 %d/%d 轮 =====%n", logicalRound, REPEAT_TIMES);
                }
                initSystems(config, "incremental", i);
                RunStats roundStats = runIncrementalTest(paths.incrementalDir, i, warmup);
                if (!warmup) statsList.add(roundStats);
                closeSystems();
            }
            printAverageStats("Incremental", statsList);
        }
    }

    // ==================== 命令行参数解析 ====================

    private static RunnerConfig parseArgs(String[] args) {
        RunnerConfig config = new RunnerConfig();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--mode": case "-m": config.mode = RunMode.fromString(nextValue(args, ++i, arg)); break;
                case "--size": case "-s": config.datasetSize = nextValue(args, ++i, arg); break;
                case "--baseDir": case "-d": config.baseDir = nextValue(args, ++i, arg); break;
                case "--coordFile": case "-c": config.coordFileOverride = nextValue(args, ++i, arg); break;
                case "--myDbBasePath": case "--mydb": config.myDbBasePath = nextValue(args, ++i, arg); break;
                case "--nativeDbPath": case "--nativedb": config.nativeDbPath = nextValue(args, ++i, arg); break;
                case "--isolateDbPerRound": case "--isolate-db-per-round": config.isolateDbPerRound = Boolean.parseBoolean(nextValue(args, ++i, arg)); break;
                case "--runTag": case "--run-tag": config.runTag = nextValue(args, ++i, arg); break;
                case "--influx-bucket": case "--bucket": config.influxBucket = nextValue(args, ++i, arg); break;
                case "--tdengine-db": case "--tdb": config.tdengineDb = nextValue(args, ++i, arg); break;
                case "--help": case "-h": config.help = true; break;
                default: throw new IllegalArgumentException("未知参数: " + arg);
            }
        }
        return config;
    }

    private static String nextValue(String[] args, int index, String optionName) {
        if (index >= args.length) throw new IllegalArgumentException("参数 " + optionName + " ���少值");
        return args[index];
    }

    // ==================== 数据集路径解析 ====================

    private static DatasetPaths resolvePaths(RunnerConfig config) {
        DatasetPaths paths = new DatasetPaths();
        if (!config.useGeneratedDataset()) {
            paths.coordFile = config.coordFileOverride != null ? config.coordFileOverride : DEFAULT_COORD_FILE;
            paths.batchDir = DEFAULT_BATCH_DIR;
            paths.incrementalDir = DEFAULT_INCREMENTAL_DIR;
            return paths;
        }
        String normalizedBaseDir = config.baseDir.endsWith("/") || config.baseDir.endsWith("\\")
                ? config.baseDir.substring(0, config.baseDir.length() - 1) : config.baseDir;
        String batchRoot = normalizedBaseDir + "/batch_" + config.datasetSize;
        String incrementalRoot = normalizedBaseDir + "/incremental_" + config.datasetSize;
        paths.batchDir = batchRoot + "/individual_lightcurves/";
        paths.incrementalDir = incrementalRoot + "/observation_records_by_time/";
        paths.coordFile = config.coordFileOverride != null
                ? config.coordFileOverride
                : pickExistingPath(batchRoot + "/source_coordinates.csv", DEFAULT_COORD_FILE);
        return paths;
    }

    private static String pickExistingPath(String... candidates) {
        for (String path : candidates) { if (new File(path).exists()) return path; }
        return candidates[candidates.length - 1];
    }

    private static void requireFile(String fp, String msg) { if (!new File(fp).isFile()) throw new IllegalArgumentException(msg + ": " + fp); }
    private static void requireDir(String dp, String msg) { if (!new File(dp).isDirectory()) throw new IllegalArgumentException(msg + ": " + dp); }

    private static void printUsage() {
        System.out.println("用法: java org.example.Experiment1Runner [options]");
        System.out.println("  --mode, -m      batch|incremental|both (默认 both)");
        System.out.println("  --size, -s      数据规模标识");
        System.out.println("  --baseDir, -d   生成数据集根目录 (默认 generated_datasets)");
        System.out.println("  --coordFile, -c Batch 模式坐标文件路径");
        System.out.println("  --myDbBasePath, --mydb   现有系统 RocksDB 根目录");
        System.out.println("  --nativeDbPath, --nativedb Native RocksDB 目录");
        System.out.println("  --isolateDbPerRound        每轮自动使用独立DB路径 (默认 true)");
        System.out.println("  --runTag                   路径前缀标签");
        System.out.println("  --help, -h      打印帮助");
    }

    // ==================== 系统初始化与关闭 ====================

    private static void initSystems(RunnerConfig config, String modeName, int round) {
        mySystemWriteTimeMs = 0;
        nativeRocksWriteTimeMs = 0;
        influxDBWriteTimeMs = 0;
        tdengineWriteTimeMs = 0;
        phase1LitecsDBMs = 0;
        phase1TDengineMs = 0;

        String myDbPath = config.myDbBasePath;
        String nativeDbPath = config.nativeDbPath;
        if (config.isolateDbPerRound) {
            String tagPrefix = (config.runTag == null || config.runTag.trim().isEmpty())
                    ? "" : sanitizeSegment(config.runTag.trim()) + "_";
            String roundSegment = tagPrefix + modeName.toLowerCase(Locale.ROOT) + "_r" + String.format(Locale.ROOT, "%02d", round);
            myDbPath = appendSubPath(config.myDbBasePath, roundSegment);
            nativeDbPath = appendSubPath(config.nativeDbPath, roundSegment);
        }

        System.out.printf("DB路径: mySystem=%s | native=%s%n", myDbPath, nativeDbPath);
        System.out.printf("TDengine DB: %s | InfluxDB Bucket: %s%n", config.tdengineDb, config.influxBucket);

        // LitecsDB: 2 nodes, level=1 → ~48 个 HEALPix 分区（对齐 32 并行度量级）
        mySystem = new MainNode(2, 1, myDbPath);
        nativeRocksDB = new NativeRocksDBWrapper(nativeDbPath);
        influxDB = new InfluxDBClientWrapper(config.influxBucket);
        tdengine = new TDengineClientWrapper(config.tdengineDb);
    }

    private static String sanitizeSegment(String input) { return input.replaceAll("[^a-zA-Z0-9._-]", "_"); }

    private static String appendSubPath(String basePath, String segment) {
        String cleanBase = basePath == null ? "" : basePath.trim();
        if (cleanBase.isEmpty()) return segment;
        if (cleanBase.endsWith("/") || cleanBase.endsWith("\\")) return cleanBase + segment;
        return cleanBase + File.separator + segment;
    }

    private static void closeSystems() {
        if (mySystem != null) { mySystem.shutdown(); mySystem = null; }
        if (nativeRocksDB != null) { nativeRocksDB.close(); nativeRocksDB = null; }
        if (influxDB != null) { influxDB.close(); influxDB = null; }
        if (tdengine != null) { tdengine.close(); tdengine = null; }
    }

    // ==================== Batch 模式测试 ====================

    private static RunStats runBatchTest(String coordFile, String lcDir, int round, boolean warmup) throws Exception {
        System.out.printf(">>> 开始执行 [Batch] 批量聚集导入测试 (round=%d%s)%n", round, warmup ? ", 预热" : "");

        Map<Long, String> coordsMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(coordFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts[0].equals("source_id")) continue;
                coordsMap.put(Long.parseLong(parts[0]), parts[1] + "," + parts[2]);
            }
        }

        long totalRecords = 0;
        List<String> currentBatch = new ArrayList<>(BATCH_SIZE);
        boolean isFirstBatch = true;

        File dir = new File(lcDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".csv"));
        if (files != null) {
            Arrays.sort(files, Experiment1Runner::compareByTrailingNumberThenName);
            batchLoop:
            for (File file : files) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split(",");
                        if (parts[0].equals("source_id")) continue;
                        long sourceId = Long.parseLong(parts[0]);
                        String coords = coordsMap.getOrDefault(sourceId, "0.0,0.0");
                        String joinedLine = parts[0] + "," + coords + "," +
                                String.join(",", Arrays.copyOfRange(parts, 1, parts.length));
                        currentBatch.add(joinedLine);
                        totalRecords++;
                        if (currentBatch.size() >= BATCH_SIZE) {
                            if (isFirstBatch) { executePhase1PreCreate(currentBatch); isFirstBatch = false; }
                            dispatchBatch(currentBatch);
                            currentBatch.clear();
                        }
                        if (totalRecords >= MAX_RECORDS) {
                            System.out.println("已达到最大记录数限制，停止导入。");
                            break batchLoop;
                        }
                    }
                }
            }
            if (!currentBatch.isEmpty()) {
                if (isFirstBatch) { executePhase1PreCreate(currentBatch); isFirstBatch = false; }
                dispatchBatch(currentBatch);
            }
        }
        return collectAndPrintStats("Batch", round, totalRecords, warmup);
    }

    // ==================== Incremental 模式测试 ====================

    private static RunStats runIncrementalTest(String timeDir, int round, boolean warmup) throws Exception {
        System.out.printf(">>> 开始执行 [Incremental] 增量碎片化导入测试 (round=%d%s)%n", round, warmup ? ", 预热" : "");

        long totalRecords = 0;
        List<String> currentBatch = new ArrayList<>(BATCH_SIZE);
        boolean isFirstBatch = true;

        File dir = new File(timeDir);
        File[] files = dir.listFiles((d, name) -> name.startsWith("time_") && name.endsWith(".csv"));
        if (files != null) {
            Arrays.sort(files, Experiment1Runner::compareByTrailingNumberThenName);
            incrementalLoop:
            for (File file : files) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        if (line.trim().isEmpty() || line.startsWith("source_id,")) continue;
                        currentBatch.add(line);
                        totalRecords++;
                        if (currentBatch.size() >= BATCH_SIZE) {
                            if (isFirstBatch) { executePhase1PreCreate(currentBatch); isFirstBatch = false; }
                            dispatchBatch(currentBatch);
                            currentBatch.clear();
                        }
                        if (totalRecords >= MAX_RECORDS) {
                            System.out.println("已达到最大记录数限制，停止导入。");
                            break incrementalLoop;
                        }
                    }
                }
            }
            if (!currentBatch.isEmpty()) {
                if (isFirstBatch) { executePhase1PreCreate(currentBatch); isFirstBatch = false; }
                dispatchBatch(currentBatch);
            }
        }
        return collectAndPrintStats("Incremental", round, totalRecords, warmup);
    }

    // ==================== 两阶段策略核心方法 ====================

    /**
     * Phase 1: 预创建所有分区/子表 + 预解析CSV（不计入写入时间）
     *
     * 公平性说明：
     *   所有系统的 Phase 2 都接收已解析好的 LightCurvePoint 对象。
     *   CSV 解析是数据加载开销，不是存储引擎写入性能的一部分。
     *   TDengine/NativeRocksDB/InfluxDB 在 Phase 2 也是接收 LightCurvePoint 对象。
     *   将 LitecsDB 的 CSV 解析也移到 Phase 1，消除不公平。
     */
    private static void executePhase1PreCreate(List<String> sampleBatch) {
        System.out.println();
        System.out.println("--------------------------------------------------");
        System.out.println("  Phase 1: 预创建分区/子表 + 预解析CSV（不计入写入时间）");
        System.out.println("--------------------------------------------------");

        // LitecsDB: 扫描 healpixId + 预创建 + 预解析为 LightCurvePoint
        long start = System.currentTimeMillis();
        int preCreated = mySystem.preCreateHealpixDatabases(sampleBatch);
        preParsedHealpixData = mySystem.preParseData(sampleBatch);
        phase1LitecsDBMs = System.currentTimeMillis() - start;
        System.out.printf("  LitecsDB:     预创建 %d 个 HEALPix 分区 + 预解析数据, 耗时 %d ms%n", preCreated, phase1LitecsDBMs);

        System.out.printf("  TDengine:     子表已在初始化时预创建完毕%n");
        System.out.printf("  NativeRocksDB: ColumnFamily 已在初始化时预创建完毕%n");
        System.out.printf("  InfluxDB:     无需预创建（自动分片）%n");

        System.out.println("--------------------------------------------------");
        System.out.println("  Phase 1 完成，开始 Phase 2: 多线程并行写入");
        System.out.println("--------------------------------------------------");
        System.out.println();
    }

    /**
     * Phase 2: 多线程并行批量写入（计时到写入返回）
     *
     * 持久化语义对齐：
     *   所有系统计时到"写入 API 返回"为止：
     *   - LitecsDB:     db.write() 返回 → 数据在 WAL + memtable
     *   - NativeRocksDB: db.write() 返回 → 数据在 WAL + memtable
     *   - InfluxDB:     writeRecords() 返回 → 数据在 WAL
     *   - TDengine:     stmt.execute() 返回 → 数据在 WAL
     *
     *   后台异步落盘（memtable→SST / WAL→TSM / WAL→vnode）
     *   均不计入写入时间，与各系统真实使用场景一致。
     */
    private static void dispatchBatch(List<String> csvLines) {
        // ===== 预解析（不计入任何系统的写入时间） =====
        List<LightCurvePoint> parsedPoints = parseLines(csvLines);
        Map<Long, List<LightCurvePoint>> healpixGrouped = mySystem.preParseData(csvLines);

        // ===== 以下计时：仅写入 API 调用（不含 flush/compact） =====

        // 1. LitecsDB（WAL=on, 写入返回即持久化到 WAL）
        long start = System.currentTimeMillis();
        mySystem.distributePreParsedData(healpixGrouped);
        mySystemWriteTimeMs += (System.currentTimeMillis() - start);

        // 2. NativeRocksDB（WAL=on, 写入返回即持久化到 WAL）
        start = System.currentTimeMillis();
        nativeRocksDB.putBatch(parsedPoints);
        nativeRocksWriteTimeMs += (System.currentTimeMillis() - start);

        // 3. InfluxDB v2（writeApiBlocking 返回即持久化到 WAL）
        start = System.currentTimeMillis();
        influxDB.putBatch(parsedPoints);
        influxDBWriteTimeMs += (System.currentTimeMillis() - start);

        // 4. TDengine（execute 返回即持久化到 WAL）
        start = System.currentTimeMillis();
        tdengine.putBatch(parsedPoints);
        tdengineWriteTimeMs += (System.currentTimeMillis() - start);
    }

    // ==================== 统计与输出 ====================

    private static RunStats collectAndPrintStats(String mode, int round, long totalRecords, boolean warmup) {
        System.out.println("正在强制刷盘并执行 Compaction 以计算准确空间放大，请稍候...");

        // flush + compact 仅用于 SA 统计，不影响写入计时
        mySystem.forceFlushAll();
        nativeRocksDB.forceFlush();
        influxDB.forceFlush();
        tdengine.forceFlush();

        try { Thread.sleep(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        double mySystemThroughput = mySystemWriteTimeMs > 0 ? (totalRecords * 1000.0) / mySystemWriteTimeMs : 0;
        double nativeThroughput = nativeRocksWriteTimeMs > 0 ? (totalRecords * 1000.0) / nativeRocksWriteTimeMs : 0;
        double influxThroughput = influxDBWriteTimeMs > 0 ? (totalRecords * 1000.0) / influxDBWriteTimeMs : 0;
        double tdengineThroughput = tdengineWriteTimeMs > 0 ? (totalRecords * 1000.0) / tdengineWriteTimeMs : 0;

        double mySystemSA = mySystem.getOverallWriteAmplification();
        double nativeSA = nativeRocksDB.getWriteAmplification();
        double influxSA = influxDB.getWriteAmplification();
        double tdengineSA = tdengine.getWriteAmplification();

        System.out.println();
        System.out.println("=========================================================================");
        System.out.printf("  [%s] round=%d%s 测试完成! 数据总条数: %,d 条%n", mode, round, warmup ? " (预热)" : "", totalRecords);
        System.out.println("=========================================================================");
        System.out.println("  持久化语义: 写入API返回(数据在WAL) | flush/compact异步(不计时)");
        System.out.printf("  配置: 写入线程=%d, 分区/vgroup=%d, 每批=%d行, 缓冲=%dMB/分区%n",
                RocksDBGlobalResourceManager.WRITE_THREADS,
                RocksDBGlobalResourceManager.DISTRIBUTION_PARALLELISM,
                RocksDBGlobalResourceManager.BATCH_SIZE,
                RocksDBGlobalResourceManager.MEMTABLE_FLUSH_THRESHOLD / (1024 * 1024));
        if (phase1LitecsDBMs > 0)
            System.out.printf("  Phase 1 耗时（不计入写入）: LitecsDB=%dms%n", phase1LitecsDBMs);
        System.out.println("-------------------------------------------------------------------------");
        System.out.printf("| %-25s | %-15s | %-18s | %-10s |%n", "存储系统名称", "Phase2耗时(ms)", "吞吐量(条/秒)", "空间放大(SA)");
        System.out.println("|---------------------------|-----------------|--------------------|-----------:|");
        System.out.printf("| %-25s | %-15d | %-18.2f | %-10.2f |%n", "现有系统: LitecsDB", mySystemWriteTimeMs, mySystemThroughput, mySystemSA);
        System.out.printf("| %-25s | %-15d | %-18.2f | %-10.2f |%n", "基线 1: Native RocksDB", nativeRocksWriteTimeMs, nativeThroughput, nativeSA);
        System.out.printf("| %-25s | %-15d | %-18.2f | %-10.2f |%n", "基线 2: InfluxDB v2", influxDBWriteTimeMs, influxThroughput, influxSA);
        System.out.printf("| %-25s | %-15d | %-18.2f | %-10.2f |%n", "基线 3: TDengine", tdengineWriteTimeMs, tdengineThroughput, tdengineSA);
        System.out.println("=========================================================================");
        System.out.println();

        if (influxDB.hasWriteFailures()) System.out.println("[警告] InfluxDB 写入存在失败: " + influxDB.getFailureSummary());
        if (!"failedRows=0".equals(tdengine.getFailureSummary())) System.out.println("[警告] TDengine 写入存在失败: " + tdengine.getFailureSummary());

        RunStats stats = new RunStats();
        stats.totalRecords = totalRecords;
        stats.mySystemWriteTimeMs = mySystemWriteTimeMs;
        stats.nativeRocksWriteTimeMs = nativeRocksWriteTimeMs;
        stats.influxWriteTimeMs = influxDBWriteTimeMs;
        stats.tdengineWriteTimeMs = tdengineWriteTimeMs;
        stats.mySystemThroughput = mySystemThroughput;
        stats.nativeThroughput = nativeThroughput;
        stats.influxThroughput = influxThroughput;
        stats.tdengineThroughput = tdengineThroughput;
        stats.mySystemSA = mySystemSA;
        stats.nativeSA = nativeSA;
        stats.influxSA = influxSA;
        stats.tdengineSA = tdengineSA;
        return stats;
    }

    private static void printAverageStats(String mode, List<RunStats> statsList) {
        if (statsList.isEmpty()) return;

        double avgRecords = statsList.stream().mapToLong(s -> s.totalRecords).average().orElse(0);
        double avgMyWriteMs = statsList.stream().mapToLong(s -> s.mySystemWriteTimeMs).average().orElse(0);
        double avgNativeWriteMs = statsList.stream().mapToLong(s -> s.nativeRocksWriteTimeMs).average().orElse(0);
        double avgInfluxWriteMs = statsList.stream().mapToLong(s -> s.influxWriteTimeMs).average().orElse(0);
        double avgTdengineWriteMs = statsList.stream().mapToLong(s -> s.tdengineWriteTimeMs).average().orElse(0);
        double avgMyThroughput = statsList.stream().mapToDouble(s -> s.mySystemThroughput).average().orElse(0);
        double avgNativeThroughput = statsList.stream().mapToDouble(s -> s.nativeThroughput).average().orElse(0);
        double avgInfluxThroughput = statsList.stream().mapToDouble(s -> s.influxThroughput).average().orElse(0);
        double avgTdengineThroughput = statsList.stream().mapToDouble(s -> s.tdengineThroughput).average().orElse(0);
        double avgMySA = statsList.stream().mapToDouble(s -> s.mySystemSA).average().orElse(0);
        double avgNativeSA = statsList.stream().mapToDouble(s -> s.nativeSA).average().orElse(0);
        double avgInfluxSA = statsList.stream().mapToDouble(s -> s.influxSA).average().orElse(0);
        double avgTdengineSA = statsList.stream().mapToDouble(s -> s.tdengineSA).average().orElse(0);

        System.out.println();
        System.out.println("#########################################################################");
        System.out.printf("  [%s] %d 轮平均结果 | 平均数据条数: %,.0f 条%n", mode, statsList.size(), avgRecords);
        System.out.println("  策略: 两阶段(Phase1预创建+预解析 + Phase2多线程写入)");
        System.out.printf("  配置: 写入线程=%d, 分区/vgroup=%d, 每批=%d行, 缓冲=%dMB/分区%n",
                RocksDBGlobalResourceManager.WRITE_THREADS,
                RocksDBGlobalResourceManager.DISTRIBUTION_PARALLELISM,
                RocksDBGlobalResourceManager.BATCH_SIZE,
                RocksDBGlobalResourceManager.MEMTABLE_FLUSH_THRESHOLD / (1024 * 1024));
        System.out.println("#########################################################################");
        System.out.printf("| %-25s | %-17s | %-18s | %-10s |%n", "存储系统名称", "平均Phase2耗时(ms)", "平均吞吐量(条/秒)", "平均空间放大");
        System.out.println("|---------------------------|-------------------|--------------------|-----------:|");
        System.out.printf("| %-25s | %-17.2f | %-18.2f | %-10.2f |%n", "现有系统: LitecsDB", avgMyWriteMs, avgMyThroughput, avgMySA);
        System.out.printf("| %-25s | %-17.2f | %-18.2f | %-10.2f |%n", "基线 1: Native RocksDB", avgNativeWriteMs, avgNativeThroughput, avgNativeSA);
        System.out.printf("| %-25s | %-17.2f | %-18.2f | %-10.2f |%n", "基线 2: InfluxDB v2", avgInfluxWriteMs, avgInfluxThroughput, avgInfluxSA);
        System.out.printf("| %-25s | %-17.2f | %-18.2f | %-10.2f |%n", "基线 3: TDengine", avgTdengineWriteMs, avgTdengineThroughput, avgTdengineSA);
        System.out.println("#########################################################################");
        System.out.println();
    }

    // ==================== 工具方法 ====================

    private static int compareByTrailingNumberThenName(File a, File b) {
        long na = extractTrailingNumber(a.getName());
        long nb = extractTrailingNumber(b.getName());
        if (na != nb) return Long.compare(na, nb);
        return a.getName().compareTo(b.getName());
    }

    private static long extractTrailingNumber(String fileName) {
        Matcher matcher = TRAILING_NUMBER_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            try { return Long.parseLong(matcher.group(1)); } catch (NumberFormatException ignored) {}
        }
        return Long.MAX_VALUE;
    }

    private static List<LightCurvePoint> parseLines(List<String> csvLines) {
        List<LightCurvePoint> points = new ArrayList<>(csvLines.size());
        for (String line : csvLines) {
            try {
                if (line == null || line.isEmpty()) continue;
                String[] parts = line.split(",", -1);
                if (parts.length < 14 || "source_id".equals(parts[0].trim())) continue;
                points.add(new LightCurvePoint(
                        (long) Double.parseDouble(parts[0].trim()),
                        Double.parseDouble(parts[1].trim()),
                        Double.parseDouble(parts[2].trim()),
                        (long) Double.parseDouble(parts[3].trim()),
                        parts[4].trim(),
                        Double.parseDouble(parts[5].trim()),
                        Double.parseDouble(parts[6].trim()),
                        Double.parseDouble(parts[7].trim()),
                        Double.parseDouble(parts[8].trim()),
                        Double.parseDouble(parts[9].trim()),
                        Boolean.parseBoolean(parts[10].trim()),
                        Boolean.parseBoolean(parts[11].trim()),
                        (int) Double.parseDouble(parts[12].trim()),
                        (long) Double.parseDouble(parts[13].trim())
                ));
            } catch (Exception ignored) {}
        }
        return points;
    }
}