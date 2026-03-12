package org.example.utils;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.wrapper.InfluxDBClientWrapper;
import org.example.wrapper.NativeRocksDBWrapper;
import org.example.wrapper.TDengineClientWrapper;
import org.example.MainNode;
import org.example.RocksDBServer.StarMetadata;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DataManager — 统一的数据导入与删除管理工具
 *
 * 用法：
 *   java DataManager insert batch <size> [--coords <file>] [--data-dir <dir>]
 *   java DataManager insert incremental <size> [--data-dir <dir>]
 *   java DataManager delete [--data-dir <dir>]
 *
 * 写入策略（对齐 Experiment1Runner 两阶段高性能写入）：
 *   Phase 1: 预创建分区/子表 + 预解析 CSV（不计入写入时间）
 *   Phase 2: 顺序写入各系统（每个系统内部已有 16 线程并行）
 *
 * 性能关键点：
 *   - 大批量: 200 万行/批（与 Experiment1Runner 对齐）
 *   - 一次解析: CSV 只解析一次为 LightCurvePoint，所有系统复用
 *   - 直接分组: LitecsDB 直接按 healpixId 分组写入，跳过 CSV 反序列化
 *   - 顺序写入: 各系统顺序执行（避免 4 线程协调开销，系统内部已并行）
 *   - TDengine: 使用 TDengineClientWrapper.putBatch() 内部多线程写入
 */
public class DataManager {

    // ==================== 路径配置 ====================
    private static String DATA_BASE_DIR = "generated_datasets/";
    private static final String RESULT_DIR = "experiment9_results/";

    // LitecsDB
    private static final int HEALPIX_LEVEL = 1;
    private static final int NODE_COUNT = 2;
    private static String LITECSDB_PATH = RESULT_DIR + "litecsdb_exp9/";

    // NativeRocksDB
    private static String ROCKSDB_PATH = RESULT_DIR + "rocksdb_exp9/";

    // InfluxDB
    private static final String INFLUX_BUCKET = "gaia_lightcurves";

    // TDengine
    private static final String TDENGINE_DB = "astro_exp9";

    // 写入批次大小 — 对齐 Experiment1Runner 的 2M
    private static final int BATCH_SIZE = 2_000_000;

    // ==================== 统计 ====================
    private final AtomicLong totalRowsFailed = new AtomicLong(0);
    private PrintWriter logWriter;

    // ==================== Main ====================
    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        DataManager manager = new DataManager();

        // 解析可选参数
        String coordsFile = null;
        for (int i = 0; i < args.length - 1; i++) {
            if ("--coords".equals(args[i])) coordsFile = args[i + 1];
            if ("--data-dir".equals(args[i])) DATA_BASE_DIR = args[i + 1];
        }

        String action = args[0].toLowerCase();

        switch (action) {
            case "insert":
                if (args.length < 3) {
                    System.err.println("[ERROR] insert 需要参数: <batch|incremental> <size>");
                    printUsage();
                    System.exit(1);
                }
                String dataType = args[1].toLowerCase();
                String size = args[2];
                manager.runInsert(dataType, size, coordsFile);
                break;

            case "delete":
                manager.runDelete();
                break;

            default:
                System.err.println("[ERROR] 未知操作: " + action);
                printUsage();
                System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("用法:");
        System.out.println("  java DataManager insert batch <size> [--coords <file>] [--data-dir <dir>]");
        System.out.println("  java DataManager insert incremental <size> [--data-dir <dir>]");
        System.out.println("  java DataManager delete [--data-dir <dir>]");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  java DataManager insert batch 2000000 --coords star_coordinates.csv");
        System.out.println("  java DataManager insert incremental 2000000");
        System.out.println("  java DataManager delete");
    }

    // ==================== 插入流程 ====================
    public void runInsert(String dataType, String size, String coordsFile) {
        initResultDir();
        initLogWriter("import");

        log("================================================================================");
        log("DataManager: Data Import (High-Performance Two-Phase)");
        log("Time: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        log("DataType: " + dataType + ", Size: " + size + ", BatchSize: " + BATCH_SIZE);
        log("================================================================================");

        try {
            // Step 1: 加载坐标映射（batch 模式必须）
            Map<Long, String> coordMap = null;   // sourceId -> "ra,dec"
            if ("batch".equals(dataType)) {
                coordMap = loadCoordinates(coordsFile, size);
                if (coordMap.isEmpty()) {
                    log("[ERROR] batch 模式需要坐标文件，但坐标为空");
                    return;
                }
                log("[坐标] 加载了 " + coordMap.size() + " 个天体的坐标");
            }

            // Step 2: 定位数据文件
            String dataDir = locateDataDir(dataType, size);
            if (dataDir == null) return;

            List<File> dataFiles = listDataFiles(dataType, dataDir);
            log("[数据] 找到 " + dataFiles.size() + " 个数据文件");

            // Step 3: 初始化四个数据库系统
            log("\n--- 初始化数据库系统 ---");

            log("[LitecsDB] 初始化...");
            MainNode mainNode = new MainNode(NODE_COUNT, HEALPIX_LEVEL, LITECSDB_PATH);

            log("[TDengine] 初始化...");
            TDengineClientWrapper tdengine = new TDengineClientWrapper(TDENGINE_DB);

            log("[InfluxDB] 初始化...");
            InfluxDBClientWrapper influxDB = new InfluxDBClientWrapper(INFLUX_BUCKET);

            log("[NativeRocksDB] 初始化...");
            NativeRocksDBWrapper nativeRocksDB = new NativeRocksDBWrapper(ROCKSDB_PATH);

            // Step 4: 两阶段写入
            log("\n--- Phase 1: 预创建分区 + Phase 2: 批量写入 ---");
            long importStart = System.currentTimeMillis();
            long totalRows = 0;

            // 累积大批量的 CSV 行缓冲区
            List<String> csvBuffer = new ArrayList<>(BATCH_SIZE);
            boolean phase1Done = false;
            int fileIdx = 0;

            for (File file : dataFiles) {
                fileIdx++;

                try (BufferedReader reader = new BufferedReader(new FileReader(file), 1 << 16)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isEmpty() || line.startsWith("source_id")) continue;

                        // batch 模式：拼接坐标为 14 列格式
                        if ("batch".equals(dataType)) {
                            String joined = joinBatchLineWithCoords(line, coordMap);
                            if (joined == null) { totalRowsFailed.incrementAndGet(); continue; }
                            csvBuffer.add(joined);
                        } else {
                            // incremental 模式：已经是 14 列
                            csvBuffer.add(line);
                        }

                        totalRows++;

                        // 达到批量大小时执行写入
                        if (csvBuffer.size() >= BATCH_SIZE) {
                            if (!phase1Done) {
                                executePhase1(csvBuffer, mainNode);
                                phase1Done = true;
                            }
                            dispatchBatch(csvBuffer, mainNode, tdengine, influxDB, nativeRocksDB);
                            csvBuffer.clear();
                        }
                    }
                }

                // 每20000个文件打印进度
                if (fileIdx % 20000 == 0 || fileIdx == dataFiles.size()) {
                    double pct = (double) fileIdx / dataFiles.size() * 100;
                    long elapsed = System.currentTimeMillis() - importStart;
                    double rowsPerSec = totalRows * 1000.0 / Math.max(1, elapsed);
                    log(String.format("  [进度] %d/%d 文件 (%.1f%%), 累计 %,d 行, %.0f rows/s",
                            fileIdx, dataFiles.size(), pct, totalRows, rowsPerSec));
                }
            }

            // 写入剩余数据
            if (!csvBuffer.isEmpty()) {
                if (!phase1Done) {
                    executePhase1(csvBuffer, mainNode);
                }
                dispatchBatch(csvBuffer, mainNode, tdengine, influxDB, nativeRocksDB);
                csvBuffer.clear();
            }

            long importDuration = System.currentTimeMillis() - importStart;
            log(String.format("\n[导入完成] 共 %,d 行, 耗时 %.1f 秒, 速率 %.0f rows/s",
                    totalRows, importDuration / 1000.0, totalRows * 1000.0 / importDuration));
            log("  失败行数: " + totalRowsFailed.get());

            // Step 5: Flush 所有系统
            log("\n--- Flush 数据到磁盘 ---");

            log("[LitecsDB] Flushing...");
            mainNode.forceFlushAllNoCompaction();

            log("[TDengine] Flushing...");
            tdengine.forceFlush();

            log("[InfluxDB] Flushing...");
            influxDB.forceFlush();

            log("[NativeRocksDB] Flushing...");
            nativeRocksDB.forceFlush();

            try { Thread.sleep(5000); } catch (InterruptedException e) {}

            // Step 6: 构建索引
            log("\n--- 构建索引 ---");

            log("[LitecsDB] 构建时间桶索引 (DP 自适应分桶)...");
            long idxStart = System.currentTimeMillis();
            mainNode.setTimeBucketDPParams(0.6, 10.0);
            mainNode.buildAllTimeBucketsOffline();
            log("[LitecsDB] 时间桶索引构建完成, 耗时 " +
                    (System.currentTimeMillis() - idxStart) + " ms");

            log("[LitecsDB] Compaction...");
            mainNode.compactOnly();

            log("[TDengine] TAG 索引已自动维护");
            log("[InfluxDB] TSI 索引已自动构建");

            log("[NativeRocksDB] 执行全量 compaction...");
            idxStart = System.currentTimeMillis();
            nativeRocksDB.forceFlush();
            log("[NativeRocksDB] compaction 完成, 耗时 " +
                    (System.currentTimeMillis() - idxStart) + " ms");

            // Step 7: 验证
            log("\n--- 导入验证 ---");
            verifyImport(mainNode, totalRows);

            // Step 8: 关闭资源
            log("\n--- 关闭资源 ---");
            mainNode.shutdown();
            tdengine.close();
            influxDB.close();
            nativeRocksDB.close();

            log("\n[完成] 数据导入与索引构建全部完成");

        } catch (Exception e) {
            log("[ERROR] 导入失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeLogWriter();
        }
    }

    // ==================== 两阶段策略核心 ====================

    /**
     * Phase 1: 预创建 LitecsDB 的 HEALPix 分区（不计入写入时间）
     *
     * 对齐 Experiment1Runner.executePhase1PreCreate：
     *   扫描样本数据提取 healpixId → 并行预创建 RocksDB 实例
     *   TDengine/NativeRocksDB/InfluxDB 的分区已在初始化时创建
     */
    private void executePhase1(List<String> sampleBatch, MainNode mainNode) {
        log("\n  Phase 1: 预创建 HEALPix 分区...");
        long start = System.currentTimeMillis();
        int preCreated = mainNode.preCreateHealpixDatabases(sampleBatch);
        log(String.format("  LitecsDB: 预创建 %d 个 HEALPix 分区, 耗时 %d ms",
                preCreated, System.currentTimeMillis() - start));
        log("  TDengine/NativeRocksDB/InfluxDB: 分区已在初始化时创建");
        log("  Phase 1 完成，开始 Phase 2: 批量写入\n");
    }

    /**
     * Phase 2: 批量写入四个系统
     *
     * 对齐 Experiment1Runner.dispatchBatch：
     *   1. CSV 只解析一次 → List<LightCurvePoint> (所有系统共用)
     *   2. LitecsDB 额外按 healpixId 分组 → Map<Long, List<LightCurvePoint>>
     *   3. 顺序写入各系统（每个系统内部已有 16 线程并行写入）
     *      不使用外层 4 线程并行，避免线程协调开销和资源争抢
     */
    private void dispatchBatch(List<String> csvLines,
                               MainNode mainNode,
                               TDengineClientWrapper tdengine,
                               InfluxDBClientWrapper influxDB,
                               NativeRocksDBWrapper nativeRocksDB) {

        // ===== 预解析（不计入任何系统的写入时间） =====
        // 1. 解析为 LightCurvePoint 列表（通用，所有系统复用）
        List<LightCurvePoint> parsedPoints = parseLines(csvLines);

        // 2. LitecsDB 专用：按 healpixId 分组（直接从已解析对象分组，不经 CSV）
        Map<Long, List<LightCurvePoint>> healpixGrouped = groupByHealpix(parsedPoints, mainNode);

        // ===== 以下是写入（顺序执行，每个系统内部多线程并行） =====

        // 1. LitecsDB
        mainNode.distributePreParsedData(healpixGrouped);

        // 2. TDengine（内部 16 线程 hash 分桶并行写入）
        tdengine.putBatch(parsedPoints);

        // 3. InfluxDB（内部 16 线程 line protocol 并行写入）
        influxDB.putBatch(parsedPoints);

        // 4. NativeRocksDB（内部 16 线程 CF 并行写入）
        nativeRocksDB.putBatch(parsedPoints);
    }

    /**
     * 直接将已解析的 LightCurvePoint 按 healpixId 分组
     *
     * 关键优化：跳过 CSV 重解析，直接用 calculateHealpixId 计算分区 ID
     */
    private Map<Long, List<LightCurvePoint>> groupByHealpix(
            List<LightCurvePoint> points, MainNode mainNode) {
        Map<Long, List<LightCurvePoint>> map = new HashMap<>();
        for (LightCurvePoint p : points) {
            long healpixId = mainNode.calculateHealpixId(p.ra, p.dec);
            map.computeIfAbsent(healpixId, k -> new ArrayList<>()).add(p);
        }
        return map;
    }

    // ==================== 数据解析 ====================

    /**
     * Batch 模式：将 12 列 CSV 行拼接坐标变成 14 列
     * 对齐 Experiment1Runner.runBatchTest 的坐标拼接逻辑
     */
    private String joinBatchLineWithCoords(String line, Map<Long, String> coordMap) {
        try {
            int firstComma = line.indexOf(',');
            if (firstComma < 0) return null;
            long sourceId = Long.parseLong(line.substring(0, firstComma).trim());
            String coords = coordMap.getOrDefault(sourceId, "0.0,0.0");
            return line.substring(0, firstComma) + "," + coords + "," + line.substring(firstComma + 1);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 解析 14 列 CSV → LightCurvePoint
     * 完全对齐 Experiment1Runner.parseLines
     */
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

    // ==================== 删除流程 ====================
    public void runDelete() {
        initLogWriter("delete");

        log("================================================================================");
        log("DataManager: Delete All Data");
        log("Time: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        log("================================================================================");

        // 1. 删除 LitecsDB
        log("\n[LitecsDB] 删除数据...");
        try {
            File litecsDir = new File(LITECSDB_PATH);
            if (litecsDir.exists()) {
                deleteDirectory(litecsDir);
                log("  已删除: " + LITECSDB_PATH);
            } else {
                log("  目录不存在: " + LITECSDB_PATH);
            }
        } catch (Exception e) {
            log("  删除失败: " + e.getMessage());
        }

        // 2. 删除 TDengine
        log("\n[TDengine] 删除数据...");
        try {
            String host = System.getenv().getOrDefault("TDENGINE_HOST", "127.0.0.1");
            int port = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_PORT", "6030"));
            String url = String.format("jdbc:TAOS://%s:%d/?charset=UTF-8", host, port);
            Properties props = new Properties();
            props.setProperty("user", System.getenv().getOrDefault("TDENGINE_USER", "root"));
            props.setProperty("password", System.getenv().getOrDefault("TDENGINE_PASSWORD", "taosdata"));

            try (Connection conn = DriverManager.getConnection(url, props);
                 Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SHOW DATABASES");
                List<String> dbsToDelete = new ArrayList<>();
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (name.startsWith("astro_exp9") || name.equals("astro_db")) {
                        dbsToDelete.add(name);
                    }
                }
                rs.close();
                for (String db : dbsToDelete) {
                    stmt.execute("DROP DATABASE IF EXISTS " + db);
                    log("  已删除数据库: " + db);
                }
                if (dbsToDelete.isEmpty()) log("  无数据库需要删除");
            }
        } catch (Exception e) {
            log("  TDengine 删除失败: " + e.getMessage());
        }

        // 3. 删除 InfluxDB
        log("\n[InfluxDB] 删除数据...");
        try {
            InfluxDBClientWrapper influx = new InfluxDBClientWrapper(INFLUX_BUCKET);
            influx.clearData();
            influx.close();
            log("  已清除 bucket: " + INFLUX_BUCKET);
        } catch (Exception e) {
            log("  InfluxDB 删除失败: " + e.getMessage());
        }

        // 4. 删除 NativeRocksDB
        log("\n[NativeRocksDB] 删除数据...");
        try {
            File rocksDir = new File(ROCKSDB_PATH);
            if (rocksDir.exists()) {
                deleteDirectory(rocksDir);
                log("  已删除: " + ROCKSDB_PATH);
            } else {
                log("  目录不存在: " + ROCKSDB_PATH);
            }
        } catch (Exception e) {
            log("  NativeRocksDB 删除失败: " + e.getMessage());
        }

        log("\n[完成] 所有数据库数据已删除");
        closeLogWriter();
    }

    // ==================== 坐标加载 ====================

    /**
     * 加载坐标映射（对齐 Experiment1Runner 的 coordsMap 格式）
     * 存储格式：sourceId -> "ra,dec" （字符串拼接，避免后续格式化开销）
     */
    private Map<Long, String> loadCoordinates(String coordsFile, String size) {
        Map<Long, String> coordMap = new HashMap<>();

        List<String> candidates = new ArrayList<>();
        if (coordsFile != null) candidates.add(coordsFile);
        candidates.add(DATA_BASE_DIR + "batch_" + size + "/source_coordinates.csv");
        candidates.add(DATA_BASE_DIR + "batch_" + size + "/star_coordinates.csv");
        candidates.add(DATA_BASE_DIR + "batch_" + size + "/gaia_source_coordinates.csv");
        candidates.add(DATA_BASE_DIR + "source_coordinates.csv");

        for (String path : candidates) {
            File f = new File(path);
            if (f.exists()) {
                log("[坐标] 从文件加载: " + path);
                try (BufferedReader reader = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("source_id") || line.startsWith("#")) continue;
                        String[] parts = line.split(",");
                        if (parts.length >= 3) {
                            long sourceId = Long.parseLong(parts[0].trim());
                            coordMap.put(sourceId, parts[1].trim() + "," + parts[2].trim());
                        }
                    }
                } catch (IOException e) {
                    log("[WARN] 坐标文件读取失败: " + e.getMessage());
                }
                if (!coordMap.isEmpty()) return coordMap;
            }
        }

        // 回退：从增量数据集提取
        String incrDir = DATA_BASE_DIR + "incremental_" + size + "/observation_records_by_time/";
        File incrDirFile = new File(incrDir);
        if (incrDirFile.exists()) {
            log("[坐标] 从增量数据集提取坐标: " + incrDir);
            File[] incrFiles = incrDirFile.listFiles((dir, name) -> name.endsWith(".csv"));
            if (incrFiles != null && incrFiles.length > 0) {
                try (BufferedReader reader = new BufferedReader(new FileReader(incrFiles[0]))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("source_id")) continue;
                        String[] parts = line.split(",", 4);
                        if (parts.length >= 3) {
                            long sourceId = Long.parseLong(parts[0].trim());
                            coordMap.putIfAbsent(sourceId, parts[1].trim() + "," + parts[2].trim());
                        }
                    }
                } catch (IOException e) {
                    log("[WARN] 增量文件读取失败: " + e.getMessage());
                }
            }
        }
        return coordMap;
    }

    // ==================== 文件查找 ====================

    private String locateDataDir(String dataType, String size) {
        String dir;
        if ("batch".equals(dataType)) {
            dir = DATA_BASE_DIR + "batch_" + size + "/individual_lightcurves/";
        } else {
            dir = DATA_BASE_DIR + "incremental_" + size + "/observation_records_by_time/";
        }
        File dirFile = new File(dir);
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            log("[ERROR] 数据目录不存在: " + dir);
            return null;
        }
        log("[数据] 数据目录: " + dir);
        return dir;
    }

    private List<File> listDataFiles(String dataType, String dataDir) {
        File dir = new File(dataDir);
        File[] files;
        if ("batch".equals(dataType)) {
            files = dir.listFiles((d, name) -> name.endsWith(".csv"));
        } else {
            files = dir.listFiles((d, name) -> name.startsWith("time_") && name.endsWith(".csv"));
        }
        if (files == null) return Collections.emptyList();
        List<File> fileList = Arrays.asList(files);
        fileList.sort(Comparator.comparing(File::getName));
        return fileList;
    }

    // ==================== 验证 ====================

    private void verifyImport(MainNode mainNode, long expectedRows) {
        try {
            List<StarMetadata> stars = mainNode.getAllStarsMetadata();
            log(String.format("  LitecsDB:      %d 个天体", stars.size()));
        } catch (Exception e) {
            log("  LitecsDB:      验证失败 - " + e.getMessage());
        }
        log(String.format("  预期总行数:     %,d", expectedRows));
    }

    // ==================== 工具方法 ====================

    private void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) deleteDirectory(child);
            }
        }
        dir.delete();
    }

    private void initResultDir() {
        try { Files.createDirectories(Paths.get(RESULT_DIR)); } catch (IOException e) {}
    }

    private void initLogWriter(String prefix) {
        try {
            initResultDir();
            String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            logWriter = new PrintWriter(new FileWriter(RESULT_DIR + "datamanager_" + prefix + "_" + ts + ".log"));
        } catch (IOException e) {
            System.err.println("Log writer init failed: " + e.getMessage());
        }
    }

    private void closeLogWriter() {
        if (logWriter != null) { logWriter.close(); logWriter = null; }
    }

    private void log(String msg) {
        System.out.println(msg);
        if (logWriter != null) { logWriter.println(msg); logWriter.flush(); }
    }
}