package org.example.utils;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.wrapper.InfluxDBClientWrapper;
import org.example.wrapper.NativeRocksDBWrapper;
import org.example.MainNode;
import org.example.RocksDBServer.StarMetadata;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * DataManager — 统一的数据导入与删除管理工具
 *
 * 用法：
 *   java DataManager insert batch <size> [--coords <file>] [--data-dir <dir>]
 *   java DataManager insert incremental <size> [--data-dir <dir>]
 *   java DataManager delete [--data-dir <dir>]
 *
 * 功能：
 *   insert: 将指定数据集导入 LitecsDB / TDengine / InfluxDB / NativeRocksDB 四个系统，
 *           导入后自动构建各系统索引（LitecsDB 时间桶索引、TDengine 子表 TAG 索引、
 *           InfluxDB TSI 索引、RocksDB LSM compaction）
 *   delete: 清除四个数据库的所有数据
 *
 * 数据集格式：
 *   batch:       generated_datasets/batch_{size}/individual_lightcurves/lightcurve_{sid}.csv
 *                表头：source_id,transit_id,band,time,mag,flux,flux_error,...  (12列，无坐标)
 *                需提供坐标文件 --coords star_coordinates.csv (source_id,ra,dec)
 *
 *   incremental: generated_datasets/incremental_{size}/observation_records_by_time/time_{ts}.csv
 *                表头：source_id,ra,dec,transit_id,band,time,mag,...  (14列，含坐标)
 *
 * TDengine 索引优化：
 *   使用 per-source 子表（每个 source_id 一张子表），TAG 含 source_id/ra/dec_val，
 *   查询时 PARTITION BY TBNAME 可高效聚合 + 获取空间坐标。
 */
public class DataManager {

    // ==================== 路径配置 ====================
    private static String DATA_BASE_DIR = "generated_datasets/";
    private static String COORDS_FILE = "gaiadr2/source_coordinates.csv";
    private static final String RESULT_DIR = "experiment9_results/";

    // LitecsDB
    private static final int HEALPIX_LEVEL = 1;
    private static final int NODE_COUNT = 2;
    private static String LITECSDB_PATH = RESULT_DIR + "litecsdb_exp9/";

    // TDengine
    private static final String TDENGINE_HOST = "127.0.0.1";
    private static final int TDENGINE_PORT = 6030;
    private static final String TDENGINE_USER = "root";
    private static final String TDENGINE_PASSWORD = "taosdata";
    private static final int TDENGINE_VGROUPS = 32;
    // per-source 子表上限（超过则回退到 hash 分桶）
    private static final int TDENGINE_MAX_SUBTABLES = 600_000;

    // InfluxDB
    private static final String INFLUX_BUCKET = "gaia_lightcurves";

    // NativeRocksDB
    private static String ROCKSDB_PATH = RESULT_DIR + "rocksdb_exp9/";

    // 写入批次大小
    private static final int WRITE_BATCH_SIZE = 50_000;

    // GAIA 时间转换
    private static final long GAIA_EPOCH_UNIX_US = 1420070400000000L;
    private static final long US_PER_DAY = 86400000000L;
    private static final long GAIA_EPOCH_UNIX_MS = 1420070400000L;
    private static final long MS_PER_DAY = 86400000L;

    // ==================== 统计 ====================
    private final AtomicLong totalRowsLoaded = new AtomicLong(0);
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
        String coordsFile = COORDS_FILE;
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
        log("DataManager: Data Import");
        log("Time: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        log("DataType: " + dataType + ", Size: " + size);
        log("================================================================================");

        try {
            // Step 1: 加载坐标映射（batch 模式必须）
            Map<Long, double[]> coordMap = null;
            if ("batch".equals(dataType)) {
                coordMap = loadCoordinates(coordsFile, size);
                if (coordMap.isEmpty()) {
                    log("[ERROR] batch 模式需要坐标文件，但坐标为空");
                    log("  请提供 --coords <file> 参数，文件格式: source_id,ra,dec");
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
            String tdDbName = "astro_exp9_" + size;

            // 3a. LitecsDB
            log("[LitecsDB] 初始化...");
            MainNode mainNode = new MainNode(NODE_COUNT, HEALPIX_LEVEL, LITECSDB_PATH);

            // 3b. TDengine（per-source 子表模式）
            log("[TDengine] 初始化...");
            Connection tdConn = createTDengineConnection();
            initTDengineSchema(tdConn, tdDbName);

            // 3c. InfluxDB
            log("[InfluxDB] 初始化...");
            InfluxDBClientWrapper influxClient = new InfluxDBClientWrapper(INFLUX_BUCKET);

            // 3d. NativeRocksDB
            log("[NativeRocksDB] 初始化...");
            NativeRocksDBWrapper rocksdbClient = new NativeRocksDBWrapper(ROCKSDB_PATH);

            // Step 4: 分批读取并写入
            log("\n--- 开始数据导入 ---");
            long importStart = System.currentTimeMillis();
            long totalRows = 0;

            // TDengine 写入连接池
            final int TD_WRITE_THREADS = 16;
            Connection[] tdWriteConns = new Connection[TD_WRITE_THREADS];
            for (int i = 0; i < TD_WRITE_THREADS; i++) {
                tdWriteConns[i] = createTDengineConnection();
                tdWriteConns[i].createStatement().execute("USE " + tdDbName);
            }
            ExecutorService tdWritePool = Executors.newFixedThreadPool(TD_WRITE_THREADS);

            // 已创建的 TDengine 子表集合
            Set<Long> createdTDSubtables = ConcurrentHashMap.newKeySet();

            int fileIdx = 0;
            for (File file : dataFiles) {
                fileIdx++;
                List<LightCurvePoint> batch = new ArrayList<>();

                // 读取文件并解析
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    boolean headerSkipped = false;
                    while ((line = reader.readLine()) != null) {
                        if (!headerSkipped) {
                            headerSkipped = true;
                            if (line.startsWith("source_id")) continue;
                        }
                        try {
                            LightCurvePoint point = parseLine(line, dataType, coordMap);
                            if (point != null) {
                                batch.add(point);
                            }
                        } catch (Exception e) {
                            totalRowsFailed.incrementAndGet();
                        }

                        // 达到批次大小时写入
                        if (batch.size() >= WRITE_BATCH_SIZE) {
                            writeBatchToAll(batch, mainNode, tdWriteConns, tdWritePool,
                                    tdDbName, createdTDSubtables, influxClient, rocksdbClient);
                            totalRows += batch.size();
                            batch.clear();
                        }
                    }
                }

                // 写入剩余数据
                if (!batch.isEmpty()) {
                    writeBatchToAll(batch, mainNode, tdWriteConns, tdWritePool,
                            tdDbName, createdTDSubtables, influxClient, rocksdbClient);
                    totalRows += batch.size();
                    batch.clear();
                }

                // 每100个文件打印进度
                if (fileIdx % 100 == 0 || fileIdx == dataFiles.size()) {
                    double pct = (double) fileIdx / dataFiles.size() * 100;
                    long elapsed = System.currentTimeMillis() - importStart;
                    double rowsPerSec = totalRows * 1000.0 / Math.max(1, elapsed);
                    log(String.format("  [进度] %d/%d 文件 (%.1f%%), 累计 %d 行, %.0f rows/s",
                            fileIdx, dataFiles.size(), pct, totalRows, rowsPerSec));
                }
            }

            long importDuration = System.currentTimeMillis() - importStart;
            log(String.format("\n[导入完成] 共 %d 行, 耗时 %.1f 秒, 速率 %.0f rows/s",
                    totalRows, importDuration / 1000.0, totalRows * 1000.0 / importDuration));
            log("  失败行数: " + totalRowsFailed.get());
            log("  TDengine 子表数: " + createdTDSubtables.size());

            // Step 5: Flush 所有系统
            log("\n--- Flush 数据到磁盘 ---");

            log("[LitecsDB] Flushing...");
            mainNode.forceFlushAllNoCompaction();

            log("[TDengine] Flushing...");
            try (Statement stmt = tdConn.createStatement()) {
                stmt.execute("FLUSH DATABASE " + tdDbName);
            }

            log("[InfluxDB] Flushing...");
            influxClient.forceFlush();

            log("[NativeRocksDB] Flushing...");
            rocksdbClient.forceFlush();

            // Step 6: 构建索引
            log("\n--- 构建索引 ---");

            // 6a. LitecsDB: 构建 DP 自适应时间桶索引
            log("[LitecsDB] 构建时间桶索引 (DP 自适应分桶)...");
            long idxStart = System.currentTimeMillis();
            mainNode.setTimeBucketDPParams(0.6, 10.0);
            mainNode.buildAllTimeBucketsOffline();
            log("[LitecsDB] 时间桶索引构建完成, 耗时 " +
                    (System.currentTimeMillis() - idxStart) + " ms");

            // Compact LitecsDB
            log("[LitecsDB] Compaction...");
            mainNode.compactOnly();

            // 6b. TDengine: 子表 TAG 索引由引擎自动维护
            //     per-source 子表的 TAG (source_id, ra, dec_val) 已在写入时创建
            //     TDengine 自动为 TAG 建立索引，无需额外操作
            log("[TDengine] TAG 索引已自动维护（per-source 子表模式）");

            // 6c. InfluxDB: TSI (Time Series Index) 由引擎自动构建
            //     基于 source_id 和 band 两个 tag 的索引在写入时自动更新
            //     强制等待 TSI compaction 完成
            log("[InfluxDB] TSI 索引已自动构建，等待 compaction 完成...");
            try { Thread.sleep(5000); } catch (InterruptedException e) {}

            // 6d. NativeRocksDB: 执行全量 compaction，构建 LSM 树索引
            log("[NativeRocksDB] 执行全量 compaction（构建 LSM 索引）...");
            idxStart = System.currentTimeMillis();
            rocksdbClient.forceFlush();  // flush + compactRange
            log("[NativeRocksDB] compaction 完成, 耗时 " +
                    (System.currentTimeMillis() - idxStart) + " ms");

            // Step 7: 验证
            log("\n--- 导入验证 ---");
            verifyImport(mainNode, tdConn, tdDbName, influxClient, rocksdbClient);

            // Step 8: 关闭资源
            log("\n--- 关闭资源 ---");
            mainNode.shutdown();
            tdWritePool.shutdown();
            tdWritePool.awaitTermination(60, TimeUnit.SECONDS);
            for (Connection c : tdWriteConns) {
                try { if (c != null) c.close(); } catch (Exception e) {}
            }
            tdConn.close();
            influxClient.close();
            rocksdbClient.close();

            log("\n[完成] 数据导入与索引构建全部完成");

        } catch (Exception e) {
            log("[ERROR] 导入失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            closeLogWriter();
        }
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
        try (Connection conn = createTDengineConnection();
             Statement stmt = conn.createStatement()) {
            // 查找并删除所有 astro_exp9_ 开头的数据库
            ResultSet rs = stmt.executeQuery("SHOW DATABASES");
            List<String> dbsToDelete = new ArrayList<>();
            while (rs.next()) {
                String name = rs.getString(1);
                if (name.startsWith("astro_exp9_") || name.equals("astro_db")) {
                    dbsToDelete.add(name);
                }
            }
            rs.close();

            for (String db : dbsToDelete) {
                stmt.execute("DROP DATABASE IF EXISTS " + db);
                log("  已删除数据库: " + db);
            }
            if (dbsToDelete.isEmpty()) {
                log("  无数据库需要删除");
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

    // ==================== 数据解析 ====================

    /**
     * 解析一行 CSV 数据为 LightCurvePoint
     *
     * @param line     CSV 行
     * @param dataType "batch" (12列) 或 "incremental" (14列)
     * @param coordMap batch 模式的坐标映射 (source_id -> [ra, dec])
     */
    private LightCurvePoint parseLine(String line, String dataType, Map<Long, double[]> coordMap) {
        String[] parts = line.split(",", -1);

        if ("incremental".equals(dataType)) {
            // 14列格式: source_id,ra,dec,transit_id,band,time,mag,...
            if (parts.length < 14) return null;
            return new LightCurvePoint(
                    (long) Double.parseDouble(parts[0].trim()),  // source_id
                    Double.parseDouble(parts[1].trim()),          // ra
                    Double.parseDouble(parts[2].trim()),          // dec
                    (long) Double.parseDouble(parts[3].trim()),  // transit_id
                    parts[4].trim(),                              // band
                    Double.parseDouble(parts[5].trim()),          // time
                    Double.parseDouble(parts[6].trim()),          // mag
                    Double.parseDouble(parts[7].trim()),          // flux
                    Double.parseDouble(parts[8].trim()),          // flux_error
                    Double.parseDouble(parts[9].trim()),          // flux_over_error
                    Boolean.parseBoolean(parts[10].trim()),       // rejected_by_photometry
                    Boolean.parseBoolean(parts[11].trim()),       // rejected_by_variability
                    (int) Double.parseDouble(parts[12].trim()),  // other_flags
                    (long) Double.parseDouble(parts[13].trim())  // solution_id
            );
        } else {
            // batch 12列格式: source_id,transit_id,band,time,mag,...
            if (parts.length < 12) return null;
            long sourceId = (long) Double.parseDouble(parts[0].trim());

            // 从坐标映射查找 ra/dec
            double ra = 0.0, dec = 0.0;
            if (coordMap != null) {
                double[] coord = coordMap.get(sourceId);
                if (coord != null) {
                    ra = coord[0];
                    dec = coord[1];
                }
            }

            return new LightCurvePoint(
                    sourceId,                                     // source_id
                    ra, dec,                                      // ra, dec (from coord map)
                    (long) Double.parseDouble(parts[1].trim()),  // transit_id
                    parts[2].trim(),                              // band
                    Double.parseDouble(parts[3].trim()),          // time
                    Double.parseDouble(parts[4].trim()),          // mag
                    Double.parseDouble(parts[5].trim()),          // flux
                    Double.parseDouble(parts[6].trim()),          // flux_error
                    Double.parseDouble(parts[7].trim()),          // flux_over_error
                    Boolean.parseBoolean(parts[8].trim()),        // rejected_by_photometry
                    Boolean.parseBoolean(parts[9].trim()),        // rejected_by_variability
                    (int) Double.parseDouble(parts[10].trim()),  // other_flags
                    (long) Double.parseDouble(parts[11].trim())  // solution_id
            );
        }
    }

    // ==================== 批量写入 ====================

    /**
     * 将一批数据同时写入四个数据库系统
     */
    private void writeBatchToAll(List<LightCurvePoint> batch,
                                 MainNode mainNode,
                                 Connection[] tdWriteConns,
                                 ExecutorService tdWritePool,
                                 String tdDbName,
                                 Set<Long> createdTDSubtables,
                                 InfluxDBClientWrapper influxClient,
                                 NativeRocksDBWrapper rocksdbClient) {

        List<LightCurvePoint> batchCopy = new ArrayList<>(batch);

        // 四个系统并行写入
        CountDownLatch latch = new CountDownLatch(4);

        // 1. LitecsDB: 通过 MainNode 分发
        Thread litecsThread = new Thread(() -> {
            try {
                Map<Long, List<LightCurvePoint>> healpixMap = mainNode.preParseData(
                        batchCopy.stream()
                                .map(this::toLitecsCSV)
                                .collect(Collectors.toList()));
                mainNode.distributePreParsedData(healpixMap);
            } catch (Exception e) {
                System.err.println("[LitecsDB] 写入失败: " + e.getMessage());
            } finally {
                latch.countDown();
            }
        }, "LitecsDB-Writer");
        litecsThread.start();

        // 2. TDengine: per-source 子表写入
        Thread tdThread = new Thread(() -> {
            try {
                writeBatchToTDengine(batchCopy, tdWriteConns, tdWritePool,
                        tdDbName, createdTDSubtables);
            } catch (Exception e) {
                System.err.println("[TDengine] 写入失败: " + e.getMessage());
            } finally {
                latch.countDown();
            }
        }, "TDengine-Writer");
        tdThread.start();

        // 3. InfluxDB
        Thread influxThread = new Thread(() -> {
            try {
                influxClient.putBatch(batchCopy);
            } catch (Exception e) {
                System.err.println("[InfluxDB] 写入失败: " + e.getMessage());
            } finally {
                latch.countDown();
            }
        }, "InfluxDB-Writer");
        influxThread.start();

        // 4. NativeRocksDB
        Thread rocksThread = new Thread(() -> {
            try {
                rocksdbClient.putBatch(batchCopy);
            } catch (Exception e) {
                System.err.println("[NativeRocksDB] 写入失败: " + e.getMessage());
            } finally {
                latch.countDown();
            }
        }, "NativeRocksDB-Writer");
        rocksThread.start();

        try {
            latch.await(300, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 将 LightCurvePoint 转为 LitecsDB 需要的 14 列 CSV
     * (source_id,ra,dec,transit_id,band,time,mag,flux,flux_error,flux_over_error,
     *  rejected_by_photometry,rejected_by_variability,other_flags,solution_id)
     */
    private String toLitecsCSV(LightCurvePoint p) {
        return String.format(Locale.US,
                "%d,%.6f,%.6f,%d,%s,%.6f,%.6f,%.6f,%.6f,%.6f,%s,%s,%d,%d",
                p.sourceId, p.ra, p.dec, p.transitId, p.band, p.time, p.mag,
                p.flux, p.fluxError, p.fluxOverError,
                p.rejectedByPhotometry, p.rejectedByVariability,
                p.otherFlags, p.solutionId);
    }

    // ==================== TDengine 写入 ====================

    /**
     * 初始化 TDengine schema（per-source 子表模式）
     *
     * 超级表结构不变，但子表按 source_id 创建（不再是 hash 分桶），
     * 每张子表的 TAGS 携带真实的 source_id, ra, dec_val。
     */
    private void initTDengineSchema(Connection conn, String dbName) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + dbName);
            log("  已清理残留数据库: " + dbName);

            stmt.execute(String.format(
                    "CREATE DATABASE IF NOT EXISTS %s " +
                            "VGROUPS %d " +
                            "BUFFER 256 " +
                            "WAL_LEVEL 1 " +
                            "WAL_FSYNC_PERIOD 3000 " +
                            "PAGES 256 " +
                            "PRECISION 'us'",
                    dbName, TDENGINE_VGROUPS));

            stmt.execute("USE " + dbName);

            // 超级表：数据列 + TAG
            stmt.execute(
                    "CREATE STABLE IF NOT EXISTS lightcurve (" +
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

            log("  TDengine 超级表创建完成 (per-source 子表模式)");
        }
    }

    /**
     * 批量写入 TDengine（per-source 子表）
     *
     * 对每个 source_id：
     *   1. 若子表不存在，CREATE TABLE IF NOT EXISTS s_{sourceId} USING lightcurve TAGS(...)
     *   2. INSERT INTO s_{sourceId} VALUES (...)
     */
    private void writeBatchToTDengine(List<LightCurvePoint> points,
                                      Connection[] conns,
                                      ExecutorService pool,
                                      String dbName,
                                      Set<Long> createdSubtables) {
        // 按 source_id 分组
        Map<Long, List<LightCurvePoint>> bySource = new HashMap<>();
        for (LightCurvePoint p : points) {
            bySource.computeIfAbsent(p.sourceId, k -> new ArrayList<>()).add(p);
        }

        // 分配到多个线程
        int numConns = conns.length;
        List<Map.Entry<Long, List<LightCurvePoint>>> entries = new ArrayList<>(bySource.entrySet());
        int chunkSize = Math.max(1, (entries.size() + numConns - 1) / numConns);

        CountDownLatch latch = new CountDownLatch(
                Math.min(numConns, (entries.size() + chunkSize - 1) / chunkSize));

        for (int t = 0; t < numConns && t * chunkSize < entries.size(); t++) {
            final int threadIdx = t;
            final int start = t * chunkSize;
            final int end = Math.min(start + chunkSize, entries.size());

            pool.submit(() -> {
                try {
                    Connection conn = conns[threadIdx];
                    for (int i = start; i < end; i++) {
                        Map.Entry<Long, List<LightCurvePoint>> entry = entries.get(i);
                        long sourceId = entry.getKey();
                        List<LightCurvePoint> srcPoints = entry.getValue();

                        // 创建子表（如果还没创建）
                        if (createdSubtables.add(sourceId)) {
                            double ra = srcPoints.get(0).ra;
                            double dec = srcPoints.get(0).dec;
                            String createSql = String.format(Locale.US,
                                    "CREATE TABLE IF NOT EXISTS %s.s_%d USING %s.lightcurve TAGS(%d, %.6f, %.6f)",
                                    dbName, sourceId, dbName, sourceId, ra, dec);
                            try (Statement stmt = conn.createStatement()) {
                                stmt.execute(createSql);
                            }
                        }

                        // 批量插入
                        StringBuilder sql = new StringBuilder();
                        sql.append("INSERT INTO ").append(dbName).append(".s_").append(sourceId).append(" VALUES ");

                        for (int j = 0; j < srcPoints.size(); j++) {
                            LightCurvePoint p = srcPoints.get(j);
                            long tsUs = GAIA_EPOCH_UNIX_US + (long) (p.time * US_PER_DAY)
                                    + Math.abs(p.transitId % 999_000);

                            if (j > 0) sql.append(" ");
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
                } catch (Exception e) {
                    System.err.println("TDengine write thread " + threadIdx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(120, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ==================== 坐标加载 ====================

    /**
     * 加载坐标映射文件
     * 搜索顺序：
     *   1. 用户指定的 --coords 文件
     *   2. generated_datasets/batch_{size}/star_coordinates.csv
     *   3. generated_datasets/batch_{size}/gaia_source_coordinates.csv
     *   4. 从同规模增量数据集中提取坐标
     */
    private Map<Long, double[]> loadCoordinates(String coordsFile, String size) {
        Map<Long, double[]> coordMap = new HashMap<>();

        // 候选坐标文件列表
        List<String> candidates = new ArrayList<>();
        if (coordsFile != null) candidates.add(coordsFile);
        candidates.add(DATA_BASE_DIR + "batch_" + size + "/star_coordinates.csv");
        candidates.add(DATA_BASE_DIR + "batch_" + size + "/gaia_source_coordinates.csv");
        candidates.add(DATA_BASE_DIR + "star_coordinates.csv");

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
                            long sourceId = (long) Double.parseDouble(parts[0].trim());
                            double ra = Double.parseDouble(parts[1].trim());
                            double dec = Double.parseDouble(parts[2].trim());
                            coordMap.put(sourceId, new double[]{ra, dec});
                        }
                    }
                } catch (IOException e) {
                    log("[WARN] 坐标文件读取失败: " + e.getMessage());
                }
                if (!coordMap.isEmpty()) return coordMap;
            }
        }

        // 回退：尝试从增量数据集提取坐标
        String incrDir = DATA_BASE_DIR + "incremental_" + size + "/observation_records_by_time/";
        File incrDirFile = new File(incrDir);
        if (incrDirFile.exists()) {
            log("[坐标] 从增量数据集提取坐标: " + incrDir);
            File[] incrFiles = incrDirFile.listFiles((dir, name) -> name.endsWith(".csv"));
            if (incrFiles != null && incrFiles.length > 0) {
                // 只需要读第一个文件（包含所有天体的快照）
                try (BufferedReader reader = new BufferedReader(new FileReader(incrFiles[0]))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.startsWith("source_id")) continue;
                        String[] parts = line.split(",", 4);
                        if (parts.length >= 3) {
                            long sourceId = (long) Double.parseDouble(parts[0].trim());
                            double ra = Double.parseDouble(parts[1].trim());
                            double dec = Double.parseDouble(parts[2].trim());
                            coordMap.putIfAbsent(sourceId, new double[]{ra, dec});
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
            files = dir.listFiles((d, name) -> name.startsWith("lightcurve_") && name.endsWith(".csv"));
        } else {
            files = dir.listFiles((d, name) -> name.startsWith("time_") && name.endsWith(".csv"));
        }
        if (files == null) return Collections.emptyList();

        List<File> fileList = Arrays.asList(files);
        // 排序确保确定性
        fileList.sort(Comparator.comparing(File::getName));
        return fileList;
    }

    // ==================== 导入验证 ====================

    private void verifyImport(MainNode mainNode, Connection tdConn, String tdDbName,
                              InfluxDBClientWrapper influx, NativeRocksDBWrapper rocksdb) {
        // LitecsDB
        try {
            List<StarMetadata> stars = mainNode.getAllStarsMetadata();
            log(String.format("  LitecsDB:     %d 个天体", stars.size()));
        } catch (Exception e) {
            log("  LitecsDB:     验证失败 - " + e.getMessage());
        }

        // TDengine
        try (Statement stmt = tdConn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) FROM " + tdDbName + ".lightcurve")) {
            if (rs.next()) {
                log(String.format("  TDengine:     %d 行", rs.getLong(1)));
            }
        } catch (Exception e) {
            log("  TDengine:     验证失败 - " + e.getMessage());
        }

        // InfluxDB (行数难以精确统计，跳过)
        log("  InfluxDB:     写入完成 (bucket=" + INFLUX_BUCKET + ")");

        // NativeRocksDB (行数统计需要全量扫描，跳过)
        log("  NativeRocksDB: 写入完成");
    }

    // ==================== TDengine 连接 ====================

    private Connection createTDengineConnection() throws SQLException {
        String url = String.format("jdbc:TAOS://%s:%d/?charset=UTF-8", TDENGINE_HOST, TDENGINE_PORT);
        Properties props = new Properties();
        props.setProperty("user", TDENGINE_USER);
        props.setProperty("password", TDENGINE_PASSWORD);
        return DriverManager.getConnection(url, props);
    }

    // ==================== 工具方法 ====================

    private void deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteDirectory(child);
                }
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