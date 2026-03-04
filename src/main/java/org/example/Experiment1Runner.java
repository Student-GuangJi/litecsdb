package org.example;

import org.example.wrapper.*;
import org.example.RocksDBServer.LightCurvePoint;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class Experiment1Runner {
    private static final int BATCH_SIZE = 2000000;
    private static final long MAX_RECORDS = 2000000;

    // 初始化系统
    static MainNode mySystem = new MainNode(4, 2);
    static NativeRocksDBWrapper nativeRocksDB = new NativeRocksDBWrapper();
    static InfluxDBClientWrapper influxDB = new InfluxDBClientWrapper();
    // static TDengineClientWrapper tdengine = new TDengineClientWrapper();

    // 用于分别统计各系统的累积纯写入耗时 (ms)
    static long mySystemWriteTimeMs = 0;
    static long nativeRocksWriteTimeMs = 0;
    static long influxDBWriteTimeMs = 0;
    static long tdengineWriteTimeMs = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("=== 实验一：时域光变曲线写入性能评估 ===\n");

        // ---------------- 模式 1：Batch 测试 ----------------
        initSystems();
        runBatchTest("gaiadr2/source_coordinates.csv", "gaiadr2/individual_lightcurves/");
        closeSystems();

        // 🚀 核心修复：彻底清空硬盘数据，保证下一个测试是绝对干净的初始状态！
//        cleanDirectories();

        // ---------------- 模式 2：Incremental 测试 ----------------
        initSystems();
        runIncrementalTest("gaiadr2/observation_records_by_time/");
        closeSystems();
    }

    private static void initSystems() {
        mySystemWriteTimeMs = 0;
        nativeRocksWriteTimeMs = 0;
        influxDBWriteTimeMs = 0;
    }

    private static void closeSystems() {
        if (mySystem != null) mySystem.shutdown();
        if (nativeRocksDB != null) nativeRocksDB.close();
        if (influxDB != null) influxDB.close();
    }

    private static void cleanDirectories() {
        System.out.println("\n--- 正在彻底清理底层物理存储，确保测试环境隔离 ---");
        deleteDirectory(new File("LitecsDB_Data"));
        deleteDirectory(new File("nativeRocksDB"));
        if (influxDB != null) {
            influxDB.clearData(); // 调用 InfluxDB 的清理 API
        }
        try { Thread.sleep(3000); } catch (InterruptedException e) {}
    }

    private static void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) deleteDirectory(file);
                    else file.delete();
                }
            }
            dir.delete();
        }
    }

    private static void runBatchTest(String coordFile, String lcDir) throws Exception {
        System.out.println(">>> 开始执行 [Batch] 批量聚集导入测试");;
        Map<Long, String> coordsMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(coordFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if(parts[0].equals("source_id")) continue; // 跳过表头
                coordsMap.put(Long.parseLong(parts[0]), parts[1] + "," + parts[2]);
            }
        }

        long totalRecords = 0;
        List<String> currentBatch = new ArrayList<>(BATCH_SIZE);

        // 2. 遍历单个天体文件并做 Join
        File dir = new File(lcDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".csv"));
        if (files != null) {
            batchLoop:
            for (File file : files) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split(",");
                        if(parts[0].equals("source_id")) continue; // 跳过表头

                        long sourceId = Long.parseLong(parts[0]);
                        String coords = coordsMap.getOrDefault(sourceId, "0.0,0.0");

                        // 组装成 MainNode 需要的 14 字段格式
                        String joinedLine = parts[0] + "," + coords + "," +
                                String.join(",", Arrays.copyOfRange(parts, 1, parts.length));

                        currentBatch.add(joinedLine);
                        totalRecords++;

                        if (currentBatch.size() >= BATCH_SIZE) {
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
                dispatchBatch(currentBatch);
            }
        }

        printStats("Batch", totalRecords);
    }

    private static void runIncrementalTest(String timeDir) throws Exception {
        System.out.println(">>> 开始执行 [Incremental] 增量碎片化导入测试");
        long totalRecords = 0;
        List<String> currentBatch = new ArrayList<>(BATCH_SIZE);

        File dir = new File(timeDir);
        File[] files = dir.listFiles((d, name) -> name.startsWith("time_") && name.endsWith(".csv"));

        if (files != null) {
            // 确保按时间顺序流式读取
            Arrays.sort(files, Comparator.comparing(File::getName));
            incrementalLoop:
            for (File file : files) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        currentBatch.add(line);
                        totalRecords++;

                        if (currentBatch.size() >= BATCH_SIZE) {
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
                dispatchBatch(currentBatch);
            }
        }

        printStats("Incremental", totalRecords);
    }

    private static void dispatchBatch(List<String> csvLines) {
        // 1. 测试现有系统 (HEALPix+RocksDB)
        long start = System.currentTimeMillis();
        mySystem.distributeDataBatch(csvLines);
        mySystemWriteTimeMs += (System.currentTimeMillis() - start);

        // 解析对象供基线系统使用
        List<LightCurvePoint> points = parseLines(csvLines);

        // 2. 测试原生 RocksDB
        start = System.currentTimeMillis();
        nativeRocksDB.putBatch(points);
        nativeRocksWriteTimeMs += (System.currentTimeMillis() - start);

        // 3. 测试 InfluxDB
        start = System.currentTimeMillis();
        influxDB.putBatch(points);
        influxDBWriteTimeMs += (System.currentTimeMillis() - start);

        // 4. 测试 TDengine (如有)
        // start = System.currentTimeMillis();
        // tdengine.putBatch(points);
        // tdengineWriteTimeMs += (System.currentTimeMillis() - start);
    }

    private static void printStats(String mode, long totalRecords) {
        System.out.println("正在强制刷盘并执行合并(Compaction)以计算准确写放大，请稍候...");

        // 强制所有系统刷盘以计算静态物理占用
        nativeRocksDB.forceFlush();
        mySystem.forceFlushAll();
        influxDB.forceFlush();
        // tdengine.forceFlush();

        // 给予后台线程足够的落盘时间
        try { Thread.sleep(5000); } catch (InterruptedException e) {}

        // 计算各系统吞吐量
        double mySystemThroughput = mySystemWriteTimeMs > 0 ? (totalRecords * 1000.0) / mySystemWriteTimeMs : 0;
        double nativeThroughput = nativeRocksWriteTimeMs > 0 ? (totalRecords * 1000.0) / nativeRocksWriteTimeMs : 0;
        double influxThroughput = influxDBWriteTimeMs > 0 ? (totalRecords * 1000.0) / influxDBWriteTimeMs : 0;

        // 获取各系统写放大 (WA)
        double mySystemWA = mySystem.getOverallWriteAmplification();
        double nativeWA = nativeRocksDB.getWriteAmplification();
        double influxWA = influxDB.getWriteAmplification();

        // 打印精美对比表格
        System.out.println("\n=========================================================================");
        System.out.printf("  [%s] 模式测试完成！数据总条数: %,d 条\n", mode, totalRecords);
        System.out.println("=========================================================================");
        System.out.printf("| %-25s | %-15s | %-18s | %-10s |\n", "存储系统名称", "纯写入耗时(ms)", "吞吐量(条/秒)", "写放大(WA)");
        System.out.println("|---------------------------|-----------------|--------------------|------------|");
        System.out.printf("| %-25s | %-15d | %-18.2f | %-10.2f |\n", "现有系统 (HEALPix 分区)", mySystemWriteTimeMs, mySystemThroughput, mySystemWA);
        System.out.printf("| %-25s | %-15d | %-18.2f | %-10.2f |\n", "基线 1: Native RocksDB", nativeRocksWriteTimeMs, nativeThroughput, nativeWA);
        System.out.printf("| %-25s | %-15d | %-18.2f | %-10.2f |\n", "基线 2: InfluxDB v2", influxDBWriteTimeMs, influxThroughput, influxWA);
        // System.out.printf("| %-25s | %-15d | %-18.2f | %-10.2f |\n", "基线 3: TDengine", tdengineWriteTimeMs, tdengineThroughput, tdengineWA);
        System.out.println("=========================================================================\n");
    }

    private static List<LightCurvePoint> parseLines(List<String> csvLines) {
        return csvLines.stream()
                .map(line -> {
                    try {
                        if (line == null || line.trim().isEmpty()) {
                            return null;
                        }
                        String[] parts = line.split(",", -1);
                        if (parts.length < 14) {
                            System.out.println(line);
                            return null;
                        }
                        return new LightCurvePoint(
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
                        );
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}