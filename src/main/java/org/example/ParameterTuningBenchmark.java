package org.example;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.HealpixUtil;
import org.rocksdb.*;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * LitecsDB 参数调优基准测试
 *
 * 计时策略（与 Experiment1Runner 完全对齐）：
 *   Phase 1（不计时）：preCreateHealpixDatabases + preParseData
 *   Phase 2（计时）  ：distributePreParsedData() 返回即停表
 *   flush/compact    ：不计入写入时间，仅用于 SA 统计
 *
 * 两种模式：
 *   A) 可复用 MainNode 的参数（nodesCount, level）→ 直接用 MainNode
 *   B) 需要自定义 RocksDB 参数（writeBufferSize, compression 等）→ 用 NodeWriter
 *
 * 用法：
 *   mvn clean package -DskipTests
 *   java -Xmx8g -cp LitecsDB-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *       org.example.ParameterTuningBenchmark \
 *       generated_datasets/incremental_2000000/observation_records_by_time/ 2000000
 */
public class ParameterTuningBenchmark {

    // =============== 基线默认值（与 Experiment1Runner 的 initSystems 完全对齐）===============
    static final int DEF_WRITE_THREADS = 16;
    static final int DEF_BATCH_SIZE    = 10_000;
    static final int DEF_NODES         = 2;     // Experiment1Runner: new MainNode(2, 1)
    static final int DEF_LEVEL         = 1;     // Experiment1Runner: new MainNode(2, 1)
    static final long DEF_WB_MB        = 256;
    static final int DEF_MAX_WB        = 4;
    static final int DEF_MIN_MERGE     = 2;
    static final int DEF_L0_TRIGGER    = 8;
    static final CompressionType DEF_COMP = CompressionType.LZ4_COMPRESSION;
    static final boolean DEF_SYNC      = false;
    static final boolean DEF_NO_WAL    = false;

    // =============== 实验控制 ===============
    static final int REPS   = 10;
    static final int WARMUP = 1;
    static final String OUT_DIR = "experiment_results/";
    static final String TMP_DIR = "experiment_tmp/";

    // =============== 数据缓存 ===============
    private List<String> csvLines;
    private int totalRecords;
    private final List<Record> results = new ArrayList<>();

    // =============== 入口 ===============
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("用法: java -cp LitecsDB-1.0-SNAPSHOT-jar-with-dependencies.jar \\");
            System.err.println("      org.example.ParameterTuningBenchmark <incremental_data_dir> [max_records]");
            System.exit(1);
        }
        RocksDB.loadLibrary();
        String dataDir = args[0];
        int maxRecords = args.length > 1 ? Integer.parseInt(args[1]) : 2_000_000;
        new ParameterTuningBenchmark().run(dataDir, maxRecords);
    }

    // =============== 主流程 ===============
    public void run(String dataDir, int maxRecords) throws Exception {
        banner("LitecsDB Parameter Tuning Benchmark");
        System.out.println("  数据目录:   " + dataDir);
        System.out.println("  最大记录数: " + maxRecords);
        System.out.println("  基线配置:   nodes=" + DEF_NODES + ", level=" + DEF_LEVEL
                + " (与 Experiment1Runner 对齐)");
        System.out.println("  计时策略:   仅计 distributePreParsedData()，不含 flush");
        System.out.println("  重复次数:   " + REPS + " (+ " + WARMUP + " warmup)");
        System.out.println();

        loadData(dataDir, maxRecords);
        new File(OUT_DIR).mkdirs();

        // Baseline
        section("Baseline");
        execMainNode(baseline(), "BASELINE", "DEFAULT");

        // 实验 1: WRITE_THREADS
        section("Exp 1/7: WRITE_THREADS");
        for (int v : new int[]{1, 2, 4, 8, 16, 32}) {
            Cfg c = baseline(); c.threads = v;
            execMainNode(c, "WRITE_THREADS", String.valueOf(v));
        }

        // 实验 2: BATCH_SIZE
        section("Exp 2/7: BATCH_SIZE");
        for (int v : new int[]{100, 500, 1_000, 5_000, 10_000, 50_000, 100_000}) {
            Cfg c = baseline(); c.batch = v;
            execMainNode(c, "BATCH_SIZE", String.valueOf(v));
        }

        // 实验 3: NODES_COUNT
        section("Exp 3/7: NODES_COUNT");
        for (int v : new int[]{1, 2, 4, 8, 16}) {
            Cfg c = baseline(); c.nodes = v;
            execMainNode(c, "NODES_COUNT", String.valueOf(v));
        }

        // 实验 4: HEALPIX_LEVEL
        section("Exp 4/7: HEALPIX_LEVEL");
        for (int v : new int[]{1, 2, 3}) {
            Cfg c = baseline(); c.level = v;
            execMainNode(c, "HEALPIX_LEVEL", String.valueOf(v));
        }

        // 实验 5: WRITE_BUFFER_SIZE_MB（需要自定义 RocksDB 参数）
        section("Exp 5/7: WRITE_BUFFER_SIZE_MB");
        for (long v : new long[]{16, 64, 128, 256, 512}) {
            Cfg c = baseline(); c.wbMB = v;
            execNodeWriter(c, "WRITE_BUFFER_SIZE_MB", String.valueOf(v));
        }

        // 实验 6: WRITE_BUFFER_NUMBER
        section("Exp 6/7: WRITE_BUFFER_NUM (max/minMerge)");
        for (int[] p : new int[][]{{2, 1}, {4, 1}, {4, 2}, {6, 2}, {8, 2}, {8, 4}}) {
            Cfg c = baseline(); c.maxWB = p[0]; c.minMerge = p[1];
            execNodeWriter(c, "WRITE_BUFFER_NUM", p[0] + "/" + p[1]);
        }

        // 实验 7: WAL / Sync / Compression
        section("Exp 7/7: WAL / Sync / Compression");
        { Cfg c = baseline(); c.noWAL = false; c.sync = false;
            execNodeWriter(c, "WAL_SYNC_COMP", "WAL=ON,Sync=OFF,LZ4"); }
        { Cfg c = baseline(); c.noWAL = true; c.sync = false;
            execNodeWriter(c, "WAL_SYNC_COMP", "WAL=OFF,Sync=OFF,LZ4"); }
        { Cfg c = baseline(); c.noWAL = false; c.sync = true;
            execNodeWriter(c, "WAL_SYNC_COMP", "WAL=ON,Sync=ON,LZ4"); }
        for (Object[] ct : new Object[][]{
                {"NONE", CompressionType.NO_COMPRESSION},
                {"SNAPPY", CompressionType.SNAPPY_COMPRESSION},
                {"LZ4", CompressionType.LZ4_COMPRESSION},
                {"ZSTD", CompressionType.ZSTD_COMPRESSION}}) {
            Cfg c = baseline(); c.comp = (CompressionType) ct[1];
            execNodeWriter(c, "WAL_SYNC_COMP", "WAL=ON,Sync=OFF," + ct[0]);
        }

        generateReport();
        System.out.println("\n✅ 全部实验完成！结果保存至 " + OUT_DIR);
    }

    // =============== 模式 A：复用 MainNode（实验 1-4）===============

    /**
     * 通过真实的 MainNode 写入，计时方式与 Experiment1Runner 完全一致：
     *   Phase 1（不计时）：preCreateHealpixDatabases + preParseData
     *   Phase 2（计时）  ：distributePreParsedData() 返回即停表
     *   flush            ：不计入写入时间
     */
    private void execMainNode(Cfg cfg, String expName, String paramVal) throws Exception {
        System.out.printf("  ▶ %-24s = %-22s ", expName, paramVal);
        System.out.flush();

        // 修改全局配置
        setStaticInt(RocksDBGlobalResourceManager.class, "WRITE_THREADS", cfg.threads);
        setStaticInt(RocksDBGlobalResourceManager.class, "BATCH_SIZE", cfg.batch);

        List<Long> times = new ArrayList<>();
        List<Double> tps = new ArrayList<>();

        for (int run = -WARMUP; run < REPS; run++) {
            String runDir = TMP_DIR + "run_" + System.nanoTime() + "/";

            // 重置全局资源管理器（应用新线程数）
            resetGlobalManager();

            MainNode mainNode = new MainNode(cfg.nodes, cfg.level, runDir);
            try {
                // Phase 1（不计时）：预创建 + 预解析
                mainNode.preCreateHealpixDatabases(csvLines);
                Map<Long, List<LightCurvePoint>> parsed = mainNode.preParseData(csvLines);

                System.gc();
                Thread.sleep(100);

                // Phase 2（计时）：仅 distributePreParsedData
                long t0 = System.currentTimeMillis();
                MainNode.DistributionResult result = mainNode.distributePreParsedData(parsed);
                long elapsed = System.currentTimeMillis() - t0;
                // ↑ 停表！不含 flush

                if (run >= 0) {
                    times.add(elapsed);
                    double tp = result.totalSuccess > 0 && elapsed > 0
                            ? result.totalSuccess * 1000.0 / elapsed : 0;
                    tps.add(tp);
                }
            } finally {
                mainNode.shutdown();
                deleteDirectory(new File(runDir));
            }
        }

        // 恢复默认值
        setStaticInt(RocksDBGlobalResourceManager.class, "WRITE_THREADS", DEF_WRITE_THREADS);
        setStaticInt(RocksDBGlobalResourceManager.class, "BATCH_SIZE", DEF_BATCH_SIZE);
        resetGlobalManager();

        recordResult(expName, paramVal, times, tps);
    }

    // =============== 模式 B：自定义 RocksDB 参数（实验 5-7）===============

    /**
     * 使用 NodeWriter 自定义 RocksDB 参数（writeBufferSize, compression, WAL 等）。
     * 计时策略同样对齐：仅计写入时间，不含 flush。
     */
    private void execNodeWriter(Cfg cfg, String expName, String paramVal) throws Exception {
        System.out.printf("  ▶ %-24s = %-22s ", expName, paramVal);
        System.out.flush();

        // 预解析（不计时，只做一次）
        Map<Integer, Map<Long, List<LightCurvePoint>>> nodeGrouped =
                parseAndGroupByNode(cfg.level, cfg.nodes);

        List<Long> times = new ArrayList<>();
        List<Double> tps = new ArrayList<>();

        for (int run = -WARMUP; run < REPS; run++) {
            String runDir = TMP_DIR + "run_" + System.nanoTime() + "/";

            // 每个 node 一个 RocksDB 实例
            Map<Integer, NodeWriter> writers = new LinkedHashMap<>();
            try {
                for (int nodeId : nodeGrouped.keySet()) {
                    String dbPath = runDir + "node" + nodeId;
                    new File(dbPath).mkdirs();
                    writers.put(nodeId, new NodeWriter(dbPath, nodeId, cfg));
                }

                System.gc();
                Thread.sleep(100);

                // Phase 2（计时）：多线程写入，停表不含 flush
                long t0 = System.currentTimeMillis();
                AtomicLong okCount = new AtomicLong(0);

                ExecutorService pool = Executors.newFixedThreadPool(cfg.threads,
                        new DaemonThreadFactory("bench-writer"));

                List<WriteTask> allTasks = new ArrayList<>();
                for (var nodeEntry : nodeGrouped.entrySet()) {
                    for (var hEntry : nodeEntry.getValue().entrySet()) {
                        allTasks.add(new WriteTask(nodeEntry.getKey(), hEntry.getKey(), hEntry.getValue()));
                    }
                }

                int numShards = Math.max(1, Math.min(cfg.threads, allTasks.size()));
                @SuppressWarnings("unchecked")
                List<WriteTask>[] shards = new List[numShards];
                for (int i = 0; i < numShards; i++) shards[i] = new ArrayList<>();
                for (int i = 0; i < allTasks.size(); i++) shards[i % numShards].add(allTasks.get(i));

                CountDownLatch latch = new CountDownLatch(numShards);
                for (int tid = 0; tid < numShards; tid++) {
                    final List<WriteTask> shard = shards[tid];
                    pool.submit(() -> {
                        try {
                            for (WriteTask task : shard) {
                                NodeWriter nw = writers.get(task.nodeId);
                                if (nw == null) continue;
                                List<LightCurvePoint> pts = task.points;
                                for (int i = 0; i < pts.size(); i += cfg.batch) {
                                    int end = Math.min(i + cfg.batch, pts.size());
                                    try {
                                        nw.writeBatch(task.healpixId, pts.subList(i, end));
                                        okCount.addAndGet(end - i);
                                    } catch (RocksDBException ex) { /* skip */ }
                                }
                            }
                        } finally { latch.countDown(); }
                    });
                }
                latch.await(600, TimeUnit.SECONDS);
                long elapsed = System.currentTimeMillis() - t0;
                // ↑ 停表！不含 flush

                pool.shutdown();
                pool.awaitTermination(30, TimeUnit.SECONDS);

                if (run >= 0) {
                    times.add(elapsed);
                    double tp = okCount.get() > 0 && elapsed > 0
                            ? okCount.get() * 1000.0 / elapsed : 0;
                    tps.add(tp);
                }
            } finally {
                for (NodeWriter nw : writers.values()) {
                    try { nw.close(); } catch (Exception ignored) {}
                }
                writers.clear();
                deleteDirectory(new File(runDir));
            }
        }

        recordResult(expName, paramVal, times, tps);
    }

    // =============== 结果记录 ===============

    private void recordResult(String expName, String paramVal, List<Long> times, List<Double> tps) {
        double avgTime = times.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgTP = tps.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double stdTime = stddev(times.stream().map(Long::doubleValue).collect(Collectors.toList()), avgTime);
        double stdTP = stddev(tps, avgTP);

        results.add(new Record(expName, paramVal, avgTime, stdTime, avgTP, stdTP,
                totalRecords, times, tps));

        System.out.printf("→ %7.0f ± %5.0f ms | %9.0f ± %7.0f rec/s%n",
                avgTime, stdTime, avgTP, stdTP);
    }

    // =============== 全局资源管理器重置 ===============

    private void resetGlobalManager() {
        try {
            Field f = RocksDBGlobalResourceManager.class.getDeclaredField("instance");
            f.setAccessible(true);
            RocksDBGlobalResourceManager old = (RocksDBGlobalResourceManager) f.get(null);
            if (old != null) old.close();
            f.set(null, null);
        } catch (Exception e) {
            System.err.println("[WARN] 无法重置 GlobalResourceManager: " + e.getMessage());
        }
    }

    private void setStaticInt(Class<?> clazz, String fieldName, int value) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            // 尝试 Java 8-11
            try {
                Field modifiers = Field.class.getDeclaredField("modifiers");
                modifiers.setAccessible(true);
                modifiers.setInt(field, field.getModifiers() & ~java.lang.reflect.Modifier.FINAL);
                field.setInt(null, value);
            } catch (NoSuchFieldException e) {
                // Java 12+: 使用 Unsafe
                sun.misc.Unsafe unsafe = getUnsafe();
                if (unsafe != null) {
                    long offset = unsafe.staticFieldOffset(field);
                    unsafe.putInt(clazz, offset, value);
                }
            }
        } catch (Exception e) {
            System.err.println("[WARN] 无法设置 " + fieldName + "=" + value + ": " + e.getMessage());
        }
    }

    private sun.misc.Unsafe getUnsafe() {
        try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (sun.misc.Unsafe) f.get(null);
        } catch (Exception e) { return null; }
    }

    // =============== NodeWriter（实验 5-7 用）===============

    static class NodeWriter implements AutoCloseable {
        private final RocksDB db;
        private final ColumnFamilyHandle lcHandle;
        private final List<ColumnFamilyHandle> allHandles;
        private final WriteOptions writeOpts;
        private final DBOptions dbOpts;
        private final ColumnFamilyOptions cfOpts;

        NodeWriter(String path, int nodeId, Cfg cfg) throws RocksDBException {
            BlockBasedTableConfig tblCfg = new BlockBasedTableConfig().setBlockSize(16 * 1024);

            cfOpts = new ColumnFamilyOptions()
                    .setCompressionType(cfg.comp)
                    .setTableFormatConfig(tblCfg)
                    .setWriteBufferSize(cfg.wbMB * 1024L * 1024L)
                    .setMaxWriteBufferNumber(cfg.maxWB)
                    .setMinWriteBufferNumberToMerge(cfg.minMerge)
                    .setLevel0FileNumCompactionTrigger(cfg.l0Trigger)
                    .setLevel0SlowdownWritesTrigger(32)
                    .setLevel0StopWritesTrigger(48);

            List<ColumnFamilyDescriptor> descs = Arrays.asList(
                    new ColumnFamilyDescriptor("default".getBytes(StandardCharsets.UTF_8), cfOpts),
                    new ColumnFamilyDescriptor("lightcurve".getBytes(StandardCharsets.UTF_8), cfOpts),
                    new ColumnFamilyDescriptor("metadata".getBytes(StandardCharsets.UTF_8), cfOpts),
                    new ColumnFamilyDescriptor("time_buckets".getBytes(StandardCharsets.UTF_8), cfOpts));

            dbOpts = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setMaxBackgroundJobs(Math.min(cfg.threads, 16))
                    .setIncreaseParallelism(Math.max(4, Runtime.getRuntime().availableProcessors()))
                    .setBytesPerSync(4L * 1024 * 1024);

            allHandles = new ArrayList<>();
            db = RocksDB.open(dbOpts, path, descs, allHandles);
            lcHandle = allHandles.get(1);

            writeOpts = new WriteOptions().setSync(cfg.sync).setDisableWAL(cfg.noWAL);
        }

        void writeBatch(long healpixId, List<LightCurvePoint> pts) throws RocksDBException {
            try (WriteBatch batch = new WriteBatch()) {
                for (LightCurvePoint p : pts) {
                    batch.put(lcHandle, buildKey(healpixId, p.sourceId, p.band, p.time), serialize(p));
                }
                db.write(writeOpts, batch);
            }
        }

        @Override
        public void close() {
            if (writeOpts != null) writeOpts.close();
            for (ColumnFamilyHandle h : allHandles) { if (h != null) h.close(); }
            if (db != null) db.close();
            if (dbOpts != null) dbOpts.close();
            if (cfOpts != null) cfOpts.close();
        }

        private byte[] buildKey(long healpixId, long sourceId, String band, double time) {
            byte[] b = band.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(25 + b.length);
            buf.putLong(healpixId).putLong(sourceId).put((byte) b.length).put(b).putDouble(time);
            return buf.array();
        }

        private byte[] serialize(LightCurvePoint p) {
            byte[] b = p.band.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(90 + b.length);
            buf.putLong(p.sourceId).putDouble(p.ra).putDouble(p.dec).putLong(p.transitId);
            buf.putInt(b.length).put(b);
            buf.putDouble(p.time).putDouble(p.mag).putDouble(p.flux);
            buf.putDouble(p.fluxError).putDouble(p.fluxOverError);
            buf.put((byte) (p.rejectedByPhotometry ? 1 : 0));
            buf.put((byte) (p.rejectedByVariability ? 1 : 0));
            buf.putInt(p.otherFlags).putLong(p.solutionId);
            return buf.array();
        }
    }

    static class WriteTask {
        final int nodeId;
        final long healpixId;
        final List<LightCurvePoint> points;
        WriteTask(int nid, long hid, List<LightCurvePoint> pts) {
            nodeId = nid; healpixId = hid; points = pts;
        }
    }

    // =============== 数据加载 ===============

    private void loadData(String dataDir, int maxRecords) throws IOException {
        System.out.println("正在加载增量数据集: " + dataDir);
        long t0 = System.currentTimeMillis();

        File dir = new File(dataDir);
        if (!dir.isDirectory()) throw new FileNotFoundException("数据目录不存在: " + dataDir);

        File[] files = dir.listFiles((d, name) -> name.startsWith("time_") && name.endsWith(".csv"));
        if (files == null || files.length == 0)
            throw new FileNotFoundException("目录中无 time_*.csv 文件: " + dataDir);
        Arrays.sort(files, Comparator.comparing(File::getName));

        csvLines = new ArrayList<>(Math.min(maxRecords, 2_000_000));
        int loaded = 0;
        for (File file : files) {
            if (loaded >= maxRecords) break;
            try (BufferedReader reader = new BufferedReader(new FileReader(file), 1 << 16)) {
                String line; boolean hdr = true;
                while ((line = reader.readLine()) != null) {
                    if (hdr) { hdr = false; continue; }
                    if (loaded >= maxRecords) break;
                    line = line.trim();
                    if (!line.isEmpty()) { csvLines.add(line); loaded++; }
                }
            }
        }

        totalRecords = csvLines.size();
        System.out.printf("加载完成: %,d 条记录, %d 个文件, 耗时 %d ms%n%n",
                totalRecords, files.length, System.currentTimeMillis() - t0);
    }

    private Map<Integer, Map<Long, List<LightCurvePoint>>> parseAndGroupByNode(int level, int nodes) {
        Map<Integer, Map<Long, List<LightCurvePoint>>> result = new HashMap<>();
        for (String line : csvLines) {
            try {
                String[] p = line.split(",", -1);
                if (p.length < 14 || p[0].equals("source_id")) continue;
                double ra = Double.parseDouble(p[1].trim());
                double dec = Double.parseDouble(p[2].trim());
                long hid = HealpixUtil.raDecToHealpix(ra, dec, level);
                int nodeId = (int) (hid % nodes);
                LightCurvePoint point = new LightCurvePoint(
                        (long) Double.parseDouble(p[0].trim()), ra, dec,
                        (long) Double.parseDouble(p[3].trim()), p[4].trim(),
                        Double.parseDouble(p[5].trim()), Double.parseDouble(p[6].trim()),
                        Double.parseDouble(p[7].trim()), Double.parseDouble(p[8].trim()),
                        Double.parseDouble(p[9].trim()),
                        Boolean.parseBoolean(p[10].trim()), Boolean.parseBoolean(p[11].trim()),
                        (int) Double.parseDouble(p[12].trim()), (long) Double.parseDouble(p[13].trim()));
                result.computeIfAbsent(nodeId, k -> new HashMap<>())
                        .computeIfAbsent(hid, k -> new ArrayList<>()).add(point);
            } catch (Exception ignored) {}
        }
        return result;
    }

    // =============== 报告生成 ===============

    private void generateReport() throws IOException {
        String ts = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

        String csvFile = OUT_DIR + "results_" + ts + ".csv";
        try (PrintWriter w = new PrintWriter(new FileWriter(csvFile))) {
            w.println("experiment,param_value,avg_time_ms,std_time_ms,"
                    + "avg_throughput_rec_per_sec,std_throughput,total_records,"
                    + "raw_times_ms,raw_throughputs");
            for (Record r : results) {
                w.printf("%s,%s,%.2f,%.2f,%.2f,%.2f,%d,\"%s\",\"%s\"%n",
                        r.exp, r.pv, r.at, r.st, r.atp, r.stp, r.n,
                        r.rt.stream().map(String::valueOf).collect(Collectors.joining(";")),
                        r.rtp.stream().map(v -> String.format("%.0f", v))
                                .collect(Collectors.joining(";")));
            }
        }

        String txtFile = OUT_DIR + "summary_" + ts + ".txt";
        try (PrintWriter w = new PrintWriter(new FileWriter(txtFile))) {
            w.println("══════════════════════════════════════════════════════════════════");
            w.println("  LitecsDB Parameter Tuning Benchmark — Summary Report");
            w.println("  Date:     " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            w.println("  Records:  " + totalRecords);
            w.println("  Baseline: nodes=" + DEF_NODES + ", level=" + DEF_LEVEL);
            w.println("  Timing:   Phase2 only (no flush), aligned with Experiment1Runner");
            w.println("  Reps:     " + REPS + " (+ " + WARMUP + " warmup)");
            w.println("══════════════════════════════════════════════════════════════════");

            String cur = "";
            for (Record r : results) {
                if (!r.exp.equals(cur)) {
                    cur = r.exp; w.println();
                    w.println("─── " + cur + " ───");
                    w.printf("  %-28s %10s %10s %12s %12s%n",
                            "Value", "Time(ms)", "±Std", "TP(rec/s)", "±Std");
                    w.println("  " + repeat("-", 72));
                }
                w.printf("  %-28s %10.0f %10.0f %12.0f %12.0f%n",
                        r.pv, r.at, r.st, r.atp, r.stp);
            }

            w.println();
            w.println("══════════════════════════════════════════════════════════════════");
            w.println("  Best Configuration per Experiment (by throughput)");
            w.println("══════════════════════════════════════════════════════════════════");
            Map<String, Record> best = new LinkedHashMap<>();
            for (Record r : results) best.merge(r.exp, r, (a, b) -> a.atp > b.atp ? a : b);
            for (var e : best.entrySet())
                w.printf("  %-28s → %-20s %.0f rec/s%n", e.getKey(), e.getValue().pv, e.getValue().atp);
        }

        // 控制台
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                      Experiment Results Summary                      ║");
        System.out.println("╠═══════════════════════════════════════════════════════════════════════╣");
        System.out.printf("║ %-22s %-22s %8s %12s ║%n", "Experiment", "Param", "Time ms", "TP rec/s");
        System.out.println("╠═══════════════════════════════════════════════════════════════════════╣");
        String cur = "";
        for (Record r : results) {
            if (!r.exp.equals(cur)) {
                if (!cur.isEmpty())
                    System.out.println("╠───────────────────────────────────────────────────────────────────────╣");
                cur = r.exp;
            }
            System.out.printf("║ %-22s %-22s %8.0f %12.0f ║%n", r.exp, r.pv, r.at, r.atp);
        }
        System.out.println("╚═══════════════════════════════════════════════════════════════════════╝");

        System.out.println("\n🏆 Best per experiment:");
        Map<String, Record> best = new LinkedHashMap<>();
        for (Record r : results) best.merge(r.exp, r, (a, b) -> a.atp > b.atp ? a : b);
        for (var e : best.entrySet())
            System.out.printf("   %-28s → %-20s %.0f rec/s%n", e.getKey(), e.getValue().pv, e.getValue().atp);

        System.out.println("\n📊 CSV → " + csvFile);
        System.out.println("📝 TXT → " + txtFile);
    }

    // =============== 工具方法 ===============

    private Cfg baseline() { return new Cfg(); }

    private double stddev(List<Double> vals, double mean) {
        if (vals.size() <= 1) return 0;
        return Math.sqrt(vals.stream().mapToDouble(v -> (v - mean) * (v - mean)).sum() / (vals.size() - 1));
    }

    private void deleteDirectory(File dir) {
        if (dir == null || !dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) for (File f : files) {
            if (f.isDirectory()) deleteDirectory(f); else f.delete();
        }
        dir.delete();
    }

    private void banner(String t) {
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.printf("║  %-58s║%n", t);
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    private void section(String t) { System.out.println("\n========== " + t + " =========="); }

    private static String repeat(String s, int n) {
        StringBuilder sb = new StringBuilder(); for (int i = 0; i < n; i++) sb.append(s); return sb.toString();
    }

    // =============== 数据类 ===============

    static class Cfg {
        int threads = DEF_WRITE_THREADS;
        int batch = DEF_BATCH_SIZE;
        int nodes = DEF_NODES;
        int level = DEF_LEVEL;
        long wbMB = DEF_WB_MB;
        int maxWB = DEF_MAX_WB;
        int minMerge = DEF_MIN_MERGE;
        int l0Trigger = DEF_L0_TRIGGER;
        CompressionType comp = DEF_COMP;
        boolean sync = DEF_SYNC;
        boolean noWAL = DEF_NO_WAL;
    }

    static class Record {
        final String exp, pv;
        final double at, st, atp, stp;
        final int n;
        final List<Long> rt;
        final List<Double> rtp;
        Record(String e, String p, double at, double st, double atp, double stp,
               int n, List<Long> rt, List<Double> rtp) {
            this.exp=e; this.pv=p; this.at=at; this.st=st; this.atp=atp; this.stp=stp;
            this.n=n; this.rt=new ArrayList<>(rt); this.rtp=new ArrayList<>(rtp);
        }
    }

    static class DaemonThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(0);
        DaemonThreadFactory(String prefix) { this.prefix = prefix; }
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r); t.setDaemon(true);
            t.setName(prefix + "-" + counter.getAndIncrement()); return t;
        }
    }
}