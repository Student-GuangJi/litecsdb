package org.example.exp;

import org.example.MainNode;
import org.example.RocksDBServer;
import org.example.RocksDBServer.LightCurvePoint;
import org.example.StorageNode;
import org.example.wrapper.NativeRocksDBWrapper;
import org.example.wrapper.InfluxDBClientWrapper;
import org.example.wrapper.TDengineClientWrapper;
import org.example.utils.HealpixUtil;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 查询性能实验
 *
 * 自变量: 数据规模(5) × 系统(4) × 查询方式(4)
 * 因变量: 平均查询延迟(ms)、查询吞吐量(queries/s)
 *
 * 使用：
 *   java -Xmx300g -Xms100g -cp LitecsDB-1.0-SNAPSHOT-jar-with-dependencies.jar org.example.exp.QueryBenchmark \
 *       --data-dir /mnt/nvme/home/wangxc/litecsdb/generated_datasets/batch_4000000/individual_lightcurves \
 *       --coord-file /mnt/nvme/home/wangxc/litecsdb/gaiadr2/source_coordinates.csv \
 *       --output-dir query_results \
 *       --rounds 1
 */
public class QueryBenchmark {

    // ==================== 实验参数 ====================

    static final int[] DATA_SCALES = {250_000, 500_000, 1_000_000, 2_000_000, 4_000_000};
    static final int HEALPIX_LEVEL = 1;
    static final int NODES_COUNT = 2;
    static final int QUERY_COUNT = 100;        // 每种查询正式执行100次
    static final double CONE_RADIUS = 0.1;     // 锥形检索半径(度)

    // ==================== 字段 ====================

    final String dataDir;
    final String coordFile;
    final String outputDir;
    final int rounds;
    PrintWriter csv;
    PrintWriter log;

    /** sourceId → (ra, dec) */
    Map<Long, double[]> coordMap;

    // ==================== 入口 ====================

    public static void main(String[] args) throws Exception {
        String dataDir = null, coordFile = null, outputDir = null;
        int rounds = 3;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--data-dir":   dataDir = args[++i]; break;
                case "--coord-file": coordFile = args[++i]; break;
                case "--output-dir": outputDir = args[++i]; break;
                case "--rounds":     rounds = Integer.parseInt(args[++i]); break;
            }
        }
        if (dataDir == null || coordFile == null || outputDir == null) {
            System.err.println("Usage: --data-dir <path> --coord-file <path> --output-dir <path> [--rounds N]");
            System.exit(1);
        }
        new QueryBenchmark(dataDir, coordFile, outputDir, rounds).run();
    }

    QueryBenchmark(String dataDir, String coordFile, String outputDir, int rounds) throws IOException {
        this.dataDir = dataDir;
        this.coordFile = coordFile;
        this.outputDir = outputDir;
        this.rounds = rounds;
        new File(outputDir).mkdirs();
        String ts = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        csv = new PrintWriter(new FileWriter(outputDir + "/query_bench_" + ts + ".csv"));
        csv.println("system,scale,query_type,round,avg_latency_ms,throughput_qps,total_rows,num_queries");
        log = new PrintWriter(new FileWriter(outputDir + "/query_bench_" + ts + ".log"), true);
    }

    // ==================== 主流程 ====================

    void run() throws Exception {
        log("========== Query Benchmark ==========");
        coordMap = loadCoordinates(coordFile);
        log("Loaded %,d coordinates", coordMap.size());

        for (int scale : DATA_SCALES) {
            log("\n====== Scale: %,d ======", scale);

            // 加载数据
            LoadResult loaded = loadBatchData(dataDir, coordMap, scale);
            log("Loaded %,d records, %,d sources", loaded.points.size(), loaded.sourceIds.size());

            // 构造查询参数（各系统共用）
            QueryParams qp = buildQueryParams(loaded);
            log("Time range: [%.2f, %.2f]", qp.tStart, qp.tEnd);
            log("Cone centers: %d points, radius=%.2f deg", qp.coneCenters.size(), CONE_RADIUS);
            log("Q1 sample: %d sourceIds", qp.q1SourceIds.size());

            // 依次测试四个系统
            benchLitecsDB(scale, loaded, qp);
            benchNativeRocksDB(scale, loaded, qp);
            benchTDengine(scale, loaded, qp);
            benchInfluxDB(scale, loaded, qp);
        }

        csv.close();
        log.close();
        log("========== Done ==========");
    }

    // ==================== LitecsDB ====================

    void benchLitecsDB(int scale, LoadResult data, QueryParams qp) throws Exception {
        String sys = "LitecsDB";
        log("\n--- %s (scale=%,d) ---", sys, scale);
        String dbPath = outputDir + "/tmp_litecs_" + scale + "/";
        try {
            MainNode mn = new MainNode(NODES_COUNT, HEALPIX_LEVEL, dbPath);
            // 写入
            Map<Long, List<LightCurvePoint>> hpxMap = groupByHealpix(data.points);
            List<String> dummyCsv = new ArrayList<>();
            for (var e : hpxMap.entrySet()) {
                LightCurvePoint p = e.getValue().get(0);
                dummyCsv.add(String.format("%d,%.6f,%.6f,0,G,0,0,0,0,0,false,false,0,0", p.sourceId, p.ra, p.dec));
            }
            mn.preCreateHealpixDatabases(dummyCsv);
            mn.distributePreParsedData(hpxMap);
            mn.forceFlushAllNoCompaction();
            mn.compactOnly();
            log("  Write done, running queries...");

            // 构建 sourceId → healpixId 映射
            Map<Long, Long> s2h = new HashMap<>();
            for (var e : hpxMap.entrySet())
                for (LightCurvePoint p : e.getValue()) s2h.putIfAbsent(p.sourceId, e.getKey());

            for (int r = 0; r < rounds; r++) {
                // Q1: 按sourceId
                runQ(sys, scale, "Q1_SourceID", r, QUERY_COUNT, i -> {
                    long sid = qp.q1SourceIds.get(i % qp.q1SourceIds.size());
                    Long hpx = s2h.get(sid);
                    if (hpx == null) return 0;
                    return mn.getLightCurveFromPartition(hpx, sid, "G").size();
                });
                // Q2: 时间范围 — 对采样天体做时间范围检索
                runQ(sys, scale, "Q2_TimeRange", r, QUERY_COUNT, i -> {
                    long sid = qp.q1SourceIds.get(i % qp.q1SourceIds.size());
                    Long hpx = s2h.get(sid);
                    if (hpx == null) return 0;
                    // LitecsDB: 读取天体光变曲线后在应用层过滤时间范围
                    List<LightCurvePoint> lc = mn.getLightCurveFromPartition(hpx, sid, "G");
                    return (int) lc.stream().filter(p -> p.time >= qp.tStart && p.time <= qp.tEnd).count();
                });
                // Q3: 锥形检索
                runQ(sys, scale, "Q3_Cone", r, QUERY_COUNT, i -> {
                    double[] c = qp.coneCenters.get(i % qp.coneCenters.size());
                    Set<Long> hpxIds = mn.calculateHealpixIdsInConePublic(c[0], c[1], CONE_RADIUS);
                    int total = 0;
                    for (Long hpx : hpxIds) {
                        // 遍历该分区内的天体，精确空间过滤
                        StorageNode node = mn.getStorageNode(hpx);
                        if (node == null) continue;
                        List<RocksDBServer.StarMetadata> stars = node.getAllStarsInHealpix(hpx);
                        for (RocksDBServer.StarMetadata s : stars) {
                            if (angularDist(s.ra, s.dec, c[0], c[1]) <= CONE_RADIUS) total++;
                        }
                    }
                    return total;
                });
                // Q4: 时间+锥形
                runQ(sys, scale, "Q4_ConeTime", r, QUERY_COUNT, i -> {
                    double[] c = qp.coneCenters.get(i % qp.coneCenters.size());
                    Set<Long> hpxIds = mn.calculateHealpixIdsInConePublic(c[0], c[1], CONE_RADIUS);
                    int total = 0;
                    for (Long hpx : hpxIds) {
                        StorageNode node = mn.getStorageNode(hpx);
                        if (node == null) continue;
                        List<RocksDBServer.StarMetadata> stars = node.getAllStarsInHealpix(hpx);
                        for (RocksDBServer.StarMetadata s : stars) {
                            if (angularDist(s.ra, s.dec, c[0], c[1]) <= CONE_RADIUS) {
                                List<LightCurvePoint> lc = mn.getLightCurveFromPartition(hpx, s.sourceId, "G");
                                total += (int) lc.stream().filter(p -> p.time >= qp.tStart && p.time <= qp.tEnd).count();
                            }
                        }
                    }
                    return total;
                });
            }
            mn.shutdown();
        } finally {
            deleteDir(new File(dbPath));
        }
    }

    // ==================== NativeRocksDB ====================

    void benchNativeRocksDB(int scale, LoadResult data, QueryParams qp) throws Exception {
        String sys = "NativeRocksDB";
        log("\n--- %s (scale=%,d) ---", sys, scale);
        String dbPath = outputDir + "/tmp_native_" + scale + "/";
        try (NativeRocksDBWrapper db = new NativeRocksDBWrapper(dbPath)) {
            db.putBatch(data.points);
            db.forceFlush();
            log("  Write done, running queries...");

            for (int r = 0; r < rounds; r++) {
                // Q1
                runQ(sys, scale, "Q1_SourceID", r, QUERY_COUNT, i -> {
                    long sid = qp.q1SourceIds.get(i % qp.q1SourceIds.size());
                    return db.queryBySourceIdFull(sid).size();
                });
                // Q2
                runQ(sys, scale, "Q2_TimeRange", r, QUERY_COUNT, i -> {
                    long sid = qp.q1SourceIds.get(i % qp.q1SourceIds.size());
                    return db.queryBySourceIdTimeRange(sid, qp.tStart, qp.tEnd).size();
                });
                // Q3: 原生RocksDB没有空间索引，需全表扫描+坐标过滤
                runQ(sys, scale, "Q3_Cone", r, QUERY_COUNT, i -> {
                    double[] c = qp.coneCenters.get(i % qp.coneCenters.size());
                    // 扫描所有已知sourceId，检查坐标
                    int total = 0;
                    for (Long sid : qp.allSourceIdsSample) {
                        double[] coord = coordMap.get(sid);
                        if (coord != null && angularDist(coord[0], coord[1], c[0], c[1]) <= CONE_RADIUS) total++;
                    }
                    return total;
                });
                // Q4
                runQ(sys, scale, "Q4_ConeTime", r, QUERY_COUNT, i -> {
                    double[] c = qp.coneCenters.get(i % qp.coneCenters.size());
                    int total = 0;
                    for (Long sid : qp.allSourceIdsSample) {
                        double[] coord = coordMap.get(sid);
                        if (coord != null && angularDist(coord[0], coord[1], c[0], c[1]) <= CONE_RADIUS) {
                            total += db.queryBySourceIdTimeRange(sid, qp.tStart, qp.tEnd).size();
                        }
                    }
                    return total;
                });
            }
        } finally {
            deleteDir(new File(dbPath));
        }
    }

    // ==================== TDengine ====================

    void benchTDengine(int scale, LoadResult data, QueryParams qp) throws Exception {
        String sys = "TDengine";
        log("\n--- %s (scale=%,d) ---", sys, scale);
        String dbName = "qbench_td_" + scale;
        TDengineClientWrapper td = new TDengineClientWrapper(dbName);
        try {
            td.putBatch(data.points);
            td.forceFlush();
            log("  Write done, running queries...");

            for (int r = 0; r < rounds; r++) {
                // Q1
                runQ(sys, scale, "Q1_SourceID", r, QUERY_COUNT, i -> {
                    long sid = qp.q1SourceIds.get(i % qp.q1SourceIds.size());
                    double[] coord = coordMap.get(sid);
                    if (coord == null) return 0;
                    return td.querySingle(sid, coord[0], coord[1]);
                });
                // Q2
                runQ(sys, scale, "Q2_TimeRange", r, QUERY_COUNT, i -> {
                    long sid = qp.q1SourceIds.get(i % qp.q1SourceIds.size());
                    double[] coord = coordMap.get(sid);
                    if (coord == null) return 0;
                    return td.queryTimeRange(sid, coord[0], coord[1], qp.tStart, qp.tEnd);
                });
                // Q3: 通过STMK接口做锥形检索（不限星等不限点数）
                runQ(sys, scale, "Q3_Cone", r, QUERY_COUNT, i -> {
                    double[] c = qp.coneCenters.get(i % qp.coneCenters.size());
                    List<double[]> params = new ArrayList<>();
                    params.add(new double[]{c[0], c[1], CONE_RADIUS,
                            qp.globalTMin, qp.globalTMax,
                            -100, 100, 1, 0, 0});
                    List<int[]> res = td.executeBatchSTMKQueries(params);
                    return res.isEmpty() ? 0 : res.get(0)[0];
                });
                // Q4
                runQ(sys, scale, "Q4_ConeTime", r, QUERY_COUNT, i -> {
                    double[] c = qp.coneCenters.get(i % qp.coneCenters.size());
                    List<double[]> params = new ArrayList<>();
                    params.add(new double[]{c[0], c[1], CONE_RADIUS,
                            qp.tStart, qp.tEnd,
                            -100, 100, 1, 0, 0});
                    List<int[]> res = td.executeBatchSTMKQueries(params);
                    return res.isEmpty() ? 0 : res.get(0)[0];
                });
            }
        } finally {
            // 清理数据库
            try {
                java.sql.Connection c = java.sql.DriverManager.getConnection(
                        "jdbc:TAOS://127.0.0.1:6030/", "root", "taosdata");
                c.createStatement().execute("DROP DATABASE IF EXISTS " + dbName);
                c.close();
            } catch (Exception ignored) {}
            td.close();
        }
    }

    // ==================== InfluxDB ====================

    void benchInfluxDB(int scale, LoadResult data, QueryParams qp) throws Exception {
        String sys = "InfluxDB";
        log("\n--- %s (scale=%,d) ---", sys, scale);
        String bkt = "qbench_influx_" + scale;
        InfluxDBClientWrapper influx = new InfluxDBClientWrapper(bkt);
        try {
            influx.ensureBucketExists();
            influx.putBatch(data.points);
            influx.forceFlush();
            log("  Write done, running queries...");

            long startNsGlobal = influx.gaiaToInfluxNs(qp.globalTMin);
            long endNsGlobal   = influx.gaiaToInfluxNs(qp.globalTMax);
            long startNsQ2     = influx.gaiaToInfluxNs(qp.tStart);
            long endNsQ2       = influx.gaiaToInfluxNs(qp.tEnd);

            for (int r = 0; r < rounds; r++) {
                // Q1
                runQ(sys, scale, "Q1_SourceID", r, QUERY_COUNT, i -> {
                    long sid = qp.q1SourceIds.get(i % qp.q1SourceIds.size());
                    double[] coord = coordMap.get(sid);
                    if (coord == null) return 0;
                    long hpid = HealpixUtil.raDecToHealpix(coord[0], coord[1], HEALPIX_LEVEL);
                    String flux = String.format(
                            "from(bucket: \"%s\") |> range(start: %d, stop: %d) "
                                    + "|> filter(fn: (r) => r._measurement == \"hp_%d\" and r.source_id == \"%d\" and r._field == \"mag\")",
                            bkt, startNsGlobal, endNsGlobal, hpid, sid);
                    return influx.executeFluxQueryHttp(flux);
                });
                // Q2
                runQ(sys, scale, "Q2_TimeRange", r, QUERY_COUNT, i -> {
                    long sid = qp.q1SourceIds.get(i % qp.q1SourceIds.size());
                    double[] coord = coordMap.get(sid);
                    if (coord == null) return 0;
                    long hpid = HealpixUtil.raDecToHealpix(coord[0], coord[1], HEALPIX_LEVEL);
                    String flux = String.format(
                            "from(bucket: \"%s\") |> range(start: %d, stop: %d) "
                                    + "|> filter(fn: (r) => r._measurement == \"hp_%d\" and r.source_id == \"%d\" and r._field == \"mag\")",
                            bkt, startNsQ2, endNsQ2, hpid, sid);
                    return influx.executeFluxQueryHttp(flux);
                });
                // Q3
                runQ(sys, scale, "Q3_Cone", r, QUERY_COUNT, i -> {
                    double[] c = qp.coneCenters.get(i % qp.coneCenters.size());
                    List<double[]> params = new ArrayList<>();
                    params.add(new double[]{c[0], c[1], CONE_RADIUS,
                            qp.globalTMin, qp.globalTMax,
                            -100, 100, 1, 0, 0});
                    List<int[]> res = influx.executeBatchSTMKQueries(params);
                    return res.isEmpty() ? 0 : res.get(0)[0];
                });
                // Q4
                runQ(sys, scale, "Q4_ConeTime", r, QUERY_COUNT, i -> {
                    double[] c = qp.coneCenters.get(i % qp.coneCenters.size());
                    List<double[]> params = new ArrayList<>();
                    params.add(new double[]{c[0], c[1], CONE_RADIUS,
                            qp.tStart, qp.tEnd,
                            -100, 100, 1, 0, 0});
                    List<int[]> res = influx.executeBatchSTMKQueries(params);
                    return res.isEmpty() ? 0 : res.get(0)[0];
                });
            }
        } finally {
            influx.deleteBucket();
            influx.close();
        }
    }

    // ==================== 查询执行框架 ====================

    @FunctionalInterface
    interface QueryFunc { int execute(int index) throws Exception; }

    void runQ(String sys, int scale, String qType, int round,
              int count, QueryFunc func) {
        long totalNs = 0;
        long totalRows = 0;
        int executed = 0;
        for (int i = 0; i < count; i++) {
            try {
                long t0 = System.nanoTime();
                int rows = func.execute(i);
                long t1 = System.nanoTime();
                totalNs += (t1 - t0);
                totalRows += rows;
                executed++;
            } catch (Exception e) {
                log("  WARN: %s %s query %d failed: %s", sys, qType, i, e.getMessage());
            }
        }
        double avgMs = executed > 0 ? (totalNs / 1e6) / executed : 0;
        double qps = executed > 0 ? executed / (totalNs / 1e9) : 0;
        csv.printf("%s,%d,%s,%d,%.3f,%.1f,%d,%d%n",
                sys, scale, qType, round, avgMs, qps, totalRows, executed);
        csv.flush();
        log("  %s %s round%d: avg=%.2fms  qps=%.1f  rows=%,d  n=%d",
                sys, qType, round, avgMs, qps, totalRows, executed);
    }

    // ==================== 查询参数构建 ====================

    QueryParams buildQueryParams(LoadResult data) {
        QueryParams qp = new QueryParams();

        // 采样sourceId（Q1/Q2使用）
        List<Long> ids = new ArrayList<>(data.sourceIds);
        Collections.shuffle(ids, new Random(42));
        qp.q1SourceIds = ids.subList(0, Math.min(QUERY_COUNT, ids.size()));

        // 采样全量sourceId的子集用于Q3/Q4的NativeRocksDB全表扫描
        qp.allSourceIdsSample = ids.subList(0, Math.min(5000, ids.size()));

        // 时间范围：整体跨度的中间20%
        double tMin = Double.MAX_VALUE, tMax = -Double.MAX_VALUE;
        for (LightCurvePoint p : data.points) {
            if (p.time < tMin) tMin = p.time;
            if (p.time > tMax) tMax = p.time;
        }
        qp.globalTMin = tMin;
        qp.globalTMax = tMax;
        double span = tMax - tMin;
        qp.tStart = tMin + span * 0.4;
        qp.tEnd = tMin + span * 0.6;

        // 锥形中心：从已有天体坐标中均匀采样5个
        List<Long> coordIds = new ArrayList<>(data.sourceIds);
        Collections.shuffle(coordIds, new Random(123));
        qp.coneCenters = new ArrayList<>();
        int step = Math.max(1, coordIds.size() / 5);
        for (int i = 0; i < 5 && i * step < coordIds.size(); i++) {
            double[] coord = coordMap.get(coordIds.get(i * step));
            if (coord != null) qp.coneCenters.add(coord);
        }

        return qp;
    }

    // ==================== 数据加载 ====================

    /**
     * 从 individual_lightcurves/ 目录加载：每个天体一个CSV文件，
     * 按天体逐个写入直到总记录数达到上限
     */
    LoadResult loadBatchData(String dir, Map<Long, double[]> coords, int maxRecords) throws IOException {
        List<LightCurvePoint> allPoints = new ArrayList<>();
        Set<Long> sourceIds = new LinkedHashSet<>();

        File[] files = new File(dir).listFiles((d, n) -> n.endsWith(".csv"));
        if (files == null || files.length == 0)
            throw new IOException("No CSV files in " + dir);
        Arrays.sort(files, Comparator.comparing(File::getName));

        for (File f : files) {
            if (allPoints.size() >= maxRecords) break;

            // 从文件名提取sourceId: lightcurve_{sourceId}.csv
            long sourceId = -1;
            try {
                String name = f.getName().replace(".csv", "");
                if (name.contains("_")) {
                    sourceId = Long.parseLong(name.substring(name.lastIndexOf('_') + 1));
                }
            } catch (Exception e) { continue; }

            double[] coord = coords.get(sourceId);
            if (coord == null) continue;  // 无坐标则跳过

            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null && allPoints.size() < maxRecords) {
                    if (line.startsWith("source_id") || line.trim().isEmpty()) continue;
                    try {
                        // CSV字段: source_id,transit_id,band,time,mag,flux,flux_error,
                        //          flux_over_error,rejected_by_photometry,rejected_by_variability,
                        //          other_flags,solution_id
                        String[] parts = line.split(",", -1);
                        if (parts.length < 12) continue;
                        LightCurvePoint p = new LightCurvePoint(
                                sourceId, coord[0], coord[1],
                                (long) Double.parseDouble(parts[1].trim()),
                                parts[2].trim(),
                                Double.parseDouble(parts[3].trim()),
                                Double.parseDouble(parts[4].trim()),
                                Double.parseDouble(parts[5].trim()),
                                Double.parseDouble(parts[6].trim()),
                                Double.parseDouble(parts[7].trim()),
                                Boolean.parseBoolean(parts[8].trim()),
                                Boolean.parseBoolean(parts[9].trim()),
                                (int) Double.parseDouble(parts[10].trim()),
                                (long) Double.parseDouble(parts[11].trim())
                        );
                        allPoints.add(p);
                    } catch (Exception ignored) {}
                }
            }
            sourceIds.add(sourceId);
        }

        LoadResult result = new LoadResult();
        result.points = allPoints;
        result.sourceIds = sourceIds;
        return result;
    }

    Map<Long, double[]> loadCoordinates(String path) throws IOException {
        Map<Long, double[]> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("source_id")) continue;
                String[] parts = line.split(",");
                if (parts.length < 3) continue;
                try {
                    long sid = (long) Double.parseDouble(parts[0].trim());
                    double ra = Double.parseDouble(parts[1].trim());
                    double dec = Double.parseDouble(parts[2].trim());
                    map.put(sid, new double[]{ra, dec});
                } catch (Exception ignored) {}
            }
        }
        return map;
    }

    // ==================== 工具方法 ====================

    Map<Long, List<LightCurvePoint>> groupByHealpix(List<LightCurvePoint> pts) {
        Map<Long, List<LightCurvePoint>> m = new HashMap<>();
        for (LightCurvePoint p : pts) {
            long hpx = HealpixUtil.raDecToHealpix(p.ra, p.dec, HEALPIX_LEVEL);
            m.computeIfAbsent(hpx, k -> new ArrayList<>()).add(p);
        }
        return m;
    }

    static double angularDist(double ra1, double dec1, double ra2, double dec2) {
        double r1 = Math.toRadians(ra1), d1 = Math.toRadians(dec1);
        double r2 = Math.toRadians(ra2), d2 = Math.toRadians(dec2);
        double a = Math.sin((d2 - d1) / 2); a *= a;
        double b = Math.sin((r2 - r1) / 2); b *= b;
        a += Math.cos(d1) * Math.cos(d2) * b;
        return Math.toDegrees(2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
    }

    void log(String f, Object... a) {
        String m = String.format(f, a);
        System.out.println(m);
        if (log != null) log.println(m);
    }

    static void deleteDir(File d) {
        if (d == null || !d.exists()) return;
        File[] fs = d.listFiles();
        if (fs != null) for (File f : fs) { if (f.isDirectory()) deleteDir(f); else f.delete(); }
        d.delete();
    }

    // ==================== 数据类 ====================

    static class LoadResult {
        List<LightCurvePoint> points;
        Set<Long> sourceIds;
    }

    static class QueryParams {
        List<Long> q1SourceIds;       // Q1/Q2 采样的sourceId列表
        List<Long> allSourceIdsSample; // Q3/Q4 NativeRocksDB全扫描子集
        double tStart, tEnd;          // Q2/Q4 时间窗口
        double globalTMin, globalTMax; // 全局时间范围
        List<double[]> coneCenters;   // Q3/Q4 锥形中心 [(ra,dec), ...]
    }
}
