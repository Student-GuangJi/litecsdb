package org.example.exp;

import org.example.MainNode;
import org.example.RocksDBServer;
import org.example.RocksDBServer.Config;
import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.HealpixUtil;
import org.example.utils.StorageUtils;
import org.rocksdb.TickerType;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 边界对齐碎片化 Flush 策略消融实验
 *
 * 使用方式：
 *   java -Xmx300g -Xms100g -cp LitecsDB-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *       org.example.exp.FlushAblationExperiment \
 *       --data-dir /mnt/nvme/home/wangxc/litecsdb/generated_datasets/batch_32000000/individual_lightcurves/ \
 *       --coord-file /mnt/nvme/home/wangxc/litecsdb/gaiadr2/source_coordinates.csv \
 *       --output-dir baf/results \
 *       --total-records 32000000 \
 *       --rounds 5
 */
public class FlushAblationExperiment {

    // ==================== 实验常量 ====================

    public enum FlushStrategy {
        DEFAULT,
        FRAG,
        FRAG_BA
    }

    private static final double[] DISORDER_RATES = {0.0,0.05,0.10,0.20,0.50};
    private static final int HEALPIX_LEVEL = 1;
    private static final int NODES_COUNT = 2;
    private static final int READ_SAMPLE_SIZE = 1000;
    private static final long BYTES_PER_RECORD = 90L;

    // ==================== 运行参数 ====================

    private final String dataDir;
    private final String coordFile;
    private final String outputDir;
    private final int totalRecords;
    private final int rounds;
    private final PrintWriter csvWriter;
    private final PrintWriter logWriter;

    private Map<Long, double[]> coordMap;

    // ==================== 构造 ====================

    public FlushAblationExperiment(String dataDir, String coordFile,
                                   String outputDir,
                                   int totalRecords, int rounds) throws IOException {
        this.dataDir = dataDir;
        this.coordFile = coordFile;
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
                + "compact_write_bytes,flush_write_bytes,"
                + "read_latency_median_us,read_latency_p99_us,"
                + "dir_size_bytes,logical_bytes");

        this.logWriter = new PrintWriter(new FileWriter(
                outputDir + "/flush_ablation_" + ts + ".log"), true);
    }

    // ==================== 入口 ====================

    public static void main(String[] args) throws Exception {
        String dataDir = null, coordFile = null, outputDir = null;
        int totalRecords = 32_000_000, rounds = 5;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--data-dir":      dataDir = args[++i]; break;
                case "--coord-file":    coordFile = args[++i]; break;
                case "--output-dir":    outputDir = args[++i]; break;
                case "--total-records": totalRecords = Integer.parseInt(args[++i]); break;
                case "--rounds":        rounds = Integer.parseInt(args[++i]); break;
            }
        }
        if (dataDir == null || coordFile == null || outputDir == null) {
            System.err.println("Usage: --data-dir <path> --coord-file <path> --output-dir <path> "
                    + "[--total-records N] [--rounds N]");
            System.exit(1);
        }

        FlushAblationExperiment exp = new FlushAblationExperiment(
                dataDir, coordFile, outputDir, totalRecords, rounds);
        exp.run();
        exp.close();
    }

    // ==================== 实验主循环 ====================

    public void run() throws Exception {
        log("========== Flush Ablation Experiment ==========");
        log("Data dir:       %s", dataDir);
        log("Coord file:     %s", coordFile);
        log("Total records:  %,d", totalRecords);
        log("Rounds:         %d", rounds);
        log("Strategies:     %s", Arrays.toString(FlushStrategy.values()));
        log("Disorder rates: %s", Arrays.toString(DISORDER_RATES));
        log("================================================\n");

        log("[Phase 0] Loading coordinates ...");
        coordMap = loadCoordinates(coordFile);
        log("[Phase 0] Loaded %,d source coordinates", coordMap.size());

        log("[Phase 0] Loading individual lightcurve files ...");
        List<LightCurvePoint> allPoints = loadIndividualLightcurves(dataDir, coordMap, totalRecords);
        log("[Phase 0] Loaded %,d records", allPoints.size());

        List<Long> allSourceIds = allPoints.stream()
                .map(p -> p.sourceId).distinct()
                .collect(Collectors.toList());
        log("[Phase 0] Distinct sources: %,d\n", allSourceIds.size());

        for (FlushStrategy strategy : FlushStrategy.values()) {
            for (double disorderRate : DISORDER_RATES) {
                log("===== Strategy=%s, DisorderRate=%.2f =====", strategy, disorderRate);

                List<LightCurvePoint> disorderedData = applyDisorder(allPoints, disorderRate);

//                log("  [Warmup] ...");
//                runSingleRound(strategy, /disorderedData, allSourceIds, -1);

                for (int r = 0; r < rounds; r++) {
                    log("  [Round %d/%d]", r + 1, rounds);
                    RoundResult res = runSingleRound(strategy, disorderedData, allSourceIds, r);
                    recordResult(strategy, disorderRate, r, res);
                    log("    Write=%,dms  TP=%,d rec/s  WA=%.3f  SA=%.3f",
                            res.writeTimeMs, res.throughput, res.waRatio, res.saRatio);
                    log("    CompactW=%,d  FlushW=%,d  ReadMed=%.0fus  ReadP99=%.0fus",
                            res.compactWriteBytes, res.flushWriteBytes,
                            res.readLatencyMedianUs, res.readLatencyP99Us);
                }
                log("");
            }
        }
        log("========== Experiment Complete ==========");
    }

    // ==================== 单轮实验 ====================

    private RoundResult runSingleRound(FlushStrategy strategy,
                                       List<LightCurvePoint> points,
                                       List<Long> allSourceIds,
                                       int roundIdx) throws Exception {

        String dbBase = String.format("%s/tmp_%s_r%d_%d/",
                outputDir, strategy, roundIdx, System.nanoTime());

        try {
            MainNode mainNode = new MainNode(NODES_COUNT, HEALPIX_LEVEL, dbBase);
            mainNode.setFlushStrategy(strategy);

            Map<Long, List<LightCurvePoint>> healpixMap = groupByHealpix(points);

            List<String> dummyCsv = buildDummyCsvForPreCreate(healpixMap);
            mainNode.preCreateHealpixDatabases(dummyCsv);

            System.gc();
            Thread.sleep(300);

            long t0 = System.currentTimeMillis();
            MainNode.DistributionResult wr =
                    mainNode.distributePreParsedData(healpixMap);
            long t1 = System.currentTimeMillis();
            long writeTimeMs = t1 - t0;

            mainNode.forceFlushAllNoCompaction();
            mainNode.compactOnly();

            long logicalBytes = (long) points.size() * BYTES_PER_RECORD;
            long dirSize = StorageUtils.getDirectorySize(dbBase);
            double saRatio = logicalBytes > 0 ? (double) dirSize / logicalBytes : 1.0;
            double waRatio = mainNode.getOverallWriteAmplification();
            long throughput = writeTimeMs > 0 ? points.size() * 1000L / writeTimeMs : 0;

            long compactWriteBytes = 0, flushWriteBytes = 0;
            try {
                compactWriteBytes = mainNode.getAggregatedTickerCount(TickerType.COMPACT_WRITE_BYTES);
                flushWriteBytes = mainNode.getAggregatedTickerCount(TickerType.FLUSH_WRITE_BYTES);
            } catch (Exception ignored) {}

            mainNode.shutdown();

            RoundResult res = new RoundResult();
            res.writeTimeMs = writeTimeMs;
            res.throughput = throughput;
            res.waRatio = waRatio;
            res.saRatio = saRatio;
            res.compactWriteBytes = compactWriteBytes;
            res.flushWriteBytes = flushWriteBytes;
            res.readLatencyMedianUs = 1;
            res.readLatencyP99Us = 1;
            res.dirSizeBytes = dirSize;
            res.logicalBytes = logicalBytes;
            return res;

        } finally {
            deleteDir(new File(dbBase));
        }
    }

    // ==================== Flush 策略 → Config 映射 ====================

    public static void applyFlushStrategy(Config config, FlushStrategy strategy) {
        switch (strategy) {
            case DEFAULT:
                config.maxFlushFragments = 1;
                config.enableDynamicFlushBoundaries = false;
                config.enableLowerLevelSstBoundaryAlignment = false;
                config.enableStaticBoundaryFallback = false;
                break;
            case FRAG:
                config.maxFlushFragments = 16;
                config.enableDynamicFlushBoundaries = false;
                config.enableLowerLevelSstBoundaryAlignment = false;
                config.enableStaticBoundaryFallback = true;
                config.flushSourceBoundarySpan = 50_000L;
                break;
            case FRAG_BA:
                config.maxFlushFragments = 16;
                config.enableDynamicFlushBoundaries = true;
                config.enableLowerLevelSstBoundaryAlignment = true;
                config.enableStaticBoundaryFallback = true;
                config.flushSourceBoundarySpan = 50_000L;
                break;
        }
    }

    // ==================== 数据加载 ====================

    /**
     * 加载坐标文件
     * 格式: source_id,ra,dec
     */
    private Map<Long, double[]> loadCoordinates(String path) throws IOException {
        Map<Long, double[]> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("source_id") || line.trim().isEmpty()) continue;
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

    /**
     * 从 individual_lightcurves/ 目录加载光变曲线数据
     *
     * 文件名格式: lightcurve_{sourceId}.csv
     * CSV字段(12列,不含ra/dec): source_id,transit_id,band,time,mag,flux,
     *   flux_error,flux_over_error,rejected_by_photometry,rejected_by_variability,
     *   other_flags,solution_id
     *
     * ra/dec 从 coordMap 按 sourceId 关联
     */
    private List<LightCurvePoint> loadIndividualLightcurves(
            String dir, Map<Long, double[]> coords, int maxRecords) throws IOException {

        List<LightCurvePoint> allPoints = new ArrayList<>();

        File[] files = new File(dir).listFiles((d, n) -> n.endsWith(".csv"));
        if (files == null || files.length == 0)
            throw new IOException("No CSV files in " + dir);
        Arrays.sort(files, Comparator.comparing(File::getName));
        log("  Found %,d lightcurve files", files.length);

        int skippedNoCoord = 0;

        for (File f : files) {
            if (allPoints.size() >= maxRecords) break;

            long sourceId = extractSourceIdFromFilename(f.getName());
            if (sourceId < 0) continue;

            double[] coord = coords.get(sourceId);
            if (coord == null) { skippedNoCoord++; continue; }
            double ra = coord[0], dec = coord[1];

            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null && allPoints.size() < maxRecords) {
                    if (line.startsWith("source_id") || line.trim().isEmpty()) continue;
                    try {
                        String[] parts = line.split(",", -1);
                        if (parts.length < 12) continue;

                        LightCurvePoint p = new LightCurvePoint(
                                sourceId, ra, dec,
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
        }

        if (skippedNoCoord > 0) {
            log("  Skipped %,d files (no matching coordinates)", skippedNoCoord);
        }
        return allPoints;
    }

    /**
     * 从文件名提取 sourceId
     * 支持: lightcurve_12345.csv / lc_12345.csv / 12345.csv
     */
    private static long extractSourceIdFromFilename(String filename) {
        try {
            String name = filename.replace(".csv", "");
            int lastUnderscore = name.lastIndexOf('_');
            String numPart = (lastUnderscore >= 0) ? name.substring(lastUnderscore + 1) : name;
            return Long.parseLong(numPart);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    // ==================== 错序控制 ====================

    private List<LightCurvePoint> applyDisorder(List<LightCurvePoint> src, double r) {
        if (r <= 0.0) return new ArrayList<>(src);
        int n = src.size(), numMove = (int) (n * r);
        log("  Disorder: moving %,d of %,d records (r=%.2f)", numMove, n, r);

        List<LightCurvePoint> result = new ArrayList<>(src);
        Random rng = new Random(42 + (long) (r * 10000));

        Set<Integer> selected = new HashSet<>();
        while (selected.size() < numMove) selected.add(rng.nextInt(n));

        List<LightCurvePoint> displaced = new ArrayList<>();
        List<Integer> indices = new ArrayList<>(selected);
        indices.sort(Collections.reverseOrder());
        for (int idx : indices) displaced.add(result.remove(idx));

        for (LightCurvePoint p : displaced)
            result.add(rng.nextInt(result.size() + 1), p);
        return result;
    }

    // ==================== HEALPix 分组 ====================

    private Map<Long, List<LightCurvePoint>> groupByHealpix(List<LightCurvePoint> pts) {
        Map<Long, List<LightCurvePoint>> m = new HashMap<>();
        for (LightCurvePoint p : pts) {
            long hpx = HealpixUtil.raDecToHealpix(p.ra, p.dec, HEALPIX_LEVEL);
            m.computeIfAbsent(hpx, k -> new ArrayList<>()).add(p);
        }
        return m;
    }

    private List<String> buildDummyCsvForPreCreate(Map<Long, List<LightCurvePoint>> hpxMap) {
        List<String> lines = new ArrayList<>();
        Set<Long> seen = new HashSet<>();
        for (var e : hpxMap.entrySet()) {
            if (!e.getValue().isEmpty() && seen.add(e.getKey())) {
                LightCurvePoint p = e.getValue().get(0);
                lines.add(String.format("%d,%.6f,%.6f,0,G,0,0,0,0,0,false,false,0,0",
                        p.sourceId, p.ra, p.dec));
            }
        }
        return lines;
    }

    // ==================== 读取性能测试 ====================

    private ReadPerfResult measureReadPerf(MainNode mn, List<Long> ids,
                                           Map<Long, List<LightCurvePoint>> hpxMap) {
        Map<Long, Long> s2h = new HashMap<>();
        for (var e : hpxMap.entrySet())
            for (LightCurvePoint p : e.getValue()) s2h.putIfAbsent(p.sourceId, e.getKey());

        List<Long> sample = new ArrayList<>(ids);
        Collections.shuffle(sample, new Random(12345));
        sample = sample.subList(0, Math.min(READ_SAMPLE_SIZE, sample.size()));

        List<Double> lats = new ArrayList<>();
        for (Long sid : sample) {
            Long hpx = s2h.get(sid);
            if (hpx == null) continue;
            long t0 = System.nanoTime();
            mn.getLightCurveFromPartition(hpx, sid, "G");
            lats.add((System.nanoTime() - t0) / 1000.0);
        }
        Collections.sort(lats);
        ReadPerfResult r = new ReadPerfResult();
        if (!lats.isEmpty()) {
            r.medianUs = lats.get(lats.size() / 2);
            r.p99Us = lats.get((int) (lats.size() * 0.99));
        }
        return r;
    }

    // ==================== 结果记录 ====================

    private void recordResult(FlushStrategy strategy, double disorderRate,
                              int round, RoundResult result) {
        csvWriter.printf("%s,%.2f,%d,%d,%d,%.4f,%.4f,%d,%d,%.1f,%.1f,%d,%d%n",
                strategy, disorderRate, round,
                result.writeTimeMs, result.throughput,
                result.waRatio, result.saRatio,
                result.compactWriteBytes, result.flushWriteBytes,
                result.readLatencyMedianUs, result.readLatencyP99Us,
                result.dirSizeBytes, result.logicalBytes);
        csvWriter.flush();
    }

    // ==================== 日志与工具 ====================

    private void log(String f, Object... a) {
        String m = String.format(f, a); System.out.println(m); logWriter.println(m);
    }
    public void close() { csvWriter.close(); logWriter.close(); }

    private static void deleteDir(File d) {
        if (d == null || !d.exists()) return;
        File[] fs = d.listFiles();
        if (fs != null) for (File f : fs) { if (f.isDirectory()) deleteDir(f); else f.delete(); }
        d.delete();
    }

    // ==================== 内部数据类 ====================

    static class RoundResult {
        long writeTimeMs, throughput, compactWriteBytes, flushWriteBytes, dirSizeBytes, logicalBytes;
        double waRatio, saRatio, readLatencyMedianUs, readLatencyP99Us;
    }
    static class ReadPerfResult { double medianUs, p99Us; }
}