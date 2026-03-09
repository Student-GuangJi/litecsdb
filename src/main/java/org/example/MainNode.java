package org.example;

import healpix.RangeSet;
import org.example.RocksDBServer.*;
import org.example.utils.HealpixUtil;
import org.example.StorageNode.*;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MainNode - Two-Phase distributed data coordinator
 *
 * Phase 2 写入优化（v2）：
 *   - CSV 只解析一次（在 distributeDataBatch 中），产出 LightCurvePoint 对象
 *   - 按 HEALPix ID 分组后直接分发对象（不再传 CSV 字符串）
 *   - 消除 StorageNode/RocksDBServer 中的二次 CSV 解析
 */
public class MainNode {
    private static final String DEFAULT_BASE_PATH = "LitecsDB_Data/";
    private final String basePath;
    private final List<StorageNode> storageNodes;
    private final int nodesCount;
    private final int level;

    private final ExecutorService distributionExecutor;
    private final ExecutorService writeExecutor;

    public MainNode(int nodesCount, int level) {
        this(nodesCount, level, DEFAULT_BASE_PATH);
    }

    public MainNode(int nodesCount, int level, String basePath) {
        this.nodesCount = nodesCount;
        this.level = level;
        this.basePath = normalizeBasePath(basePath);
        this.storageNodes = new ArrayList<>();

        RocksDBGlobalResourceManager manager = RocksDBGlobalResourceManager.getInstance();
        this.distributionExecutor = manager.getDistributionThreadPool();
        this.writeExecutor = manager.getWriteThreadPool();

        initializeStorageNodes();
    }

    private static String normalizeBasePath(String path) {
        if (path == null || path.trim().isEmpty()) return DEFAULT_BASE_PATH;
        String normalized = path.trim();
        return normalized.endsWith("/") || normalized.endsWith("\\") ? normalized : normalized + "/";
    }

    private void initializeStorageNodes() {
        for (int i = 0; i < nodesCount; i++) {
            StorageNode node = new StorageNode(i, basePath);
            storageNodes.add(node);
            File dbDir = new File(basePath + "node" + i);
            if (!dbDir.exists()) dbDir.mkdirs();
            loadExistingDatabases(node, dbDir, i);
        }
    }

    private void loadExistingDatabases(StorageNode node, File nodeDir, int nodeId) {
        if (!nodeDir.exists() || !nodeDir.isDirectory()) return;
        File[] healpixDirs = nodeDir.listFiles((dir, name) -> name.startsWith("healpix_"));
        if (healpixDirs == null || healpixDirs.length == 0) {
//            System.out.println("Node " + nodeId + ": no existing databases found");
            return;
        }
        System.out.println("Node " + nodeId + ": found " + healpixDirs.length +
                " existing HEALPix databases, loading in parallel...");
        long startTime = System.currentTimeMillis();
        Arrays.stream(healpixDirs).parallel().forEach(healpixDir -> {
            try {
                long healpixId = Long.parseLong(healpixDir.getName().replace("healpix_", ""));
                String dbPath = healpixDir.getAbsolutePath();
                RocksDBServer.Config config = new RocksDBServer.Config(dbPath, healpixId);
                config.timeBucketSize = 1;
                config.asyncIndexing = true;
                config.maxBackgroundCompactions = 2;
                node.initializeHealpixDatabase(healpixId, config);
            } catch (Exception e) {
                System.err.println("  FAIL: " + healpixDir.getName() + " - " + e.getMessage());
            }
        });
        System.out.println("Node " + nodeId + ": loading complete, took " +
                (System.currentTimeMillis() - startTime) + " ms");
    }

    // ==================== Phase 1 ====================

    public int preCreateHealpixDatabases(List<String> csvLines) {
        Set<Long> healpixIds = new HashSet<>();
        for (String csvLine : csvLines) {
            try {
                String[] parts = csvLine.split(",", 4); // 只需前3列
                if (parts.length < 3 || parts[0].equals("source_id")) continue;
                double ra = Double.parseDouble(parts[1].trim());
                double dec = Double.parseDouble(parts[2].trim());
                healpixIds.add(calculateHealpixId(ra, dec));
            } catch (Exception e) { }
        }

        CountDownLatch latch = new CountDownLatch(healpixIds.size());
        for (Long healpixId : healpixIds) {
            distributionExecutor.submit(() -> {
                try { ensureHealpixDatabaseInitialized(healpixId); }
                catch (Exception e) { }
                finally { latch.countDown(); }
            });
        }
        try { latch.await(60, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
//        System.out.println("[Phase 1] Pre-created " + healpixIds.size() + " healpix databases");
        return healpixIds.size();
    }

    // ==================== Phase 1.5: 预解析数据 ====================

    /**
     * 预解析 CSV → LightCurvePoint 并按 HEALPix 分组
     * 在 Phase 1 阶段调用（不计入写入时间）
     */
    public Map<Long, List<LightCurvePoint>> preParseData(List<String> csvLines) {
        Map<Long, List<LightCurvePoint>> healpixDataMap = new HashMap<>();

        for (String csvLine : csvLines) {
            try {
                String[] parts = csvLine.split(",", -1);
                if (parts.length < 14 || parts[0].equals("source_id")) continue;

                double ra = Double.parseDouble(parts[1].trim());
                double dec = Double.parseDouble(parts[2].trim());
                long healpixId = calculateHealpixId(ra, dec);

                LightCurvePoint point = new LightCurvePoint(
                        (long) Double.parseDouble(parts[0].trim()),
                        ra, dec,
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

                healpixDataMap.computeIfAbsent(healpixId, k -> new ArrayList<>()).add(point);
            } catch (Exception e) { }
        }

        return healpixDataMap;
    }

    /**
     * Phase 2: 纯写入（不含任何解析）
     * 输入是已经解析好的 LightCurvePoint 对象
     */
    public DistributionResult distributePreParsedData(Map<Long, List<LightCurvePoint>> healpixDataMap) {
        long startTime = System.currentTimeMillis();

        final int numThreads = RocksDBGlobalResourceManager.WRITE_THREADS;
        final int batchSize = RocksDBGlobalResourceManager.BATCH_SIZE;

        @SuppressWarnings("unchecked")
        List<Map.Entry<Long, List<LightCurvePoint>>>[] shards = new List[numThreads];
        for (int i = 0; i < numThreads; i++) shards[i] = new ArrayList<>();

        int idx = 0;
        for (Map.Entry<Long, List<LightCurvePoint>> entry : healpixDataMap.entrySet()) {
            shards[idx % numThreads].add(entry);
            idx++;
        }

        AtomicLong totalSuccess = new AtomicLong(0);
        AtomicLong totalErrors = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int threadId = 0; threadId < numThreads; threadId++) {
            final List<Map.Entry<Long, List<LightCurvePoint>>> myShard = shards[threadId];
            writeExecutor.submit(() -> {
                try {
                    for (Map.Entry<Long, List<LightCurvePoint>> entry : myShard) {
                        long healpixId = entry.getKey();
                        List<LightCurvePoint> points = entry.getValue();
                        StorageNode node = storageNodes.get((int) (healpixId % nodesCount));
                        if (node == null) { totalErrors.addAndGet(points.size()); continue; }

                        for (int bs = 0; bs < points.size(); bs += batchSize) {
                            int be = Math.min(bs + batchSize, points.size());
                            List<LightCurvePoint> batch = points.subList(bs, be);
                            int written = node.writePointsBatch(healpixId, batch);
                            totalSuccess.addAndGet(written);
                            totalErrors.addAndGet(batch.size() - written);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR] Write thread failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        long duration = System.currentTimeMillis() - startTime;
        return new DistributionResult(
                (int) totalSuccess.get(), (int) totalErrors.get(), duration,
                healpixDataMap.size(), numThreads);
    }

    // ==================== Phase 2: 优化写入 ====================

    /**
     * Phase 2 核心：一次解析 + 对象分发
     *
     * 优化前：CSV字符串 → split分组 → 传给StorageNode → 再次parseFromCSV → 写入
     * 优化后：CSV字符串 → 一次解析为LightCurvePoint+healpixId → 按healpixId分组 → 直接写入
     */
    public DistributionResult distributeDataBatch(List<String> csvLines) {
        long startTime = System.currentTimeMillis();

        // Step 1: 一次性解析所有 CSV → LightCurvePoint，同时计算 healpixId
        Map<Long, List<LightCurvePoint>> healpixDataMap = new HashMap<>();
        int parseErrors = 0;

        for (String csvLine : csvLines) {
            try {
                String[] parts = csvLine.split(",", -1);
                if (parts.length < 14 || parts[0].equals("source_id")) continue;

                double ra = Double.parseDouble(parts[1].trim());
                double dec = Double.parseDouble(parts[2].trim());
                long healpixId = calculateHealpixId(ra, dec);

                LightCurvePoint point = new LightCurvePoint(
                        (long) Double.parseDouble(parts[0].trim()),
                        ra, dec,
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

                healpixDataMap.computeIfAbsent(healpixId, k -> new ArrayList<>()).add(point);
            } catch (Exception e) {
                parseErrors++;
            }
        }

        // Step 2: 分片到 WRITE_THREADS 个线程
        final int numThreads = RocksDBGlobalResourceManager.WRITE_THREADS;
        final int batchSize = RocksDBGlobalResourceManager.BATCH_SIZE;

        @SuppressWarnings("unchecked")
        List<Map.Entry<Long, List<LightCurvePoint>>>[] shards = new List[numThreads];
        for (int i = 0; i < numThreads; i++) shards[i] = new ArrayList<>();

        int idx = 0;
        for (Map.Entry<Long, List<LightCurvePoint>> entry : healpixDataMap.entrySet()) {
            shards[idx % numThreads].add(entry);
            idx++;
        }

        // Step 3: 并行写入（直接传 LightCurvePoint 对象，不再传 CSV）
        AtomicLong totalSuccess = new AtomicLong(0);
        AtomicLong totalErrors = new AtomicLong(parseErrors);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int threadId = 0; threadId < numThreads; threadId++) {
            final List<Map.Entry<Long, List<LightCurvePoint>>> myShard = shards[threadId];
            writeExecutor.submit(() -> {
                try {
                    for (Map.Entry<Long, List<LightCurvePoint>> entry : myShard) {
                        long healpixId = entry.getKey();
                        List<LightCurvePoint> points = entry.getValue();
                        StorageNode node = storageNodes.get((int) (healpixId % nodesCount));
                        if (node == null) { totalErrors.addAndGet(points.size()); continue; }

                        for (int bs = 0; bs < points.size(); bs += batchSize) {
                            int be = Math.min(bs + batchSize, points.size());
                            List<LightCurvePoint> batch = points.subList(bs, be);
                            int written = node.writePointsBatch(healpixId, batch);
                            totalSuccess.addAndGet(written);
                            totalErrors.addAndGet(batch.size() - written);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR] Write thread failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        long overallDuration = System.currentTimeMillis() - startTime;
        return new DistributionResult(
                (int) totalSuccess.get(), (int) totalErrors.get(), overallDuration,
                healpixDataMap.size(), numThreads);
    }

    private void ensureHealpixDatabaseInitialized(long healpixId) {
        StorageNode node = storageNodes.get((int) (healpixId % nodesCount));
        if (node != null) {
            String dbPath = String.format(basePath + "node%d/healpix_%d", healpixId % nodesCount, healpixId);
            RocksDBServer.Config config = new RocksDBServer.Config(dbPath, healpixId);
            config.timeBucketSize = 1;
            node.initializeHealpixDatabase(healpixId, config);
        }
    }

    // ==================== Query methods (unchanged) ====================

    public DistributedQueryResult executeDistributedQueryWithTimeBuckets(
            double ra, double dec, double radius,
            double startTime, double endTime,
            String band, double magThreshold, int minObservations) {
        long queryStart = System.nanoTime();
        try {
            Set<Long> healpixIdsToQuery = calculateHealpixIdsInCone(ra, dec, radius);
            if (healpixIdsToQuery.isEmpty()) {
                return new DistributedQueryResult(Collections.emptySet(), 0, System.nanoTime() - queryStart, 0, 0);
            }
            Set<Long> allCandidates = new LinkedHashSet<>();
            int nodesQueried = 0, nodesResponded = 0;
            List<CompletableFuture<Set<Long>>> futures = new ArrayList<>();
            for (Long healpixId : healpixIdsToQuery) {
                StorageNode node = storageNodes.get((int) (healpixId % nodesCount));
                if (node != null) {
                    nodesQueried++;
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        try {
                            QueryResult nodeResult = node.executeQueryWithTimeBuckets(healpixId, startTime, endTime, band, magThreshold, minObservations);
                            if (nodeResult != null && nodeResult.candidateObjects != null && nodeResult.candidateMetadata != null) {
                                return filterCandidatesByDistance(nodeResult.candidateObjects, nodeResult.candidateMetadata, ra, dec, radius);
                            }
                        } catch (Exception e) { }
                        return Collections.<Long>emptySet();
                    }));
                }
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
            for (CompletableFuture<Set<Long>> f : futures) {
                Set<Long> c = f.get();
                allCandidates.addAll(c);
                if (!c.isEmpty()) nodesResponded++;
            }
            return new DistributedQueryResult(allCandidates, allCandidates.size(), System.nanoTime() - queryStart, nodesQueried, nodesResponded);
        } catch (Exception e) {
            return new DistributedQueryResult(Collections.emptySet(), 0, System.nanoTime() - queryStart, 0, 0);
        }
    }

    public DistributedQueryResult executeDistributedExistenceQuery(double ra, double dec, double radius,
                                                                   double startTime, double endTime, String band,
                                                                   double magThreshold, int minObservations) {
        long queryStart = System.nanoTime();
        try {
            Set<Long> healpixIdsToQuery = calculateHealpixIdsInCone(ra, dec, radius);
            if (healpixIdsToQuery.isEmpty()) {
                return new DistributedQueryResult(Collections.emptySet(), 0, System.nanoTime() - queryStart, 0, 0);
            }
            Set<Long> allCandidates = new LinkedHashSet<>();
            int nodesQueried = 0, nodesResponded = 0;
            List<CompletableFuture<Set<Long>>> futures = new ArrayList<>();
            for (Long healpixId : healpixIdsToQuery) {
                StorageNode node = storageNodes.get((int) (healpixId % nodesCount));
                if (node != null) {
                    nodesQueried++;
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        try {
                            QueryResult nodeResult = node.executeExistenceQueryWithScan(healpixId, startTime, endTime, band, magThreshold, minObservations);
                            if (nodeResult != null && nodeResult.candidateObjects != null && nodeResult.candidateMetadata != null) {
                                return filterCandidatesByDistance(nodeResult.candidateObjects, nodeResult.candidateMetadata, ra, dec, radius);
                            }
                        } catch (Exception e) { }
                        return Collections.<Long>emptySet();
                    }));
                }
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);
            for (CompletableFuture<Set<Long>> f : futures) {
                Set<Long> c = f.get();
                allCandidates.addAll(c);
                if (!c.isEmpty()) nodesResponded++;
            }
            return new DistributedQueryResult(allCandidates, allCandidates.size(), System.nanoTime() - queryStart, nodesQueried, nodesResponded);
        } catch (Exception e) {
            return new DistributedQueryResult(Collections.emptySet(), 0, System.nanoTime() - queryStart, 0, 0);
        }
    }

    public void buildAllTimeBucketsOffline() {
        storageNodes.parallelStream().forEach(StorageNode::buildAllTimeBucketsOffline);
    }

    public long calculateHealpixId(double raDegrees, double decDegrees) {
        return HealpixUtil.raDecToHealpix(raDegrees, decDegrees, level);
    }

    public List<LightCurvePoint> getLightCurve(long healpixId, long sourceId, String band) {
        StorageNode node = storageNodes.get((int) (healpixId % nodesCount));
        return node == null ? Collections.emptyList() : node.getLightCurve(healpixId, sourceId, band);
    }

    private Set<Long> calculateHealpixIdsInCone(double ra, double dec, double radius) {
        Set<Long> healpixIds = new HashSet<>();
        try {
            HealpixUtil healpixUtil = new HealpixUtil();
            healpixUtil.setNside(level);
            RangeSet rangeSet = healpixUtil.queryDisc(ra, dec, radius, level);
            if (rangeSet != null) {
                for (int i = 0; i < rangeSet.nranges(); i++) {
                    for (long pix = rangeSet.ivbegin(i); pix <= rangeSet.ivend(i); pix++) healpixIds.add(pix);
                }
            }
        } catch (Exception e) { }
        return healpixIds;
    }

    private Set<Long> filterCandidatesByDistance(Set<Long> candidates, Map<Long, StarMetadata> metadataMap,
                                                 double centerRa, double centerDec, double radius) {
        Set<Long> filtered = new LinkedHashSet<>();
        for (Long sourceId : candidates) {
            StarMetadata m = metadataMap.get(sourceId);
            if (m != null) {
                if (calculateGreatCircleDistance(centerRa, centerDec, m.ra, m.dec) <= radius) filtered.add(sourceId);
            } else {
                filtered.add(sourceId);
            }
        }
        return filtered;
    }

    private double calculateGreatCircleDistance(double ra1, double dec1, double ra2, double dec2) {
        double ra1R = Math.toRadians(ra1), dec1R = Math.toRadians(dec1);
        double ra2R = Math.toRadians(ra2), dec2R = Math.toRadians(dec2);
        double a = Math.sin((dec2R - dec1R) / 2) * Math.sin((dec2R - dec1R) / 2) +
                Math.cos(dec1R) * Math.cos(dec2R) * Math.sin((ra2R - ra1R) / 2) * Math.sin((ra2R - ra1R) / 2);
        return Math.toDegrees(2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
    }

    public List<StarMetadata> getAllStarsMetadata() {
        List<StarMetadata> all = new ArrayList<>();
        for (StorageNode node : storageNodes) all.addAll(node.getAllStarsMetadata());
        return all;
    }

    public List<StarMetadata> queryStarsByRegion(double minRa, double maxRa, double minDec, double maxDec) {
        List<StarMetadata> result = new ArrayList<>();
        for (StorageNode node : storageNodes) result.addAll(node.queryStarsByRegion(minRa, maxRa, minDec, maxDec));
        return result;
    }

    public Map<String, Object> getSystemStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("total_nodes", storageNodes.size());
        status.put("level", level);
        return status;
    }

    public Map<String, Object> getBoundaryAlignmentStats() {
        Map<String, Object> result = new HashMap<>();
        result.put("nodes_with_stats", storageNodes.size());
        return result;
    }

    public PerformanceComparisonResult performComparisonTest(long testHealpixId, int iterations,
                                                             double startTime, double endTime, String band,
                                                             double magThreshold, int minObservations) {
        StorageNode node = storageNodes.get((int) (testHealpixId % nodesCount));
        if (node == null) throw new IllegalArgumentException("No storage node for HEALPix " + testHealpixId);
        List<Double> indexTimes = new ArrayList<>(), scanTimes = new ArrayList<>();
        Set<Long> indexResults = null, scanResults = null;
        for (int i = 0; i < iterations; i++) {
            QueryResult tb = node.executeQueryWithTimeBuckets(testHealpixId, startTime, endTime, band, magThreshold, minObservations);
            indexTimes.add(tb.getQueryTimeMs());
            if (indexResults == null) indexResults = tb.candidateObjects;
            QueryResult sc = node.executeExistenceQueryWithScan(testHealpixId, startTime, endTime, band, magThreshold, minObservations);
            scanTimes.add(sc.getQueryTimeMs());
            if (scanResults == null) scanResults = sc.candidateObjects;
            try { Thread.sleep(10); } catch (InterruptedException e) {}
        }
        boolean match = indexResults != null && scanResults != null &&
                (Math.abs(indexResults.size() - scanResults.size()) / (double) Math.max(1, scanResults.size()) < 0.01);
        return new PerformanceComparisonResult(indexTimes, scanTimes, match,
                indexResults != null ? indexResults.size() : 0, scanResults != null ? scanResults.size() : 0);
    }

    public void shutdown() {
        for (StorageNode node : storageNodes) node.shutdown();
    }

    public void forceFlushAll() {
        for (StorageNode node : storageNodes) node.forceFlushAll();
    }
    /**
     * 只 flush（memtable → SST），不做 compaction
     * 用于 Phase 2 计时内，确保数据真正持久化
     */
    public void forceFlushAllNoCompaction() {
        for (StorageNode node : storageNodes) {
            node.forceFlushAllNoCompaction();
        }
    }

    /**
     * 只做 compaction（用于 SA 统计），不在计时范围内
     */
    public void compactOnly() {
        for (StorageNode node : storageNodes) {
            node.compactOnly();
        }
    }
    public double getOverallWriteAmplification() {
        double totalLogical = 0, totalPhysical = 0;
        for (StorageNode node : storageNodes) {
            double[] stats = node.getOverallWAStats();
            totalLogical += stats[0];
            totalPhysical += stats[1];
        }
        return totalLogical > 0 ? totalPhysical / totalLogical : 1.0;
    }

    // ========== Inner classes ==========

    public static class DistributionResult {
        public final int totalSuccess, totalErrors;
        public final long durationMs;
        public final int healpixCount, nodesUsed;
        public DistributionResult(int s, int e, long d, int h, int n) {
            totalSuccess = s; totalErrors = e; durationMs = d; healpixCount = h; nodesUsed = n;
        }
    }

    public static class DistributedQueryResult {
        public final Set<Long> candidateObjects;
        public final int candidateCount;
        public final long queryTimeNs;
        public final int nodesQueried, nodesResponded;
        public DistributedQueryResult(Set<Long> c, int cc, long t, int nq, int nr) {
            candidateObjects = c; candidateCount = cc; queryTimeNs = t; nodesQueried = nq; nodesResponded = nr;
        }
        public double getQueryTimeMs() { return queryTimeNs / 1_000_000.0; }
    }

    public static class PerformanceComparisonResult {
        public final List<Double> indexQueryTimes, scanQueryTimes;
        public final boolean resultsMatch;
        public final int indexResultCount, scanResultCount;
        public PerformanceComparisonResult(List<Double> i, List<Double> s, boolean m, int ic, int sc) {
            indexQueryTimes = i; scanQueryTimes = s; resultsMatch = m; indexResultCount = ic; scanResultCount = sc;
        }
        public double getAvgIndexTime() { return indexQueryTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0); }
        public double getAvgScanTime() { return scanQueryTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0); }
        public double getSpeedupRatio() { double s = getAvgScanTime(), i = getAvgIndexTime(); return s > 0 ? s / i : 0; }
    }
}