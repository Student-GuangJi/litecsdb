package org.example;

import healpix.RangeSet;
import org.example.demo.RocksDBServer.*;
import org.example.utils.HealpixUtil;

import java.io.File;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
/**
 * MainNode - 负责数据分发和查询协调
 */
public class MainNode {
    private final List<StorageNode> storageNodes;
    private final ExecutorService distributionExecutor;
    private final int nodesCount;

    // HEALPix配置
    private final int level; // HEALPix 层级参数

    public MainNode(int nodesCount, int level) {
        this.nodesCount = nodesCount;
        this.level = level;
        this.storageNodes = new ArrayList<>();
        this.distributionExecutor = Executors.newFixedThreadPool(Math.max(4, nodesCount));

        initializeStorageNodes();
    }

    /**
     * 初始化存储节点
     */
    private void initializeStorageNodes() {
        String basePath = "src/demo";
        for (int i = 0; i < nodesCount; i++) {
            StorageNode node = new StorageNode(i, basePath);
            storageNodes.add(node);
            File dbDir = new File(basePath + "/node" + i);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }
            // 重启时重新打开已存在的数据库
            loadExistingDatabases(node, dbDir, i);
        }
    }

    /**
     * 加载已存在的HEALPix数据库（并行版本）
     */
    private void loadExistingDatabases(StorageNode node, File nodeDir, int nodeId) {
        if (!nodeDir.exists() || !nodeDir.isDirectory()) {
            return;
        }

        File[] healpixDirs = nodeDir.listFiles((dir, name) -> name.startsWith("healpix_"));

        if (healpixDirs == null || healpixDirs.length == 0) {
            System.out.println("Node " + nodeId + ": 没有找到已存在的数据库");
            return;
        }

        System.out.println("Node " + nodeId + ": 找到 " + healpixDirs.length + " 个已存在的HEALPix数据库，开始并行加载...");

        // ✅ 使用并行流并发加载
        long startTime = System.currentTimeMillis();

        Arrays.stream(healpixDirs).parallel().forEach(healpixDir -> {
            try {
                String dirName = healpixDir.getName();
                long healpixId = Long.parseLong(dirName.replace("healpix_", ""));

                String dbPath = healpixDir.getAbsolutePath();
                RocksDBServer.Config config = new RocksDBServer.Config(dbPath, healpixId);
                config.timeBucketSize = 1;
                config.bitmapSize = 10000;

                // 禁用启动时的统计和压缩
                config.asyncIndexing = true;  // 保持异步
                config.maxBackgroundCompactions = 2;  // 减少后台压缩线程

                boolean initialized = node.initializeHealpixDatabase(healpixId, config);

                if (initialized) {
                    System.out.println("  ✓ 加载 HEALPix " + healpixId);
                }

            } catch (Exception e) {
                System.err.println("  ✗ 加载失败: " + healpixDir.getName() + " - " + e.getMessage());
            }
        });

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Node " + nodeId + ": 加载完成，耗时 " + duration + " ms");
    }

    /**
     * 执行基于时间桶的分布式查询
     */
    public DistributedQueryResult executeDistributedQueryWithTimeBuckets(
            double ra, double dec, double radius,
            double startTime, double endTime,
            String band, double magThreshold, int minObservations) {
        long queryStart = System.nanoTime();

        try {
            // 1. 计算锥形检索范围内的HEALPix天区
            Set<Long> healpixIdsToQuery = calculateHealpixIdsInCone(ra, dec, radius);

//            System.out.println("时间桶查询 - 锥形检索范围: RA=" + ra + ", Dec=" + dec +
//                    ", Radius=" + radius + "度，涉及 " + healpixIdsToQuery.size() +
//                    " 个HEALPix天区");

            if (healpixIdsToQuery.isEmpty()) {
                return new DistributedQueryResult(Collections.emptySet(), 0,
                        System.nanoTime() - queryStart, 0, 0);
            }

            Set<Long> allCandidates = new LinkedHashSet<>();
            int nodesQueried = 0;
            int nodesResponded = 0;

            // 2. 并行查询所有相关的HEALPix天区
            List<CompletableFuture<Set<Long>>> futures = new ArrayList<>();

            for (Long healpixId : healpixIdsToQuery) {
                StorageNode node = storageNodes.get((int)(healpixId % nodesCount));

                if (node != null) {
                    nodesQueried++;

                    CompletableFuture<Set<Long>> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            StorageNode.QueryResult nodeResult =
                                    node.executeQueryWithTimeBuckets(healpixId, startTime, endTime,
                                            band, magThreshold, minObservations);

                            if (nodeResult != null && nodeResult.candidateObjects != null &&
                                    nodeResult.candidateMetadata != null) {
                                // 对边界天区的天体进行精确距离筛选
                                return filterCandidatesByDistance(nodeResult.candidateObjects,
                                        nodeResult.candidateMetadata,
                                        ra, dec, radius);
                            }
                        } catch (Exception e) {
                            System.err.println("查询HEALPix " + healpixId + " 失败: " + e.getMessage());
                        }
                        return Collections.<Long>emptySet();
                    });

                    futures.add(future);
                }
            }

            // 等待所有查询完成
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
            );

            try {
                allFutures.get(10, TimeUnit.SECONDS);

                for (CompletableFuture<Set<Long>> future : futures) {
                    Set<Long> candidates = future.get();
                    allCandidates.addAll(candidates);
                    if (!candidates.isEmpty()) {
                        nodesResponded++;
                    }
                }

            } catch (Exception e) {
                System.err.println("查询超时或失败: " + e.getMessage());
            }

            long queryDuration = System.nanoTime() - queryStart;

//            System.out.println("时间桶查询完成: 查询 " + nodesQueried + " 个天区，响应 " +
//                    nodesResponded + " 个天区，找到 " + allCandidates.size() +
//                    " 个候选天体");

            return new DistributedQueryResult(allCandidates, allCandidates.size(),
                    queryDuration, nodesQueried, nodesResponded);

        } catch (Exception e) {
            System.err.println("分布式时间桶查询失败: " + e.getMessage());
            e.printStackTrace();
            return new DistributedQueryResult(Collections.emptySet(), 0,
                    System.nanoTime() - queryStart, 0, 0);
        }
    }
    /**
     * 根据坐标计算HEALPix ID
     */
    public long calculateHealpixId(double raDegrees, double decDegrees) {
        return HealpixUtil.raDecToHealpix(raDegrees, decDegrees, level);
    }
    public List<LightCurvePoint> getLightCurve(long healpixId, long sourceId, String band) {
        StorageNode node = storageNodes.get((int)(healpixId % nodesCount));
        if (node == null) {
            return Collections.emptyList();
        }
        return node.getLightCurve(healpixId, sourceId, band);
    }
    public BitmapIndexEntry getBitmapIndex(long healpixId, long sourceId, String band) {
        StorageNode node = storageNodes.get((int)(healpixId % nodesCount));
        if (node == null) {
            return null;
        }
        return node.getBitmapIndex(healpixId, sourceId, band);
    }

    /**
     * 批量分发数据到存储节点
     */
    public DistributionResult distributeDataBatch(List<String> csvLines) {
        long startTime = System.currentTimeMillis();

        // 按HEALPix天区分组数据
        Map<Long, List<String>> healpixDataMap = new HashMap<>();

        for (String csvLine : csvLines) {
            try {
                RocksDBServer.LightCurvePoint point = RocksDBServer.parseFromCSV(csvLine);
                long healpixId = calculateHealpixId(point.ra, point.dec);
                healpixDataMap.computeIfAbsent(healpixId, k -> new ArrayList<>()).add(csvLine);
            } catch (Exception e) {
                // 跳过解析错误的数据
            }
        }

        for (long healpixId : healpixDataMap.keySet()) {
            ensureHealpixDatabaseInitialized(healpixId);
        }

        // 并行分发到存储节点
        List<CompletableFuture<StorageNode.WriteResult>> futures = new ArrayList<>();

        for (Map.Entry<Long, List<String>> entry : healpixDataMap.entrySet()) {
            long healpixId = entry.getKey();
            List<String> dataLines = entry.getValue();
            StorageNode node = storageNodes.get((int)(healpixId % nodesCount));

            if (node != null) {
                CompletableFuture<StorageNode.WriteResult> future = CompletableFuture.supplyAsync(
                        () -> node.processDataBatch(healpixId, dataLines), distributionExecutor);
                futures.add(future);
            }
        }

        // 等待所有分发完成
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
        );

        try {
            allFutures.get(30, TimeUnit.SECONDS); // 30秒超时

            // 收集结果
            int totalSuccess = 0;
            int totalErrors = 0;
            long totalDuration = 0;

            for (CompletableFuture<StorageNode.WriteResult> future : futures) {
                StorageNode.WriteResult result = future.get();
                totalSuccess += result.successCount;
                totalErrors += result.errorCount;
                totalDuration = Math.max(totalDuration, result.durationMs);
            }

            long overallDuration = System.currentTimeMillis() - startTime;
            return new DistributionResult(totalSuccess, totalErrors, overallDuration,
                    healpixDataMap.size(), futures.size());

        } catch (Exception e) {
            return new DistributionResult(0, csvLines.size(),
                    System.currentTimeMillis() - startTime, 0, 0);
        }
    }
    private void ensureHealpixDatabaseInitialized(long healpixId) {
        StorageNode node = storageNodes.get((int)(healpixId % nodesCount));
        if (node != null) {
            // 创建数据库配置
            String dbPath = String.format("src/demo/node%d/healpix_%d",
                    healpixId % nodesCount, healpixId);
            RocksDBServer.Config config = new RocksDBServer.Config(dbPath, healpixId);
            config.timeBucketSize = 1;
            config.bitmapSize = 10000;

            // 初始化数据库
            node.initializeHealpixDatabase(healpixId, config);
        }
    }

    /**
     * 执行分布式完整锥形检索查询
     */
    public DistributedQueryResult executeDistributedExistenceQuery(double ra, double dec, double radius,
                                                                   double startTime, double endTime,
                                                                   String band, double magThreshold,
                                                                   int minObservations) {
        long queryStart = System.nanoTime();

        try {
            // 1. 计算锥形检索范围内的HEALPix天区
            Set<Long> healpixIdsToQuery = calculateHealpixIdsInCone(ra, dec, radius);

//            System.out.println("锥形检索范围: RA=" + ra + ", Dec=" + dec + ", Radius=" + radius +
//                    "度，星等阈值=" + magThreshold + "，涉及 " + healpixIdsToQuery.size() + " 个HEALPix天区");

            if (healpixIdsToQuery.isEmpty()) {
                return new DistributedQueryResult(Collections.emptySet(), 0,
                        System.nanoTime() - queryStart, 0, 0);
            }

            Set<Long> allCandidates = new LinkedHashSet<>();
            int nodesQueried = 0;
            int nodesResponded = 0;

            // 2. 并行查询所有相关的HEALPix天区
            List<CompletableFuture<Set<Long>>> futures = new ArrayList<>();

            for (Long healpixId : healpixIdsToQuery) {
                StorageNode node = storageNodes.get((int)(healpixId % nodesCount));

                if (node != null) {
                    nodesQueried++;

                    CompletableFuture<Set<Long>> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            StorageNode.QueryResult nodeResult;
                            nodeResult = node.executeExistenceQueryWithScan(healpixId, startTime, endTime, band, magThreshold, minObservations);

                            if (nodeResult != null && nodeResult.candidateObjects != null &&
                                    nodeResult.candidateMetadata != null) {
                                // 对边界天区的天体进行精确距离筛选
                                return filterCandidatesByDistance(nodeResult.candidateObjects,
                                        nodeResult.candidateMetadata,
                                        ra, dec, radius);
                            }
                        } catch (Exception e) {
                            System.err.println("查询HEALPix " + healpixId + " 失败: " + e.getMessage());
                        }
                        return Collections.<Long>emptySet();
                    });

                    futures.add(future);
                }
            }

            // 等待所有查询完成
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
            );

            try {
                allFutures.get(10, TimeUnit.SECONDS); // 10秒超时

                // 收集结果
                for (CompletableFuture<Set<Long>> future : futures) {
                    Set<Long> candidates = future.get();
//                    System.out.println("MainNode: 一个天区返回 " + candidates);
                    allCandidates.addAll(candidates);
                    if (!candidates.isEmpty()) {
                        nodesResponded++;
                    }
                }

            } catch (Exception e) {
                System.err.println("查询超时或失败: " + e.getMessage());
            }

            long queryDuration = System.nanoTime() - queryStart;

//            System.out.println("锥形检索完成: 查询 " + nodesQueried + " 个天区，响应 " + nodesResponded +
//                    " 个天区，找到 " + allCandidates.size() + " 个候选天体（星等阈值=" + magThreshold + "）");


            return new DistributedQueryResult(allCandidates, allCandidates.size(),
                    queryDuration, nodesQueried, nodesResponded);

        } catch (Exception e) {
            System.err.println("分布式查询失败: " + e.getMessage());
            e.printStackTrace();
            return new DistributedQueryResult(Collections.emptySet(), 0,
                    System.nanoTime() - queryStart, 0, 0);
        }
    }

    /**
     * 计算锥形检索范围内的HEALPix天区
     */
    private Set<Long> calculateHealpixIdsInCone(double ra, double dec, double radius) {
        Set<Long> healpixIds = new HashSet<>();

        try {
            // 使用HEALPix工具计算圆盘范围内的天区
            HealpixUtil healpixUtil = new HealpixUtil();
            healpixUtil.setNside(level);
            RangeSet rangeSet = healpixUtil.queryDisc(ra, dec, radius, level);

            // 将RangeSet转换为具体的HEALPix ID集合
            if (rangeSet != null) {
                for (int i = 0; i < rangeSet.nranges(); i++) {
                    long start = rangeSet.ivbegin(i);
                    long end = rangeSet.ivend(i);
                    for (long pix = start; pix <= end; pix++) {
                        healpixIds.add(pix);
                    }
                }
            }
//            System.out.println("HEALPix天区: " + healpixIds);

        } catch (Exception e) {
            System.err.println("计算HEALPix天区失败: " + e.getMessage());
            e.printStackTrace();
        }

        return healpixIds;
    }

    /**
     * 根据距离筛选候选天体
     */
    private Set<Long> filterCandidatesByDistance(Set<Long> candidates,
                                                 Map<Long, RocksDBServer.StarMetadata> metadataMap,
                                                 double centerRa, double centerDec, double radius) {
        if (candidates.isEmpty()) {
            return candidates;
        }

        // 使用LinkedHashSet确保结果有序且无重复
        Set<Long> filtered = new LinkedHashSet<>();

        for (Long sourceId : candidates) {
            try {
                RocksDBServer.StarMetadata metadata = metadataMap.get(sourceId);
                if (metadata != null) {
                    double starRa = metadata.ra;
                    double starDec = metadata.dec;

                    // 计算大圆距离
                    double distance = calculateGreatCircleDistance(centerRa, centerDec, starRa, starDec);

                    if (distance <= radius) {
                        filtered.add(sourceId);
                    }
                }
            } catch (Exception e) {
                System.err.println("筛选天体 " + sourceId + " 失败: " + e.getMessage());
                // 如果无法获取坐标信息，保守地包含该天体
                filtered.add(sourceId);
            }
        }

        System.out.println("距离筛选: " + candidates.size() + " -> " + filtered.size() + " 个天体");

        return filtered;
    }

    /**
     * 计算大圆距离（度）
     */
    private double calculateGreatCircleDistance(double ra1, double dec1, double ra2, double dec2) {
        // 使用球面三角公式计算大圆距离
        double ra1Rad = Math.toRadians(ra1);
        double dec1Rad = Math.toRadians(dec1);
        double ra2Rad = Math.toRadians(ra2);
        double dec2Rad = Math.toRadians(dec2);

        double deltaRa = ra2Rad - ra1Rad;
        double deltaDec = dec2Rad - dec1Rad;

        double a = Math.sin(deltaDec / 2) * Math.sin(deltaDec / 2) +
                Math.cos(dec1Rad) * Math.cos(dec2Rad) *
                        Math.sin(deltaRa / 2) * Math.sin(deltaRa / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return Math.toDegrees(c);
    }

    /**
     * 执行性能对比测试
     */
    public PerformanceComparisonResult performComparisonTest(long testHealpixId,
                                                             int iterations,
                                                             double startTime, double endTime,
                                                             String band, double magThreshold, int minObservations) {
        StorageNode node = storageNodes.get((int)(testHealpixId % nodesCount));
        if (node == null) {
            throw new IllegalArgumentException("No storage node for HEALPix " + testHealpixId);
        }

        List<Double> timeBucketsTimes = new ArrayList<>();
        List<Double> scanTimes = new ArrayList<>();
        Set<Long> timeBucketsResults = null;
        Set<Long> scanResults = null;

        // 执行测试
        for (int i = 0; i < iterations; i++) {
            // 索引查询
            StorageNode.QueryResult timeBuckets =
                    node.executeQueryWithTimeBuckets(testHealpixId, startTime, endTime, band, magThreshold, minObservations);
            timeBucketsTimes.add(timeBuckets.getQueryTimeMs());
            if (timeBucketsResults == null) timeBucketsResults = timeBuckets.candidateObjects;

            // 全表扫描查询
            StorageNode.QueryResult scanResult =
                    node.executeExistenceQueryWithScan(testHealpixId, startTime, endTime, band, magThreshold, minObservations);
            scanTimes.add(scanResult.getQueryTimeMs());
            if (scanResults == null) scanResults = scanResult.candidateObjects;

            // 短暂休眠避免资源竞争
            try { Thread.sleep(10); } catch (InterruptedException e) {}
        }

        // 验证结果一致性
        boolean resultsMatch = timeBucketsResults != null && scanResults != null &&
                (Math.abs(timeBucketsResults.size()-scanResults.size())/scanResults.size()<0.01);

        return new PerformanceComparisonResult(timeBucketsTimes, scanTimes, resultsMatch,
                timeBucketsResults != null ? timeBucketsResults.size() : 0,
                scanResults != null ? scanResults.size() : 0);
    }

    /**
     * 获取所有天体的元数据
     */
    public List<RocksDBServer.StarMetadata> getAllStarsMetadata() {
        List<RocksDBServer.StarMetadata> allStars = new ArrayList<>();

        for (StorageNode node : storageNodes) {
            List<RocksDBServer.StarMetadata> nodeStars = node.getAllStarsMetadata();
            allStars.addAll(nodeStars);
        }

        return allStars;
    }

    /**
     * 根据坐标范围查询天体
     */
    public List<RocksDBServer.StarMetadata> queryStarsByRegion(double minRa, double maxRa,
                                                               double minDec, double maxDec) {
        List<RocksDBServer.StarMetadata> result = new ArrayList<>();

        for (StorageNode node : storageNodes) {
            List<RocksDBServer.StarMetadata> regionStars =
                    node.queryStarsByRegion(minRa, maxRa, minDec, maxDec);
            result.addAll(regionStars);
        }

        return result;
    }

    /**
     * 获取指定天体的详细信息
     */
    public Map<String, Object> getStarDetails(long sourceId) {
        // 计算sourceId对应的HEALPix ID（需要知道坐标，这里需要先获取元数据）
        // 这里我们暂时保留原来的遍历方式，或者可以修改为更高效的方式
        for (StorageNode node : storageNodes) {
            Map<String, Object> details = node.getStarDetails(sourceId);
            if (!details.isEmpty()) {
                return details;
            }
        }
        return Collections.emptyMap();
    }

    /**
     * 获取系统状态
     */
    public Map<String, Object> getSystemStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("total_nodes", storageNodes.size());
        status.put("level", level);

        List<Map<String, Object>> nodeStatuses = new ArrayList<>();
        for (StorageNode node : storageNodes) {
            try {
                nodeStatuses.add(node.getNodeStatus());
            } catch (Exception e) {
                // 跳过错误节点
            }
        }
        status.put("nodes", nodeStatuses);

        return status;
    }

    public void shutdown() {
        distributionExecutor.shutdown();
        for (StorageNode node : storageNodes) {
            node.shutdown();
        }
    }

    // ========== 内部类 ==========

    public static class DistributionResult {
        public final int totalSuccess;
        public final int totalErrors;
        public final long durationMs;
        public final int healpixCount;
        public final int nodesUsed;

        public DistributionResult(int totalSuccess, int totalErrors, long durationMs,
                                  int healpixCount, int nodesUsed) {
            this.totalSuccess = totalSuccess;
            this.totalErrors = totalErrors;
            this.durationMs = durationMs;
            this.healpixCount = healpixCount;
            this.nodesUsed = nodesUsed;
        }
    }

    public static class DistributedQueryResult {
        public final Set<Long> candidateObjects;
        public final int candidateCount;
        public final long queryTimeNs;
        public final int nodesQueried;
        public final int nodesResponded;

        public DistributedQueryResult(Set<Long> candidateObjects, int candidateCount,
                                      long queryTimeNs, int nodesQueried, int nodesResponded) {
            this.candidateObjects = candidateObjects;
            this.candidateCount = candidateCount;
            this.queryTimeNs = queryTimeNs;
            this.nodesQueried = nodesQueried;
            this.nodesResponded = nodesResponded;
        }

        public double getQueryTimeMs() {
            return queryTimeNs / 1_000_000.0;
        }
    }

    public static class PerformanceComparisonResult {
        public final List<Double> bitmapQueryTimes; // ms
        public final List<Double> scanQueryTimes;   // ms
        public final boolean resultsMatch;
        public final int bitmapResultCount;
        public final int scanResultCount;

        public PerformanceComparisonResult(List<Double> bitmapQueryTimes, List<Double> scanQueryTimes,
                                           boolean resultsMatch, int bitmapResultCount, int scanResultCount) {
            this.bitmapQueryTimes = bitmapQueryTimes;
            this.scanQueryTimes = scanQueryTimes;
            this.resultsMatch = resultsMatch;
            this.bitmapResultCount = bitmapResultCount;
            this.scanResultCount = scanResultCount;
        }

        public double getAvgBitmapTime() {
            return bitmapQueryTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        }

        public double getAvgScanTime() {
            return scanQueryTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        }

        public double getSpeedupRatio() {
            double avgScan = getAvgScanTime();
            double avgBitmap = getAvgBitmapTime();
            return avgScan > 0 ? avgScan / avgBitmap : 0;
        }
    }
}