package org.example;

import org.example.demo.RocksDBServer.*;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * StorageNode - 管理多个HEALPix天区的数据存储
 */
public class StorageNode {
    private final int nodeId;
    private final String baseDataPath;
    private final Map<Long, RocksDBServer> healpixDatabases;
    private final ExecutorService queryExecutor;
    private final AtomicInteger queryCounter;

    // 性能统计
    private final Map<String, PerformanceStats> performanceStats;

    public StorageNode(int nodeId, String baseDataPath) {
        this.nodeId = nodeId;
        this.baseDataPath = baseDataPath;
        this.healpixDatabases = new ConcurrentHashMap<>();
        this.queryExecutor = Executors.newFixedThreadPool(8);
        this.queryCounter = new AtomicInteger(0);
        this.performanceStats = new ConcurrentHashMap<>();
    }
    /**
     * 使用时间桶的高效查询
     */
    public QueryResult executeQueryWithTimeBuckets(long healpixId, double startTime, double endTime,
                                                   String band, double magThreshold, int minObservations) {
        long queryStart = System.nanoTime();
        int queryId = queryCounter.incrementAndGet();

        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) {
            return new QueryResult(Collections.emptySet(), Collections.emptyMap(), 0,
                    System.nanoTime() - queryStart, queryId, "TIME_BUCKET");
        }

        try {
            // 获取该天区所有天体的元数据
            List<RocksDBServer.StarMetadata> allStars = dbService.getAllStarsMetadata();
            Set<Long> resultSet = new LinkedHashSet<>();

            int totalStars = allStars.size();
            int prunedStars = 0;
            int confirmedStars = 0;
            int refinedStars = 0;
            long totalRefineTime = 0;

            for (RocksDBServer.StarMetadata star : allStars) {
                try {
                    long starStartTime = System.nanoTime();

                    // 第一步：使用时间桶快速判断
                    RocksDBServer.TimeBucketQueryResult bucketResult =
                            dbService.queryWithTimeBuckets(star.sourceId, band, startTime, endTime,
                                    magThreshold, minObservations);

                    if (bucketResult.status.equals("PRUNED")) {
                        // 不可能满足，跳过
                        prunedStars++;
                        continue;
                    } else if (bucketResult.status.equals("CONFIRMED")) {
                        // 已经满足条件
                        resultSet.add(star.sourceId);
                        confirmedStars++;
                    } else if (bucketResult.status.equals("AMBIGUOUS")) {
                        // 需要精炼
                        long refineStart = System.nanoTime();
                        int refinedCount = dbService.refineAmbiguousBuckets(
                                star.sourceId, band, bucketResult.ambiguousBuckets,
                                startTime, endTime, magThreshold);
                        totalRefineTime += (System.nanoTime() - refineStart);

                        int totalCount = bucketResult.confirmedCount + refinedCount;
                        if (totalCount >= minObservations) {
                            resultSet.add(star.sourceId);
                            refinedStars++;
                        }
                    }
                } catch (Exception e) {
                    System.err.println("查询天体 " + star.sourceId + " 失败: " + e.getMessage());
                }
            }

            // 批量获取元数据
            Map<Long, RocksDBServer.StarMetadata> candidateMetadata =
                    getStarsMetadataBatch(healpixId, resultSet);

            long duration = System.nanoTime() - queryStart;

            // ✅ 打印详细统计
            System.out.println("========== 时间桶查询汇总 ==========");
            System.out.println("HEALPix ID: " + healpixId);
            System.out.println("总天体数: " + totalStars);
            System.out.println("剪枝天体数: " + prunedStars + " (" + (prunedStars * 100.0 / totalStars) + "%)");
            System.out.println("快速确认天体数: " + confirmedStars + " (" + (confirmedStars * 100.0 / totalStars) + "%)");
            System.out.println("需要精炼天体数: " + refinedStars + " (" + (refinedStars * 100.0 / totalStars) + "%)");
            System.out.println("最终结果数: " + resultSet.size());
            System.out.println("总查询耗时: " + (duration / 1_000_000.0) + " ms");
            System.out.println("精炼总耗时: " + (totalRefineTime / 1_000_000.0) + " ms");
            System.out.println("精炼占比: " + (totalRefineTime * 100.0 / duration) + "%");
            System.out.println("====================================");

            return new QueryResult(resultSet, candidateMetadata, resultSet.size(),
                    duration, queryId, "TIME_BUCKET");

        } catch (Exception e) {
            System.err.println("HEALPix " + healpixId + " 时间桶查询失败: " + e.getMessage());
            e.printStackTrace();
            return new QueryResult(Collections.emptySet(), Collections.emptyMap(), 0,
                    System.nanoTime() - queryStart, queryId, "TIME_BUCKET");
        }
    }
    /**
     * 初始化HEALPix数据库
     */
    public boolean initializeHealpixDatabase(long healpixId, RocksDBServer.Config config) {
        try {
            if (!healpixDatabases.containsKey(healpixId)) {
                // 确保目录存在
                File dbDir = new File(config.dbPath).getParentFile();
                if (!dbDir.exists()) {
                    dbDir.mkdirs();
                }

                RocksDBServer dbService = new RocksDBServer(config);
                healpixDatabases.put(healpixId, dbService);
                performanceStats.put("healpix_" + healpixId, new PerformanceStats());
                System.out.println("Initialized HEALPix database: " + healpixId + " at " + config.dbPath);
                return true;
            }
            return false;
        } catch (RocksDBException e) {
            System.err.println("Failed to initialize HEALPix " + healpixId + ": " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 批量处理数据写入
     */
    public WriteResult processDataBatch(long healpixId, List<String> csvLines) {
        long startTime = System.currentTimeMillis();
        RocksDBServer dbService = healpixDatabases.get(healpixId);

        if (dbService == null) {
            return new WriteResult(0, csvLines.size(), System.currentTimeMillis() - startTime, false);
        }

        try {
            List<RocksDBServer.LightCurvePoint> points = new ArrayList<>();
            int parseErrors = 0;

            for (String csvLine : csvLines) {
                try {
                    RocksDBServer.LightCurvePoint point = RocksDBServer.parseFromCSV(csvLine);
                    points.add(point);
                } catch (Exception e) {
                    parseErrors++;
                }
            }

            dbService.putLightCurveBatch(points);
            long duration = System.currentTimeMillis() - startTime;

            // 更新统计
            PerformanceStats stats = performanceStats.get("healpix_" + healpixId);
            if (stats != null) {
                stats.recordWrite(points.size(), duration);
            }

            return new WriteResult(points.size(), parseErrors, duration, true);

        } catch (Exception e) {
            return new WriteResult(0, csvLines.size(), System.currentTimeMillis() - startTime, false);
        }
    }

    /**
     * 查询单个天体的光变曲线
     */
    public List<LightCurvePoint> getLightCurve(long healpixId, long sourceId, String band) {
        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) {
            return Collections.emptyList();
        }
        try {
            return dbService.getLightCurve(sourceId);
        } catch (Exception e) {
            System.err.println("Failed to get light curve for source " + sourceId + " in HEALPix " + healpixId + ": " + e.getMessage());
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    /**
     * 查询单个天体的Bitmap
     */
    public BitmapIndexEntry getBitmapIndex(long healpixId, long sourceId, String band) {
        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) {
            return null;
        }
        try {
            return dbService.getBitmapIndex(sourceId, band);
        } catch (Exception e) {
            System.err.println("Failed to get time bitmap for source " + sourceId + " in HEALPix " + healpixId + ": " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 使用位图索引的存在性查询（优化版本）
     */
    public QueryResult executeExistenceQueryWithBitmap(long healpixId, double startTime, double endTime,
                                                       String band, int minObservations) {
        long queryStart = System.nanoTime();
        int queryId = queryCounter.incrementAndGet();

        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) {
            System.out.println("HEALPix " + healpixId + " 数据库未初始化");
            return new QueryResult(Collections.emptySet(), Collections.emptyMap(), 0,
                    System.nanoTime() - queryStart, queryId, "BITMAP");
        }

        try {
            Set<Long> candidateIds = new LinkedHashSet<>(dbService.existenceQuery(startTime, endTime, band, minObservations));

            // 批量获取候选天体的元数据（直接通过healpixId定位）
            Map<Long, RocksDBServer.StarMetadata> candidateMetadata =
                    getStarsMetadataBatch(healpixId, candidateIds);

            long duration = System.nanoTime() - queryStart;

            // 更新统计
            PerformanceStats stats = performanceStats.get("healpix_" + healpixId);
            if (stats != null) {
                stats.recordBitmapQuery(duration);
            }

            System.out.println("StorageNode.executeExistenceQueryWithBitmap: HEALPix " + healpixId +
                    " 返回 " + candidateIds);

            return new QueryResult(candidateIds, candidateMetadata, candidateIds.size(),
                    duration, queryId, "BITMAP");

        } catch (Exception e) {
            System.err.println("HEALPix " + healpixId + " 位图索引查询失败: " + e.getMessage());
            return new QueryResult(Collections.emptySet(), Collections.emptyMap(), 0,
                    System.nanoTime() - queryStart, queryId, "BITMAP");
        }
    }

    /**
     * 全表扫描的存在性查询
     */
    public QueryResult executeExistenceQueryWithScan(long healpixId, double startTime, double endTime,
                                                     String band, double magThreshold, int minObservations) {
        long queryStart = System.nanoTime();
        int queryId = queryCounter.incrementAndGet();

        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) {
            return new QueryResult(Collections.emptySet(), Collections.emptyMap(), 0,
                    System.nanoTime() - queryStart, queryId, "SCAN");
        }

        try {
            Set<Long> candidateIds = new HashSet<>();
            Map<Long, Integer> observationCounts = new HashMap<>();

            // 获取所有光变曲线数据
            List<RocksDBServer.LightCurvePoint> allPoints = dbService.queryByTimeRange(startTime, endTime);

            // 统计每个天体的观测次数
            for (RocksDBServer.LightCurvePoint point : allPoints) {
                if (point.band.equals(band) && point.mag <= magThreshold) {
                    observationCounts.merge(point.sourceId, 1, Integer::sum);
                }
            }

            // 过滤满足条件的候选天体
            for (Map.Entry<Long, Integer> entry : observationCounts.entrySet()) {
                if (entry.getValue() >= minObservations) {
                    candidateIds.add(entry.getKey());
                }
            }

            // 批量获取候选天体的元数据（直接通过healpixId定位）
            Map<Long, RocksDBServer.StarMetadata> candidateMetadata =
                    getStarsMetadataBatch(healpixId, candidateIds);

            long duration = System.nanoTime() - queryStart;

            // 更新统计
            PerformanceStats stats = performanceStats.get("healpix_" + healpixId);
            if (stats != null) {
                stats.recordScanQuery(duration);
            }

            System.out.println("HEALPix " + healpixId + " 全表扫描查询: " + candidateIds.size() +
                    " 个候选天体（星等阈值=" + magThreshold + "）");
            return new QueryResult(candidateIds, candidateMetadata, candidateIds.size(),
                    duration, queryId, "SCAN");

        } catch (Exception e) {
            System.err.println("HEALPix " + healpixId + " 全表扫描查询失败: " + e.getMessage());
            return new QueryResult(Collections.emptySet(), Collections.emptyMap(), 0,
                    System.nanoTime() - queryStart, queryId, "SCAN");
        }
    }

    /**
     * 获取所有天体的元数据
     */
    public List<RocksDBServer.StarMetadata> getAllStarsMetadata() {
        List<RocksDBServer.StarMetadata> allStars = new ArrayList<>();

        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                List<RocksDBServer.StarMetadata> healpixStars = dbService.getAllStarsMetadata();
                allStars.addAll(healpixStars);
            } catch (Exception e) {
                System.err.println("Failed to get stars metadata: " + e.getMessage());
            }
        }

        return allStars;
    }

    /**
     * 根据sourceId和healpixId获取天体元数据
     */
    public RocksDBServer.StarMetadata getStarMetadata(long healpixId, long sourceId) {
        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) {
            return null;
        }
        try {
            return dbService.getStarMetadata(sourceId);
        } catch (Exception e) {
            System.err.println("Failed to get star metadata for source " + sourceId +
                    " in HEALPix " + healpixId + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * 批量获取天体元数据
     */
    public Map<Long, RocksDBServer.StarMetadata> getStarsMetadataBatch(long healpixId, Set<Long> sourceIds) {
        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) {
            return Collections.emptyMap();
        }
        try {
            return dbService.getStarsMetadataBatch(sourceIds);
        } catch (Exception e) {
            System.err.println("Failed to batch get star metadata in HEALPix " + healpixId + ": " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * 根据坐标范围查询天体
     */
    public List<RocksDBServer.StarMetadata> queryStarsByRegion(double minRa, double maxRa,
                                                               double minDec, double maxDec) {
        List<RocksDBServer.StarMetadata> result = new ArrayList<>();

        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                List<RocksDBServer.StarMetadata> regionStars =
                        dbService.queryStarsByRegion(minRa, maxRa, minDec, maxDec);
                result.addAll(regionStars);
            } catch (Exception e) {
                System.err.println("Failed to query stars by region: " + e.getMessage());
            }
        }

        return result;
    }
    /**
     * 获取指定天体的详细信息
     */
    public Map<String, Object> getStarDetails(long sourceId) {
        for (RocksDBServer dbService: healpixDatabases.values()) {
            if (dbService == null) {
                return Collections.emptyMap();
            }
            try {
                // 获取光变曲线
                List<RocksDBServer.LightCurvePoint> lightCurve = dbService.getLightCurve(sourceId);
                if (!lightCurve.isEmpty()) {
                    // 获取位图索引
                    Map<String, RocksDBServer.BitmapIndexEntry> bitmapIndices = new HashMap<>();
                    String[] bands = {"G", "B", "R"};
                    for (String band : bands) {
                        try {
                            RocksDBServer.BitmapIndexEntry bitmapIndex = dbService.getBitmapIndex(sourceId, band);
                            if (bitmapIndex != null) {
                                bitmapIndices.put(band, bitmapIndex);
                            }
                        } catch (Exception e) {
                            // 忽略不存在的波段索引
                        }
                    }
                    Map<String, Object> details = new HashMap<>();
                    details.put("sourceId", sourceId);
                    details.put("lightCurve", lightCurve);
                    details.put("lightCurveCount", lightCurve.size());
                    details.put("bitmapIndices", bitmapIndices);

                    // 计算统计信息
                    if (!lightCurve.isEmpty()) {
                        double minMag = Double.MAX_VALUE;
                        double maxMag = Double.MIN_VALUE;
                        double totalMag = 0;
                        Map<String, Integer> bandCounts = new HashMap<>();

                        for (RocksDBServer.LightCurvePoint point : lightCurve) {
                            minMag = Math.min(minMag, point.mag);
                            maxMag = Math.max(maxMag, point.mag);
                            totalMag += point.mag;
                            bandCounts.put(point.band, bandCounts.getOrDefault(point.band, 0) + 1);
                        }
                        details.put("minMag", minMag);
                        details.put("maxMag", maxMag);
                        details.put("avgMag", totalMag / lightCurve.size());
                        details.put("bandCounts", bandCounts);
                        // 时间范围
                        double minTime = lightCurve.stream().mapToDouble(p -> p.time).min().orElse(0);
                        double maxTime = lightCurve.stream().mapToDouble(p -> p.time).max().orElse(0);
                        details.put("timeRange", new double[]{minTime, maxTime});
                    }
                    return details;
                }
            } catch (Exception e) {
                System.err.println("获取天体详情失败: " + e.getMessage());
            }
        }
        return Collections.emptyMap();
    }

    /**
     * 获取性能统计
     */
    public Map<String, Object> getPerformanceStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("node_id", nodeId);
        stats.put("total_queries", queryCounter.get());

        Map<String, Object> healpixStats = new HashMap<>();
        for (Map.Entry<String, PerformanceStats> entry : performanceStats.entrySet()) {
            healpixStats.put(entry.getKey(), entry.getValue().getStats());
        }
        stats.put("healpix_performance", healpixStats);

        return stats;
    }

    /**
     * 获取存储节点状态
     */
    public Map<String, Object> getNodeStatus() throws RocksDBException {
        Map<String, Object> status = new HashMap<>();
        status.put("node_id", nodeId);
        status.put("managed_healpix_count", healpixDatabases.size());
        status.put("active_queries", queryCounter.get());

        List<Map<String, Object>> dbStatusList = new ArrayList<>();
        for (Map.Entry<Long, RocksDBServer> entry : healpixDatabases.entrySet()) {
            Map<String, Object> dbStatus = entry.getValue().getHealpixStatistics();
            dbStatus.put("healpix_id", entry.getKey());
            dbStatusList.add(dbStatus);
        }
        status.put("databases", dbStatusList);

        return status;
    }

    public void shutdown() {
        queryExecutor.shutdown();
        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                dbService.close();
            } catch (Exception e) {
                System.err.println("Error closing database: " + e.getMessage());
            }
        }
        healpixDatabases.clear();
    }

    // ========== 内部类 ==========

    public static class WriteResult {
        public final int successCount;
        public final int errorCount;
        public final long durationMs;
        public final boolean success;

        public WriteResult(int successCount, int errorCount, long durationMs, boolean success) {
            this.successCount = successCount;
            this.errorCount = errorCount;
            this.durationMs = durationMs;
            this.success = success;
        }
    }

    public static class QueryResult {
        public final Set<Long> candidateObjects;
        public final Map<Long, RocksDBServer.StarMetadata> candidateMetadata;
        public final int candidateCount;
        public final long queryTimeNs;
        public final int queryId;
        public final String queryType;

        public QueryResult(Set<Long> candidateObjects,
                           Map<Long, RocksDBServer.StarMetadata> candidateMetadata,
                           int candidateCount, long queryTimeNs, int queryId, String queryType) {
            this.candidateObjects = candidateObjects;
            this.candidateMetadata = candidateMetadata;
            this.candidateCount = candidateCount;
            this.queryTimeNs = queryTimeNs;
            this.queryId = queryId;
            this.queryType = queryType;
        }

        public double getQueryTimeMs() {
            return queryTimeNs / 1_000_000.0;
        }
    }

    private static class PerformanceStats {
        private long bitmapQueryCount = 0;
        private long scanQueryCount = 0;
        private long totalBitmapQueryTimeNs = 0;
        private long totalScanQueryTimeNs = 0;
        private long writeCount = 0;
        private long totalWriteTimeMs = 0;

        public synchronized void recordBitmapQuery(long queryTimeNs) {
            bitmapQueryCount++;
            totalBitmapQueryTimeNs += queryTimeNs;
        }

        public synchronized void recordScanQuery(long queryTimeNs) {
            scanQueryCount++;
            totalScanQueryTimeNs += queryTimeNs;
        }

        public synchronized void recordWrite(int count, long durationMs) {
            writeCount += count;
            totalWriteTimeMs += durationMs;
        }

        public synchronized Map<String, Object> getStats() {
            Map<String, Object> stats = new HashMap<>();
            stats.put("bitmap_query_count", bitmapQueryCount);
            stats.put("scan_query_count", scanQueryCount);
            stats.put("avg_bitmap_query_time_ms",
                    bitmapQueryCount > 0 ? (totalBitmapQueryTimeNs / bitmapQueryCount) / 1_000_000.0 : 0);
            stats.put("avg_scan_query_time_ms",
                    scanQueryCount > 0 ? (totalScanQueryTimeNs / scanQueryCount) / 1_000_000.0 : 0);
            stats.put("total_write_count", writeCount);
            stats.put("avg_write_time_ms", writeCount > 0 ? (double) totalWriteTimeMs / writeCount : 0);
            return stats;
        }
    }
}