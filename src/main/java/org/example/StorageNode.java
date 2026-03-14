package org.example;

import org.example.RocksDBServer.*;
import org.rocksdb.RocksDBException;
import org.rocksdb.TickerType;

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

    public long getAggregatedTickerCount(TickerType ticker) {
        long total = 0;
        for (RocksDBServer db : healpixDatabases.values()) {
            total += db.getTickerCount(ticker);
        }
        return total;
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

            // 更新统计
            PerformanceStats stats = performanceStats.get("healpix_" + healpixId);
            if (stats != null) {
                stats.recordIndexQuery(duration);
            }

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

    public void buildAllTimeBucketsOffline() {
        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                dbService.buildAllTimeBucketsOffline();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // 获取指定 HEALPix 分区内的所有天体元数据
    public List<RocksDBServer.StarMetadata> getAllStarsInHealpix(long healpixId) {
        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) return Collections.emptyList();
        try {
            return dbService.getAllStarsMetadata();
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    /**
     * 使用固定时间窗口离线构建时间桶索引（实验1对比基线）
     */
    public void buildAllTimeBucketsOfflineFixed(double deltaDays) {
        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                dbService.buildAllTimeBucketsOfflineFixed(deltaDays);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取 time_buckets 列族的估计存储大小（字节）
     */
    public long getTimeBucketStorageSize() {
        long total = 0;
        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                total += dbService.getTimeBucketStorageSize();
            } catch (Exception e) { }
        }
        return total;
    }

    /**
     * 初始化HEALPix数据库
     */
    public boolean initializeHealpixDatabase(long healpixId, RocksDBServer.Config config) {
        // 利用 computeIfAbsent 保证原子性和单次执行
        RocksDBServer existing = healpixDatabases.computeIfAbsent(healpixId, id -> {
            try {
                File dbDir = new File(config.dbPath).getParentFile();
                if (!dbDir.exists()) {
                    dbDir.mkdirs();
                }
                RocksDBServer dbService = new RocksDBServer(config);
                performanceStats.put("healpix_" + healpixId, new PerformanceStats());
                return dbService;
            } catch (RocksDBException e) {
                System.err.println("Failed to initialize HEALPix " + healpixId + ": " + e.getMessage());
                return null;
            }
        });
        return existing != null;
    }

    /**
     * 直接写入 LightCurvePoint 对象（跳过 CSV 解析）
     * 由 MainNode Phase 2 优化路径调用
     */
    public int writePointsBatch(long healpixId, List<LightCurvePoint> points) {
        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) return 0;
        try {
            dbService.putLightCurveBatch(points);
            return points.size();
        } catch (Exception e) {
            System.err.println("writePointsBatch failed for healpix " + healpixId + ": " + e.getMessage());
            return 0;
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
            return dbService.getLightCurveByBand(sourceId, band);
        } catch (Exception e) {
            System.err.println("Failed to get light curve for source " + sourceId + " in HEALPix " + healpixId + ": " + e.getMessage());
            e.printStackTrace();
            return Collections.emptyList();
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
        status.put("boundary_alignment", getBoundaryAlignmentStats());

        return status;
    }

    public Map<String, Object> getBoundaryAlignmentStats() {
        long fragmentedFlushTotal = 0;
        long lowerLevelAlignedFlushTotal = 0;
        long lowerLevelBoundaryHits = 0;
        long staticBoundaryHits = 0;
        long dynamicBoundaryHits = 0;
        int dbCountWithStats = 0;

        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                Map<String, Object> dbStats = dbService.getHealpixStatistics();
                fragmentedFlushTotal += asLong(dbStats.get("fragmented_flush_total"));
                lowerLevelAlignedFlushTotal += asLong(dbStats.get("lower_level_aligned_flush_total"));
                lowerLevelBoundaryHits += asLong(dbStats.get("last_flush_lower_level_boundary_hits"));
                staticBoundaryHits += asLong(dbStats.get("last_flush_static_boundary_hits"));
                dynamicBoundaryHits += asLong(dbStats.get("last_flush_dynamic_boundary_hits"));
                dbCountWithStats++;
            } catch (Exception e) {
                // Ignore broken partition stats to keep aggregation robust.
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("node_id", nodeId);
        result.put("partitions_with_stats", dbCountWithStats);
        result.put("fragmented_flush_total", fragmentedFlushTotal);
        result.put("lower_level_aligned_flush_total", lowerLevelAlignedFlushTotal);
        result.put("lower_level_aligned_flush_ratio",
                fragmentedFlushTotal > 0 ? (double) lowerLevelAlignedFlushTotal / fragmentedFlushTotal : 0.0);
        result.put("last_flush_lower_level_boundary_hits_sum", lowerLevelBoundaryHits);
        result.put("last_flush_static_boundary_hits_sum", staticBoundaryHits);
        result.put("last_flush_dynamic_boundary_hits_sum", dynamicBoundaryHits);
        return result;
    }

    private long asLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException ignored) {
                return 0;
            }
        }
        return 0;
    }

    public void shutdown() {
        queryExecutor.shutdown();

        forceFlushAll();

        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                dbService.close();
            } catch (Exception e) {
                System.err.println("Error closing database: " + e.getMessage());
            }
        }
        healpixDatabases.clear();
    }

    public void forceFlushAll() {
        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                dbService.forceFlush();
            } catch (Exception e) {
                System.err.println("Error flushing database: " + e.getMessage());
            }
        }
    }
    public void forceFlushAllNoCompaction() {
        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                dbService.forceFlushNoCompaction();
            } catch (Exception e) {
                System.err.println("Flush failed: " + e.getMessage());
            }
        }
    }

    public void compactOnly() {
        for (RocksDBServer dbService : healpixDatabases.values()) {
            try {
                dbService.compactOnly();
            } catch (Exception e) {
                System.err.println("Compact failed: " + e.getMessage());
            }
        }
    }

    /**
     * 汇总该节点下所有 RocksDB 实例的逻辑和物理写入量
     * @return double数组: [0] 逻辑写入总字节, [1] 物理写入总字节
     */
    public double[] getOverallWAStats() {
        double totalLogical = 0;
        double totalPhysical = 0;
        for (RocksDBServer db : healpixDatabases.values()) {
            Map<String, Double> stats = db.getWriteAmplificationStats();
            if (stats != null && !stats.isEmpty()) {
                totalLogical += stats.getOrDefault("logical_mb", 0.0);
                totalPhysical += stats.getOrDefault("physical_mb", 0.0);
            }
        }
        return new double[]{totalLogical, totalPhysical};
    }

    private Map<Long, RocksDBServer.StarMetadata> getStarsMetadataBatch(
            long healpixId, Set<Long> sourceIds) {
        RocksDBServer dbService = healpixDatabases.get(healpixId);
        if (dbService == null) return Collections.emptyMap();
        try {
            return dbService.getStarsMetadataBatch(sourceIds);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
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
        private long indexQueryCount = 0;
        private long scanQueryCount = 0;
        private long totalIndexQueryTimeNs = 0;
        private long totalScanQueryTimeNs = 0;
        private long writeCount = 0;
        private long totalWriteTimeMs = 0;

        public synchronized void recordIndexQuery(long queryTimeNs) {
            indexQueryCount++;
            totalIndexQueryTimeNs += queryTimeNs;
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
            stats.put("index_query_count", indexQueryCount);
            stats.put("scan_query_count", scanQueryCount);
            stats.put("avg_index_query_time_ms",
                    indexQueryCount > 0 ? (totalIndexQueryTimeNs / indexQueryCount) / 1_000_000.0 : 0);
            stats.put("avg_scan_query_time_ms",
                    scanQueryCount > 0 ? (totalScanQueryTimeNs / scanQueryCount) / 1_000_000.0 : 0);
            stats.put("total_write_count", writeCount);
            stats.put("avg_write_time_ms", writeCount > 0 ? (double) totalWriteTimeMs / writeCount : 0);
            return stats;
        }
    }

    // ==================== 以下方法需要添加到 StorageNode.java ====================

    /**
     * 设置所有 HEALPix DB 的 DP 分桶参数
     */
    public void setTimeBucketDPParams(double lambda, double storeCost) {
        for (Map.Entry<Long, RocksDBServer> entry : healpixDatabases.entrySet()) {
            RocksDBServer server = entry.getValue();
            server.getConfig().lambda = lambda;
            server.getConfig().storeCost = storeCost;
        }
    }

    /**
     * STMK 详细查询：对指定 healpixId 内的天体执行 STMK 查询，
     * 返回每个天体的判定状态用于统计剪枝率/精炼率
     *
     * 流程严格对齐论文 Algorithm 2:
     *   1. 获取 cell 内所有天体
     *   2. 精确空间过滤 InsideRegion
     *   3. 遍历时间桶做双向剪枝
     *   4. 精炼候选集
     */
    public MainNode.DetailedSTMKResult executeSTMKQueryDetailed(
            long healpixId, double centerRa, double centerDec, double radius,
            double startTime, double endTime, String band,
            double magMin, double magMax, int minObservations) {

        MainNode.DetailedSTMKResult result = new MainNode.DetailedSTMKResult();

        RocksDBServer server = healpixDatabases.get(healpixId);
        if (server == null) return result;

        try {
            // Level 2: 获取天区内所有天体
            List<RocksDBServer.StarMetadata> allStars = server.getAllStarsMetadata();

            for (RocksDBServer.StarMetadata star : allStars) {
                // Level 1 精确空间剪枝
                double dist = greatCircleDistance(centerRa, centerDec, star.ra, star.dec);
                if (dist > radius) continue;

                result.totalObjectsEvaluated++;

                // Level 3: 时间桶剪枝查询
                RocksDBServer.TimeBucketQueryResult tbResult =
                        server.queryWithTimeBucketsSTMK(
                                star.sourceId, band,
                                startTime, endTime,
                                magMin, magMax, minObservations);

                switch (tbResult.status) {
                    case "CONFIRMED":
                        result.confirmedCount++;
                        result.resultCount++;
                        break;
                    case "PRUNED":
                        result.prunedCount++;
                        break;
                    case "AMBIGUOUS":
                        result.ambiguousCount++;
                        // 精炼：扫描黄桶
                        int refinedCount = server.refineAmbiguousBucketsSTMK(
                                star.sourceId, band,
                                tbResult.ambiguousBuckets,
                                startTime, endTime,
                                magMin, magMax);
                        int totalCount = tbResult.confirmedCount + refinedCount;
                        if (totalCount >= minObservations) {
                            result.resultCount++;
                        }
                        break;
                }
            }
        } catch (Exception e) {
            System.err.println("DetailedSTMK query failed on healpix " + healpixId + ": " + e.getMessage());
        }

        return result;
    }

    /**
     * STMK 查询（对齐论文 Algorithm 2，返回候选集 + 元数据）
     * 供 MainNode.executeSTMKQuery() 调用
     *
     * 严格按照论文 Algorithm 2 的执行顺序：
     *   1. 获取 cell 内所有天体（Level 2）
     *   2. 精确空间剪枝 InsideRegion（Level 1 精确过滤）
     *   3. 时间桶剪枝 + 精炼（Level 3）
     */
    public QueryResult executeSTMKQueryWithTimeBuckets(
            long healpixId, double centerRa, double centerDec, double radius,
            double startTime, double endTime,
            String band, double magMin, double magMax, int minObservations) {

        long queryStart = System.nanoTime();
        int queryId = queryCounter.incrementAndGet();
        RocksDBServer server = healpixDatabases.get(healpixId);
        if (server == null) {
            return new QueryResult(Collections.emptySet(), new HashMap<>(), 0,System.nanoTime() - queryStart, queryId, "STMK");
        }

        try {
            Set<Long> candidates = new LinkedHashSet<>();
            Map<Long, RocksDBServer.StarMetadata> metadataMap = new HashMap<>();

            List<RocksDBServer.StarMetadata> allStars = server.getAllStarsMetadata();

            for (RocksDBServer.StarMetadata star : allStars) {
                // 第一步：精确空间剪枝 InsideRegion(ra, dec, S)
                double dist = greatCircleDistance(centerRa, centerDec, star.ra, star.dec);
                if (dist > radius) continue;

                metadataMap.put(star.sourceId, star);

                // 第二步：时间桶剪枝
                RocksDBServer.TimeBucketQueryResult tbResult =
                        server.queryWithTimeBucketsSTMK(
                                star.sourceId, band,
                                startTime, endTime,
                                magMin, magMax, minObservations);

                switch (tbResult.status) {
                    case "CONFIRMED":
                        candidates.add(star.sourceId);
                        break;
                    case "PRUNED":
                        break;
                    case "AMBIGUOUS":
                        // 第三步：精炼候选集
                        int refinedCount = server.refineAmbiguousBucketsSTMK(
                                star.sourceId, band,
                                tbResult.ambiguousBuckets,
                                startTime, endTime,
                                magMin, magMax);
                        if (tbResult.confirmedCount + refinedCount >= minObservations) {
                            candidates.add(star.sourceId);
                        }
                        break;
                }
            }

            return new QueryResult(candidates, metadataMap, 0,System.nanoTime() - queryStart, queryId, "STMK");
        } catch (Exception e) {
            return new QueryResult(Collections.emptySet(), new HashMap<>(), 0,System.nanoTime() - queryStart, queryId, "STMK");
        }
    }

    /**
     * 获取时间桶的聚合统计（供实验3统计用）
     */
    public Map<String, Object> getTimeBucketAggregateStats() {
        Map<String, Object> stats = new HashMap<>();
        long totalBuckets = 0;
        Set<Long> objectIds = new HashSet<>();
        double totalVarianceSum = 0;
        long totalPointsInBuckets = 0;
        long bucketCountForVariance = 0;
        long bucketSizeSqSum = 0;

        for (Map.Entry<Long, RocksDBServer> entry : healpixDatabases.entrySet()) {
            RocksDBServer server = entry.getValue();
            try {
                Map<String, Object> serverStats = server.getTimeBucketAggregateStats();
                totalBuckets += (long) serverStats.getOrDefault("totalBuckets", 0L);
                objectIds.addAll((Set<Long>) serverStats.getOrDefault("objectIds", Collections.emptySet()));
                totalVarianceSum += (double) serverStats.getOrDefault("totalVarianceSum", 0.0);
                totalPointsInBuckets += (long) serverStats.getOrDefault("totalPointsInBuckets", 0L);
                bucketCountForVariance += (long) serverStats.getOrDefault("bucketCountForVariance", 0L);
                bucketSizeSqSum += (long) serverStats.getOrDefault("bucketSizeSqSum", 0L);
            } catch (Exception e) { }
        }

        stats.put("totalBuckets", totalBuckets);
        stats.put("totalObjects", (long) objectIds.size());
        stats.put("totalVarianceSum", totalVarianceSum);
        stats.put("totalPointsInBuckets", totalPointsInBuckets);
        stats.put("bucketCountForVariance", bucketCountForVariance);
        stats.put("bucketSizeSqSum", bucketSizeSqSum);
        return stats;
    }

    private static double greatCircleDistance(double ra1, double dec1, double ra2, double dec2) {
        double ra1R = Math.toRadians(ra1), dec1R = Math.toRadians(dec1);
        double ra2R = Math.toRadians(ra2), dec2R = Math.toRadians(dec2);
        double a = Math.sin((dec2R - dec1R) / 2) * Math.sin((dec2R - dec1R) / 2) +
                Math.cos(dec1R) * Math.cos(dec2R) * Math.sin((ra2R - ra1R) / 2) * Math.sin((ra2R - ra1R) / 2);
        return Math.toDegrees(2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a)));
    }
}