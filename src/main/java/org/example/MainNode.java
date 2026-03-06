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
 * Aligned to TDengine C++ two-phase strategy:
 *   Phase 1: Pre-create all healpix databases (not counted in write time)
 *   Phase 2: Multi-threaded parallel batch write
 *
 * Config alignment:
 *   NUM_THREADS  = 16  (parallel write threads)
 *   NUM_VGROUPS  = 32  (distribution parallelism)
 *   BATCH_SIZE   = 10000 (points per write batch)
 *   BUFFER_SIZE  = 256MB (per-partition memtable flush threshold)
 */
public class MainNode {
    private static final String DEFAULT_BASE_PATH = "LitecsDB_Data/";
    private final String basePath;
    private final List<StorageNode> storageNodes;
    private final int nodesCount;
    private final int level; // HEALPix level

    // Use global thread pools instead of per-MainNode executor
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

        // Use globally managed thread pools (aligned to TDengine config)
        RocksDBGlobalResourceManager manager = RocksDBGlobalResourceManager.getInstance();
        this.distributionExecutor = manager.getDistributionThreadPool();
        this.writeExecutor = manager.getWriteThreadPool();

        initializeStorageNodes();
    }

    private static String normalizeBasePath(String path) {
        if (path == null || path.trim().isEmpty()) {
            return DEFAULT_BASE_PATH;
        }
        String normalized = path.trim();
        if (normalized.endsWith("/") || normalized.endsWith("\\")) {
            return normalized;
        }
        return normalized + "/";
    }

    /**
     * Initialize storage nodes
     */
    private void initializeStorageNodes() {
        for (int i = 0; i < nodesCount; i++) {
            StorageNode node = new StorageNode(i, basePath);
            storageNodes.add(node);
            File dbDir = new File(basePath + "node" + i);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }
            loadExistingDatabases(node, dbDir, i);
        }
    }

    /**
     * Load existing HEALPix databases (parallel)
     */
    private void loadExistingDatabases(StorageNode node, File nodeDir, int nodeId) {
        if (!nodeDir.exists() || !nodeDir.isDirectory()) {
            return;
        }

        File[] healpixDirs = nodeDir.listFiles((dir, name) -> name.startsWith("healpix_"));

        if (healpixDirs == null || healpixDirs.length == 0) {
            System.out.println("Node " + nodeId + ": no existing databases found");
            return;
        }

        System.out.println("Node " + nodeId + ": found " + healpixDirs.length +
                " existing HEALPix databases, loading in parallel...");

        long startTime = System.currentTimeMillis();

        Arrays.stream(healpixDirs).parallel().forEach(healpixDir -> {
            try {
                String dirName = healpixDir.getName();
                long healpixId = Long.parseLong(dirName.replace("healpix_", ""));

                String dbPath = healpixDir.getAbsolutePath();
                RocksDBServer.Config config = new RocksDBServer.Config(dbPath, healpixId);
                config.timeBucketSize = 1;
                config.asyncIndexing = true;
                config.maxBackgroundCompactions = 2;

                boolean ok = node.initializeHealpixDatabase(healpixId, config);
                if (!ok) {
                    System.err.println("  FAIL: healpix_" + healpixId);
                }
            } catch (Exception e) {
                System.err.println("  FAIL: " + healpixDir.getName() + " - " + e.getMessage());
            }
        });

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Node " + nodeId + ": loading complete, took " + duration + " ms");
    }

    // ==================== Phase 1: Pre-create databases ====================

    /**
     * Phase 1: Pre-create all healpix databases for known healpix IDs.
     * Call this BEFORE timing the write phase.
     * Equivalent to TDengine "CREATE TABLE IF NOT EXISTS" phase.
     */
    public int preCreateHealpixDatabases(List<String> csvLines) {
        // Quick scan to discover all healpix IDs
        Set<Long> healpixIds = new HashSet<>();
        for (String csvLine : csvLines) {
            try {
                String[] parts = csvLine.split(",", -1);
                if (parts.length < 14 || parts[0].equals("source_id")) continue;
                double ra = Double.parseDouble(parts[1].trim());
                double dec = Double.parseDouble(parts[2].trim());
                long healpixId = calculateHealpixId(ra, dec);
                healpixIds.add(healpixId);
            } catch (Exception e) {
                // skip bad lines
            }
        }

        // Parallel pre-creation using distribution thread pool
        final int PRECREATE_PARALLELISM = RocksDBGlobalResourceManager.DISTRIBUTION_PARALLELISM;
        List<Long> idList = new ArrayList<>(healpixIds);
        CountDownLatch latch = new CountDownLatch(idList.size());

        for (Long healpixId : idList) {
            distributionExecutor.submit(() -> {
                try {
                    ensureHealpixDatabaseInitialized(healpixId);
                } catch (Exception e) {
                    System.err.println("Pre-create healpix_" + healpixId + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("[Phase 1] Pre-created " + healpixIds.size() + " healpix databases");
        return healpixIds.size();
    }

    // ==================== Phase 2: Multi-threaded parallel write ====================

    /**
     * Phase 2: Multi-threaded parallel batch write.
     *
     * Strategy (aligned to TDengine C++ insert_worker):
     *   1. Group data by healpix ID (like C++ groupByBucketTable)
     *   2. Shard groups across WRITE_THREADS=16 threads (round-robin)
     *   3. Each thread processes its shards in BATCH_SIZE=10000 chunks
     *   4. Each RocksDBServer partition has 256MB memtable (like TDengine BUFFER=256)
     */
    public DistributionResult distributeDataBatch(List<String> csvLines) {
        long startTime = System.currentTimeMillis();

        // Step 1: Group by healpix ID
        Map<Long, List<String>> healpixDataMap = new HashMap<>();
        for (String csvLine : csvLines) {
            try {
                String[] parts = csvLine.split(",", -1);
                if (parts.length < 14 || parts[0].equals("source_id")) continue;
                double ra = Double.parseDouble(parts[1].trim());
                double dec = Double.parseDouble(parts[2].trim());
                long healpixId = calculateHealpixId(ra, dec);
                healpixDataMap.computeIfAbsent(healpixId, k -> new ArrayList<>()).add(csvLine);
            } catch (Exception e) {
                // skip bad data
            }
        }

        // Step 2: Shard healpix groups across WRITE_THREADS threads (round-robin)
        final int numThreads = RocksDBGlobalResourceManager.WRITE_THREADS;
        final int batchSize = RocksDBGlobalResourceManager.BATCH_SIZE;

        @SuppressWarnings("unchecked")
        List<Map.Entry<Long, List<String>>>[] shards = new List[numThreads];
        for (int i = 0; i < numThreads; i++) {
            shards[i] = new ArrayList<>();
        }

        int idx = 0;
        for (Map.Entry<Long, List<String>> entry : healpixDataMap.entrySet()) {
            shards[idx % numThreads].add(entry);
            idx++;
        }

        // Step 3: Launch parallel write workers
        AtomicLong totalSuccess = new AtomicLong(0);
        AtomicLong totalErrors = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int threadId = 0; threadId < numThreads; threadId++) {
            final List<Map.Entry<Long, List<String>>> myShard = shards[threadId];
            final int tid = threadId;

            writeExecutor.submit(() -> {
                try {
                    writeWorker(tid, myShard, batchSize, totalSuccess, totalErrors);
                } catch (Exception e) {
                    System.err.println("[ERROR] Write thread " + tid + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long overallDuration = System.currentTimeMillis() - startTime;
        return new DistributionResult(
                (int) totalSuccess.get(),
                (int) totalErrors.get(),
                overallDuration,
                healpixDataMap.size(),
                numThreads);
    }

    /**
     * Single write worker thread (equivalent to TDengine insert_worker / C++ direct_worker_thread).
     *
     * Each thread:
     *   1. Iterates over assigned healpix groups
     *   2. For each group, splits into BATCH_SIZE chunks
     *   3. Calls StorageNode.processDataBatch for each chunk
     */
    private void writeWorker(int threadId,
                             List<Map.Entry<Long, List<String>>> healpixGroups,
                             int batchSize,
                             AtomicLong totalSuccess,
                             AtomicLong totalErrors) {
        for (Map.Entry<Long, List<String>> entry : healpixGroups) {
            long healpixId = entry.getKey();
            List<String> dataLines = entry.getValue();
            StorageNode node = storageNodes.get((int) (healpixId % nodesCount));

            if (node == null) {
                totalErrors.addAndGet(dataLines.size());
                continue;
            }

            // Split into BATCH_SIZE chunks (aligned to TDengine BATCH_SIZE=10000)
            for (int batchStart = 0; batchStart < dataLines.size(); batchStart += batchSize) {
                int batchEnd = Math.min(batchStart + batchSize, dataLines.size());
                List<String> batch = dataLines.subList(batchStart, batchEnd);

                StorageNode.WriteResult result = node.processDataBatch(healpixId, batch);
                totalSuccess.addAndGet(result.successCount);
                totalErrors.addAndGet(result.errorCount);
            }
        }
    }

    private void ensureHealpixDatabaseInitialized(long healpixId) {
        StorageNode node = storageNodes.get((int) (healpixId % nodesCount));
        if (node != null) {
            String dbPath = String.format(basePath + "node%d/healpix_%d",
                    healpixId % nodesCount, healpixId);
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
                return new DistributedQueryResult(Collections.emptySet(), 0,
                        System.nanoTime() - queryStart, 0, 0);
            }

            Set<Long> allCandidates = new LinkedHashSet<>();
            int nodesQueried = 0;
            int nodesResponded = 0;

            List<CompletableFuture<Set<Long>>> futures = new ArrayList<>();

            for (Long healpixId : healpixIdsToQuery) {
                StorageNode node = storageNodes.get((int) (healpixId % nodesCount));

                if (node != null) {
                    nodesQueried++;

                    CompletableFuture<Set<Long>> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            QueryResult nodeResult =
                                    node.executeQueryWithTimeBuckets(healpixId, startTime, endTime,
                                            band, magThreshold, minObservations);

                            if (nodeResult != null && nodeResult.candidateObjects != null &&
                                    nodeResult.candidateMetadata != null) {
                                return filterCandidatesByDistance(nodeResult.candidateObjects,
                                        nodeResult.candidateMetadata,
                                        ra, dec, radius);
                            }
                        } catch (Exception e) {
                            System.err.println("Query HEALPix " + healpixId + " failed: " + e.getMessage());
                        }
                        return Collections.<Long>emptySet();
                    });

                    futures.add(future);
                }
            }

            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0]));

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
                System.err.println("Query timeout or failed: " + e.getMessage());
            }

            long queryDuration = System.nanoTime() - queryStart;

            return new DistributedQueryResult(allCandidates, allCandidates.size(),
                    queryDuration, nodesQueried, nodesResponded);

        } catch (Exception e) {
            System.err.println("Distributed time-bucket query failed: " + e.getMessage());
            e.printStackTrace();
            return new DistributedQueryResult(Collections.emptySet(), 0,
                    System.nanoTime() - queryStart, 0, 0);
        }
    }

    public void buildAllTimeBucketsOffline() {
        System.out.println("=== Starting distributed time-bucket parallel build ===");
        storageNodes.parallelStream().forEach(StorageNode::buildAllTimeBucketsOffline);
    }

    public long calculateHealpixId(double raDegrees, double decDegrees) {
        return HealpixUtil.raDecToHealpix(raDegrees, decDegrees, level);
    }

    public List<LightCurvePoint> getLightCurve(long healpixId, long sourceId, String band) {
        StorageNode node = storageNodes.get((int) (healpixId % nodesCount));
        if (node == null) {
            return Collections.emptyList();
        }
        return node.getLightCurve(healpixId, sourceId, band);
    }

    public DistributedQueryResult executeDistributedExistenceQuery(double ra, double dec, double radius,
                                                                   double startTime, double endTime,
                                                                   String band, double magThreshold,
                                                                   int minObservations) {
        long queryStart = System.nanoTime();

        try {
            Set<Long> healpixIdsToQuery = calculateHealpixIdsInCone(ra, dec, radius);

            if (healpixIdsToQuery.isEmpty()) {
                return new DistributedQueryResult(Collections.emptySet(), 0,
                        System.nanoTime() - queryStart, 0, 0);
            }

            Set<Long> allCandidates = new LinkedHashSet<>();
            int nodesQueried = 0;
            int nodesResponded = 0;

            List<CompletableFuture<Set<Long>>> futures = new ArrayList<>();

            for (Long healpixId : healpixIdsToQuery) {
                StorageNode node = storageNodes.get((int) (healpixId % nodesCount));

                if (node != null) {
                    nodesQueried++;

                    CompletableFuture<Set<Long>> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            QueryResult nodeResult;
                            nodeResult = node.executeExistenceQueryWithScan(healpixId, startTime, endTime,
                                    band, magThreshold, minObservations);

                            if (nodeResult != null && nodeResult.candidateObjects != null &&
                                    nodeResult.candidateMetadata != null) {
                                return filterCandidatesByDistance(nodeResult.candidateObjects,
                                        nodeResult.candidateMetadata,
                                        ra, dec, radius);
                            }
                        } catch (Exception e) {
                            System.err.println("Query HEALPix " + healpixId + " failed: " + e.getMessage());
                        }
                        return Collections.<Long>emptySet();
                    });

                    futures.add(future);
                }
            }

            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0]));

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
                System.err.println("Query timeout or failed: " + e.getMessage());
            }

            long queryDuration = System.nanoTime() - queryStart;

            return new DistributedQueryResult(allCandidates, allCandidates.size(),
                    queryDuration, nodesQueried, nodesResponded);

        } catch (Exception e) {
            System.err.println("Distributed query failed: " + e.getMessage());
            e.printStackTrace();
            return new DistributedQueryResult(Collections.emptySet(), 0,
                    System.nanoTime() - queryStart, 0, 0);
        }
    }

    private Set<Long> calculateHealpixIdsInCone(double ra, double dec, double radius) {
        Set<Long> healpixIds = new HashSet<>();
        try {
            HealpixUtil healpixUtil = new HealpixUtil();
            healpixUtil.setNside(level);
            RangeSet rangeSet = healpixUtil.queryDisc(ra, dec, radius, level);

            if (rangeSet != null) {
                for (int i = 0; i < rangeSet.nranges(); i++) {
                    long start = rangeSet.ivbegin(i);
                    long end = rangeSet.ivend(i);
                    for (long pix = start; pix <= end; pix++) {
                        healpixIds.add(pix);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to calculate HEALPix cone: " + e.getMessage());
            e.printStackTrace();
        }
        return healpixIds;
    }

    private Set<Long> filterCandidatesByDistance(Set<Long> candidates,
                                                 Map<Long, RocksDBServer.StarMetadata> metadataMap,
                                                 double centerRa, double centerDec, double radius) {
        if (candidates.isEmpty()) {
            return candidates;
        }

        Set<Long> filtered = new LinkedHashSet<>();

        for (Long sourceId : candidates) {
            try {
                RocksDBServer.StarMetadata metadata = metadataMap.get(sourceId);
                if (metadata != null) {
                    double distance = calculateGreatCircleDistance(centerRa, centerDec,
                            metadata.ra, metadata.dec);
                    if (distance <= radius) {
                        filtered.add(sourceId);
                    }
                }
            } catch (Exception e) {
                filtered.add(sourceId);
            }
        }

        System.out.println("Distance filter: " + candidates.size() + " -> " + filtered.size());

        return filtered;
    }

    private double calculateGreatCircleDistance(double ra1, double dec1, double ra2, double dec2) {
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

    public PerformanceComparisonResult performComparisonTest(long testHealpixId,
                                                             int iterations,
                                                             double startTime, double endTime,
                                                             String band, double magThreshold, int minObservations) {
        StorageNode node = storageNodes.get((int) (testHealpixId % nodesCount));
        if (node == null) {
            throw new IllegalArgumentException("No storage node for HEALPix " + testHealpixId);
        }

        List<Double> timeBucketsTimes = new ArrayList<>();
        List<Double> scanTimes = new ArrayList<>();
        Set<Long> timeBucketsResults = null;
        Set<Long> scanResults = null;

        for (int i = 0; i < iterations; i++) {
            QueryResult timeBuckets =
                    node.executeQueryWithTimeBuckets(testHealpixId, startTime, endTime,
                            band, magThreshold, minObservations);
            timeBucketsTimes.add(timeBuckets.getQueryTimeMs());
            if (timeBucketsResults == null) timeBucketsResults = timeBuckets.candidateObjects;

            QueryResult scanResult =
                    node.executeExistenceQueryWithScan(testHealpixId, startTime, endTime,
                            band, magThreshold, minObservations);
            scanTimes.add(scanResult.getQueryTimeMs());
            if (scanResults == null) scanResults = scanResult.candidateObjects;

            try { Thread.sleep(10); } catch (InterruptedException e) {}
        }

        boolean resultsMatch = timeBucketsResults != null && scanResults != null &&
                (Math.abs(timeBucketsResults.size() - scanResults.size()) / (double) Math.max(1, scanResults.size()) < 0.01);

        return new PerformanceComparisonResult(timeBucketsTimes, scanTimes, resultsMatch,
                timeBucketsResults != null ? timeBucketsResults.size() : 0,
                scanResults != null ? scanResults.size() : 0);
    }

    public List<RocksDBServer.StarMetadata> getAllStarsMetadata() {
        List<RocksDBServer.StarMetadata> allStars = new ArrayList<>();
        for (StorageNode node : storageNodes) {
            allStars.addAll(node.getAllStarsMetadata());
        }
        return allStars;
    }

    public List<RocksDBServer.StarMetadata> queryStarsByRegion(double minRa, double maxRa,
                                                               double minDec, double maxDec) {
        List<RocksDBServer.StarMetadata> result = new ArrayList<>();
        for (StorageNode node : storageNodes) {
            result.addAll(node.queryStarsByRegion(minRa, maxRa, minDec, maxDec));
        }
        return result;
    }

    public Map<String, Object> getSystemStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("total_nodes", storageNodes.size());
        status.put("level", level);
        status.put("write_threads", RocksDBGlobalResourceManager.WRITE_THREADS);
        status.put("distribution_parallelism", RocksDBGlobalResourceManager.DISTRIBUTION_PARALLELISM);
        status.put("batch_size", RocksDBGlobalResourceManager.BATCH_SIZE);
        status.put("memtable_flush_threshold_mb",
                RocksDBGlobalResourceManager.MEMTABLE_FLUSH_THRESHOLD / (1024 * 1024));

        List<Map<String, Object>> nodeStatuses = new ArrayList<>();
        for (StorageNode node : storageNodes) {
            try {
                nodeStatuses.add(node.getNodeStatus());
            } catch (Exception e) {
                // skip
            }
        }
        status.put("nodes", nodeStatuses);
        status.put("boundary_alignment", getBoundaryAlignmentStats());

        return status;
    }

    public Map<String, Object> getBoundaryAlignmentStats() {
        long fragmentedFlushTotal = 0;
        long lowerLevelAlignedFlushTotal = 0;
        long lowerLevelBoundaryHits = 0;
        long staticBoundaryHits = 0;
        long dynamicBoundaryHits = 0;
        int nodesWithStats = 0;

        for (StorageNode node : storageNodes) {
            try {
                Map<String, Object> nodeStats = node.getBoundaryAlignmentStats();
                fragmentedFlushTotal += getLong(nodeStats.get("fragmented_flush_total"));
                lowerLevelAlignedFlushTotal += getLong(nodeStats.get("lower_level_aligned_flush_total"));
                lowerLevelBoundaryHits += getLong(nodeStats.get("last_flush_lower_level_boundary_hits_sum"));
                staticBoundaryHits += getLong(nodeStats.get("last_flush_static_boundary_hits_sum"));
                dynamicBoundaryHits += getLong(nodeStats.get("last_flush_dynamic_boundary_hits_sum"));
                nodesWithStats++;
            } catch (Exception e) {
                // ignore
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("nodes_with_stats", nodesWithStats);
        result.put("fragmented_flush_total", fragmentedFlushTotal);
        result.put("lower_level_aligned_flush_total", lowerLevelAlignedFlushTotal);
        result.put("lower_level_aligned_flush_ratio",
                fragmentedFlushTotal > 0 ? (double) lowerLevelAlignedFlushTotal / fragmentedFlushTotal : 0.0);
        result.put("last_flush_lower_level_boundary_hits_sum", lowerLevelBoundaryHits);
        result.put("last_flush_static_boundary_hits_sum", staticBoundaryHits);
        result.put("last_flush_dynamic_boundary_hits_sum", dynamicBoundaryHits);
        return result;
    }

    private long getLong(Object value) {
        if (value instanceof Number) return ((Number) value).longValue();
        if (value instanceof String) {
            try { return Long.parseLong((String) value); } catch (NumberFormatException ignored) {}
        }
        return 0;
    }

    public void shutdown() {
        // Do NOT shutdown global thread pools here - they are shared
        for (StorageNode node : storageNodes) {
            node.shutdown();
        }
    }

    public void forceFlushAll() {
        for (StorageNode node : storageNodes) {
            node.forceFlushAll();
        }
    }

    public double getOverallWriteAmplification() {
        double totalLogical = 0;
        double totalPhysical = 0;
        for (StorageNode node : storageNodes) {
            double[] stats = node.getOverallWAStats();
            totalLogical += stats[0];
            totalPhysical += stats[1];
        }
        return totalLogical > 0 ? totalPhysical / totalLogical : 1.0;
    }

    // ========== Inner classes (unchanged) ==========

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
        public final List<Double> indexQueryTimes;
        public final List<Double> scanQueryTimes;
        public final boolean resultsMatch;
        public final int indexResultCount;
        public final int scanResultCount;

        public PerformanceComparisonResult(List<Double> indexQueryTimes, List<Double> scanQueryTimes,
                                           boolean resultsMatch, int indexResultCount, int scanResultCount) {
            this.indexQueryTimes = indexQueryTimes;
            this.scanQueryTimes = scanQueryTimes;
            this.resultsMatch = resultsMatch;
            this.indexResultCount = indexResultCount;
            this.scanResultCount = scanResultCount;
        }

        public double getAvgIndexTime() {
            return indexQueryTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        }

        public double getAvgScanTime() {
            return scanQueryTimes.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        }

        public double getSpeedupRatio() {
            double avgScan = getAvgScanTime();
            double avgIndex = getAvgIndexTime();
            return avgScan > 0 ? avgScan / avgIndex : 0;
        }
    }
}