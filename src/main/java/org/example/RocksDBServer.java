package org.example;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 天文光变曲线RocksDB服务 - 针对HEALPix分区的分布式存储系统设计
 * 每个实例负责一个HEALPix天区的数据管理
 */
public class RocksDBServer implements AutoCloseable {
    private final RocksDB db;
    private final String dbPath;
    private final long healpixId; // 当前实例负责的HEALPix天区ID
    private final Config config;

    // 列族句柄
    private final ColumnFamilyHandle defaultHandle;
    private final ColumnFamilyHandle lightcurveHandle;
    private final ColumnFamilyHandle bitmapIndexHandle;
    private final ColumnFamilyHandle metadataHandle;
    private final ColumnFamilyHandle timeBucketsHandle;

    // 异步处理
    private final ExecutorService asyncExecutor;
    private final BlockingQueue<IndexUpdateTask> indexUpdateQueue;
    private volatile boolean running = true;

    // 统计信息
    private final AtomicLong writeCount = new AtomicLong(0);
    private final AtomicLong readCount = new AtomicLong(0);
    private final Statistics statistics;
    private final long startTime;
    // 记录时间起点：2455197.5，儒略日 2455197.5，也就是UTC时间2010年1月1日 00:00:00。

    /**
     * 光变曲线数据点
     */
    public static class LightCurvePoint {
        public final long sourceId;
        public final long transitId;
        public final String band;
        public final double time; // MJD
        public final double mag;
        public final double flux;
        public final double fluxError;
        public final double fluxOverError;
        public final boolean rejectedByPhotometry;
        public final boolean rejectedByVariability;
        public final int otherFlags;
        public final long solutionId;

        // 坐标信息（虽然由HEALPix分区管理，但保留用于查询）
        public final double ra;
        public final double dec;

        public LightCurvePoint(long sourceId, double ra, double dec, long transitId,
                               String band, double time, double mag, double flux,
                               double fluxError, double fluxOverError,
                               boolean rejectedByPhotometry, boolean rejectedByVariability,
                               int otherFlags, long solutionId) {
            this.sourceId = sourceId;
            this.ra = ra;
            this.dec = dec;
            this.transitId = transitId;
            this.band = band;
            this.time = time;
            this.mag = mag;
            this.flux = flux;
            this.fluxError = fluxError;
            this.fluxOverError = fluxOverError;
            this.rejectedByPhotometry = rejectedByPhotometry;
            this.rejectedByVariability = rejectedByVariability;
            this.otherFlags = otherFlags;
            this.solutionId = solutionId;
        }
    }

    /**
     * 天体元数据类
     */
    public static class StarMetadata {
        public final long sourceId;
        public final double ra;
        public final double dec;
        public final int observationCount;
        public final double avgMag;
        public final double firstObsTime;
        public final double lastObsTime;

        public StarMetadata(long sourceId, double ra, double dec, int observationCount,
                            double avgMag, double firstObsTime, double lastObsTime) {
            this.sourceId = sourceId;
            this.ra = ra;
            this.dec = dec;
            this.observationCount = observationCount;
            this.avgMag = avgMag;
            this.firstObsTime = firstObsTime;
            this.lastObsTime = lastObsTime;
        }
    }

    /**
     * 时间桶类
     */
    public static class TimeBucket {
        public final long sourceId;
        public final String band;
        public final double startTime;
        public final double endTime;
        public final double minMag;
        public final double maxMag;
        public final int totalCount;
        public final int bucketIndex; // 桶的序号

        public TimeBucket(long sourceId, String band, int bucketIndex,
                          double startTime, double endTime,
                          double minMag, double maxMag, int totalCount) {
            this.sourceId = sourceId;
            this.band = band;
            this.bucketIndex = bucketIndex;
            this.startTime = startTime;
            this.endTime = endTime;
            this.minMag = minMag;
            this.maxMag = maxMag;
            this.totalCount = totalCount;
        }
    }

    /**
     * 位图索引条目
     */
    public static class BitmapIndexEntry {
        public final long sourceId;
        public final String band;
        public final BitSet timeBitmap; // 时间位图
        public final double minMag;     // 最小星等
        public final double maxMag;     // 最大星等
        public final int observationCount; // 观测次数

        public BitmapIndexEntry(long sourceId, String band, BitSet timeBitmap,
                                double minMag, double maxMag, int observationCount) {
            this.sourceId = sourceId;
            this.band = band;
            this.timeBitmap = timeBitmap;
            this.minMag = minMag;
            this.maxMag = maxMag;
            this.observationCount = observationCount;
        }
    }

    /**
     * 时间桶查询结果类
     */
    public static class TimeBucketQueryResult {
        public final Boolean satisfied; // null表示需要精炼
        public final int confirmedCount;
        public final List<TimeBucket> ambiguousBuckets;
        public final String status;

        public TimeBucketQueryResult(Boolean satisfied, int confirmedCount,
                                     List<TimeBucket> ambiguousBuckets, String status) {
            this.satisfied = satisfied;
            this.confirmedCount = confirmedCount;
            this.ambiguousBuckets = ambiguousBuckets;
            this.status = status;
        }
    }

    /**
     * 配置类
     */
    public static class Config {
        public String dbPath;
        public long healpixId;
        public long timeBucketSize = 1; // 天为单位
        public int bitmapSize = 3650; // 位图大小（覆盖约10年）
        public boolean asyncIndexing = true;
        public int maxBackgroundCompactions = 4;
        public long blockCacheSize = 2 * SizeUnit.GB;

        // 时间桶参数
        public double lambda = 0.5; // 代价函数中的权重参数
        public double storeCost = 1.0; // 存储代价
        public int maxBucketGap = 100; // 最大桶间隔（K参数）

        public Config(String dbPath, long healpixId) {
            this.dbPath = dbPath;
            this.healpixId = healpixId;
        }
    }

    /**
     * 索引更新任务
     */
    private static class IndexUpdateTask {
        public final long sourceId;
        public final String band;
        public final double time;
        public final double mag;

        public IndexUpdateTask(long sourceId, String band, double time, double mag) {
            this.sourceId = sourceId;
            this.band = band;
            this.time = time;
            this.mag = mag;
        }
    }

    public RocksDBServer(Config config) throws RocksDBException {
        this.healpixId = config.healpixId;
        this.dbPath = config.dbPath;
        this.startTime = System.currentTimeMillis();
        this.config = config;

        RocksDB.loadLibrary();

        // 初始化统计
        this.statistics = new Statistics();

        // 创建列族描述
        List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor("default".getBytes()),
                new ColumnFamilyDescriptor("lightcurve".getBytes()),
                new ColumnFamilyDescriptor("bitmap_index".getBytes()),
                new ColumnFamilyDescriptor("metadata".getBytes()),
                new ColumnFamilyDescriptor("time_buckets".getBytes())
        );

        // 数据库选项
        try (final DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundCompactions(config.maxBackgroundCompactions)
                .setStatistics(statistics)
                .setSkipStatsUpdateOnDbOpen(true)  // 跳过启动时的统计更新
                .setAvoidFlushDuringRecovery(true) // 恢复时避免刷盘
                .setMaxOpenFiles(100)) {            // 限制打开文件数

            // 列族选项
            try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(64 * SizeUnit.MB)
                    .setMaxWriteBufferNumber(4)
                    .setOptimizeFiltersForHits(true)) {  // 优化过滤器

                List<ColumnFamilyHandle> handles = new ArrayList<>();
                this.db = RocksDB.open(dbOptions, config.dbPath, cfDescriptors, handles);

                // 存储列族句柄
                this.defaultHandle = handles.get(0);
                this.lightcurveHandle = handles.get(1);
                this.bitmapIndexHandle = handles.get(2);
                this.metadataHandle = handles.get(3);
                this.timeBucketsHandle = handles.get(4);
            }
        }

        // 初始化异步处理
        this.asyncExecutor = Executors.newSingleThreadExecutor();
        this.indexUpdateQueue = new LinkedBlockingQueue<>(10000);

        if (config.asyncIndexing) {
            startIndexUpdateWorker(config);
        }

        // 存储配置元数据
        storeMetadata(config);
    }

    /**
     * 启动索引更新工作线程
     */
    private void startIndexUpdateWorker(Config config) {
        asyncExecutor.submit(() -> {
            Map<String, List<IndexUpdateTask>> batchMap = new HashMap<>();

            while (running || !indexUpdateQueue.isEmpty()) {
                try {
                    // 批量处理索引更新
                    List<IndexUpdateTask> batch = new ArrayList<>();
                    indexUpdateQueue.drainTo(batch, 1000);

                    if (!batch.isEmpty()) {
                        // 按(sourceId, band)分组
                        for (IndexUpdateTask task : batch) {
                            String key = task.sourceId + "|" + task.band;
                            batchMap.computeIfAbsent(key, k -> new ArrayList<>()).add(task);
                        }

                        // 批量更新位图索引
                        updateBitmapIndexBatch(batchMap, config);
                        batchMap.clear();
                    } else {
                        Thread.sleep(100); // 短暂休眠
                    }
                } catch (Exception e) {
                    // 记录错误，但继续运行
                    System.err.println("Index update error: " + e.getMessage());
                }
            }
        });
    }

    // ========== 数据写入接口 ==========
    /**
     * 批量写入光变曲线数据
     */
    public void putLightCurveBatch(List<LightCurvePoint> points) throws RocksDBException {
        try (final WriteBatch batch = new WriteBatch()) {
            for (LightCurvePoint point : points) {
                byte[] key = buildDataPointKey(point.sourceId, point.band, point.time);
                byte[] value = serializeLightCurvePoint(point);
                batch.put(lightcurveHandle, key, value);

                // 添加到索引更新队列
                indexUpdateQueue.offer(new IndexUpdateTask(
                        point.sourceId, point.band, point.time, point.mag
                ));
            }
            db.write(new WriteOptions(), batch);
            writeCount.addAndGet(points.size());

            // 批量更新天体元数据
            updateStarsMetadataBatch(points);
            // 更新时间桶
            updateTimeBucketsForBatch(points);
        }
    }

    /**
     * 批量更新天体元数据
     */
    private void updateStarsMetadataBatch(List<LightCurvePoint> points) throws RocksDBException {
        Map<Long, List<LightCurvePoint>> pointsByStar = new HashMap<>();

        // 按天体分组
        for (LightCurvePoint point : points) {
            pointsByStar.computeIfAbsent(point.sourceId, k -> new ArrayList<>()).add(point);
        }

        // 批量更新每个天体的元数据
        try (final WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<Long, List<LightCurvePoint>> entry : pointsByStar.entrySet()) {
                long sourceId = entry.getKey();
                List<LightCurvePoint> starPoints = entry.getValue();

                // 获取现有元数据
                byte[] key = buildStarMetadataKey(sourceId);
                byte[] existingValue = db.get(metadataHandle, key);

                StarMetadata metadata;
                if (existingValue != null) {
                    // 基于现有元数据更新
                    metadata = deserializeStarMetadata(existingValue);
                    for (LightCurvePoint point : starPoints) {
                        metadata = new StarMetadata(
                                metadata.sourceId,
                                metadata.ra,
                                metadata.dec,
                                metadata.observationCount + 1,
                                (metadata.avgMag * metadata.observationCount + point.mag) / (metadata.observationCount + 1),
                                Math.min(metadata.firstObsTime, point.time),
                                Math.max(metadata.lastObsTime, point.time)
                        );
                    }
                } else {
                    // 创建新元数据（使用第一个数据点）
                    LightCurvePoint firstPoint = starPoints.get(0);
                    metadata = new StarMetadata(
                            sourceId, firstPoint.ra, firstPoint.dec,
                            starPoints.size(), firstPoint.mag,
                            firstPoint.time, firstPoint.time
                    );
                }

                byte[] value = serializeStarMetadata(metadata);
                batch.put(metadataHandle, key, value);
            }

            db.write(new WriteOptions().setSync(false), batch);
        }
    }

    /**
     * 更新时间桶
     */
    private void updateTimeBucketsForBatch(List<LightCurvePoint> points) throws RocksDBException {
        // 按(sourceId, band)分组
        Map<String, List<LightCurvePoint>> groupedPoints = new HashMap<>();
        for (LightCurvePoint point : points) {
            String key = point.sourceId + "|" + point.band;
            groupedPoints.computeIfAbsent(key, k -> new ArrayList<>()).add(point);
        }

        // 为每组重新计算时间桶
        for (Map.Entry<String, List<LightCurvePoint>> entry : groupedPoints.entrySet()) {
            String[] keyParts = entry.getKey().split("\\|");
            long sourceId = Long.parseLong(keyParts[0]);
            String band = keyParts[1];

            // 获取该天体该波段的所有数据点
            List<LightCurvePoint> allPoints = getLightCurveByBand(sourceId, band);

            if (allPoints.isEmpty()) continue;

            // 按时间排序
            allPoints.sort(Comparator.comparingDouble(p -> p.time));

            // 使用DP算法划分时间桶
            List<TimeBucket> buckets = partitionTimeBuckets(sourceId, band, allPoints, config);

            // 存储时间桶
            storeTimeBuckets(sourceId, band, buckets);
        }
    }
    /**
     * 时间桶划分算法 - 动态规划
     */
    private List<TimeBucket> partitionTimeBuckets(long sourceId, String band,
                                                  List<LightCurvePoint> points, Config config) {
        int n = points.size();
        if (n == 0) return new ArrayList<>();

        // DP数组：dp[i]表示前i个点的最优划分代价
        double[] dp = new double[n + 1];
        int[] partition = new int[n + 1]; // 记录划分位置

        Arrays.fill(dp, Double.MAX_VALUE);
        dp[0] = 0;

        // 动态规划
        for (int i = 1; i <= n; i++) {
            // 尝试不同的划分点j，满足 i - K <= j < i
            int startJ = Math.max(0, i - config.maxBucketGap);
            for (int j = startJ; j < i; j++) {
                double cost = calculateBucketCost(points, j, i - 1, config);
                if (dp[j] + cost < dp[i]) {
                    dp[i] = dp[j] + cost;
                    partition[i] = j;
                }
            }
        }

        // 回溯构建桶
        List<TimeBucket> buckets = new ArrayList<>();
        int end = n;
        int bucketIndex = 0;

        List<Integer> boundaries = new ArrayList<>();
        while (end > 0) {
            boundaries.add(end);
            end = partition[end];
        }
        boundaries.add(0);
        Collections.reverse(boundaries);

        for (int k = 0; k < boundaries.size() - 1; k++) {
            int start = boundaries.get(k);
            int endIdx = boundaries.get(k + 1) - 1;

            if (start > endIdx) continue;

            double minMag = Double.MAX_VALUE;
            double maxMag = Double.MIN_VALUE;
            double startTime = points.get(start).time;
            double endTime = points.get(endIdx).time;
            int count = endIdx - start + 1;

            for (int i = start; i <= endIdx; i++) {
                minMag = Math.min(minMag, points.get(i).mag);
                maxMag = Math.max(maxMag, points.get(i).mag);
            }

            buckets.add(new TimeBucket(sourceId, band, bucketIndex++,
                    startTime, endTime, minMag, maxMag, count));
        }

        // ✅ 打印桶的统计信息
        System.out.println("=== 时间桶划分统计 ===");
        System.out.println("天体ID: " + sourceId + ", 波段: " + band);
        System.out.println("数据点总数: " + n);
        System.out.println("创建桶数: " + buckets.size());
        System.out.println("平均每桶点数: " + (n / (double) buckets.size()));
        if (!buckets.isEmpty()) {
            double avgMagRange = buckets.stream()
                    .mapToDouble(b -> b.maxMag - b.minMag)
                    .average().orElse(0);
            System.out.println("平均星等范围: " + avgMagRange);
        }
        System.out.println("======================");

        return buckets;
    }
    /**
     * 计算桶的代价函数 L(i,j)
     */
    private double calculateBucketCost(List<LightCurvePoint> points, int start, int end, Config config) {
        if (start > end) return 0;

        double minMag = Double.MAX_VALUE;
        double maxMag = Double.MIN_VALUE;
        double startTime = points.get(start).time;
        double endTime = points.get(end).time;

        for (int i = start; i <= end; i++) {
            minMag = Math.min(minMag, points.get(i).mag);
            maxMag = Math.max(maxMag, points.get(i).mag);
        }

        double timeDiff = endTime - startTime;
        double magRange = maxMag - minMag;

        // L(i,j) = λ·C_store + (1-λ)·(t_j - t_i)·(max(m) - min(m))
        return config.lambda * config.storeCost +
                (1 - config.lambda) * timeDiff * magRange;
    }
    /**
     * 存储时间桶
     */
    private void storeTimeBuckets(long sourceId, String band, List<TimeBucket> buckets) throws RocksDBException {
        try (final WriteBatch batch = new WriteBatch()) {
            // 先删除旧的桶
            byte[] prefix = buildTimeBucketPrefix(sourceId, band);
            try (RocksIterator iterator = db.newIterator(timeBucketsHandle)) {
                for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                    byte[] key = iterator.key();
                    if (!startsWith(key, prefix)) break;
                    batch.delete(timeBucketsHandle, key);
                }
            }

            // 存储新的桶
            for (TimeBucket bucket : buckets) {
                byte[] key = buildTimeBucketKey(sourceId, band, bucket.bucketIndex);
                byte[] value = serializeTimeBucket(bucket);
                batch.put(timeBucketsHandle, key, value);
            }

            db.write(new WriteOptions(), batch);
        }
    }

    /**
     * 获取时间桶
     */
    public List<TimeBucket> getTimeBuckets(long sourceId, String band) throws RocksDBException {
        List<TimeBucket> buckets = new ArrayList<>();
        byte[] prefix = buildTimeBucketPrefix(sourceId, band);

        try (RocksIterator iterator = db.newIterator(timeBucketsHandle)) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) break;

                byte[] value = iterator.value();
                buckets.add(deserializeTimeBucket(value));
            }
        }

        return buckets;
    }

    /**
     * 写入天体元数据
     */
    private void putStarMetadata(LightCurvePoint point) throws RocksDBException {
        // 检查是否已存在该天体的元数据
        byte[] key = buildStarMetadataKey(point.sourceId);
        byte[] existingValue = db.get(metadataHandle, key);

        StarMetadata metadata;
        if (existingValue != null) {
            // 更新现有元数据
            metadata = updateStarMetadata(existingValue, point);
        } else {
            // 创建新元数据
            metadata = new StarMetadata(
                    point.sourceId, point.ra, point.dec,
                    1, point.mag, point.time, point.time
            );
        }

        // 序列化并存储元数据
        byte[] value = serializeStarMetadata(metadata);
        db.put(metadataHandle, key, value);
    }
    /**
     * 更新天体元数据
     */
    private StarMetadata updateStarMetadata(byte[] existingValue, LightCurvePoint newPoint) {
        StarMetadata existing = deserializeStarMetadata(existingValue);

        return new StarMetadata(
                existing.sourceId,
                existing.ra, // 坐标保持不变
                existing.dec,
                existing.observationCount + 1,
                (existing.avgMag * existing.observationCount + newPoint.mag) / (existing.observationCount + 1),
                Math.min(existing.firstObsTime, newPoint.time),
                Math.max(existing.lastObsTime, newPoint.time)
        );
    }

    /**
     * 从CSV行创建光变曲线数据点
     */
    public static LightCurvePoint parseFromCSV(String csvLine) {
        String[] parts = csvLine.split(",");
        if (parts.length < 14) {
            throw new IllegalArgumentException("Invalid CSV format: " + csvLine);
        }

        return new LightCurvePoint(
                Long.parseLong(parts[0].trim()),      // source_id
                Double.parseDouble(parts[1].trim()),  // ra
                Double.parseDouble(parts[2].trim()),  // dec
                Long.parseLong(parts[3].trim()),      // transit_id
                parts[4].trim(),                      // band
                Double.parseDouble(parts[5].trim()),  // time
                Double.parseDouble(parts[6].trim()),  // mag
                Double.parseDouble(parts[7].trim()),  // flux
                Double.parseDouble(parts[8].trim()),  // flux_error
                Double.parseDouble(parts[9].trim()),  // flux_over_error
                Boolean.parseBoolean(parts[10].trim()), // rejected_by_photometry
                Boolean.parseBoolean(parts[11].trim()), // rejected_by_variability
                Integer.parseInt(parts[12].trim()),   // other_flags
                Long.parseLong(parts[13].trim())      // solution_id
        );
    }

    // ========== 数据查询接口 ==========
    /**
     * 获取指定天体的完整光变曲线
     */
    public List<LightCurvePoint> getLightCurve(long sourceId) throws RocksDBException {
        byte[] prefix = buildSourceIdPrefix(sourceId);
        List<LightCurvePoint> result = new ArrayList<>();

        try (final RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) break;

                byte[] value = iterator.value();
                LightCurvePoint point = deserializeLightCurvePoint(value);
                System.out.println("RocksDBServer-getLightCurve: 读取到数据点{ sourceId=" + point.sourceId +
                        ", band=" + point.band + ", time=" + point.time + ", mag=" + point.mag +
                        ", flux=" + point.flux + ", fluxError=" + point.fluxError +
                        ", fluxOverError=" + point.fluxOverError +
                        ", rejectedByPhotometry=" + point.rejectedByPhotometry +
                        ", rejectedByVariability=" + point.rejectedByVariability +
                        ", otherFlags=" + point.otherFlags +
                        ", solutionId=" + point.solutionId + " }");
                result.add(point);
                readCount.incrementAndGet();
            }
        }
        return result;
    }

    /**
     * 获取指定天体和波段的光变曲线
     */
    public List<LightCurvePoint> getLightCurveByBand(long sourceId, String band) throws RocksDBException {
        byte[] prefix = buildSourceIdBandPrefix(sourceId, band);
        List<LightCurvePoint> result = new ArrayList<>();

        try (RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) break;

                byte[] value = iterator.value();
                LightCurvePoint point = deserializeLightCurvePoint(value);
                result.add(point);
                readCount.incrementAndGet();
            }
        }

        return result;
    }

    /**
     * 时间范围查询
     */
    public List<LightCurvePoint> queryByTimeRange(double startTime, double endTime) throws RocksDBException {
        List<LightCurvePoint> result = new ArrayList<>();

        try (final RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    byte[] value = iterator.value();
                    LightCurvePoint point = deserializeLightCurvePoint(value);

                    if (point.time >= startTime && point.time <= endTime) {
                        result.add(point);
                        readCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    System.err.println("解析光变曲线数据点失败: " + e.getMessage());
                    // 继续处理下一个数据点
                }
            }
        }

        return result;
    }

    /**
     * 获取所有天体元数据
     */
    public List<StarMetadata> getAllStarsMetadata() throws RocksDBException {
        List<StarMetadata> stars = new ArrayList<>();

        try (final RocksIterator iterator = db.newIterator(metadataHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();

                // 只处理天体元数据（以"star_"开头的key）
                String keyStr = new String(key);
                if (keyStr.startsWith("star_")) {
                    StarMetadata metadata = deserializeStarMetadata(value);
                    stars.add(metadata);
                }
            }
        }

        return stars;
    }

    /**
     * 根据坐标范围查询天体
     */
    public List<StarMetadata> queryStarsByRegion(double minRa, double maxRa,
                                                 double minDec, double maxDec) throws RocksDBException {
        List<StarMetadata> result = new ArrayList<>();

        // 由于RocksDB没有空间索引，这里需要扫描所有元数据
        // 在实际生产环境中，可以考虑使用空间索引库
        List<StarMetadata> allStars = getAllStarsMetadata();

        for (StarMetadata star : allStars) {
            if (star.ra >= minRa && star.ra <= maxRa &&
                    star.dec >= minDec && star.dec <= maxDec) {
                result.add(star);
            }
        }

        return result;
    }

    /**
     * 根据sourceId直接获取天体元数据
     */
    public StarMetadata getStarMetadata(long sourceId) throws RocksDBException {
        byte[] key = buildStarMetadataKey(sourceId);
        byte[] value = db.get(metadataHandle, key);

        if (value == null) {
            return null;
        }

        return deserializeStarMetadata(value);
    }

    /**
     * 批量获取天体元数据
     */
    public Map<Long, StarMetadata> getStarsMetadataBatch(Set<Long> sourceIds) throws RocksDBException {
        Map<Long, StarMetadata> result = new HashMap<>();

        for (Long sourceId : sourceIds) {
            byte[] key = buildStarMetadataKey(sourceId);
            byte[] value = db.get(metadataHandle, key);

            if (value != null) {
                result.put(sourceId, deserializeStarMetadata(value));
            }
        }
        return result;
    }

    /**
     * 基于时间桶的高效查询
     */
    public TimeBucketQueryResult queryWithTimeBuckets(long sourceId, String band,
                                                      double startTime, double endTime,
                                                      double magThreshold, int minCount) throws RocksDBException {
        long startQueryTime = System.nanoTime();

        List<TimeBucket> buckets = getTimeBuckets(sourceId, band);

        // ✅ 统计信息
        int totalBuckets = buckets.size();
        int prunedBuckets = 0;
        int confirmedBuckets = 0;
        int ambiguousBucketCount = 0;

        int confirmedCount = 0;
        List<TimeBucket> ambiguousBuckets = new ArrayList<>();
        int maxPossibleCount = 0;

        for (TimeBucket bucket : buckets) {
            // 检查桶与查询时间范围的关系
            boolean noOverlap = bucket.endTime < startTime || bucket.startTime > endTime;
            boolean fullyWithinQuery = bucket.startTime >= startTime && bucket.endTime <= endTime;
            boolean partialOverlap = !noOverlap && !fullyWithinQuery;

            if (noOverlap) {
                // ✅ 完全不重叠，剪枝
                prunedBuckets++;
                continue;
            }

            if (fullyWithinQuery) {
                // 完全在查询范围内的桶
                if (bucket.maxMag <= magThreshold) { // ✅ 修改：使用 <= 而不是 <
                    // 全部满足
                    confirmedCount += bucket.totalCount;
                    confirmedBuckets++;
                } else if (bucket.minMag > magThreshold) {
                    // 全部不满足，剪枝
                    prunedBuckets++;
                    continue;
                } else {
                    // 不确定
                    ambiguousBuckets.add(bucket);
                    maxPossibleCount += bucket.totalCount;
                    ambiguousBucketCount++;
                }
            } else if (partialOverlap) {
                // ✅ 部分重叠的桶，需要精炼
                ambiguousBuckets.add(bucket);
                maxPossibleCount += bucket.totalCount;
                ambiguousBucketCount++;
            }
        }

        maxPossibleCount += confirmedCount;

        long bucketAnalysisTime = System.nanoTime() - startQueryTime;

//        // ✅ 打印详细统计
//        System.out.println("=== 时间桶查询统计 ===");
//        System.out.println("天体ID: " + sourceId + ", 波段: " + band);
//        System.out.println("查询范围: [" + startTime + ", " + endTime + "], 星等阈值: " + magThreshold);
//        System.out.println("总桶数: " + totalBuckets);
//        System.out.println("剪枝桶数: " + prunedBuckets + " (" + (prunedBuckets * 100.0 / totalBuckets) + "%)");
//        System.out.println("确认桶数: " + confirmedBuckets + " (" + (confirmedBuckets * 100.0 / totalBuckets) + "%)");
//        System.out.println("不确定桶数: " + ambiguousBucketCount + " (" + (ambiguousBucketCount * 100.0 / totalBuckets) + "%)");
//        System.out.println("已确认计数: " + confirmedCount);
//        System.out.println("最大可能计数: " + maxPossibleCount);
//        System.out.println("桶分析耗时: " + (bucketAnalysisTime / 1_000_000.0) + " ms");

        // 快速判断
        if (maxPossibleCount < minCount) {
            // 不可能满足条件
//            System.out.println("结果: PRUNED (最大可能计数不足)");
//            System.out.println("=====================");
            return new TimeBucketQueryResult(false, confirmedCount, null, "PRUNED");
        }

        if (confirmedCount >= minCount) {
            // 已经满足条件，无需精炼
//            System.out.println("结果: CONFIRMED (已满足条件)");
//            System.out.println("=====================");
            return new TimeBucketQueryResult(true, confirmedCount, null, "CONFIRMED");
        }

        // 需要精炼
//        System.out.println("结果: AMBIGUOUS (需要精炼 " + ambiguousBucketCount + " 个桶)");
//        System.out.println("=====================");
        return new TimeBucketQueryResult(null, confirmedCount, ambiguousBuckets, "AMBIGUOUS");
    }

    /**
     * 精炼阶段：精确计算不确定桶的贡献
     */
    public int refineAmbiguousBuckets(long sourceId, String band,
                                      List<TimeBucket> ambiguousBuckets,
                                      double startTime, double endTime,
                                      double magThreshold) throws RocksDBException {
        long startRefineTime = System.nanoTime();
        int count = 0;
        int totalPointsScanned = 0;

        // 只读取不确定桶涉及的时间范围
        for (TimeBucket bucket : ambiguousBuckets) {
            double queryStart = Math.max(startTime, bucket.startTime);
            double queryEnd = Math.min(endTime, bucket.endTime);

            List<LightCurvePoint> points = queryByTimeRangeAndBand(
                    sourceId, band, queryStart, queryEnd);

            totalPointsScanned += points.size();

            for (LightCurvePoint point : points) {
                if (point.mag <= magThreshold) { // ✅ 修改：使用 <= 而不是 <
                    count++;
                }
            }
        }

        long refineTime = System.nanoTime() - startRefineTime;

        System.out.println("--- 精炼阶段统计 ---");
        System.out.println("精炼桶数: " + ambiguousBuckets.size());
        System.out.println("扫描数据点: " + totalPointsScanned);
        System.out.println("满足条件的点: " + count);
        System.out.println("精炼耗时: " + (refineTime / 1_000_000.0) + " ms");
        System.out.println("-------------------");

        return count;
    }

    /**
     * 按时间范围和波段查询
     */
    private List<LightCurvePoint> queryByTimeRangeAndBand(long sourceId, String band,
                                                          double startTime, double endTime) throws RocksDBException {
        List<LightCurvePoint> result = new ArrayList<>();
        byte[] prefix = buildSourceIdBandPrefix(sourceId, band);

        try (RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) break;

                byte[] value = iterator.value();
                LightCurvePoint point = deserializeLightCurvePoint(value);

                if (point.time >= startTime && point.time <= endTime) {
                    result.add(point);
                }
            }
        }
        return result;
    }

    // ========== 位图索引查询 ==========

    /**
     * 存在性查询 - 使用位图索引快速筛选
     */
    public Set<Long> existenceQuery(double startTime, double endTime,
                                    String band, int minObservations) throws RocksDBException {
        // 使用LinkedHashSet确保结果有序且无重复
        Set<Long> candidates = new LinkedHashSet<>();

        try (final RocksIterator iterator = db.newIterator(bitmapIndexHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    BitmapIndexEntry indexEntry = deserializeBitmapIndex(iterator.key(), iterator.value());

                    // 检查波段匹配
                    if (!indexEntry.band.equals(band)) continue;

                    // 使用位图索引快速判断
                    if (hasObservationsInRange(indexEntry, startTime, endTime, minObservations)) {
                        candidates.add(indexEntry.sourceId);
                    }
                } catch (Exception e) {
                    System.err.println("解析位图索引失败: " + e.getMessage());
                    // 继续处理下一个索引条目
                }
            }
        }

        System.out.println("RocksDBServer.existenceQuery: 返回 " + candidates);
        return candidates;
    }
    /**
     * 获取天体的位图索引
     */
    public BitmapIndexEntry getBitmapIndex(long sourceId, String band) throws RocksDBException {
        byte[] key = buildBitmapIndexKey(sourceId, band);
        byte[] value = db.get(bitmapIndexHandle, key);

        if (value == null) return null;
        return deserializeBitmapIndex(key, value);
    }

    // ========== 系统管理接口 ==========

    /**
     * 获取HEALPix天区统计信息
     */
    public Map<String, Object> getHealpixStatistics() throws RocksDBException {
        Map<String, Object> stats = new LinkedHashMap<>();

        stats.put("healpix_id", healpixId);
        stats.put("db_path", dbPath);
        stats.put("total_data_points", writeCount.get());
        stats.put("total_reads", readCount.get());
        stats.put("uptime_minutes", (System.currentTimeMillis() - startTime) / 60000);

        // 估算数据点数量
        try (final RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            long count = 0;
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
            stats.put("estimated_data_points", count);
        }

        // 数据库属性
        stats.put("approximate_size_mb", getApproximateSize() / (1024 * 1024));

        return stats;
    }

    /**
     * 手动触发位图索引重建
     */
    public void rebuildBitmapIndex(Config config) throws RocksDBException {
        Map<String, List<IndexUpdateTask>> allUpdates = new HashMap<>();

        // 扫描所有数据点构建索引
        try (final RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] value = iterator.value();
                LightCurvePoint point = deserializeLightCurvePoint(value);

                String key = point.sourceId + "|" + point.band;
                allUpdates.computeIfAbsent(key, k -> new ArrayList<>())
                        .add(new IndexUpdateTask(point.sourceId, point.band, point.time, point.mag));
            }
        }

        // 批量更新索引
        updateBitmapIndexBatch(allUpdates, config);
    }

    /**
     * 压缩数据库
     */
    public void compact() throws RocksDBException {
        db.compactRange();
    }

    @Override
    public void close() {
        running = false;
        asyncExecutor.shutdown();

        if (defaultHandle != null) defaultHandle.close();
        if (lightcurveHandle != null) lightcurveHandle.close();
        if (bitmapIndexHandle != null) bitmapIndexHandle.close();
        if (metadataHandle != null) metadataHandle.close();
        if (db != null) db.close();
        if (statistics != null) statistics.close();
    }

    // ========== 序列化/反序列化 ==========

    private byte[] buildDataPointKey(long sourceId, String band, double time) {
        // 键格式: sourceId|band|time
        return String.format("%d|%s|%.6f", sourceId, band, time).getBytes();
    }
    private byte[] buildSourceIdPrefix(long sourceId) {
        return String.format("%d|", sourceId).getBytes();
    }
    private byte[] buildSourceIdBandPrefix(long sourceId, String band) {
        return String.format("%d|%s|", sourceId, band).getBytes();
    }
    private byte[] buildBitmapIndexKey(long sourceId, String band) {
        return String.format("%d|%s", sourceId, band).getBytes();
    }
    private byte[] buildTimeBucketPrefix(long sourceId, String band) {
        return String.format("%d|%s|", sourceId, band).getBytes();
    }
    private byte[] buildTimeBucketKey(long sourceId, String band, int bucketIndex) {
        return String.format("%d|%s|%d", sourceId, band, bucketIndex).getBytes();
    }
    private byte[] serializeLightCurvePoint(LightCurvePoint point) {
        // 使用紧凑的二进制格式
        // 格式: sourceId(8),ra(8),dec(8),transitId(8),band(var),time(8),mag(8),flux(8),fluxError(8),fluxOverError(8),flags(1),solutionId(8)
        // 实际实现应使用更高效的序列化库如Protobuf
        String serialized = String.format("%d,%.6f,%.6f,%d,%s,%.6f,%.6f,%.6f,%.6f,%.6f,%b,%b,%d,%d",
                point.sourceId, point.ra, point.dec, point.transitId, point.band,
                point.time, point.mag, point.flux, point.fluxError, point.fluxOverError,
                point.rejectedByPhotometry, point.rejectedByVariability,
                point.otherFlags, point.solutionId);
        return serialized.getBytes();
    }
    private LightCurvePoint deserializeLightCurvePoint(byte[] data) {
        String str = new String(data);
        String[] parts = str.split(",");

        return new LightCurvePoint(
                Long.parseLong(parts[0]),
                Double.parseDouble(parts[1]),
                Double.parseDouble(parts[2]),
                Long.parseLong(parts[3]),
                parts[4],
                Double.parseDouble(parts[5]),
                Double.parseDouble(parts[6]),
                Double.parseDouble(parts[7]),
                Double.parseDouble(parts[8]),
                Double.parseDouble(parts[9]),
                Boolean.parseBoolean(parts[10]),
                Boolean.parseBoolean(parts[11]),
                Integer.parseInt(parts[12]),
                Long.parseLong(parts[13])
        );
    }
    private byte[] serializeBitmapIndex(BitmapIndexEntry entry) {
        // 序列化位图索引
        StringBuilder sb = new StringBuilder();
        sb.append(entry.sourceId).append(",")
                .append(entry.band).append(",")
                .append(entry.minMag).append(",")
                .append(entry.maxMag).append(",")
                .append(entry.observationCount).append(",")
                .append(bitSetToString(entry.timeBitmap));
        return sb.toString().getBytes();
    }
    private BitmapIndexEntry deserializeBitmapIndex(byte[] key, byte[] value) {
        String keyStr = new String(key);
        String[] keyParts = keyStr.split("\\|");
        long sourceId = Long.parseLong(keyParts[0]);
        String band = keyParts[1];

        String valueStr = new String(value);
        String[] valueParts = valueStr.split(",");

        double minMag = Double.parseDouble(valueParts[2]);
        double maxMag = Double.parseDouble(valueParts[3]);
        int observationCount = Integer.parseInt(valueParts[4]);
        BitSet timeBitmap = stringToBitSet(valueParts[5]);

        return new BitmapIndexEntry(sourceId, band, timeBitmap, minMag, maxMag, observationCount);
    }
    private byte[] buildStarMetadataKey(long sourceId) {
        return ("star_" + sourceId).getBytes();
    }
    private byte[] serializeStarMetadata(StarMetadata metadata) {
        String serialized = String.format("%d,%.6f,%.6f,%d,%.3f,%.3f,%.3f",
                metadata.sourceId, metadata.ra, metadata.dec, metadata.observationCount,
                metadata.avgMag, metadata.firstObsTime, metadata.lastObsTime);
        return serialized.getBytes();
    }
    private StarMetadata deserializeStarMetadata(byte[] data) {
        String str = new String(data);
        String[] parts = str.split(",");

        return new StarMetadata(
                Long.parseLong(parts[0]),
                Double.parseDouble(parts[1]),
                Double.parseDouble(parts[2]),
                Integer.parseInt(parts[3]),
                Double.parseDouble(parts[4]),
                Double.parseDouble(parts[5]),
                Double.parseDouble(parts[6])
        );
    }
    private byte[] serializeTimeBucket(TimeBucket bucket) {
        String serialized = String.format("%d,%s,%d,%.6f,%.6f,%.6f,%.6f,%d",
                bucket.sourceId, bucket.band, bucket.bucketIndex,
                bucket.startTime, bucket.endTime,
                bucket.minMag, bucket.maxMag, bucket.totalCount);
        return serialized.getBytes();
    }
    private TimeBucket deserializeTimeBucket(byte[] data) {
        String str = new String(data);
        String[] parts = str.split(",");

        return new TimeBucket(
                Long.parseLong(parts[0]),
                parts[1],
                Integer.parseInt(parts[2]),
                Double.parseDouble(parts[3]),
                Double.parseDouble(parts[4]),
                Double.parseDouble(parts[5]),
                Double.parseDouble(parts[6]),
                Integer.parseInt(parts[7])
        );
    }
    // ========== 位图索引维护 ==========

    private void updateBitmapIndexBatch(Map<String, List<IndexUpdateTask>> batchMap, Config config)
            throws RocksDBException {
        try (final WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<String, List<IndexUpdateTask>> entry : batchMap.entrySet()) {
                String[] keyParts = entry.getKey().split("\\|");
                long sourceId = Long.parseLong(keyParts[0]);
                String band = keyParts[1];

                // 获取现有索引或创建新索引
                BitmapIndexEntry existingIndex = getBitmapIndex(sourceId, band);
                BitmapIndexEntry updatedIndex = updateBitmapIndex(existingIndex, entry.getValue(), config);

                // 写入更新后的索引
                byte[] key = buildBitmapIndexKey(sourceId, band);
                byte[] value = serializeBitmapIndex(updatedIndex);
                batch.put(bitmapIndexHandle, key, value);
            }
            db.write(new WriteOptions().setSync(false), batch);
        }
    }

    private BitmapIndexEntry updateBitmapIndex(BitmapIndexEntry existing, List<IndexUpdateTask> updates, Config config) {
        long sourceId = updates.get(0).sourceId;
        String band = updates.get(0).band;

        BitSet timeBitmap = (existing != null) ? existing.timeBitmap : new BitSet(config.bitmapSize);
        double minMag = (existing != null) ? existing.minMag : Double.MAX_VALUE;
        double maxMag = (existing != null) ? existing.maxMag : Double.MIN_VALUE;
        int observationCount = (existing != null) ? existing.observationCount : 0;

        for (IndexUpdateTask task : updates) {
            // 更新时间位图
            int timeBucket = (int) (task.time / config.timeBucketSize);
            if (timeBucket >= 0 && timeBucket < config.bitmapSize) {
                timeBitmap.set(timeBucket);
            }

            // 更新星等范围
            minMag = Math.min(minMag, task.mag);
            maxMag = Math.max(maxMag, task.mag);
            observationCount++;
        }

        return new BitmapIndexEntry(sourceId, band, timeBitmap, minMag, maxMag, observationCount);
    }

    private boolean hasObservationsInRange(BitmapIndexEntry index, double startTime, double endTime,
                                           int minObservations) {
        int startBucket = (int) (startTime / config.timeBucketSize);
        int endBucket = (int) (endTime / config.timeBucketSize);

        startBucket = Math.max(0, startBucket);
        endBucket = Math.min(config.bitmapSize - 1, endBucket);

        // 计算时间范围内的设置位数
        int count = 0;
        for (int i = startBucket; i <= endBucket; i++) {
            if (index.timeBitmap.get(i)) {
                count++;
                if (count >= minObservations) {
                    return true;
                }
            }
        }

        return false;
    }

    // ========== 工具方法 ==========

    private String bitSetToString(BitSet bitSet) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bitSet.length(); i++) {
            sb.append(bitSet.get(i) ? '1' : '0');
        }
        return sb.toString();
    }

    private BitSet stringToBitSet(String str) {
        BitSet bitSet = new BitSet(str.length());
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '1') {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    private boolean startsWith(byte[] array, byte[] prefix) {
        if (prefix.length > array.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (array[i] != prefix[i]) return false;
        }
        return true;
    }

    private long getApproximateSize() throws RocksDBException {
        return db.getLongProperty("rocksdb.estimate-live-data-size");
    }

    private void storeMetadata(Config config) throws RocksDBException {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("healpix_id", String.valueOf(config.healpixId));
        metadata.put("creation_time", String.valueOf(System.currentTimeMillis()));
        metadata.put("time_bucket_size", String.valueOf(config.timeBucketSize));
        metadata.put("bitmap_size", String.valueOf(config.bitmapSize));

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            db.put(metadataHandle, entry.getKey().getBytes(), entry.getValue().getBytes());
        }
    }
}