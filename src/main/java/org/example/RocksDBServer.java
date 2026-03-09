package org.example;

import org.example.utils.StorageUtils;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 天文光变曲线 RocksDB 服务 - 针对 HEALPix 分区的分布式存储
 *
 * 写入路径（已修改为完整持久化）：
 *   putLightCurveBatch(points)
 *     → WriteBatch 批量组装
 *     → db.write(writeOptions, batch)  // WAL → memtable → flush → SST
 *
 * 与基线系统对齐：
 *   - WAL_LEVEL: 使用 RocksDB 默认 WAL（与 TDengine WAL_LEVEL=1 对齐）
 *   - 持久化: 每次 write() 都经过 WAL 持久化保证
 *   - 刷盘: 由 RocksDB 原生 memtable 管理 flush 时机
 */
public class RocksDBServer implements AutoCloseable {
    private final RocksDB db;
    private final String dbPath;
    private final long healpixId;
    private final Config config;
    private final Map<Long, StarMetadata> metadataCache = new ConcurrentHashMap<>();

    // 列族句柄
    private final ColumnFamilyHandle defaultHandle;
    private final ColumnFamilyHandle lightcurveHandle;
    private final ColumnFamilyHandle metadataHandle;
    private final ColumnFamilyHandle timeBucketsHandle;

    // 写入选项：走 WAL 持久化路径（公平比较）
    private final WriteOptions writeOptions;

    // 统计信息
    private final AtomicLong writeCount = new AtomicLong(0);
    private final AtomicLong readCount = new AtomicLong(0);
    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private final Statistics statistics;
    private final long startTime;
    private final long initialDirSize;

    // Boundary-aligned flush telemetry (保留用于 getHealpixStatistics)
    private final AtomicLong totalFragmentedFlushCount = new AtomicLong(0);
    private final AtomicLong totalLowerLevelAlignedFlushCount = new AtomicLong(0);

    public RocksDBServer(Config config) throws RocksDBException {
        this.healpixId = config.healpixId;
        this.dbPath = config.dbPath;
        this.startTime = System.currentTimeMillis();
        this.config = config;

        RocksDB.loadLibrary();

        this.statistics = new Statistics();

        RocksDBGlobalResourceManager globalRes = RocksDBGlobalResourceManager.getInstance();

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setBlockCache(globalRes.getBlockCache());

        // 数据库选项
        try (final DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setEnv(globalRes.getEnv())
                .setWriteBufferManager(globalRes.getWriteBufferManager())
                .setMaxBackgroundJobs(16)
                .setIncreaseParallelism(Math.max(8, Runtime.getRuntime().availableProcessors()))
                .setBytesPerSync(4L * 1024 * 1024)
                .setStatistics(statistics)) {

            // 列族选项 - 256MB write buffer（对齐 TDengine BUFFER=256）
            try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setTableFormatConfig(tableConfig)
                    .setWriteBufferSize(256L * 1024 * 1024) // 256MB 对齐
                    .setMaxWriteBufferNumber(4)
                    .setMinWriteBufferNumberToMerge(2)
                    .setLevel0FileNumCompactionTrigger(8)
                    .setLevel0SlowdownWritesTrigger(32)
                    .setLevel0StopWritesTrigger(48)) {

                List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                        new ColumnFamilyDescriptor("default".getBytes(), cfOptions),
                        new ColumnFamilyDescriptor("lightcurve".getBytes(), cfOptions),
                        new ColumnFamilyDescriptor("metadata".getBytes(), cfOptions),
                        new ColumnFamilyDescriptor("time_buckets".getBytes(), cfOptions)
                );

                List<ColumnFamilyHandle> handles = new ArrayList<>();
                this.db = RocksDB.open(dbOptions, config.dbPath, cfDescriptors, handles);

                this.defaultHandle = handles.get(0);
                this.lightcurveHandle = handles.get(1);
                this.metadataHandle = handles.get(2);
                this.timeBucketsHandle = handles.get(3);
            }
        }

        // 写入选项：启用 WAL（公平比较）
        // sync=false 表示不每次 fsync，由 OS 缓冲（与 TDengine WAL_FSYNC_PERIOD=3000 对齐）
        this.writeOptions = new WriteOptions()
                .setSync(false)
                .setDisableWAL(false);

        storeMetadata(config);
        this.initialDirSize = StorageUtils.getDirectorySize(config.dbPath);
    }

    // ========== 数据写入接口 ==========

    /**
     * 批量写入 — 极简快速路径
     *
     * 优化：
     * 1. 二进制 key（消除 String.format）
     * 2. 不在写入路径做元数据更新（延迟到 forceFlush）
     * 3. 不在写入路径做时间桶更新
     */
    public void putLightCurveBatch(List<LightCurvePoint> points) throws RocksDBException {
        if (points == null || points.isEmpty()) return;

        long batchLogicalSize = points.size() * 90L;

        try (WriteBatch batch = new WriteBatch()) {
            for (LightCurvePoint point : points) {
                byte[] key = buildBinaryKey(point.sourceId, point.band, point.time);
                byte[] value = serializeLightCurvePoint(point);
                batch.put(lightcurveHandle, key, value);
            }
            db.write(writeOptions, batch);
        }

        logicalBytesWritten.addAndGet(batchLogicalSize);
        writeCount.addAndGet(points.size());

        // 元数据延迟聚合（内存操作，不读 RocksDB）
        for (LightCurvePoint point : points) {
            metadataCache.merge(point.sourceId,
                    new StarMetadata(point.sourceId, point.ra, point.dec, 1, point.mag, point.time, point.time),
                    (existing, newMeta) -> new StarMetadata(
                            existing.sourceId, existing.ra, existing.dec,
                            existing.observationCount + 1,
                            (existing.avgMag * existing.observationCount + newMeta.avgMag) / (existing.observationCount + 1),
                            Math.min(existing.firstObsTime, newMeta.firstObsTime),
                            Math.max(existing.lastObsTime, newMeta.lastObsTime)
                    ));
        }
    }

    /**
     * 批量更新天体元数据（内存聚合，forceFlush 时落盘）
     */
    private void updateStarsMetadataBatch(List<LightCurvePoint> points) {
        for (LightCurvePoint point : points) {
            metadataCache.compute(point.sourceId, (id, existing) -> {
                if (existing == null) {
                    try {
                        byte[] dbVal = db.get(metadataHandle, buildStarMetadataKey(id));
                        if (dbVal != null) existing = deserializeStarMetadata(dbVal);
                    } catch (RocksDBException e) {}
                }
                if (existing != null) {
                    return new StarMetadata(
                            existing.sourceId, existing.ra, existing.dec,
                            existing.observationCount + 1,
                            (existing.avgMag * existing.observationCount + point.mag) / (existing.observationCount + 1),
                            Math.min(existing.firstObsTime, point.time),
                            Math.max(existing.lastObsTime, point.time)
                    );
                } else {
                    return new StarMetadata(id, point.ra, point.dec, 1, point.mag, point.time, point.time);
                }
            });
        }
    }

    /**
     * 更新时间桶
     */
    private void updateTimeBucketsForBatch(List<LightCurvePoint> points) throws RocksDBException {
        Map<String, List<LightCurvePoint>> groupedPoints = new HashMap<>();
        for (LightCurvePoint point : points) {
            String key = point.sourceId + "|" + point.band;
            groupedPoints.computeIfAbsent(key, k -> new ArrayList<>()).add(point);
        }

        for (Map.Entry<String, List<LightCurvePoint>> entry : groupedPoints.entrySet()) {
            String[] keyParts = entry.getKey().split("\\|");
            long sourceId = Long.parseLong(keyParts[0]);
            String band = keyParts[1];

            List<LightCurvePoint> allPoints = getLightCurveByBand(sourceId, band);
            if (allPoints.isEmpty()) continue;

            allPoints.sort(Comparator.comparingDouble(p -> p.time));
            List<TimeBucket> buckets = partitionTimeBuckets(sourceId, band, allPoints, config);
            storeTimeBuckets(sourceId, band, buckets);
        }
    }
    /**
     * 基于动态规划的自适应分桶算法（严格对齐论文 Algorithm 1）
     *
     * 对固定终点 i，从 i 向前枚举起点 s，增量维护 min(m) 与 max(m)。
     * 硬约束：ΔT(s,i) ≤ ΔT_max 且 (i-s+1) ≤ N_max。
     * 代价函数：L(s,i) = λ·C_store + (1-λ)·Δt·Δm
     *
     * 时间复杂度 O(n·min(K, N_max))，空间复杂度 O(n)。
     */
    private List<TimeBucket> partitionTimeBuckets(long sourceId, String band,
                                                  List<LightCurvePoint> points, Config config) {
        int n = points.size();
        if (n == 0) return new ArrayList<>();

        double lambda = config.lambda;
        double cStore = config.storeCost;
        double deltaTMax = config.deltaTMax;
        int nMax = config.nMax;
        int K = Math.min(config.maxBucketGap, nMax); // 回看上界

        // DP[i] 表示前 i 个观测点的最小分桶代价
        double[] dp = new double[n + 1];
        int[] pre = new int[n + 1];  // pre[i] 记录最优断点 j*

        Arrays.fill(dp, Double.MAX_VALUE);
        dp[0] = 0;
        Arrays.fill(pre, -1);

        for (int i = 1; i <= n; i++) {
            double mMin = Double.MAX_VALUE;
            double mMax = -Double.MAX_VALUE;

            // 从 i 向前枚举桶起点 s（论文中 s = j+1，这里 s 是 1-indexed）
            for (int s = i; s >= Math.max(1, i - K + 1); s--) {
                // 增量维护桶内 min/max mag
                mMin = Math.min(mMin, points.get(s - 1).mag);
                mMax = Math.max(mMax, points.get(s - 1).mag);

                double deltaT = points.get(i - 1).time - points.get(s - 1).time;
                int cnt = i - s + 1;

                // 硬约束检查：违反则提前终止
                if (deltaT > deltaTMax || cnt > nMax) {
                    break;
                }

                double deltaM = mMax - mMin;
                double cost = dp[s - 1] + lambda * cStore + (1 - lambda) * deltaT * deltaM;

                if (cost < dp[i]) {
                    dp[i] = cost;
                    pre[i] = s - 1; // 断点 j* = s-1
                }
            }
        }

        // 从 i=n 反向回溯，恢复桶边界
        List<int[]> boundaries = new ArrayList<>();
        int pos = n;
        while (pos > 0) {
            int start = pre[pos] + 1; // 桶起点（1-indexed）
            boundaries.add(new int[]{start, pos});
            pos = pre[pos];
        }
        Collections.reverse(boundaries);

        // 构建 TimeBucket 对象（含增强统计量）
        List<TimeBucket> buckets = new ArrayList<>();
        int bucketIndex = 0;

        for (int[] bound : boundaries) {
            int startIdx = bound[0] - 1; // 转为 0-indexed
            int endIdx = bound[1] - 1;

            if (startIdx > endIdx) continue;

            double bMinMag = Double.MAX_VALUE;
            double bMaxMag = -Double.MAX_VALUE;
            double sumMag = 0;
            double sumMagSq = 0;
            int qualityFlags = 0;
            int count = endIdx - startIdx + 1;

            for (int k = startIdx; k <= endIdx; k++) {
                LightCurvePoint p = points.get(k);
                bMinMag = Math.min(bMinMag, p.mag);
                bMaxMag = Math.max(bMaxMag, p.mag);
                sumMag += p.mag;
                sumMagSq += p.mag * p.mag;
                if (p.rejectedByPhotometry || p.rejectedByVariability) {
                    qualityFlags++;
                }
            }

            double meanMag = sumMag / count;
            double varMag = count > 1 ? (sumMagSq / count - meanMag * meanMag) : 0.0;

            buckets.add(new TimeBucket(
                    sourceId, band, bucketIndex++,
                    points.get(startIdx).time, points.get(endIdx).time,
                    bMinMag, bMaxMag, count,
                    meanMag, varMag));
        }

        return buckets;
    }

    /**
     * 计算桶的代价函数
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

        return config.lambda * config.storeCost +
                (1 - config.lambda) * timeDiff * magRange;
    }

    /**
     * 存储时间桶（禁用 WAL - 时间桶是派生索引，可从原始数据重建）
     */
    private void storeTimeBuckets(long sourceId, String band, List<TimeBucket> buckets) throws RocksDBException {
        try (final WriteBatch batch = new WriteBatch()) {
            byte[] prefix = buildTimeBucketPrefix(sourceId, band);
            try (RocksIterator iterator = db.newIterator(timeBucketsHandle)) {
                for (iterator.seek(prefix); iterator.isValid(); iterator.next()) {
                    byte[] key = iterator.key();
                    if (!startsWith(key, prefix)) break;
                    batch.delete(timeBucketsHandle, key);
                }
            }

            for (TimeBucket bucket : buckets) {
                byte[] key = buildTimeBucketKey(sourceId, band, bucket.bucketIndex);
                byte[] value = serializeTimeBucket(bucket);
                batch.put(timeBucketsHandle, key, value);
            }

            // 时间桶是派生索引，可以禁用 WAL
            db.write(new WriteOptions().setDisableWAL(true), batch);
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
     * 离线构建时间桶索引
     */
    public void buildAllTimeBucketsOffline() throws RocksDBException {
        System.out.println("开始为 HEALPix " + healpixId + " 全量构建时间桶...");
        long start = System.currentTimeMillis();
        int processedGroups = 0;

        String currentPrefix = "";
        List<LightCurvePoint> currentPoints = new ArrayList<>();
        long currentSourceId = -1;
        String currentBand = "";

        try (RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                String keyStr = new String(key);
                String[] parts = keyStr.split("\\|");
                if (parts.length < 3) continue;

                long sourceId = Long.parseLong(parts[0]);
                String band = parts[1];
                String prefix = sourceId + "|" + band;

                LightCurvePoint point = deserializeLightCurvePoint(iterator.value());

                if (!prefix.equals(currentPrefix)) {
                    if (!currentPoints.isEmpty()) {
                        currentPoints.sort(Comparator.comparingDouble(p -> p.time));
                        List<TimeBucket> buckets = partitionTimeBuckets(currentSourceId, currentBand, currentPoints, config);
                        storeTimeBuckets(currentSourceId, currentBand, buckets);
                        processedGroups++;
                    }
                    currentPrefix = prefix;
                    currentSourceId = sourceId;
                    currentBand = band;
                    currentPoints.clear();
                }
                currentPoints.add(point);
            }

            if (!currentPoints.isEmpty()) {
                currentPoints.sort(Comparator.comparingDouble(p -> p.time));
                List<TimeBucket> buckets = partitionTimeBuckets(currentSourceId, currentBand, currentPoints, config);
                storeTimeBuckets(currentSourceId, currentBand, buckets);
                processedGroups++;
            }
        }

        db.flush(new FlushOptions().setWaitForFlush(true), timeBucketsHandle);
        db.compactRange(timeBucketsHandle);

        System.out.println("HEALPix " + healpixId + " 时间桶构建完成，共 " + processedGroups +
                " 个组，耗时: " + (System.currentTimeMillis() - start) + "ms");
    }

    /**
     * 从 CSV 行解析光变曲线数据点
     */
    public static LightCurvePoint parseFromCSV(String csvLine) {
        String[] parts = csvLine.split(",");
        if (parts.length < 14) {
            throw new IllegalArgumentException("Invalid CSV format: " + csvLine);
        }

        return new LightCurvePoint(
                Long.parseLong(parts[0].trim()),
                Double.parseDouble(parts[1].trim()),
                Double.parseDouble(parts[2].trim()),
                Long.parseLong(parts[3].trim()),
                parts[4].trim(),
                Double.parseDouble(parts[5].trim()),
                Double.parseDouble(parts[6].trim()),
                Double.parseDouble(parts[7].trim()),
                Double.parseDouble(parts[8].trim()),
                Double.parseDouble(parts[9].trim()),
                Boolean.parseBoolean(parts[10].trim()),
                Boolean.parseBoolean(parts[11].trim()),
                Integer.parseInt(parts[12].trim()),
                Long.parseLong(parts[13].trim())
        );
    }

    // ========== 数据查询接口 ==========

    public List<LightCurvePoint> getLightCurve(long sourceId) throws RocksDBException {
        byte[] prefix = buildSourceIdPrefix(sourceId);
        List<LightCurvePoint> result = new ArrayList<>();

        try (final RocksIterator iterator = db.newIterator(lightcurveHandle)) {
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
                    // 继续
                }
            }
        }

        return result;
    }

    public List<StarMetadata> getAllStarsMetadata() throws RocksDBException {
        Map<Long, StarMetadata> merged = new LinkedHashMap<>();

        try (final RocksIterator iterator = db.newIterator(metadataHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                String keyStr = new String(key);
                if (keyStr.startsWith("star_")) {
                    StarMetadata metadata = deserializeStarMetadata(iterator.value());
                    merged.put(metadata.sourceId, metadata);
                }
            }
        }

        merged.putAll(metadataCache);
        return new ArrayList<>(merged.values());
    }

    public List<StarMetadata> queryStarsByRegion(double minRa, double maxRa,
                                                 double minDec, double maxDec) throws RocksDBException {
        List<StarMetadata> result = new ArrayList<>();
        List<StarMetadata> allStars = getAllStarsMetadata();

        for (StarMetadata star : allStars) {
            if (star.ra >= minRa && star.ra <= maxRa &&
                    star.dec >= minDec && star.dec <= maxDec) {
                result.add(star);
            }
        }

        return result;
    }

    public StarMetadata getStarMetadata(long sourceId) throws RocksDBException {
        byte[] key = buildStarMetadataKey(sourceId);
        byte[] value = db.get(metadataHandle, key);
        if (value == null) return null;
        return deserializeStarMetadata(value);
    }

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
     * STMK 查询：基于时间桶索引的剪枝查询（对齐论文 Algorithm 2）
     *
     * 支持双向星等范围 [magMin, magMax]，使用 certainCount/maybeCount 双计数器实现
     * 正向剪枝（certainCount ≥ K）和负向剪枝（certainCount + maybeCount < K）。
     *
     * @param sourceId       天体对象 ID
     * @param band           波段
     * @param startTime      查询时间下界 T1
     * @param endTime        查询时间上界 T2
     * @param magMin         查询星等下界 M_min
     * @param magMax         查询星等上界 M_max
     * @param minCount       最少探测次数 K
     * @return 查询结果（CONFIRMED / PRUNED / AMBIGUOUS）
     */
    public TimeBucketQueryResult queryWithTimeBucketsSTMK(long sourceId, String band,
                                                          double startTime, double endTime,
                                                          double magMin, double magMax,
                                                          int minCount) throws RocksDBException {
        List<TimeBucket> buckets = getTimeBuckets(sourceId, band);

        // 若无时间桶索引，回退到全量扫描
        if (buckets.isEmpty()) {
            int exactCount = 0;
            for (LightCurvePoint point : queryByTimeRangeAndBand(sourceId, band, startTime, endTime)) {
                if (point.mag >= magMin && point.mag <= magMax) exactCount++;
            }
            if (exactCount >= minCount) {
                return new TimeBucketQueryResult(true, exactCount, null, "CONFIRMED");
            }
            return new TimeBucketQueryResult(false, exactCount, null, "PRUNED");
        }

        int certainCount = 0;   // 确定满足条件的观测点数
        int maybeCount = 0;     // 可能满足条件的观测点数
        List<TimeBucket> ambiguousBuckets = new ArrayList<>();

        for (TimeBucket bucket : buckets) {
            // 时间剪枝：桶与查询时间窗无交集
            if (bucket.endTime < startTime || bucket.startTime > endTime) {
                continue;
            }
            // 星等剪枝：桶的星等范围与 [magMin, magMax] 无交集
            if (bucket.maxMag < magMin || bucket.minMag > magMax) {
                continue;
            }

            // 判断桶是否完全被查询范围包含
            boolean timeFullyContained = (bucket.startTime >= startTime && bucket.endTime <= endTime);
            boolean magFullyContained = (bucket.minMag >= magMin && bucket.maxMag <= magMax);

            if (timeFullyContained && magFullyContained) {
                // 绿桶：桶内所有点必定满足条件
                certainCount += bucket.totalCount;
            } else {
                // 黄桶：部分重叠，无法确定
                ambiguousBuckets.add(bucket);
                maybeCount += bucket.totalCount;
            }

            // 正向剪枝：certainCount 已达 K，提前返回
            if (certainCount >= minCount) {
                return new TimeBucketQueryResult(true, certainCount, null, "CONFIRMED");
            }
        }

        // 负向剪枝：即使所有待定点都满足也不够 K
        if (certainCount + maybeCount < minCount) {
            return new TimeBucketQueryResult(false, certainCount, null, "PRUNED");
        }

        // 确定满足条件，无需精炼
        if (certainCount >= minCount) {
            return new TimeBucketQueryResult(true, certainCount, null, "CONFIRMED");
        }

        // 需要精炼
        return new TimeBucketQueryResult(null, certainCount, ambiguousBuckets, "AMBIGUOUS");
    }

    /**
     * 精炼黄桶：只扫描 ambiguousBuckets 指向的原始数据段
     * 支持双向星等范围 [magMin, magMax]
     */
    public int refineAmbiguousBucketsSTMK(long sourceId, String band,
                                          List<TimeBucket> ambiguousBuckets,
                                          double startTime, double endTime,
                                          double magMin, double magMax) throws RocksDBException {
        int count = 0;

        for (TimeBucket bucket : ambiguousBuckets) {
            double queryStart = Math.max(startTime, bucket.startTime);
            double queryEnd = Math.min(endTime, bucket.endTime);

            List<LightCurvePoint> points = queryByTimeRangeAndBand(
                    sourceId, band, queryStart, queryEnd);

            for (LightCurvePoint point : points) {
                if (point.mag >= magMin && point.mag <= magMax) {
                    count++;
                }
            }
        }

        return count;
    }

    /**
     * 基于时间桶的查询
     */
    public TimeBucketQueryResult queryWithTimeBuckets(long sourceId, String band,
                                                      double startTime, double endTime,
                                                      double magThreshold, int minCount) throws RocksDBException {
        long startQueryTime = System.nanoTime();

        List<TimeBucket> buckets = getTimeBuckets(sourceId, band);
        if (buckets.isEmpty()) {
            int exactCount = 0;
            for (LightCurvePoint point : queryByTimeRangeAndBand(sourceId, band, startTime, endTime)) {
                if (point.mag <= magThreshold) exactCount++;
            }
            if (exactCount >= minCount) {
                return new TimeBucketQueryResult(true, exactCount, null, "CONFIRMED");
            }
            return new TimeBucketQueryResult(false, exactCount, null, "PRUNED");
        }

        int confirmedCount = 0;
        List<TimeBucket> ambiguousBuckets = new ArrayList<>();
        int maxPossibleCount = 0;

        for (TimeBucket bucket : buckets) {
            boolean noOverlap = bucket.endTime < startTime || bucket.startTime > endTime;
            boolean fullyWithinQuery = bucket.startTime >= startTime && bucket.endTime <= endTime;

            if (noOverlap) continue;

            if (fullyWithinQuery) {
                if (bucket.maxMag <= magThreshold) {
                    confirmedCount += bucket.totalCount;
                } else if (bucket.minMag > magThreshold) {
                    continue;
                } else {
                    ambiguousBuckets.add(bucket);
                    maxPossibleCount += bucket.totalCount;
                }
            } else {
                ambiguousBuckets.add(bucket);
                maxPossibleCount += bucket.totalCount;
            }
        }

        maxPossibleCount += confirmedCount;

        if (maxPossibleCount < minCount) {
            return new TimeBucketQueryResult(false, confirmedCount, null, "PRUNED");
        }

        if (confirmedCount >= minCount) {
            return new TimeBucketQueryResult(true, confirmedCount, null, "CONFIRMED");
        }

        return new TimeBucketQueryResult(null, confirmedCount, ambiguousBuckets, "AMBIGUOUS");
    }

    public int refineAmbiguousBuckets(long sourceId, String band,
                                      List<TimeBucket> ambiguousBuckets,
                                      double startTime, double endTime,
                                      double magThreshold) throws RocksDBException {
        int count = 0;

        for (TimeBucket bucket : ambiguousBuckets) {
            double queryStart = Math.max(startTime, bucket.startTime);
            double queryEnd = Math.min(endTime, bucket.endTime);

            List<LightCurvePoint> points = queryByTimeRangeAndBand(
                    sourceId, band, queryStart, queryEnd);

            for (LightCurvePoint point : points) {
                if (point.mag <= magThreshold) count++;
            }
        }

        return count;
    }

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

    // ========== 系统管理接口 ==========

    public Map<String, Object> getHealpixStatistics() throws RocksDBException {
        Map<String, Object> stats = new LinkedHashMap<>();

        stats.put("healpix_id", healpixId);
        stats.put("db_path", dbPath);
        stats.put("total_data_points", writeCount.get());
        stats.put("total_reads", readCount.get());
        stats.put("uptime_minutes", (System.currentTimeMillis() - startTime) / 60000);

        try (final RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            long count = 0;
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
            stats.put("estimated_data_points", count);
        }

        stats.put("approximate_size_mb", getApproximateSize() / (1024 * 1024));

        // 保留兼容字段（不再使用自定义 flush）
        stats.put("fragmented_flush_total", totalFragmentedFlushCount.get());
        stats.put("lower_level_aligned_flush_total", totalLowerLevelAlignedFlushCount.get());
        stats.put("last_flush_time_ms", 0L);
        stats.put("last_flush_entries", 0);
        stats.put("last_flush_generated_sst", 0);
        stats.put("last_flush_used_lower_level_boundaries", false);
        stats.put("last_flush_lower_level_boundary_count", 0);
        stats.put("last_flush_lower_level_boundary_hits", 0);
        stats.put("last_flush_static_boundary_hits", 0);
        stats.put("last_flush_dynamic_boundary_hits", 0);

        return stats;
    }

    @Override
    public void close() {
        try {
            forceFlush();
        } catch (Exception e) {
            System.err.println("关闭前最终落盘失败: " + e.getMessage());
        }

        if (writeOptions != null) writeOptions.close();
        if (defaultHandle != null) defaultHandle.close();
        if (lightcurveHandle != null) lightcurveHandle.close();
        if (metadataHandle != null) metadataHandle.close();
        if (timeBucketsHandle != null) timeBucketsHandle.close();
        if (db != null) db.close();
        if (statistics != null) statistics.close();
    }

    // ========== 序列化/反序列化 ==========

    /**
     * 二进制 key：sourceId(8字节) + bandLen(1字节) + band + time(8字节)
     * 比 String.format("%d|%s|%.6f") 快 10 倍以上
     */
    private byte[] buildBinaryKey(long sourceId, String band, double time) {
        byte[] bandBytes = band.getBytes(StandardCharsets.UTF_8);
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(17 + bandBytes.length);
        buf.putLong(sourceId);
        buf.put((byte) bandBytes.length);
        buf.put(bandBytes);
        buf.putDouble(time);
        return buf.array();
    }
    private byte[] buildSourceIdPrefix(long sourceId) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(8);
        buf.putLong(sourceId);
        return buf.array();
    }

    private byte[] buildSourceIdBandPrefix(long sourceId, String band) {
        byte[] bandBytes = band.getBytes(StandardCharsets.UTF_8);
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(9 + bandBytes.length);
        buf.putLong(sourceId);
        buf.put((byte) bandBytes.length);
        buf.put(bandBytes);
        return buf.array();
    }

    private byte[] buildTimeBucketPrefix(long sourceId, String band) {
        return String.format("%d|%s|", sourceId, band).getBytes();
    }

    private byte[] buildTimeBucketKey(long sourceId, String band, int bucketIndex) {
        return String.format("%d|%s|%d", sourceId, band, bucketIndex).getBytes();
    }

    private byte[] serializeLightCurvePoint(LightCurvePoint point) {
        byte[] bandBytes = point.band.getBytes(StandardCharsets.UTF_8);
        int exactSize = 90 + bandBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(exactSize);
        buffer.putLong(point.sourceId).putDouble(point.ra).putDouble(point.dec).putLong(point.transitId);
        buffer.putInt(bandBytes.length).put(bandBytes);
        buffer.putDouble(point.time).putDouble(point.mag).putDouble(point.flux)
                .putDouble(point.fluxError).putDouble(point.fluxOverError);
        buffer.put((byte)(point.rejectedByPhotometry ? 1 : 0))
                .put((byte)(point.rejectedByVariability ? 1 : 0));
        buffer.putInt(point.otherFlags).putLong(point.solutionId);
        return buffer.array();
    }

    private LightCurvePoint deserializeLightCurvePoint(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long sourceId = buffer.getLong();
        double ra = buffer.getDouble();
        double dec = buffer.getDouble();
        long transitId = buffer.getLong();
        byte[] bandBytes = new byte[buffer.getInt()];
        buffer.get(bandBytes);
        String band = new String(bandBytes, StandardCharsets.UTF_8);
        return new LightCurvePoint(
                sourceId, ra, dec, transitId, band,
                buffer.getDouble(), buffer.getDouble(), buffer.getDouble(),
                buffer.getDouble(), buffer.getDouble(),
                buffer.get() == 1, buffer.get() == 1,
                buffer.getInt(), buffer.getLong()
        );
    }

    private byte[] buildStarMetadataKey(long sourceId) {
        return ("star_" + sourceId).getBytes();
    }

    private byte[] serializeStarMetadata(StarMetadata meta) {
        ByteBuffer buffer = ByteBuffer.allocate(52);
        buffer.putLong(meta.sourceId).putDouble(meta.ra).putDouble(meta.dec);
        buffer.putInt(meta.observationCount).putDouble(meta.avgMag);
        buffer.putDouble(meta.firstObsTime).putDouble(meta.lastObsTime);
        return buffer.array();
    }

    private StarMetadata deserializeStarMetadata(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return new StarMetadata(
                buffer.getLong(), buffer.getDouble(), buffer.getDouble(),
                buffer.getInt(), buffer.getDouble(), buffer.getDouble(), buffer.getDouble()
        );
    }

    private byte[] serializeTimeBucket(TimeBucket bucket) {
        byte[] bandBytes = bucket.band.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(52 + bandBytes.length);
        buffer.putLong(bucket.sourceId).putInt(bandBytes.length).put(bandBytes);
        buffer.putInt(bucket.bucketIndex).putDouble(bucket.startTime).putDouble(bucket.endTime);
        buffer.putDouble(bucket.minMag).putDouble(bucket.maxMag).putInt(bucket.totalCount);
        return buffer.array();
    }

    private TimeBucket deserializeTimeBucket(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long sourceId = buffer.getLong();
        byte[] bandBytes = new byte[buffer.getInt()];
        buffer.get(bandBytes);
        String band = new String(bandBytes, StandardCharsets.UTF_8);
        return new TimeBucket(
                sourceId, band, buffer.getInt(), buffer.getDouble(), buffer.getDouble(),
                buffer.getDouble(), buffer.getDouble(), buffer.getInt()
        );
    }

    // ========== 工具方法 ==========

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

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            db.put(metadataHandle, entry.getKey().getBytes(), entry.getValue().getBytes());
        }
    }

    // ========== 写放大统计 ==========

    public Map<String, Double> getWriteAmplificationStats() {
        Map<String, Double> stats = new HashMap<>();
        try {
            long logicalBytes = logicalBytesWritten.get();
            long currentDirSize = StorageUtils.getDirectorySize(dbPath);
            long physicalBytes = currentDirSize - initialDirSize;
            double wa = logicalBytes > 0 ? (double) physicalBytes / logicalBytes : 1.0;

            stats.put("logical_mb", logicalBytes / 1048576.0);
            stats.put("physical_mb", physicalBytes / 1048576.0);
            stats.put("write_amplification", wa);
            return stats;
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    /**
     * 强制刷盘
     *
     * 完整持久化路径下，数据已经通过 WAL 保证安全。
     * forceFlush 的作用是：
     * 1. 将内存聚合的元数据落盘
     * 2. 触发 RocksDB 原生 flush（memtable → SST）
     * 3. 执行 compaction 使 SA 统计准确
     */
    public void forceFlush() throws RocksDBException {
        // 1. 元数据内存缓存落盘
        if (!metadataCache.isEmpty()) {
            try (WriteBatch batch = new WriteBatch()) {
                for (StarMetadata meta : metadataCache.values()) {
                    batch.put(metadataHandle, buildStarMetadataKey(meta.sourceId),
                            serializeStarMetadata(meta));
                }
                db.write(writeOptions, batch);
            }
            metadataCache.clear();
        }

        // 2. RocksDB 原生 flush（memtable → SST）
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions, defaultHandle);
            db.flush(flushOptions, lightcurveHandle);
            db.flush(flushOptions, metadataHandle);
        }

        // 3. Compaction（使 SA 统计准确）
        db.compactRange(defaultHandle);
        db.compactRange(lightcurveHandle);
        db.compactRange(metadataHandle);
    }
    /**
     * 只 flush，不 compact（用于 Phase 2 计时内）
     */
    public void forceFlushNoCompaction() throws RocksDBException {
        // 元数据缓存落盘
        if (!metadataCache.isEmpty()) {
            try (WriteBatch batch = new WriteBatch()) {
                for (StarMetadata meta : metadataCache.values()) {
                    batch.put(metadataHandle, buildStarMetadataKey(meta.sourceId),
                            serializeStarMetadata(meta));
                }
                db.write(writeOptions, batch);
            }
            metadataCache.clear();
        }

        // RocksDB flush: memtable → SST（真正的 I/O）
        try (FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true)) {
            db.flush(flushOptions, defaultHandle);
            db.flush(flushOptions, lightcurveHandle);
            db.flush(flushOptions, metadataHandle);
        }
    }

    /**
     * 只 compact（用于 SA 统计，不在计时范围内）
     */
    public void compactOnly() throws RocksDBException {
        db.compactRange(defaultHandle);
        db.compactRange(lightcurveHandle);
        db.compactRange(metadataHandle);
    }

    // ========== 类定义区 ==========

    public static class LightCurvePoint {
        public final long sourceId;
        public final long transitId;
        public final String band;
        public final double time;
        public final double mag;
        public final double flux;
        public final double fluxError;
        public final double fluxOverError;
        public final boolean rejectedByPhotometry;
        public final boolean rejectedByVariability;
        public final int otherFlags;
        public final long solutionId;
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

    public static class TimeBucket {
        public final long sourceId;
        public final String band;
        public final double startTime;
        public final double endTime;
        public final double minMag;
        public final double maxMag;
        public final int totalCount;
        public final int bucketIndex;
        // === 论文要求的增强统计量 ===
        public final double meanMag;       // 桶内星等均值
        public final double varMag;        // 桶内星等方差

        /** 向后兼容构造函数（不带增强统计量） */
        public TimeBucket(long sourceId, String band, int bucketIndex,
                          double startTime, double endTime,
                          double minMag, double maxMag, int totalCount) {
            this(sourceId, band, bucketIndex, startTime, endTime,
                    minMag, maxMag, totalCount,
                    (minMag + maxMag) / 2.0, 0.0);
        }

        /** 完整构造函数（含增强统计量） */
        public TimeBucket(long sourceId, String band, int bucketIndex,
                          double startTime, double endTime,
                          double minMag, double maxMag, int totalCount,
                          double meanMag, double varMag) {
            this.sourceId = sourceId;
            this.band = band;
            this.bucketIndex = bucketIndex;
            this.startTime = startTime;
            this.endTime = endTime;
            this.minMag = minMag;
            this.maxMag = maxMag;
            this.totalCount = totalCount;
            this.meanMag = meanMag;
            this.varMag = varMag;
        }
    }

    public static class TimeBucketQueryResult {
        public final Boolean satisfied;
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

    public static class Config {
        public String dbPath;
        public long healpixId;
        public long timeBucketSize = 1;
        public boolean asyncIndexing = true;
        public int maxBackgroundCompactions = 4;
        public long blockCacheSize = 2 * SizeUnit.GB;

        // === DP 自适应分桶参数（对齐论文 Algorithm 1） ===
        public double lambda = 0.6;                // 权衡系数 λ ∈ [0,1]
        public double storeCost = 1.0;             // C_store：每个桶的固定存储代价
        public int maxBucketGap = 200;             // K：回看上界（向后兼容）
        public double deltaTMax = 365.0;           // ΔT_max：桶最大时间跨度（天）
        public int nMax = 200;                     // N_max：桶内最大点数

        // 以下参数不再用于自定义 flush，保留用于配置兼容
        public long flushSourceBoundarySpan = 50_000L;
        public int maxFlushFragments = 64;
        public boolean enableDynamicFlushBoundaries = true;
        public int dynamicBoundaryEntriesPerFragment = 200_000;
        public boolean enableLowerLevelSstBoundaryAlignment = true;
        public boolean enableStaticBoundaryFallback = true;
        public boolean enableOnlineTimeBucketUpdate = false;
        public int onlineTimeBucketMinBatchPoints = 5_000;
        public long flushAwaitTimeoutMs = 120_000L;

        public Config(String dbPath, long healpixId) {
            this.dbPath = dbPath;
            this.healpixId = healpixId;
        }
    }
}