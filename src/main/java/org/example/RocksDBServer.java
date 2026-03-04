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
 * 天文光变曲线RocksDB服务 - 针对HEALPix分区的分布式存储系统设计
 * 每个实例负责一个HEALPix天区的数据管理
 */
public class RocksDBServer implements AutoCloseable {
    private final RocksDB db;
    private final String dbPath;
    private final long healpixId; // 当前实例负责的HEALPix天区ID
    private final Config config;
    private final Map<Long, StarMetadata> metadataCache = new ConcurrentHashMap<>();

    // 列族句柄
    private final ColumnFamilyHandle defaultHandle;
    private final ColumnFamilyHandle lightcurveHandle;
    private final ColumnFamilyHandle metadataHandle;
    private final ColumnFamilyHandle timeBucketsHandle;

    // 统计信息
    private final AtomicLong writeCount = new AtomicLong(0);
    private final AtomicLong readCount = new AtomicLong(0);
    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private final Statistics statistics;
    private final long startTime;
    // 记录时间起点：2455197.5，儒略日 2455197.5，也就是UTC时间2010年1月1日 00:00:00。
    private final long initialDirSize;

    // ==========================================
    // 定制化应用层 MemTable 及控制阈值
    // ==========================================
    // 使用无符号字节比较器，严格对齐 RocksDB 原生的 BytewiseComparator
    private static final Comparator<byte[]> UNSIGNED_BYTE_COMPARATOR = (a, b) -> {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            if (a[i] != b[i]) return (a[i] & 0xFF) - (b[i] & 0xFF);
        }
        return a.length - b.length;
    };

    // 🚀 使用 volatile 保证线程可见性，这是我们当前正在接收数据的“活跃盆”
    private volatile ConcurrentSkipListMap<byte[], byte[]> activeMemTable = new ConcurrentSkipListMap<>(UNSIGNED_BYTE_COMPARATOR);
    private final AtomicLong activeMemTableSize = new AtomicLong(0);

    // 用于追踪所有后台正在执行的 Flush 任务（以便 forceFlush 时等待它们落盘）
    private final List<CompletableFuture<Void>> pendingFlushTasks = Collections.synchronizedList(new ArrayList<>());

    private final long MEMTABLE_FLUSH_THRESHOLD = 64 * 1024 * 1024; // 64MB 触发刷写
    private final long MIN_SST_FILE_SIZE = 2 * 1024 * 1024;         // 2MB 最小文件约束

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
                new ColumnFamilyDescriptor("metadata".getBytes()),
                new ColumnFamilyDescriptor("time_buckets".getBytes())
        );

        RocksDBGlobalResourceManager globalRes = RocksDBGlobalResourceManager.getInstance();

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                .setBlockCache(globalRes.getBlockCache()); // 使用全局缓存

        // 数据库选项
        try (final DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setEnv(globalRes.getEnv()) // 使用全局调度线程池
                .setWriteBufferManager(globalRes.getWriteBufferManager()) // 使用全局写限制
                .setStatistics(statistics)) {

            // 列族选项
            try (final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setTableFormatConfig(tableConfig)) {

                List<ColumnFamilyHandle> handles = new ArrayList<>();
                this.db = RocksDB.open(dbOptions, config.dbPath, cfDescriptors, handles);

                // 存储列族句柄
                this.defaultHandle = handles.get(0);
                this.lightcurveHandle = handles.get(1);
                this.metadataHandle = handles.get(2);
                this.timeBucketsHandle = handles.get(3);
            }
        }

        // 存储配置元数据
        storeMetadata(config);

        // 记录当前 HEALPix 天区目录的初始大小
        this.initialDirSize = StorageUtils.getDirectorySize(config.dbPath);
    }


    // ========== 数据写入接口 ==========
    /**
     * 极速写入路径：只写内存，绝不阻塞！
     */
    public void putLightCurveBatch(List<LightCurvePoint> points) throws RocksDBException {
        long batchLogicalSize = points.size() * 90L;

        for (LightCurvePoint point : points) {
            String keyStr = point.sourceId + "|" + point.band + "|" + point.time;
            byte[] key = keyStr.getBytes(StandardCharsets.UTF_8);
            byte[] value = serializeLightCurvePoint(point);
            // 写入当前活跃的内存表
            activeMemTable.put(key, value);
        }

        logicalBytesWritten.addAndGet(batchLogicalSize);
        writeCount.addAndGet(points.size());
        updateStarsMetadataBatch(points);

        if (activeMemTableSize.addAndGet(batchLogicalSize) >= MEMTABLE_FLUSH_THRESHOLD) {
            triggerAsyncFlush();
        }
    }
    /**
     * 【核心动作】：双缓冲原子切换。主线程毫秒级脱身！
     */
    private synchronized void triggerAsyncFlush() {
        // 双重检查，防止并发线程重复触发
        if (activeMemTableSize.get() < MEMTABLE_FLUSH_THRESHOLD || activeMemTable.isEmpty()) {
            return;
        }

        // 1. 瞬间换盆：把写满的表摘下来，换上一个全新的空表给主线程继续写
        final ConcurrentSkipListMap<byte[], byte[]> memTableToFlush = activeMemTable;
        activeMemTable = new ConcurrentSkipListMap<>(UNSIGNED_BYTE_COMPARATOR);
        activeMemTableSize.set(0);

        // 清理已完成的旧任务引用，防止内存泄漏
        pendingFlushTasks.removeIf(CompletableFuture::isDone);

        // 2. 将摘下来的满盆扔给全局后台线程池去慢慢写 SST
        CompletableFuture<Void> flushTask = CompletableFuture.runAsync(() -> {
            try {
                doFragmentedFlush(memTableToFlush);
            } catch (RocksDBException e) {
                System.err.println("异步碎片化 Flush 发生错误: " + e.getMessage());
            }
        }, RocksDBGlobalResourceManager.getInstance().getCustomAsyncFlushPool());

        // 记录任务
        pendingFlushTasks.add(flushTask);
    }
    /**
     * 后台线程专属方法：执行真实的边界对齐 SST 切分与导入
     */
    private void doFragmentedFlush(ConcurrentSkipListMap<byte[], byte[]> memTable) throws RocksDBException {
        if (memTable == null || memTable.isEmpty()) return;

        List<String> generatedSstFiles = new ArrayList<>();
        SstFileWriter sstFileWriter = null;
        String currentSstPath = null;
        long currentSstSize = 0;
        long lastSourceId = -1;

        try (EnvOptions envOptions = new EnvOptions();
             Options options = new Options().setCompressionType(CompressionType.LZ4_COMPRESSION)) {

            for (Map.Entry<byte[], byte[]> entry : memTable.entrySet()) {
                byte[] key = entry.getKey();
                byte[] value = entry.getValue();
                long currentSourceId = extractSourceId(key);

                if (sstFileWriter != null && currentSstSize >= MIN_SST_FILE_SIZE && currentSourceId != lastSourceId) {
                    sstFileWriter.finish();
                    sstFileWriter.close();
                    generatedSstFiles.add(currentSstPath);
                    sstFileWriter = null;
                }

                if (sstFileWriter == null) {
                    currentSstPath = dbPath + "/fragment_" + System.nanoTime() + "_" + generatedSstFiles.size() + ".sst";
                    sstFileWriter = new SstFileWriter(envOptions, options);
                    sstFileWriter.open(currentSstPath);
                    currentSstSize = 0;
                    lastSourceId = currentSourceId;
                }

                sstFileWriter.put(key, value);
                currentSstSize += (key.length + value.length);
                lastSourceId = currentSourceId;
            }

            if (sstFileWriter != null) {
                sstFileWriter.finish();
                sstFileWriter.close();
                generatedSstFiles.add(currentSstPath);
            }

            if (!generatedSstFiles.isEmpty()) {
                try (IngestExternalFileOptions ingestOpts = new IngestExternalFileOptions()) {
                    ingestOpts.setMoveFiles(true);
                    // 使用 Java JNI 正确的方法名调用
                    db.ingestExternalFile(lightcurveHandle, generatedSstFiles, ingestOpts);
                }
            }
        }
    }
    /**
     * 高效从前缀 Key 中提取 sourceId (避免 new String 产生 GC 开销)
     */
    private long extractSourceId(byte[] key) {
        long id = 0;
        for (byte b : key) {
            if (b == '|') break;
            id = id * 10 + (b - '0');
        }
        return id;
    }

    /**
     * 批量更新天体元数据
     */
    private void updateStarsMetadataBatch(List<LightCurvePoint> points) {
        for (LightCurvePoint point : points) {
            metadataCache.compute(point.sourceId, (id, existing) -> {
                // 如果内存没有，尝试从底层捞一次（一般只发生在重启续传时）
                if (existing == null) {
                    try {
                        byte[] dbVal = db.get(metadataHandle, buildStarMetadataKey(id));
                        if (dbVal != null) existing = deserializeStarMetadata(dbVal);
                    } catch (RocksDBException e) {}
                }
                // 在内存中完成聚合，彻底切断对 RocksDB 的频繁写入
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

            // 关键优化：禁用 WAL，因为时间桶是派生索引，即使宕机也可以从原始数据重建，没必要双写！
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
     * 高性能离线构建时间桶索引
     * 在所有数据写入完成后，全表顺序扫描一次，利用内存极速完成所有天体的 DP 划分
     */
    public void buildAllTimeBucketsOffline() throws RocksDBException {
        System.out.println("开始为 HEALPix " + healpixId + " 全量顺序扫描构建时间桶...");
        long start = System.currentTimeMillis();
        int processedGroups = 0;

        String currentPrefix = "";
        List<LightCurvePoint> currentPoints = new ArrayList<>();
        long currentSourceId = -1;
        String currentBand = "";

        // 利用 RocksDB 的顺序迭代器，速度极快
        try (RocksIterator iterator = db.newIterator(lightcurveHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                byte[] key = iterator.key();
                String keyStr = new String(key); // 格式: sourceId|band|time
                String[] parts = keyStr.split("\\|");
                if (parts.length < 3) continue;

                long sourceId = Long.parseLong(parts[0]);
                String band = parts[1];
                String prefix = sourceId + "|" + band;

                LightCurvePoint point = deserializeLightCurvePoint(iterator.value());

                // 如果遇到了一个新的 (天体+波段) 组合，说明上一组已经收集齐了
                if (!prefix.equals(currentPrefix)) {
                    if (!currentPoints.isEmpty()) {
                        // 1. 在内存中排序
                        currentPoints.sort(Comparator.comparingDouble(p -> p.time));
                        // 2. 内存中极速跑完 DP 算法
                        List<TimeBucket> buckets = partitionTimeBuckets(currentSourceId, currentBand, currentPoints, config);
                        // 3. 批量落盘
                        storeTimeBuckets(currentSourceId, currentBand, buckets);
                        processedGroups++;
                    }
                    // 重置收集器
                    currentPrefix = prefix;
                    currentSourceId = sourceId;
                    currentBand = band;
                    currentPoints.clear();
                }
                currentPoints.add(point);
            }

            // 处理最后一组数据
            if (!currentPoints.isEmpty()) {
                currentPoints.sort(Comparator.comparingDouble(p -> p.time));
                List<TimeBucket> buckets = partitionTimeBuckets(currentSourceId, currentBand, currentPoints, config);
                storeTimeBuckets(currentSourceId, currentBand, buckets);
                processedGroups++;
            }
        }

        // 终极清理：刷盘并强制压缩时间桶列族，挤掉所有水分
        db.flush(new FlushOptions().setWaitForFlush(true), timeBucketsHandle);
        db.compactRange(timeBucketsHandle);

        System.out.println("HEALPix " + healpixId + " 时间桶构建完成！共处理 " + processedGroups +
                " 个波段组，耗时: " + (System.currentTimeMillis() - start) + "ms");
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

    @Override
    public void close() {
        if (defaultHandle != null) defaultHandle.close();
        if (lightcurveHandle != null) lightcurveHandle.close();
        if (metadataHandle != null) metadataHandle.close();
        if (timeBucketsHandle != null) timeBucketsHandle.close();
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
    private byte[] buildTimeBucketPrefix(long sourceId, String band) {
        return String.format("%d|%s|", sourceId, band).getBytes();
    }
    private byte[] buildTimeBucketKey(long sourceId, String band, int bucketIndex) {
        return String.format("%d|%s|%d", sourceId, band, bucketIndex).getBytes();
    }
    private byte[] serializeLightCurvePoint(LightCurvePoint point) {
        byte[] bandBytes = point.band.getBytes(StandardCharsets.UTF_8);
        // 精确计算所需字节数：10个long/double(80) + 2个byte(2) + 1个int(4) + 1个长度int(4) = 90
        int exactSize = 90 + bandBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(exactSize);
        buffer.putLong(point.sourceId).putDouble(point.ra).putDouble(point.dec).putLong(point.transitId);
        buffer.putInt(bandBytes.length).put(bandBytes);
        buffer.putDouble(point.time).putDouble(point.mag).putDouble(point.flux).putDouble(point.fluxError).putDouble(point.fluxOverError);
        buffer.put((byte)(point.rejectedByPhotometry ? 1 : 0)).put((byte)(point.rejectedByVariability ? 1 : 0));
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
                buffer.getDouble(), buffer.getDouble(), buffer.getDouble(), buffer.getDouble(), buffer.getDouble(),
                buffer.get() == 1, buffer.get() == 1, buffer.getInt(), buffer.getLong()
        );
    }
    private byte[] buildStarMetadataKey(long sourceId) {
        return ("star_" + sourceId).getBytes();
    }
    private byte[] serializeStarMetadata(StarMetadata meta) {
        ByteBuffer buffer = ByteBuffer.allocate(52); // 8*6 + 4 = 52 bytes
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
        // 精确计算容量: 8(id)+4(长度)+bandBytes+4(index)+8(start)+8(end)+8(min)+8(max)+4(count) = 52 + band长度
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

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            db.put(metadataHandle, entry.getKey().getBytes(), entry.getValue().getBytes());
        }
    }


    // ========== 实验专供：底层物理性能与写放大统计 ==========

    /**
     * 获取存储引擎的写放大(Write Amplification)指标
     * 公式: (Flush写入字节 + Compaction写入字节) / 应用层逻辑写入字节
     */
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
            System.err.println("无法获取写放大统计: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * 实验结束时的强制收尾操作
     */
    public void forceFlush() throws RocksDBException {
        // 1. 先把当前活跃表（哪怕没装满）强行切分导入
        synchronized (this) {
            if (!activeMemTable.isEmpty()) {
                final ConcurrentSkipListMap<byte[], byte[]> memTableToFlush = activeMemTable;
                activeMemTable = new ConcurrentSkipListMap<>(UNSIGNED_BYTE_COMPARATOR);
                activeMemTableSize.set(0);
                doFragmentedFlush(memTableToFlush); // 主线程直接执行最后一波
            }
        }

        // 2. 阻塞等待所有后台 Flush 任务完成，确保所有数据已经落盘
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(
                pendingFlushTasks.toArray(new CompletableFuture[0]));
        try {
            allTasks.join();
        } catch (Exception e) {}

        // 3. 内存聚合的元数据落盘
        if (!metadataCache.isEmpty()) {
            try (WriteBatch batch = new WriteBatch()) {
                for (StarMetadata meta : metadataCache.values()) {
                    batch.put(metadataHandle, buildStarMetadataKey(meta.sourceId), serializeStarMetadata(meta));
                }
                db.write(new WriteOptions().setDisableWAL(true), batch);
            }
            metadataCache.clear();
        }

        // 4. 原生刷盘与合并（清除由于元数据写入产生的碎片）
        FlushOptions flushOptions = new FlushOptions().setWaitForFlush(true);
        db.flush(flushOptions, defaultHandle);
        db.flush(flushOptions, metadataHandle);

        db.compactRange(defaultHandle);
        db.compactRange(lightcurveHandle); // 对 Ingest 进来的文件做一次物理合并对齐
        db.compactRange(metadataHandle);
    }

    // ========== 类定义区 ==========

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
        public boolean asyncIndexing = true;
        public int maxBackgroundCompactions = 4;
        public long blockCacheSize = 2 * SizeUnit.GB;

        // 时间桶参数
        public double lambda = 0.6; // 代价函数中的权重参数
        public double storeCost = 10.0; // 存储代价
        public int maxBucketGap = 200; // 最大桶间隔（K参数）

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

}