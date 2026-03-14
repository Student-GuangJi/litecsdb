package org.example.exp;

import org.example.RocksDBServer.Config;
import org.example.RocksDBServer.LightCurvePoint;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 碎片化 Flush 管理器
 *
 * 职责：拦截 lightcurve 列族的写入请求，在应用层实现
 *       "碎片化 + 边界对齐" 的 flush 行为。
 *
 * 工作模式（由 Config 参数决定）：
 *   DISABLED (maxFlushFragments == 1)
 *       → 不拦截，直接走 RocksDB 原生 WriteBatch 路径
 *   FRAG (maxFlushFragments > 1, enableLowerLevelSstBoundaryAlignment == false)
 *       → 缓冲数据，按 sourceId 等间距静态边界切分为多个 SST 文件
 *   FRAG_BA (maxFlushFragments > 1, enableLowerLevelSstBoundaryAlignment == true)
 *       → 缓冲数据，从 L1 层 SST 文件提取对齐边界切分
 *
 * 对外接口只有三个：
 *   {@link #write(List, ColumnFamilyHandle)}  — 替代 db.write(WriteBatch)
 *   {@link #forceFlush()}                     — 强制刷写剩余缓冲
 *   {@link #close()}                          — 释放资源
 *
 * 线程安全：通过 synchronized(buffer) 保护缓冲区操作。
 */
public class FragmentedFlushManager implements AutoCloseable {

    // ==================== 常量 ====================

    /** 缓冲区刷写阈值：64 MB（与 MemTable 大小同量级） */
    private static final long BUFFER_FLUSH_THRESHOLD = 64L * 1024 * 1024;

    /** 单个 SST 分片的最小字节数，低于此值与相邻分片合并 */
    private static final long MIN_FRAGMENT_BYTES = 4L * 1024 * 1024;

    /** SST 临时文件计数器 */
    private static final AtomicLong sstFileCounter = new AtomicLong(0);

    // ==================== 实例字段 ====================

    private final RocksDB db;
    private final ColumnFamilyHandle lightcurveCfHandle;
    private final Config config;
    private final String dbPath;
    private final WriteOptions writeOptions;
    private final boolean enabled;

    /**
     * 排序缓冲区：TreeMap 以 byte[] key 为主键（字节序比较），
     * value 为序列化后的 LightCurvePoint 字节数组。
     */
    private final TreeMap<byte[], byte[]> buffer;

    /** 缓冲区当前字节数 */
    private long bufferBytes;

    /** 碎片化 flush 产生的 SST 文件临时目录 */
    private final String fragmentDir;

    /** 碎片化 flush 统计 */
    private final AtomicLong totalFragmentedFlushes = new AtomicLong(0);
    private final AtomicLong totalSstFilesProduced = new AtomicLong(0);
    private final AtomicLong totalBytesIngested = new AtomicLong(0);

    // ==================== 构造 ====================

    /**
     * @param db               RocksDB 实例
     * @param lightcurveCfHandle lightcurve 列族句柄
     * @param config           包含 flush 策略参数的 Config
     * @param writeOptions     写入选项（用于 DISABLED 模式下的透传）
     */
    public FragmentedFlushManager(RocksDB db,
                                   ColumnFamilyHandle lightcurveCfHandle,
                                   Config config,
                                   WriteOptions writeOptions) {
        this.db = db;
        this.lightcurveCfHandle = lightcurveCfHandle;
        this.config = config;
        this.dbPath = config.dbPath;
        this.writeOptions = writeOptions;
        this.enabled = config.maxFlushFragments > 1;

        this.buffer = new TreeMap<>(FragmentedFlushManager::compareBytes);
        this.bufferBytes = 0;

        this.fragmentDir = dbPath + "/sst_fragments";
        if (enabled) {
            new File(fragmentDir).mkdirs();
        }
    }

    // ==================== 核心公开接口 ====================

    /**
     * 写入一批光变曲线数据点。
     *
     * 如果碎片化 flush 未启用（DEFAULT 模式），直接以 WriteBatch 写入 RocksDB。
     * 如果已启用（FRAG / FRAG_BA 模式），将数据加入排序缓冲区，
     * 当缓冲区超过阈值时自动触发碎片化 flush。
     *
     * @param points            待写入的数据点
     * @param lightcurveCfHandle lightcurve 列族句柄
     */
    public void write(List<LightCurvePoint> points,
                      ColumnFamilyHandle lightcurveCfHandle) throws RocksDBException {
        if (!enabled) {
            // ===== DEFAULT 模式：直接写 RocksDB =====
            writeDirectly(points, lightcurveCfHandle);
            return;
        }

        // ===== FRAG / FRAG_BA 模式：写入缓冲区 =====
        synchronized (buffer) {
            for (LightCurvePoint point : points) {
                byte[] key = buildBinaryKey(point.sourceId, point.band, point.time);
                byte[] value = serializePoint(point);
                buffer.put(key, value);
                bufferBytes += key.length + value.length;
            }

            // 检查是否需要刷写
            if (bufferBytes >= BUFFER_FLUSH_THRESHOLD) {
                flushBufferInternal();
            }
        }
    }

    /**
     * 强制刷写缓冲区中的所有剩余数据。
     * 在 Phase 2 结束后调用，确保所有数据持久化。
     */
    public void forceFlush() throws RocksDBException {
        if (!enabled) return;
        synchronized (buffer) {
            if (!buffer.isEmpty()) {
                flushBufferInternal();
            }
        }
    }

    /**
     * 获取碎片化 flush 统计信息
     */
    public Map<String, Long> getStats() {
        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("total_fragmented_flushes", totalFragmentedFlushes.get());
        stats.put("total_sst_files_produced", totalSstFilesProduced.get());
        stats.put("total_bytes_ingested", totalBytesIngested.get());
        return stats;
    }

    @Override
    public void close() {
        try {
            forceFlush();
        } catch (RocksDBException e) {
            System.err.println("FragmentedFlushManager.close() flush failed: " + e.getMessage());
        }
        // 清理临时目录中的残留文件
        File dir = new File(fragmentDir);
        if (dir.exists()) {
            File[] leftovers = dir.listFiles();
            if (leftovers != null) {
                for (File f : leftovers) f.delete();
            }
            dir.delete();
        }
    }

    // ==================== 内部实现 ====================

    /**
     * DEFAULT 模式的直接写入路径
     */
    private void writeDirectly(List<LightCurvePoint> points,
                                ColumnFamilyHandle cfHandle) throws RocksDBException {
        try (WriteBatch batch = new WriteBatch()) {
            for (LightCurvePoint point : points) {
                byte[] key = buildBinaryKey(point.sourceId, point.band, point.time);
                byte[] value = serializePoint(point);
                batch.put(cfHandle, key, value);
            }
            db.write(writeOptions, batch);
        }
    }

    /**
     * 碎片化 flush 核心流程（在 synchronized(buffer) 内调用）
     *
     * 步骤：
     *   1. 计算分片边界
     *   2. 按边界将缓冲区切分为多个有序分片
     *   3. 每个分片写入一个 SST 文件
     *   4. 通过 IngestExternalFile 导入所有 SST
     *   5. 清空缓冲区
     */
    private void flushBufferInternal() throws RocksDBException {
        if (buffer.isEmpty()) return;

        totalFragmentedFlushes.incrementAndGet();

        // Step 1: 计算分片边界
        List<byte[]> boundaries = computeBoundaries();

        // Step 2 + 3: 按边界切分并写入 SST 文件
        List<String> sstFiles = writeFragmentedSstFiles(boundaries);

        if (sstFiles.isEmpty()) {
            // 如果所有数据不足以形成有效 SST，回退为直接写入
            fallbackDirectWrite();
            return;
        }

        // Step 4: Ingest 所有 SST 文件
        try (IngestExternalFileOptions ingestOpts = new IngestExternalFileOptions()) {
            ingestOpts.setMoveFiles(true);              // 移动而非复制
            ingestOpts.setSnapshotConsistency(true);
            ingestOpts.setAllowBlockingFlush(false);     // 不触发额外 flush

            db.ingestExternalFile(lightcurveCfHandle, sstFiles, ingestOpts);

            totalBytesIngested.addAndGet(
                    sstFiles.stream()
                            .mapToLong(f -> new File(f).length())
                            .sum());
        } catch (RocksDBException e) {
            System.err.println("IngestExternalFile failed, falling back to direct write: "
                    + e.getMessage());
            // 回退：清理 SST 文件，直接写入
            for (String f : sstFiles) new File(f).delete();
            fallbackDirectWrite();
            return;
        }

        // Step 5: 清空缓冲区
        buffer.clear();
        bufferBytes = 0;
    }

    /**
     * 计算分片边界。
     *
     * FRAG 模式：按 sourceId 空间等间距划分静态边界
     * FRAG_BA 模式：从 L1 层 SST 文件提取键区间端点作为对齐边界，
     *              如果 L1 层为空则回退到静态边界
     */
    private List<byte[]> computeBoundaries() {
        if (config.enableLowerLevelSstBoundaryAlignment) {
            // ===== FRAG_BA: 从 L1 提取对齐边界 =====
            List<byte[]> l1Boundaries = extractL1Boundaries();
            if (!l1Boundaries.isEmpty()) {
                return l1Boundaries;
            }
            // L1 为空（首次 flush 等场景），回退到静态边界
            if (config.enableStaticBoundaryFallback) {
                return computeStaticBoundaries();
            }
            return Collections.emptyList();
        } else {
            // ===== FRAG: 静态等间距边界 =====
            return computeStaticBoundaries();
        }
    }

    /**
     * 从 L1 层 SST 文件元数据提取对齐边界
     *
     * 流程：
     *   1. 获取 lightcurve 列族的 ColumnFamilyMetaData
     *   2. 读取 Level 1 的所有 SST 文件元数据
     *   3. 收集每个文件的 smallest_key 和 largest_key
     *   4. 合并过近的边界（间距过小时合并，避免产生过碎的分片）
     *   5. 返回排序后的边界列表
     */
    private List<byte[]> extractL1Boundaries() {
        try {
            ColumnFamilyMetaData cfMeta = db.getColumnFamilyMetaData(lightcurveCfHandle);
            if (cfMeta == null) return Collections.emptyList();

            List<LevelMetaData> levels = cfMeta.levels();
            if (levels == null || levels.size() < 2) return Collections.emptyList();

            // L1 层（levels 索引从 0 开始，levels.get(0) = L0, levels.get(1) = L1）
            LevelMetaData l1 = levels.get(1);
            List<SstFileMetaData> l1Files = l1.files();
            if (l1Files == null || l1Files.isEmpty()) return Collections.emptyList();

            // 收集所有端点
            TreeSet<byte[]> boundarySet = new TreeSet<>(FragmentedFlushManager::compareBytes);
            for (SstFileMetaData sst : l1Files) {
                byte[] smallest = sst.smallestKey();
                byte[] largest = sst.largestKey();
                if (smallest != null && smallest.length > 0) boundarySet.add(smallest);
                if (largest != null && largest.length > 0) boundarySet.add(largest);
            }

            if (boundarySet.isEmpty()) return Collections.emptyList();

            // 合并过近的边界
            List<byte[]> boundaries = new ArrayList<>();
            byte[] prev = null;
            for (byte[] b : boundarySet) {
                if (prev != null) {
                    // 如果两个边界对应的 sourceId 相同，跳过后者
                    long prevSid = extractSourceId(prev);
                    long curSid = extractSourceId(b);
                    if (prevSid == curSid) continue;
                }
                boundaries.add(b);
                prev = b;
            }

            // 限制边界数量不超过 maxFlushFragments - 1
            int maxBoundaries = config.maxFlushFragments - 1;
            if (boundaries.size() > maxBoundaries) {
                // 等间隔采样
                List<byte[]> sampled = new ArrayList<>();
                double step = (double) boundaries.size() / maxBoundaries;
                for (int i = 0; i < maxBoundaries; i++) {
                    sampled.add(boundaries.get((int) (i * step)));
                }
                boundaries = sampled;
            }

            return boundaries;

        } catch (Exception e) {
            System.err.println("extractL1Boundaries failed: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * 计算静态等间距边界
     *
     * 按缓冲区中实际存在的 sourceId 范围等分为 maxFlushFragments 个区间。
     * 边界键使用 buildBinaryKey(boundarySourceId, "", 0.0)，
     * 利用 sourceId 是键的最高有效字节这一特性进行切分。
     */
    private List<byte[]> computeStaticBoundaries() {
        int fragments = config.maxFlushFragments;
        if (fragments <= 1) return Collections.emptyList();

        // 从缓冲区中获取 sourceId 的最小值和最大值
        long minSid = extractSourceId(buffer.firstKey());
        long maxSid = extractSourceId(buffer.lastKey());

        if (minSid >= maxSid) return Collections.emptyList();

        // 等间距划分 fragments - 1 个边界
        List<byte[]> boundaries = new ArrayList<>();
        long range = maxSid - minSid;
        for (int i = 1; i < fragments; i++) {
            long boundarySid = minSid + (range * i) / fragments;
            // 边界键：该 sourceId 的最小可能键
            boundaries.add(buildBinaryKey(boundarySid, "", 0.0));
        }

        return boundaries;
    }

    /**
     * 按边界将缓冲区数据切分并写入多个 SST 文件
     *
     * @param boundaries 排序后的分片边界列表
     * @return 成功写入的 SST 文件路径列表
     */
    private List<String> writeFragmentedSstFiles(List<byte[]> boundaries) throws RocksDBException {
        List<String> sstFiles = new ArrayList<>();

        // 按边界将缓冲区分片
        List<List<Map.Entry<byte[], byte[]>>> fragments = splitBuffer(boundaries);

        for (int i = 0; i < fragments.size(); i++) {
            List<Map.Entry<byte[], byte[]>> fragment = fragments.get(i);
            if (fragment.isEmpty()) continue;

            String sstPath = String.format("%s/frag_%d_%d.sst",
                    fragmentDir, System.nanoTime(), sstFileCounter.incrementAndGet());

            try {
                writeSingleSstFile(sstPath, fragment);
                sstFiles.add(sstPath);
                totalSstFilesProduced.incrementAndGet();
            } catch (RocksDBException e) {
                System.err.println("Failed to write SST fragment " + i + ": " + e.getMessage());
                // 清理已创建的文件
                new File(sstPath).delete();
            }
        }

        return sstFiles;
    }

    /**
     * 将缓冲区按边界切分为多个有序分片
     *
     * 同时执行"小分片合并"：如果一个分片的估算大小 < MIN_FRAGMENT_BYTES，
     * 将其合并到下一个分片中。
     */
    private List<List<Map.Entry<byte[], byte[]>>> splitBuffer(List<byte[]> boundaries) {
        List<List<Map.Entry<byte[], byte[]>>> rawFragments = new ArrayList<>();

        if (boundaries.isEmpty()) {
            // 无边界：全部作为一个分片
            rawFragments.add(new ArrayList<>(buffer.entrySet()));
            return rawFragments;
        }

        // 按边界切分
        Iterator<Map.Entry<byte[], byte[]>> it = buffer.entrySet().iterator();
        int boundaryIdx = 0;
        List<Map.Entry<byte[], byte[]>> currentFragment = new ArrayList<>();

        while (it.hasNext()) {
            Map.Entry<byte[], byte[]> entry = it.next();

            // 检查是否越过当前边界
            while (boundaryIdx < boundaries.size()
                    && compareBytes(entry.getKey(), boundaries.get(boundaryIdx)) >= 0) {
                rawFragments.add(currentFragment);
                currentFragment = new ArrayList<>();
                boundaryIdx++;
            }

            currentFragment.add(entry);
        }
        rawFragments.add(currentFragment); // 最后一个分片

        // 小分片合并：不足 MIN_FRAGMENT_BYTES 的分片向后合并
        long avgEntryBytes = bufferBytes / Math.max(1, buffer.size());
        long minEntries = MIN_FRAGMENT_BYTES / Math.max(1, avgEntryBytes);

        List<List<Map.Entry<byte[], byte[]>>> merged = new ArrayList<>();
        List<Map.Entry<byte[], byte[]>> accumulator = new ArrayList<>();

        for (List<Map.Entry<byte[], byte[]>> frag : rawFragments) {
            accumulator.addAll(frag);
            if (accumulator.size() >= minEntries) {
                merged.add(accumulator);
                accumulator = new ArrayList<>();
            }
        }
        if (!accumulator.isEmpty()) {
            if (merged.isEmpty()) {
                merged.add(accumulator);
            } else {
                // 将不足一片的尾部合并到最后一个分片
                merged.get(merged.size() - 1).addAll(accumulator);
            }
        }

        return merged;
    }

    /**
     * 使用 SstFileWriter 将一个有序分片写入单个 SST 文件
     *
     * 关键约束：SstFileWriter 要求键严格递增（由 TreeMap 保证）
     */
    private void writeSingleSstFile(String filePath,
                                     List<Map.Entry<byte[], byte[]>> entries) throws RocksDBException {
        try (EnvOptions envOptions = new EnvOptions();
             Options options = new Options()
                     .setCompressionType(CompressionType.LZ4_COMPRESSION);
             SstFileWriter writer = new SstFileWriter(envOptions, options)) {

            writer.open(filePath);

            for (Map.Entry<byte[], byte[]> entry : entries) {
                writer.put(entry.getKey(), entry.getValue());
            }

            writer.finish();
        }
    }

    /**
     * 回退路径：当 SST 写入或 Ingest 失败时，直接以 WriteBatch 写入 RocksDB
     */
    private void fallbackDirectWrite() throws RocksDBException {
        try (WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<byte[], byte[]> entry : buffer.entrySet()) {
                batch.put(lightcurveCfHandle, entry.getKey(), entry.getValue());
            }
            db.write(writeOptions, batch);
        }
        buffer.clear();
        bufferBytes = 0;
    }

    // ==================== 键构建与工具方法 ====================

    /**
     * 与 RocksDBServer.buildBinaryKey 完全一致的键构建
     * 格式: [sourceId:8B][bandLen:1B][band:varB][time:8B]
     */
    static byte[] buildBinaryKey(long sourceId, String band, double time) {
        byte[] bandBytes = band.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(17 + bandBytes.length);
        buf.putLong(sourceId);
        buf.put((byte) bandBytes.length);
        buf.put(bandBytes);
        buf.putDouble(time);
        return buf.array();
    }

    /**
     * 与 RocksDBServer.serializeLightCurvePoint 完全一致的序列化
     */
    static byte[] serializePoint(LightCurvePoint point) {
        byte[] bandBytes = point.band.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(90 + bandBytes.length);
        buffer.putLong(point.sourceId).putDouble(point.ra).putDouble(point.dec)
                .putLong(point.transitId);
        buffer.putInt(bandBytes.length).put(bandBytes);
        buffer.putDouble(point.time).putDouble(point.mag).putDouble(point.flux)
                .putDouble(point.fluxError).putDouble(point.fluxOverError);
        buffer.put((byte) (point.rejectedByPhotometry ? 1 : 0))
                .put((byte) (point.rejectedByVariability ? 1 : 0));
        buffer.putInt(point.otherFlags).putLong(point.solutionId);
        return buffer.array();
    }

    /**
     * 从二进制键中提取 sourceId（前 8 字节）
     */
    static long extractSourceId(byte[] key) {
        if (key == null || key.length < 8) return 0;
        return ByteBuffer.wrap(key, 0, 8).getLong();
    }

    /**
     * 字节数组的字典序比较（无符号）
     * 与 RocksDB 的默认 BytewiseComparator 行为一致
     */
    static int compareBytes(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) return diff;
        }
        return Integer.compare(a.length, b.length);
    }
}
