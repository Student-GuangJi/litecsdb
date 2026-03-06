package org.example.wrapper;

import com.influxdb.client.*;
import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import com.influxdb.client.domain.WritePrecision;
import okhttp3.OkHttpClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InfluxDB 2.x 客户端包装器 — 仿照 TDengine C++ 两阶段（Two-Phase）策略
 *
 * <h2>与 TDengine 配置的等价对应关系</h2>
 * <pre>
 * ┌──────────────────────────┬─────────────────────────────────────────────────────────┐
 * │ TDengine 配置            │ InfluxDB 2.x 等价配置                                   │
 * ├──────────────────────────┼─────────────────────────────────────────────────────────┤
 * │ NUM_THREADS = 16         │ WRITE_THREADS = 16（客户端并发写入线程）                  │
 * │ BATCH_SIZE = 10000       │ BATCH_SIZE = 10000（每批写入行数）                        │
 * │ NUM_VGROUPS = 32         │ SHARD_COUNT = 32（数据分片数，通过 shard-group-duration   │
 * │                          │   + precreator 预创建 shard 实现逻辑等价）               │
 * │ BUFFER_SIZE = 256 MB/vg  │ storage-cache-max-memory-size = 32 × 256 MB = 8 GB     │
 * │                          │ （InfluxDB 全局缓存 = 分片数 × 每分片缓存的等效值）      │
 * └──────────────────────────┴─────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>两阶段策略</h2>
 * <ul>
 *   <li>Phase 1（构造函数）: 预热连接池 + 预创建 bucket（不计入写入时间）</li>
 *   <li>Phase 2（putBatch）: 多线程并行 Line Protocol 批量写入</li>
 * </ul>
 */
public class InfluxDBClientWrapper implements AutoCloseable {

    // ==================== 配置常量（对齐 TDengine C++ 版本）====================

    /** 并行写入线程数（对应 TDengine NUM_THREADS = 16） */
    private static final int WRITE_THREADS = 16;

    /** 每批写入行数（对应 TDengine BATCH_SIZE = 10000） */
    private static final int BATCH_SIZE = 10_000;

    /**
     * 数据分片数（对应 TDengine NUM_VGROUPS = 32）。
     * InfluxDB 没有 vgroup 概念，但通过以下方式实现等价效果：
     *   - 服务端：配置 shard-group-duration 使活跃 shard 数量 ≈ 32
     *   - 客户端：将数据按 sourceId 哈希到 32 个分片，每个分片由独立线程写入
     */
    private static final int SHARD_COUNT = 32;

    /**
     * 每个分片的内存缓存（对应 TDengine BUFFER_SIZE = 256 MB/vgroup）。
     * InfluxDB 服务端需配置：
     *   storage-cache-max-memory-size = SHARD_COUNT × CACHE_PER_SHARD_MB = 32 × 256 = 8192 MB
     *   storage-cache-snapshot-memory-size = 256 MB
     *
     * 启动 InfluxDB 时：
     *   influxd --storage-cache-max-memory-size=8589934592 \
     *           --storage-cache-snapshot-memory-size=268435456
     */
    private static final int CACHE_PER_SHARD_MB = 256;

    private static final long LOGICAL_BYTES_PER_POINT = 90L;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 300L;

    // ==================== 实例字段 ====================

    /** 每个写入线程一个独立的 InfluxDBClient + WriteApiBlocking（对应 C++ 每线程一个 taos_connect） */
    private final InfluxDBClient[] threadClients;
    private final WriteApiBlocking[] threadWriteApis;

    /** 主客户端用于 Phase 1（管理操作） */
    private final InfluxDBClient mainClient;

    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private final AtomicLong failedPoints = new AtomicLong(0);
    private final AtomicLong insertedPoints = new AtomicLong(0);

    private final String dataDir;
    private final String url;
    private final String token;
    private final String org;
    private final String bucket;
    private final long initialDirSize;

    // ==================== 构造函数（Phase 1：预热，不计入写入时间）====================

    public InfluxDBClientWrapper(String bucket) {
        this.url = readConfig("INFLUXDB_URL", "http://127.0.0.1:8086");
        this.token = readConfig("INFLUXDB_TOKEN", "wangxc");
        this.org = readConfig("INFLUXDB_ORG", "astro_research");
        this.bucket = bucket;
        this.dataDir = readConfig("INFLUXDB_DATA_DIR", "/mnt/nvme/home/wangxc/.influxdbv2/engine/data/");

        // ========== Phase 1: 预创建资源（不计入写入时间）==========

        // 1. 创建主客户端
        this.mainClient = createClient(60, 60, 30);
        BucketsApi bucketsApi = mainClient.getBucketsApi();
        if (bucketsApi.findBucketByName(this.bucket) == null) {
            String orgId = mainClient.getOrganizationsApi()
                    .findOrganizations().stream()
                    .filter(o -> o.getName().equals(org))
                    .findFirst().get().getId();
            bucketsApi.createBucket(
                    new com.influxdb.client.domain.Bucket()
                            .name(this.bucket)
                            .orgID(orgId)
                            .retentionRules(Collections.emptyList()));
        }
        // 2. 预热 bucket（等价于 TDengine CREATE DATABASE + CREATE STABLE）
        //    InfluxDB bucket 通过 API 或 CLI 预创建，此处验证连通性
        verifyBucketReady();

        // 3. 预创建写入线程的连接池（对应 C++ Phase 1 后每线程建立独立连接）
        this.threadClients = new InfluxDBClient[WRITE_THREADS];
        this.threadWriteApis = new WriteApiBlocking[WRITE_THREADS];

        for (int i = 0; i < WRITE_THREADS; i++) {
            threadClients[i] = createClient(180, 300, 30);
            threadWriteApis[i] = threadClients[i].getWriteApiBlocking();
        }

        this.initialDirSize = StorageUtils.getDirectorySize(dataDir);

        System.out.printf("[Phase 1] InfluxDB 初始化完成%n");
        System.out.printf("  write_threads=%d, batch_size=%d, shard_count=%d, cache_per_shard=%dMB%n",
                WRITE_THREADS, BATCH_SIZE, SHARD_COUNT, CACHE_PER_SHARD_MB);
        System.out.printf("  推荐 InfluxDB 服务端配置:%n");
        System.out.printf("    --storage-cache-max-memory-size=%d%n",
                (long) SHARD_COUNT * CACHE_PER_SHARD_MB * 1024L * 1024L);
        System.out.printf("    --storage-cache-snapshot-memory-size=%d%n",
                (long) CACHE_PER_SHARD_MB * 1024L * 1024L);
    }
    public InfluxDBClientWrapper() {
        this("gaia_lightcurves");
    }
    /**
     * 创建 InfluxDB 客户端（配置超时参数）
     */
    private InfluxDBClient createClient(int readTimeoutSec, int writeTimeoutSec, int connectTimeoutSec) {
        OkHttpClient.Builder okBuilder = new OkHttpClient.Builder()
                .readTimeout(readTimeoutSec, TimeUnit.SECONDS)
                .writeTimeout(writeTimeoutSec, TimeUnit.SECONDS)
                .connectTimeout(connectTimeoutSec, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true);

        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .url(url)
                .authenticateToken(token.toCharArray())
                .org(org)
                .bucket(bucket)
                .okHttpClient(okBuilder)
                .build();

        return InfluxDBClientFactory.create(options);
    }

    /**
     * Phase 1: 验证 bucket 已就绪（等价于 TDengine 的 CREATE DATABASE + CREATE STABLE）。
     * InfluxDB 中 bucket = database + retention policy，measurement 无需预建表。
     */
    private void verifyBucketReady() {
        try {
            boolean ready = mainClient.ping();
            if (!ready) {
                throw new RuntimeException("InfluxDB 服务不可达: " + url);
            }
            // 验证 bucket 存在（如不存在需通过 influx CLI 或 API 预创建）
            boolean bucketExists = mainClient.getBucketsApi().findBuckets().stream()
                    .anyMatch(b -> b.getName().equals(bucket));
            if (!bucketExists) {
                System.err.printf("[WARN] Bucket '%s' 不存在，请提前创建。%n", bucket);
                System.err.printf("  命令: influx bucket create -n %s -o %s -r 0%n", bucket, org);
            }
            System.out.printf("[Phase 1] InfluxDB 连通性验证通过 (bucket=%s)%n", bucket);
        } catch (Exception e) {
            System.err.println("[WARN] InfluxDB 预检查失败: " + e.getMessage());
        }
    }

    // ==================== Phase 2: 多线程并行批量写入 ====================

    /**
     * 对外暴露的批量写入入口。
     * 仿照 C++ 两阶段策略 Phase 2：
     *   1. 按 sourceId 哈希分片到 SHARD_COUNT 个组（对应 C++ 按 SubTable/bucket 分组）
     *   2. 将分片轮询分配到 WRITE_THREADS 个线程
     *   3. 每线程使用独立连接，按 BATCH_SIZE 分批写入 Line Protocol
     */
    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) {
            return;
        }

        // Step 1: 按 shard 分组（对应 C++ groupByBucketTable）
        @SuppressWarnings("unchecked")
        List<LightCurvePoint>[] shards = new List[SHARD_COUNT];
        for (int i = 0; i < SHARD_COUNT; i++) {
            shards[i] = new ArrayList<>();
        }

        for (LightCurvePoint p : points) {
            int shard = (int) Math.floorMod(p.sourceId, SHARD_COUNT);
            shards[shard].add(p);
        }

        // Step 2: 将 SHARD_COUNT 个分片轮询分配到 WRITE_THREADS 个线程
        @SuppressWarnings("unchecked")
        List<LightCurvePoint>[] threadBuckets = new List[WRITE_THREADS];
        for (int i = 0; i < WRITE_THREADS; i++) {
            threadBuckets[i] = new ArrayList<>();
        }

        for (int s = 0; s < SHARD_COUNT; s++) {
            threadBuckets[s % WRITE_THREADS].addAll(shards[s]);
        }

        // Step 3: 多线程并行写入（对应 C++ Phase 2 的 insert_worker 线程池）
        ExecutorService executor = Executors.newFixedThreadPool(WRITE_THREADS);
        CountDownLatch latch = new CountDownLatch(WRITE_THREADS);

        for (int threadId = 0; threadId < WRITE_THREADS; threadId++) {
            final int tid = threadId;
            final List<LightCurvePoint> myPoints = threadBuckets[tid];

            executor.submit(() -> {
                try {
                    insertWorker(tid, myPoints);
                } catch (Exception e) {
                    System.err.printf("[ERROR] Thread %d 写入异常: %s%n", tid, e.getMessage());
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
        executor.shutdown();
    }

    /**
     * 单个写入线程的工作函数（对应 C++ 的 insert_worker / direct_worker_thread）。
     *
     * 每线程使用预创建的独立 InfluxDBClient 连接（对应 C++ 每线程 taos_connect），
     * 按 BATCH_SIZE=10000 分批转换为 Line Protocol 并写入。
     */
    private void insertWorker(int threadId, List<LightCurvePoint> points) {
        if (points.isEmpty()) {
            return;
        }

        WriteApiBlocking writeApi = threadWriteApis[threadId];

        // 按 BATCH_SIZE 分批（对应 C++ for (batch_start; batch_start < total; batch_start += BATCH_SIZE)）
        for (int batchStart = 0; batchStart < points.size(); batchStart += BATCH_SIZE) {
            int batchEnd = Math.min(batchStart + BATCH_SIZE, points.size());
            int batchCount = batchEnd - batchStart;

            // 填充缓冲区（对应 C++ 填充 ts_buf / band_buf / mag_buf 等）
            List<String> lineRecords = new ArrayList<>(batchCount);
            for (int i = batchStart; i < batchEnd; i++) {
                lineRecords.add(toLineProtocol(points.get(i)));
            }

            // 写入（对应 C++ taos_stmt_bind_param_batch → taos_stmt_add_batch → taos_stmt_execute）
            if (writeChunkWithRetry(writeApi, lineRecords)) {
                insertedPoints.addAndGet(batchCount);
                logicalBytesWritten.addAndGet(batchCount * LOGICAL_BYTES_PER_POINT);
            } else {
                failedPoints.addAndGet(batchCount);
            }
        }
    }

    /**
     * 带重试的批量写入（对应 C++ 的错误处理 + continue 逻辑）
     */
    private boolean writeChunkWithRetry(WriteApiBlocking writeApi, List<String> records) {
        Exception last = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                writeApi.writeRecords(WritePrecision.MS, records);
                return true;
            } catch (Exception e) {
                last = e;
                if (!isRetryable(e) || attempt == MAX_RETRIES) {
                    break;
                }
                try {
                    Thread.sleep(RETRY_BACKOFF_MS * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        System.err.println("[ERROR] InfluxDB 批量写入失败: " + (last == null ? "unknown" : last.getMessage()));
        return false;
    }

    // ==================== Line Protocol 转换 ====================

    /**
     * 将单条数据点转换为 InfluxDB Line Protocol。
     * 对应 C++ 中填充 TAOS_MULTI_BIND 缓冲区的逻辑。
     */
    private static String toLineProtocol(LightCurvePoint p) {
        long timestampMs = (long) (p.time * 86400000L);
        String band = escapeTag(normalizeBand(p.band));

        StringBuilder sb = new StringBuilder(256);
        sb.append("lightcurve,band=").append(band)
                .append(" source_id=").append(p.sourceId).append('i')
                .append(",ra=").append(p.ra)
                .append(",dec=").append(p.dec)
                .append(",transit_id=").append(p.transitId).append('i')
                .append(",mag=").append(p.mag)
                .append(",flux=").append(p.flux)
                .append(",flux_error=").append(p.fluxError)
                .append(",flux_over_error=").append(p.fluxOverError)
                .append(",rejected_by_photometry=").append(p.rejectedByPhotometry)
                .append(",rejected_by_variability=").append(p.rejectedByVariability)
                .append(",other_flags=").append(p.otherFlags).append('i')
                .append(",solution_id=").append(p.solutionId).append('i')
                .append(' ')
                .append(timestampMs);
        return sb.toString();
    }

    // ==================== 辅助方法 ====================

    private static String normalizeBand(String band) {
        if (band == null || band.trim().isEmpty()) {
            return "unknown";
        }
        return band.trim();
    }

    private static String escapeTag(String value) {
        return value.replace("\\", "\\\\")
                .replace(",", "\\,")
                .replace(" ", "\\ ")
                .replace("=", "\\=");
    }

    private boolean isRetryable(Exception e) {
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase(Locale.ROOT);
        return lower.contains("timeout")
                || lower.contains("timed out")
                || lower.contains("http status code: 429")
                || lower.contains("http status code: 500")
                || lower.contains("http status code: 502")
                || lower.contains("http status code: 503")
                || lower.contains("http status code: 504")
                || lower.contains("connection reset")
                || lower.contains("broken pipe");
    }

    private static String readConfig(String key, String defaultValue) {
        String v = System.getenv(key);
        if (v == null || v.trim().isEmpty()) {
            v = System.getProperty(key);
        }
        return (v == null || v.trim().isEmpty()) ? defaultValue : v.trim();
    }

    // ==================== 管理操作 ====================

    public void forceFlush() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public double getWriteAmplification() {
        long logical = logicalBytesWritten.get();
        if (logical == 0) {
            System.err.println("警告：InfluxDB 逻辑写入为零，无法计算写放大，默认返回 1.0！");
            return 1.0;
        }
        long currentDirSize = StorageUtils.getDirectorySize(dataDir);
        long physical = currentDirSize - initialDirSize;
        if (physical <= 0) {
            System.err.println("警告：InfluxDB 物理占用未增长，可能是测量误差或数据未落盘！");
        }
        return physical > 0 ? (double) physical / logical : 1.0;
    }

    public boolean hasWriteFailures() {
        return failedPoints.get() > 0;
    }

    public String getFailureSummary() {
        return String.format(Locale.ROOT,
                "insertedPoints=%d, failedPoints=%d",
                insertedPoints.get(), failedPoints.get());
    }

    public void clearData() {
        try {
            java.time.OffsetDateTime start = java.time.OffsetDateTime.now().minusYears(50);
            java.time.OffsetDateTime stop = java.time.OffsetDateTime.now().plusYears(50);
            mainClient.getDeleteApi().delete(start, stop, "", bucket, org);
        } catch (Exception e) {
            System.err.println("清理 InfluxDB 数据失败：" + e.getMessage());
        }
    }

    @Override
    public void close() {
        // 关闭所有线程连接
        for (int i = 0; i < WRITE_THREADS; i++) {
            if (threadClients[i] != null) {
                try {
                    threadClients[i].close();
                } catch (Exception ignored) {
                }
            }
        }
        // 关闭主客户端
        if (mainClient != null) {
            mainClient.close();
        }
    }
}