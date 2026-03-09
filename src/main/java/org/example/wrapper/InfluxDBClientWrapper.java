package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.InfluxDBClientOptions;
import okhttp3.OkHttpClient;
import java.util.concurrent.TimeUnit;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InfluxDB v2 客户端包装器（同步阻塞写入 + 重试）
 *
 * 并行实验安全设计：
 *   - 使用 WriteApiBlocking（同步），天然限制并发请求数 = 线程数
 *   - 写入失败时指数退避重试，应对 InfluxDB 短暂过载
 *   - 通过环境变量控制并发度，并��实验时降低每实验的连接数/线程数
 *
 * 环境变量（可选，不设则使用默认值）：
 *   INFLUX_CONNECTIONS   - HTTP 连接数（默认 32）
 *   INFLUX_WRITE_THREADS - 写入线程数（默认 16）
 *   INFLUX_BATCH_SIZE    - 每批写入行数（默认 10000）
 */
public class InfluxDBClientWrapper implements AutoCloseable {

    private final int writeThreads;
    private final int numConnections;
    private final int batchSize;

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BASE_MS = 3000;

    private final InfluxDBClient[] clients;
    private final WriteApiBlocking[] writeApis;

    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private final String dataDir = "/mnt/nvme/home/wangxc/.influxdbv2/engine/data/";
    private final String url = "http://127.0.0.1:8086";
    private final String token = "wangxc";
    private final String org = "astro_research";
    private final String bucket;
    private long initialDirSize = 0;

    private final ExecutorService writePool;
    private final AtomicLong writeFailures = new AtomicLong(0);

    private static final long GAIA_EPOCH_UNIX_MS = 1420070400000L;
    private static final long MS_PER_DAY = 86400000L;

    public InfluxDBClientWrapper() {
        this("gaia_lightcurves");
    }

    public InfluxDBClientWrapper(String bucket) {
        this.bucket = bucket;

        this.numConnections = Integer.parseInt(
                System.getenv().getOrDefault("INFLUX_CONNECTIONS", "32"));
        this.writeThreads = Integer.parseInt(
                System.getenv().getOrDefault("INFLUX_WRITE_THREADS", "16"));
        this.batchSize = Integer.parseInt(
                System.getenv().getOrDefault("INFLUX_BATCH_SIZE", "10000"));

        this.clients = new InfluxDBClient[numConnections];
        this.writeApis = new WriteApiBlocking[numConnections];

        for (int i = 0; i < numConnections; i++) {
            OkHttpClient.Builder okBuilder = new OkHttpClient.Builder()
                    .readTimeout(300, TimeUnit.SECONDS)    // 加大超时，应对并行实验时排队
                    .writeTimeout(300, TimeUnit.SECONDS)
                    .connectTimeout(60, TimeUnit.SECONDS)
                    .retryOnConnectionFailure(true)
                    .proxy(java.net.Proxy.NO_PROXY);

            InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                    .url(url)
                    .authenticateToken(token.toCharArray())
                    .org(org)
                    .bucket(bucket)
                    .okHttpClient(okBuilder)
                    .build();

            clients[i] = InfluxDBClientFactory.create(options);
            writeApis[i] = clients[i].getWriteApiBlocking();
        }

        boolean healthy = clients[0].ping();
        System.out.printf("[Phase 1] InfluxDB 连接 %s (bucket=%s)%n",
                healthy ? "正常" : "失败", bucket);

        this.writePool = Executors.newFixedThreadPool(writeThreads, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("InfluxDB-Writer-" + t.getId());
            return t;
        });

        this.initialDirSize = StorageUtils.getDirectorySize(dataDir);

        System.out.printf("[Phase 1] InfluxDB 初始化完成%n");
        System.out.printf("  连接数=%d, 写入线程=%d, 批大小=%d%n",
                numConnections, writeThreads, batchSize);
    }

    /**
     * 多线程并行写入，轮询分配到多个连接
     */
    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) return;

        logicalBytesWritten.addAndGet(points.size() * 90L);

        int chunkSize = Math.max(1, (points.size() + writeThreads - 1) / writeThreads);
        List<List<LightCurvePoint>> chunks = new ArrayList<>();
        for (int i = 0; i < points.size(); i += chunkSize) {
            chunks.add(points.subList(i, Math.min(i + chunkSize, points.size())));
        }

        CountDownLatch latch = new CountDownLatch(chunks.size());
        for (int idx = 0; idx < chunks.size(); idx++) {
            final int threadIdx = idx;
            final List<LightCurvePoint> chunk = chunks.get(idx);

            writePool.submit(() -> {
                try {
                    writeChunkAsLineProtocol(threadIdx, chunk);
                } catch (Exception e) {
                    writeFailures.addAndGet(chunk.size());
                    System.err.println("InfluxDB 写入失败: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(600, TimeUnit.SECONDS);  // 加大等待时间，适应重试
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 同步写入 + 指数退避重试
     * 写入失败时自动切换连接重试，最多 MAX_RETRIES 次
     */
    private void writeChunkAsLineProtocol(int threadIdx, List<LightCurvePoint> points) {
        for (int start = 0; start < points.size(); start += batchSize) {
            int end = Math.min(start + batchSize, points.size());
            List<String> lines = new ArrayList<>(end - start);

            for (int i = start; i < end; i++) {
                LightCurvePoint p = points.get(i);
                long tsMs = GAIA_EPOCH_UNIX_MS + (long) (p.time * MS_PER_DAY);

                lines.add(String.format(Locale.US,
                        "lightcurve,source_id=%d,band=%s " +
                                "ra=%.6f,dec=%.6f,transit_id=%di,mag=%.6f,flux=%.6f," +
                                "flux_error=%.6f,flux_over_error=%.6f," +
                                "rejected_by_photometry=%s,rejected_by_variability=%s," +
                                "other_flags=%di,solution_id=%di " +
                                "%d",
                        p.sourceId, p.band,
                        p.ra, p.dec, p.transitId, p.mag, p.flux,
                        p.fluxError, p.fluxOverError,
                        p.rejectedByPhotometry, p.rejectedByVariability,
                        p.otherFlags, p.solutionId,
                        tsMs));
            }

            int connIdx = (threadIdx + start / batchSize) % numConnections;

            for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
                try {
                    writeApis[connIdx].writeRecords(WritePrecision.MS, lines);
                    break;
                } catch (Exception e) {
                    if (attempt == MAX_RETRIES) {
                        throw e;
                    }
                    long waitMs = RETRY_BASE_MS * (1L << attempt);
                    System.err.printf("[重试] InfluxDB 写入超时，%dms 后第 %d/%d 次重试: %s%n",
                            waitMs, attempt + 1, MAX_RETRIES, e.getMessage());
                    try {
                        Thread.sleep(waitMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                    // 切换连接重试
                    connIdx = (connIdx + 1) % numConnections;
                }
            }
        }
    }

    public void forceFlush() {
        // WriteApiBlocking 是同步的，方法返回时数据已写入，无需额外 flush
        try { Thread.sleep(2000); } catch (InterruptedException e) {}
    }

    public double getWriteAmplification() {
        long logical = logicalBytesWritten.get();
        if (logical == 0) return 1.0;
        long currentDirSize = StorageUtils.getDirectorySize(dataDir);
        long physical = currentDirSize - initialDirSize;
        return physical > 0 ? (double) physical / logical : 1.0;
    }

    public void clearData() {
        try {
            OffsetDateTime start = OffsetDateTime.now().minusYears(50);
            OffsetDateTime stop = OffsetDateTime.now().plusYears(50);
            clients[0].getDeleteApi().delete(start, stop, "", bucket, org);
        } catch (Exception e) {
            System.err.println("清理 InfluxDB 数据失败: " + e.getMessage());
        }
    }

    public boolean hasWriteFailures() { return writeFailures.get() > 0; }
    public String getFailureSummary() { return String.format("failures=%d", writeFailures.get()); }

    @Override
    public void close() {
        writePool.shutdown();
        try { writePool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        for (InfluxDBClient c : clients) { if (c != null) c.close(); }
    }
}