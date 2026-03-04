package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.client.InfluxDBClientOptions;
import okhttp3.OkHttpClient;
import java.util.concurrent.TimeUnit;
import java.time.OffsetDateTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class InfluxDBClientWrapper implements AutoCloseable {
    private InfluxDBClient client;
    private WriteApiBlocking writeApi;
    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private final String dataDir = "/mnt/nvme/home/wangxc/.influxdbv2/engine/data/"; // InfluxDB v2 默认存储路径
    private final String url = "http://127.0.0.1:8086";
    private final String token = "wangxc";
    private final String org = "astro_research";
    private final String bucket = "gaia_lightcurves";
    private long initialDirSize = 0;

    public InfluxDBClientWrapper() {
        // 1. 自定义底层 HTTP 客户端，将读写超时时间暴力拉长到 120 秒
        OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder()
                .readTimeout(120, TimeUnit.SECONDS)
                .writeTimeout(180, TimeUnit.SECONDS)
                .connectTimeout(60, TimeUnit.SECONDS);

        // 2. 构建 InfluxDB 客户端选项
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .url(url)
                .authenticateToken(token.toCharArray())
                .org(org)
                .bucket(bucket)
                .okHttpClient(okHttpClientBuilder)
                .build();

        // 3. 使用带长超时配置的 options 创建客户端
        this.client = InfluxDBClientFactory.create(options);
        this.writeApi = client.getWriteApiBlocking();

        // 记录初始大小
        this.initialDirSize = StorageUtils.getDirectorySize(dataDir);
    }

    public void putBatch(List<LightCurvePoint> points) {
        if (points.isEmpty()) return;

        List<Point> influxPoints = new ArrayList<>(points.size());
        long batchLogicalSize = points.size() * 90L;

        for (LightCurvePoint p : points) {
            long timestampMs = (long) (p.time * 86400000L);

            Point point = Point.measurement("lightcurve")
                    .addTag("source_id", String.valueOf(p.sourceId))
                    .addTag("band", p.band)
                    .addField("ra", p.ra)
                    .addField("dec", p.dec)
                    .addField("transitId", p.transitId)
                    .addField("mag", p.mag)
                    .addField("flux", p.flux)
                    .addField("flux_error", p.fluxError)
                    .addField("flux_over_error", p.fluxOverError)
                    .addField("rejectedByPhotometry", p.rejectedByPhotometry)
                    .addField("rejectedByVariability", p.rejectedByVariability)
                    .addField("otherFlags", p.otherFlags)
                    .addField("solution_id", p.solutionId)
                    .time(Instant.ofEpochMilli(timestampMs), WritePrecision.MS);
            influxPoints.add(point);
        }
        try {
            writeApi.writePoints(influxPoints);
            logicalBytesWritten.addAndGet(batchLogicalSize);
        } catch (Exception e) {
            System.err.println("InfluxDB 批量写入失败: " + e.getMessage());
        }
    }

    public void forceFlush() {
        // InfluxDB 引擎的 WAL 和 TSM 也是后台定期 flush 的，测试时通过稍微延时等待缓存落盘
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {}
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
            client.getDeleteApi().delete(start, stop, "", bucket, org);
        } catch (Exception e) {
            System.err.println("清理 InfluxDB 数据失败：" + e.getMessage());
        }
    }
    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }
}