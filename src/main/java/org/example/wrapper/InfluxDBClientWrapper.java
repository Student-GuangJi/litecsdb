package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.InfluxDBClientOptions;
import okhttp3.OkHttpClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InfluxDB v2 客户端包装器（同步阻塞写入 + 重试 + 并行查询）
 *
 * 查询优化（Experiment 9）：
 *   1. 多线程并行 Flux 查询（STMK 查询分片执行）
 *   2. 查询前清除缓存（重建 HTTP 连接）
 *   3. 应用层空间过滤（InfluxDB 不支持地理空间函数）
 *   4. 简单查询 + STMK 查询接口
 *
 * 环境变量（可选，不设则使用默认值）：
 *   INFLUX_CONNECTIONS   - HTTP 连接数（默认 32）
 *   INFLUX_WRITE_THREADS - 写入线程数（默认 16）
 *   INFLUX_QUERY_THREADS - 查询线程数（默认 8）
 *   INFLUX_BATCH_SIZE    - 每批写入行数（默认 10000）
 */
public class InfluxDBClientWrapper implements AutoCloseable {

    private final int writeThreads;
    private final int queryThreads;
    private final int numConnections;
    private final int batchSize;

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BASE_MS = 3000;

    private final InfluxDBClient[] clients;
    private final WriteApiBlocking[] writeApis;

    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private final String dataDir = "/mnt/nvme/home/wangxc/.influxdbv2/engine/data/";
    private final String url;
    private final String token;
    private final String org;
    private final String bucket;
    private long initialDirSize = 0;

    private final ExecutorService writePool;
    private final ExecutorService queryPool;
    private final AtomicLong writeFailures = new AtomicLong(0);

    private static final long GAIA_EPOCH_UNIX_MS = 1420070400000L;
    private static final long MS_PER_DAY = 86400000L;

    public InfluxDBClientWrapper() {
        this("gaia_lightcurves");
    }

    public InfluxDBClientWrapper(String bucket) {
        this.bucket = bucket;
        this.url = System.getenv().getOrDefault("INFLUX_URL", "http://127.0.0.1:8086");
        this.token = System.getenv().getOrDefault("INFLUX_TOKEN", "wangxc");
        this.org = System.getenv().getOrDefault("INFLUX_ORG", "astro_research");

        this.numConnections = Integer.parseInt(
                System.getenv().getOrDefault("INFLUX_CONNECTIONS", "32"));
        this.writeThreads = Integer.parseInt(
                System.getenv().getOrDefault("INFLUX_WRITE_THREADS", "16"));
        this.queryThreads = Integer.parseInt(
                System.getenv().getOrDefault("INFLUX_QUERY_THREADS", "8"));
        this.batchSize = Integer.parseInt(
                System.getenv().getOrDefault("INFLUX_BATCH_SIZE", "10000"));

        this.clients = new InfluxDBClient[numConnections];
        this.writeApis = new WriteApiBlocking[numConnections];

        for (int i = 0; i < numConnections; i++) {
            OkHttpClient.Builder okBuilder = new OkHttpClient.Builder()
                    .readTimeout(300, TimeUnit.SECONDS)
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

        this.queryPool = Executors.newFixedThreadPool(queryThreads, r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("InfluxDB-Query-" + t.getId());
            return t;
        });

        this.initialDirSize = StorageUtils.getDirectorySize(dataDir);

        System.out.printf("[Phase 1] InfluxDB 初始化完成%n");
        System.out.printf("  连接数=%d, 写入线程=%d, 查询线程=%d, 批大小=%d%n",
                numConnections, writeThreads, queryThreads, batchSize);
    }

    // ==================== 写入方法 ====================

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
            latch.await(600, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

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
                    connIdx = (connIdx + 1) % numConnections;
                }
            }
        }
    }

    // ==================== 查询方法（Experiment 9） ====================

    /**
     * 清除 InfluxDB 查询缓存
     * InfluxDB v2 没有直接的 cache clear 命令。
     * 通过以下方式尽量消除缓存影响：
     * 1. 调用内部 compact（如果 API 支持）
     * 2. 等待足够时间让内存缓存老化
     * 3. 在查询中使用不同的时间窗口避免查询计划缓存
     */
    public void clearQueryCache() {
        try {
            // 关闭并重建所有客户端连接，清除连接层面的缓存
            for (int i = 0; i < numConnections; i++) {
                if (clients[i] != null) {
                    clients[i].close();
                }
            }

            // 短暂等待
            Thread.sleep(1000);

            // 重建连接
            for (int i = 0; i < numConnections; i++) {
                OkHttpClient.Builder okBuilder = new OkHttpClient.Builder()
                        .readTimeout(300, TimeUnit.SECONDS)
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
        } catch (Exception e) {
            System.err.println("[WARN] InfluxDB cache clear failed: " + e.getMessage());
        }
    }

    /**
     * 简单查询：按 source_id 查光变曲线
     * 使用 HTTP API 直接发送 Flux 查询
     */
    public int executeSimpleQuery(long sourceId) {
        String flux = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: 0) " +
                        "|> filter(fn: (r) => r.source_id == \"%d\") " +
                        "|> filter(fn: (r) => r._field == \"mag\")",
                bucket, sourceId);
        return executeFluxQueryHttp(flux);
    }

    /**
     * 多线程并行简单查询
     */
    public List<int[]> executeSimpleQueryParallel(List<Long> sourceIds) {
        List<int[]> allResults = new ArrayList<>();
        for (int i = 0; i < sourceIds.size(); i++) {
            allResults.add(new int[]{0, 0});
        }

        int chunkSize = Math.max(1, (sourceIds.size() + queryThreads - 1) / queryThreads);
        int actualThreads = Math.min(queryThreads, (sourceIds.size() + chunkSize - 1) / chunkSize);
        CountDownLatch latch = new CountDownLatch(actualThreads);

        for (int t = 0; t < actualThreads; t++) {
            final int threadIdx = t;
            final int start = t * chunkSize;
            final int end = Math.min(start + chunkSize, sourceIds.size());

            queryPool.submit(() -> {
                try {
                    for (int i = start; i < end; i++) {
                        long qStart = System.nanoTime();
                        int count = executeSimpleQuery(sourceIds.get(i));
                        allResults.get(i)[0] = count;
                        allResults.get(i)[1] = (int) ((System.nanoTime() - qStart) / 1_000_000);
                    }
                } catch (Exception e) {
                    System.err.println("InfluxDB simple query thread " + threadIdx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return allResults;
    }

    /**
     * STMK 查询（单线程版本）
     *
     * Flux 等价查询：
     *   from(bucket) |> range(start, stop)
     *   |> filter(measurement == "lightcurve" and band == X and _field == "mag")
     *   |> filter(_value >= magMin and _value <= magMax)
     *   |> group(columns: ["source_id"])
     *   |> count()
     *   |> filter(_value >= minObs)
     *
     * 然后获取满足条件的 source_id 的 ra/dec，做应用层 cone search。
     */
    public int executeSTMKQuery(double ra, double dec, double radius,
                                double startTime, double endTime, String band,
                                double magMin, double magMax, int minObs) {

        long startMs = GAIA_EPOCH_UNIX_MS + (long) (startTime * MS_PER_DAY);
        long endMs = GAIA_EPOCH_UNIX_MS + (long) (endTime * MS_PER_DAY);

        // Step 1: 获取满足时间+波段+星等+计数条件的 source_id 列表
        String flux = String.format(Locale.US,
                "from(bucket: \"%s\") " +
                        "|> range(start: %d, stop: %d) " +
                        "|> filter(fn: (r) => r._measurement == \"lightcurve\" and r.band == \"%s\" and r._field == \"mag\") " +
                        "|> filter(fn: (r) => r._value >= %f and r._value <= %f) " +
                        "|> group(columns: [\"source_id\"]) " +
                        "|> count() " +
                        "|> filter(fn: (r) => r._value >= %d)",
                bucket, startMs, endMs, band, magMin, magMax, minObs);

        // 获取候选 source_id
        List<Long> candidateSourceIds = executeFluxQueryGetSourceIds(flux);

        if (candidateSourceIds.isEmpty()) return 0;

        // Step 2: 对候选 source_id 查询 ra/dec 做空间过滤
        return filterByConeSearch(candidateSourceIds, ra, dec, radius);
    }

    /**
     * STMK 多线程并行查询
     *
     * 将时间范围分成多段，每段由一个线程独立查询，
     * 最后合并结果并做空间过滤。
     *
     * 分片策略：按时间窗口切分（适合时序数据库的查询模式）
     */
    public int executeSTMKQueryParallel(double centerRa, double centerDec, double radius,
                                        double startTime, double endTime, String band,
                                        double magMin, double magMax, int minObs) {

        long startMs = GAIA_EPOCH_UNIX_MS + (long) (startTime * MS_PER_DAY);
        long endMs = GAIA_EPOCH_UNIX_MS + (long) (endTime * MS_PER_DAY);
        long totalRange = endMs - startMs;

        if (totalRange <= 0) return 0;

        // 分成 queryThreads 个时间段并行查询
        // 注意：由于 HAVING count >= K 是跨时间段的全局约束，
        // 分片后需要在合并阶段重新聚合计数
        int numShards = queryThreads;
        long shardSize = (totalRange + numShards - 1) / numShards;

        // 每个线程收集: source_id -> partial count
        @SuppressWarnings("unchecked")
        Map<String, Integer>[] shardResults = new Map[numShards];
        for (int i = 0; i < numShards; i++) {
            shardResults[i] = new HashMap<>();
        }

        CountDownLatch latch = new CountDownLatch(numShards);

        for (int s = 0; s < numShards; s++) {
            final int shardIdx = s;
            final long shardStart = startMs + s * shardSize;
            final long shardEnd = Math.min(shardStart + shardSize, endMs);

            queryPool.submit(() -> {
                try {
                    // 每个分片的 Flux 查询：不做 HAVING 过滤，只做分组计数
                    String flux = String.format(Locale.US,
                            "from(bucket: \"%s\") " +
                                    "|> range(start: %d, stop: %d) " +
                                    "|> filter(fn: (r) => r._measurement == \"lightcurve\" and r.band == \"%s\" and r._field == \"mag\") " +
                                    "|> filter(fn: (r) => r._value >= %f and r._value <= %f) " +
                                    "|> group(columns: [\"source_id\"]) " +
                                    "|> count()",
                            bucket, shardStart, shardEnd, band, magMin, magMax);

                    Map<String, Integer> partialCounts = executeFluxQueryGetCounts(flux);
                    shardResults[shardIdx] = partialCounts;

                } catch (Exception e) {
                    System.err.println("InfluxDB STMK query shard " + shardIdx + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // 合并各分片的计数
        Map<String, Integer> globalCounts = new HashMap<>();
        for (Map<String, Integer> shard : shardResults) {
            for (Map.Entry<String, Integer> entry : shard.entrySet()) {
                globalCounts.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }

        // 过滤满足 minObs 的 source_id
        List<Long> candidateSourceIds = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : globalCounts.entrySet()) {
            if (entry.getValue() >= minObs) {
                try {
                    candidateSourceIds.add(Long.parseLong(entry.getKey()));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }

        if (candidateSourceIds.isEmpty()) return 0;

        // 空间过滤
        return filterByConeSearch(candidateSourceIds, centerRa, centerDec, radius);
    }

    /**
     * 对候选 source_id 做空间过滤
     * 需要从 InfluxDB 获取每个 source_id 的 ra/dec
     */
    private int filterByConeSearch(List<Long> sourceIds, double centerRa, double centerDec, double radius) {
        // 并行获取 ra/dec
        AtomicInteger count = new AtomicInteger(0);
        int chunkSize = Math.max(1, (sourceIds.size() + queryThreads - 1) / queryThreads);
        int actualThreads = Math.min(queryThreads, (sourceIds.size() + chunkSize - 1) / chunkSize);
        CountDownLatch latch = new CountDownLatch(actualThreads);

        for (int t = 0; t < actualThreads; t++) {
            final int start = t * chunkSize;
            final int end = Math.min(start + chunkSize, sourceIds.size());

            queryPool.submit(() -> {
                try {
                    for (int i = start; i < end; i++) {
                        long sid = sourceIds.get(i);
                        // 获取该 source_id 的 ra/dec（查一条记录即可）
                        String flux = String.format(
                                "from(bucket: \"%s\") " +
                                        "|> range(start: 0) " +
                                        "|> filter(fn: (r) => r.source_id == \"%d\" and r._field == \"ra\") " +
                                        "|> first()",
                                bucket, sid);

                        double[] coord = executeFluxQueryGetFirstRaDec(sid);
                        if (coord != null && isInCone(coord[0], coord[1], centerRa, centerDec, radius)) {
                            count.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    latch.countDown();
                }
            });
        }

        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return count.get();
    }

    /**
     * 获取一个 source_id 的 ra 和 dec（用于空间过滤）
     */
    private double[] executeFluxQueryGetFirstRaDec(long sourceId) {
        try {
            // ra
            String fluxRa = String.format(
                    "from(bucket: \"%s\") " +
                            "|> range(start: 0) " +
                            "|> filter(fn: (r) => r.source_id == \"%d\" and r._field == \"ra\") " +
                            "|> first()",
                    bucket, sourceId);

            String fluxDec = String.format(
                    "from(bucket: \"%s\") " +
                            "|> range(start: 0) " +
                            "|> filter(fn: (r) => r.source_id == \"%d\" and r._field == \"dec\") " +
                            "|> first()",
                    bucket, sourceId);

            double ra = executeFluxQueryGetFirstValue(fluxRa);
            double dec = executeFluxQueryGetFirstValue(fluxDec);

            if (!Double.isNaN(ra) && !Double.isNaN(dec)) {
                return new double[]{ra, dec};
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private double executeFluxQueryGetFirstValue(String flux) {
        try {
            HttpURLConnection conn = (HttpURLConnection)
                    new URL(url + "/api/v2/query?org=" + org).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Token " + token);
            conn.setRequestProperty("Content-Type", "application/vnd.flux");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(flux.getBytes());
            }

            if (conn.getResponseCode() != 200) return Double.NaN;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith(",")) {
                        String[] parts = line.split(",");
                        if (parts.length > 6) {
                            try {
                                return Double.parseDouble(parts[6].trim());
                            } catch (NumberFormatException e) {
                                // continue
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // ignore
        }
        return Double.NaN;
    }

    /**
     * 执行 Flux 查询，返回 source_id 列表
     */
    private List<Long> executeFluxQueryGetSourceIds(String flux) {
        List<Long> sourceIds = new ArrayList<>();
        try {
            HttpURLConnection conn = (HttpURLConnection)
                    new URL(url + "/api/v2/query?org=" + org).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Token " + token);
            conn.setRequestProperty("Content-Type", "application/vnd.flux");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(120000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(flux.getBytes());
            }

            if (conn.getResponseCode() != 200) return sourceIds;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                String headerLine = null;
                int sourceIdColIdx = -1;

                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) continue;
                    if (line.startsWith("#")) continue;

                    // 第一个非注释行是表头
                    if (headerLine == null) {
                        headerLine = line;
                        String[] headers = line.split(",");
                        for (int i = 0; i < headers.length; i++) {
                            if (headers[i].trim().equals("source_id")) {
                                sourceIdColIdx = i;
                                break;
                            }
                        }
                        continue;
                    }

                    // 数据行
                    if (sourceIdColIdx >= 0) {
                        String[] parts = line.split(",");
                        if (parts.length > sourceIdColIdx) {
                            try {
                                sourceIds.add(Long.parseLong(parts[sourceIdColIdx].trim()));
                            } catch (NumberFormatException e) {
                                // ignore
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("InfluxDB Flux query for sourceIds failed: " + e.getMessage());
        }
        return sourceIds;
    }

    /**
     * 执行 Flux 查询，返回 source_id -> count 映射
     */
    private Map<String, Integer> executeFluxQueryGetCounts(String flux) {
        Map<String, Integer> counts = new HashMap<>();
        try {
            HttpURLConnection conn = (HttpURLConnection)
                    new URL(url + "/api/v2/query?org=" + org).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Token " + token);
            conn.setRequestProperty("Content-Type", "application/vnd.flux");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(120000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(flux.getBytes());
            }

            if (conn.getResponseCode() != 200) return counts;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                String headerLine = null;
                int sourceIdColIdx = -1;
                int valueColIdx = -1;

                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) continue;
                    if (line.startsWith("#")) continue;

                    if (headerLine == null) {
                        headerLine = line;
                        String[] headers = line.split(",");
                        for (int i = 0; i < headers.length; i++) {
                            String h = headers[i].trim();
                            if (h.equals("source_id")) sourceIdColIdx = i;
                            if (h.equals("_value")) valueColIdx = i;
                        }
                        continue;
                    }

                    if (sourceIdColIdx >= 0 && valueColIdx >= 0) {
                        String[] parts = line.split(",");
                        if (parts.length > Math.max(sourceIdColIdx, valueColIdx)) {
                            try {
                                String sid = parts[sourceIdColIdx].trim();
                                int cnt = (int) Double.parseDouble(parts[valueColIdx].trim());
                                counts.merge(sid, cnt, Integer::sum);
                            } catch (NumberFormatException e) {
                                // ignore
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("InfluxDB Flux count query failed: " + e.getMessage());
        }
        return counts;
    }

    /**
     * 通用 Flux 查询，返回结果行数
     */
    public int executeFluxQueryHttp(String flux) {
        try {
            HttpURLConnection conn = (HttpURLConnection)
                    new URL(url + "/api/v2/query?org=" + org).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Token " + token);
            conn.setRequestProperty("Content-Type", "application/vnd.flux");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(60000);
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(flux.getBytes());
            }

            if (conn.getResponseCode() != 200) return 0;

            int count = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith(",")) {
                        count++;
                    }
                }
            }
            return Math.max(0, count - 1);
        } catch (Exception e) {
            return 0;
        }
    }

    // ==================== 空间工具 ====================

    private static boolean isInCone(double ra1, double dec1, double ra2, double dec2, double radius) {
        double ra1R = Math.toRadians(ra1), dec1R = Math.toRadians(dec1);
        double ra2R = Math.toRadians(ra2), dec2R = Math.toRadians(dec2);
        double a = Math.sin((dec2R - dec1R) / 2) * Math.sin((dec2R - dec1R) / 2) +
                Math.cos(dec1R) * Math.cos(dec2R) *
                        Math.sin((ra2R - ra1R) / 2) * Math.sin((ra2R - ra1R) / 2);
        return Math.toDegrees(2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))) <= radius;
    }

    // ==================== 工具方法 ====================

    public String getBucket() { return bucket; }
    public String getUrl() { return url; }
    public String getToken() { return token; }
    public String getOrg() { return org; }

    public void forceFlush() {
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
        queryPool.shutdown();
        try { writePool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        try { queryPool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        for (InfluxDBClient c : clients) { if (c != null) c.close(); }
    }
}