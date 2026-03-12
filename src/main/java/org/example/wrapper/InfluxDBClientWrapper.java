package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.HealpixUtil;
import org.example.utils.StorageUtils;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.InfluxDBClientOptions;
import healpix.RangeSet;
import okhttp3.OkHttpClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InfluxDB v2 客户端包装器 — HEALPix 天区分区模型
 *
 * 数据结构（对标 LitecsDB）：
 *   measurement = "hp_{healpixId}"
 *   tags:  source_id, band, ra, dec
 *   fields: transit_id, mag, flux, flux_error, flux_over_error,
 *           rejected_by_photometry, rejected_by_variability, other_flags, solution_id
 *   timestamp: GAIA epoch + time * MS_PER_DAY
 *
 * STMK 查询优化路径：
 *   1. HealpixUtil.queryDisc → 天区剪枝
 *   2. 只查目标天区 measurement（避免全表扫描）
 *   3. tag 层面 source_id 过滤（TSI 索引加速）
 *   4. 应用层精确 cone search
 *   5. 时间+星等+计数聚合 → 返回 source_id 列表
 */
public class InfluxDBClientWrapper implements AutoCloseable {

    private static final int HEALPIX_LEVEL = 1;

    private final int writeThreads;
    private final int queryThreads;
    private final int numConnections;
    private final int batchSize;

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_BASE_MS = 3000;

    private InfluxDBClient[] clients;
    private WriteApiBlocking[] writeApis;

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

    public InfluxDBClientWrapper() { this("gaia_lightcurves"); }

    public InfluxDBClientWrapper(String bucket) {
        this.bucket = bucket;
        this.url = System.getenv().getOrDefault("INFLUX_URL", "http://127.0.0.1:8086");
        this.token = System.getenv().getOrDefault("INFLUX_TOKEN", "wangxc");
        this.org = System.getenv().getOrDefault("INFLUX_ORG", "astro_research");

        this.numConnections = Integer.parseInt(System.getenv().getOrDefault("INFLUX_CONNECTIONS", "32"));
        this.writeThreads = Integer.parseInt(System.getenv().getOrDefault("INFLUX_WRITE_THREADS", "16"));
        this.queryThreads = Integer.parseInt(System.getenv().getOrDefault("INFLUX_QUERY_THREADS", "8"));
        this.batchSize = Integer.parseInt(System.getenv().getOrDefault("INFLUX_BATCH_SIZE", "10000"));

        this.clients = new InfluxDBClient[numConnections];
        this.writeApis = new WriteApiBlocking[numConnections];
        for (int i = 0; i < numConnections; i++) {
            clients[i] = createClient();
            writeApis[i] = clients[i].getWriteApiBlocking();
        }

        boolean healthy = clients[0].ping();
        System.out.printf("[Phase 1] InfluxDB 连接 %s (bucket=%s, healpix_level=%d)%n",
                healthy ? "正常" : "失败", bucket, HEALPIX_LEVEL);

        this.writePool = Executors.newFixedThreadPool(writeThreads, r -> {
            Thread t = new Thread(r); t.setDaemon(true); t.setName("InfluxDB-Writer-" + t.getId()); return t;
        });
        this.queryPool = Executors.newFixedThreadPool(queryThreads, r -> {
            Thread t = new Thread(r); t.setDaemon(true); t.setName("InfluxDB-Query-" + t.getId()); return t;
        });
        this.initialDirSize = StorageUtils.getDirectorySize(dataDir);
        System.out.printf("  连接数=%d, 写入线程=%d, 查询线程=%d, 批大小=%d%n",
                numConnections, writeThreads, queryThreads, batchSize);
    }

    private InfluxDBClient createClient() {
        OkHttpClient.Builder okBuilder = new OkHttpClient.Builder()
                .readTimeout(300, TimeUnit.SECONDS).writeTimeout(300, TimeUnit.SECONDS)
                .connectTimeout(60, TimeUnit.SECONDS).retryOnConnectionFailure(true)
                .proxy(java.net.Proxy.NO_PROXY);
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
                .url(url).authenticateToken(token.toCharArray())
                .org(org).bucket(bucket).okHttpClient(okBuilder).build();
        return InfluxDBClientFactory.create(options);
    }

    // ==================== 写入 ====================

    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) return;
        logicalBytesWritten.addAndGet(points.size() * 90L);

        int chunkSize = Math.max(1, (points.size() + writeThreads - 1) / writeThreads);
        List<List<LightCurvePoint>> chunks = new ArrayList<>();
        for (int i = 0; i < points.size(); i += chunkSize)
            chunks.add(points.subList(i, Math.min(i + chunkSize, points.size())));

        CountDownLatch latch = new CountDownLatch(chunks.size());
        for (int idx = 0; idx < chunks.size(); idx++) {
            final int threadIdx = idx;
            final List<LightCurvePoint> chunk = chunks.get(idx);
            writePool.submit(() -> {
                try { writeChunk(threadIdx, chunk); }
                catch (Exception e) { writeFailures.addAndGet(chunk.size()); System.err.println("InfluxDB 写入失败: " + e.getMessage()); }
                finally { latch.countDown(); }
            });
        }
        try { latch.await(600, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /**
     * Line Protocol:
     *   hp_23,source_id=12345,band=G,ra=180.123456,dec=-23.456789
     *     transit_id=67890i,mag=15.23,flux=1234.56,...  1420070400000
     */
    private void writeChunk(int threadIdx, List<LightCurvePoint> points) {
        for (int start = 0; start < points.size(); start += batchSize) {
            int end = Math.min(start + batchSize, points.size());
            List<String> lines = new ArrayList<>(end - start);

            for (int i = start; i < end; i++) {
                LightCurvePoint p = points.get(i);
                long tsMs = GAIA_EPOCH_UNIX_MS + (long) (p.time * MS_PER_DAY);
                long hpid = HealpixUtil.raDecToHealpix(p.ra, p.dec, HEALPIX_LEVEL);

                lines.add(String.format(Locale.US,
                        "hp_%d,source_id=%d,band=%s,ra=%.6f,dec=%.6f " +
                                "transit_id=%di,mag=%.6f,flux=%.6f," +
                                "flux_error=%.6f,flux_over_error=%.6f," +
                                "rejected_by_photometry=%s,rejected_by_variability=%s," +
                                "other_flags=%di,solution_id=%di " +
                                "%d",
                        hpid, p.sourceId, p.band, p.ra, p.dec,
                        p.transitId, p.mag, p.flux,
                        p.fluxError, p.fluxOverError,
                        p.rejectedByPhotometry, p.rejectedByVariability,
                        p.otherFlags, p.solutionId, tsMs));
            }

            int connIdx = (threadIdx + start / batchSize) % numConnections;
            for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
                try { writeApis[connIdx].writeRecords(WritePrecision.MS, lines); break; }
                catch (Exception e) {
                    if (attempt == MAX_RETRIES) throw e;
                    long waitMs = RETRY_BASE_MS * (1L << attempt);
                    try { Thread.sleep(waitMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw e; }
                    connIdx = (connIdx + 1) % numConnections;
                }
            }
        }
    }

    // ==================== 查询 ====================

    public void clearQueryCache() {
        try {
            for (int i = 0; i < numConnections; i++) { if (clients[i] != null) clients[i].close(); }
            Thread.sleep(1000);
            for (int i = 0; i < numConnections; i++) { clients[i] = createClient(); writeApis[i] = clients[i].getWriteApiBlocking(); }
        } catch (Exception e) { System.err.println("[WARN] InfluxDB cache clear failed: " + e.getMessage()); }
    }

    /**
     * 简单查询：按 source_id 返回该天体全部观测数据（每行每列）
     *
     * source_id 是 tag → TSI 索引高效定位，遍历所有 measurement(hp_*)
     * 用 pivot 展开所有 field 为宽表
     */
    public List<String> executeSimpleQuery(long sourceId) {
        String flux = String.format(
                "from(bucket: \"%s\") " +
                        "|> range(start: 0) " +
                        "|> filter(fn: (r) => r.source_id == \"%d\") " +
                        "|> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")",
                bucket, sourceId);
        return executeFluxQueryReturnRows(flux);
    }

    /**
     * STMK 查询 — HEALPix 天区剪枝优化
     *
     * 步骤:
     *   1. queryDisc(ra,dec,radius) → 天区 ID 集合
     *   2. 只查目标天区 measurement "hp_{id}"
     *   3. time range + mag filter + group by source_id + count
     *   4. count >= minObs 过滤
     *   5. 从 tag 取 ra/dec → 精确 cone search
     *   6. 返回匹配的 source_id 列表
     */
    public List<Long> executeSTMKQuery(double ra, double dec, double radius,
                                       double startTime, double endTime,
                                       double magMin, double magMax, int minObs) {
        long startMs = GAIA_EPOCH_UNIX_MS + (long) (startTime * MS_PER_DAY);
        long endMs = GAIA_EPOCH_UNIX_MS + (long) (endTime * MS_PER_DAY);

        // Step 1: HEALPix 天区剪枝
        Set<Long> healpixIds = calculateHealpixIdsInCone(ra, dec, radius);
        if (healpixIds.isEmpty()) return Collections.emptyList();

        // Step 2: 构建 measurement 过滤条件
        StringBuilder mFilter = new StringBuilder();
        boolean first = true;
        for (Long hpid : healpixIds) {
            if (!first) mFilter.append(" or ");
            mFilter.append("r._measurement == \"hp_").append(hpid).append("\"");
            first = false;
        }

        // Step 3: Flux 查询 — 时间+星等+计数
        String flux = String.format(Locale.US,
                "from(bucket: \"%s\") " +
                        "|> range(start: %d, stop: %d) " +
                        "|> filter(fn: (r) => %s) " +
                        "|> filter(fn: (r) => r._field == \"mag\") " +
                        "|> filter(fn: (r) => r._value >= %f and r._value <= %f) " +
                        "|> group(columns: [\"source_id\"]) " +
                        "|> count() " +
                        "|> filter(fn: (r) => r._value >= %d)",
                bucket, startMs, endMs, mFilter.toString(), magMin, magMax, minObs);

        // Step 4: 获取候选 source_id
        List<Long> candidateIds = executeFluxQueryGetSourceIds(flux);
        if (candidateIds.isEmpty()) return Collections.emptyList();

        // Step 5: 精确 cone search 空间过滤（从 tag 读 ra/dec）
        List<Long> matchedIds = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(candidateIds.size());
        for (long sid : candidateIds) {
            queryPool.submit(() -> {
                try {
                    double[] coord = getSourceCoordFromTag(sid);
                    if (coord != null && isInCone(coord[0], coord[1], ra, dec, radius)) {
                        matchedIds.add(sid);
                    }
                } catch (Exception e) { /* ignore */ }
                finally { latch.countDown(); }
            });
        }
        try { latch.await(120, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return matchedIds;
    }

    /** 从 tag 读取天体坐标（ra/dec 现在是 tag，一条查询即可获取） */
    private double[] getSourceCoordFromTag(long sourceId) {
        String flux = String.format(
                "from(bucket: \"%s\") |> range(start: 0) " +
                        "|> filter(fn: (r) => r.source_id == \"%d\") |> first()",
                bucket, sourceId);
        try {
            HttpURLConnection conn = openFluxConnection(120000);
            try (OutputStream os = conn.getOutputStream()) { os.write(flux.getBytes()); }
            if (conn.getResponseCode() != 200) return null;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line; String header = null; int raCol = -1, decCol = -1;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) { header = null; continue; }
                    if (line.startsWith("#")) continue;
                    if (header == null) {
                        header = line;
                        String[] cols = line.split(",");
                        for (int i = 0; i < cols.length; i++) {
                            if ("ra".equals(cols[i].trim())) raCol = i;
                            if ("dec".equals(cols[i].trim())) decCol = i;
                        }
                        continue;
                    }
                    if (raCol >= 0 && decCol >= 0) {
                        String[] parts = line.split(",");
                        if (parts.length > Math.max(raCol, decCol)) {
                            return new double[]{
                                    Double.parseDouble(parts[raCol].trim()),
                                    Double.parseDouble(parts[decCol].trim())};
                        }
                    }
                }
            }
        } catch (Exception e) { /* ignore */ }
        return null;
    }

    /** HEALPix cone search 计算涉及天区 */
    private Set<Long> calculateHealpixIdsInCone(double ra, double dec, double radius) {
        Set<Long> ids = new HashSet<>();
        try {
            new HealpixUtil().setNside(HEALPIX_LEVEL);
            RangeSet rs = HealpixUtil.queryDisc(ra, dec, radius, HEALPIX_LEVEL);
            if (rs != null) {
                for (int i = 0; i < rs.nranges(); i++)
                    for (long pix = rs.ivbegin(i); pix <= rs.ivend(i); pix++) ids.add(pix);
            }
        } catch (Exception e) { System.err.println("[WARN] HEALPix queryDisc failed: " + e.getMessage()); }
        return ids;
    }

    // ==================== Flux 基础方法 ====================

    private HttpURLConnection openFluxConnection(int readTimeout) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(url + "/api/v2/query?org=" + org).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Token " + token);
        conn.setRequestProperty("Content-Type", "application/vnd.flux");
        conn.setConnectTimeout(60000);
        conn.setReadTimeout(readTimeout);
        conn.setDoOutput(true);
        return conn;
    }

    private List<String> executeFluxQueryReturnRows(String flux) {
        List<String> rows = new ArrayList<>();
        try {
            HttpURLConnection conn = openFluxConnection(120000);
            try (OutputStream os = conn.getOutputStream()) { os.write(flux.getBytes()); }
            if (conn.getResponseCode() != 200) return rows;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line; String header = null;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) { header = null; continue; }
                    if (line.startsWith("#")) continue;
                    if (header == null) { header = line; continue; }
                    rows.add(line);
                }
            }
        } catch (Exception e) { /* ignore */ }
        return rows;
    }

    private List<Long> executeFluxQueryGetSourceIds(String flux) {
        List<Long> ids = new ArrayList<>();
        try {
            HttpURLConnection conn = openFluxConnection(600000);
            try (OutputStream os = conn.getOutputStream()) { os.write(flux.getBytes()); }
            if (conn.getResponseCode() != 200) return ids;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line; int sidCol = -1;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty()) { sidCol = -1; continue; }
                    if (line.startsWith("#")) continue;
                    String[] parts = line.split(",");
                    if (sidCol < 0) {
                        for (int j = 0; j < parts.length; j++)
                            if ("source_id".equals(parts[j].trim())) { sidCol = j; break; }
                        continue;
                    }
                    if (parts.length > sidCol) {
                        try { ids.add(Long.parseLong(parts[sidCol].trim())); } catch (NumberFormatException e) {}
                    }
                }
            }
        } catch (Exception e) { System.err.println("Flux sourceId query failed: " + e.getMessage()); }
        return ids;
    }

    public int executeFluxQueryHttp(String flux) {
        try {
            HttpURLConnection conn = openFluxConnection(60000);
            try (OutputStream os = conn.getOutputStream()) { os.write(flux.getBytes()); }
            if (conn.getResponseCode() != 200) return 0;
            int count = 0;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null)
                    if (!line.isEmpty() && !line.startsWith("#") && !line.startsWith(",")) count++;
            }
            return Math.max(0, count - 1);
        } catch (Exception e) { return 0; }
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

    public void forceFlush() { try { Thread.sleep(2000); } catch (InterruptedException e) {} }

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
        } catch (Exception e) { System.err.println("清理 InfluxDB 数据失败: " + e.getMessage()); }
    }

    public boolean hasWriteFailures() { return writeFailures.get() > 0; }
    public String getFailureSummary() { return String.format("failures=%d", writeFailures.get()); }

    @Override
    public void close() {
        writePool.shutdown(); queryPool.shutdown();
        try { writePool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        try { queryPool.awaitTermination(30, TimeUnit.SECONDS); } catch (InterruptedException e) {}
        for (InfluxDBClient c : clients) { if (c != null) c.close(); }
    }
}