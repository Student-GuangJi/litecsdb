package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.HealpixUtil;
import org.example.utils.StorageUtils;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.BucketRetentionRules;
import com.influxdb.client.domain.Organization;
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
        this.queryThreads = Integer.parseInt(System.getenv().getOrDefault("INFLUX_QUERY_THREADS", "32"));
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

    // 将 Gaia 时间（MJD 天数）转换为 InfluxDB 纳秒时间戳
    public long gaiaToInfluxNs(double gaiaTime) {
        return (GAIA_EPOCH_UNIX_MS + (long)(gaiaTime * MS_PER_DAY)) * 1_000_000L;
    }

    // ==================== 批量并行查询接口 ====================

    /** 批量并行简单查询（自包含），返回 int[]（每个查询的行数） */
    public List<int[]> executeBatchSimpleQueries(List<long[]> sourceIdsWithCoords) {
        List<int[]> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(sourceIdsWithCoords.size());
        for (long[] q : sourceIdsWithCoords) {
            queryPool.submit(() -> {
                try {
                    long sourceId = q[0];
                    double ra = Double.longBitsToDouble(q[1]);
                    double dec = Double.longBitsToDouble(q[2]);
                    long hpid = HealpixUtil.raDecToHealpix(ra, dec, HEALPIX_LEVEL);
                    long startNs = GAIA_EPOCH_UNIX_MS * 1_000_000L;
                    long endNs = (GAIA_EPOCH_UNIX_MS + 4000L * MS_PER_DAY) * 1_000_000L;
                    String flux = String.format(
                            "from(bucket: \"%s\") " +
                                    "|> range(start: %d, stop: %d) " +
                                    "|> filter(fn: (r) => r._measurement == \"hp_%d\" and r.source_id == \"%d\" and r._field == \"mag\")",
                            bucket, startNs, endNs, hpid, sourceId);
                    results.add(new int[]{executeFluxQueryHttp(flux)});
                } catch (Exception e) {
                    results.add(new int[]{0});
                } finally { latch.countDown(); }
            });
        }
        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return results;
    }

    /** 批量并行 STMK 查询（自包含），返回 int[]（每个查询匹配的sourceId数） */
    public List<int[]> executeBatchSTMKQueries(List<double[]> paramsList) {
        List<int[]> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(paramsList.size());
        for (double[] p : paramsList) {
            queryPool.submit(() -> {
                try {
                    double ra = p[0], dec = p[1], radius = p[2];
                    double startTime = p[3], endTime = p[4];
                    double magMin = p[5], magMax = p[6];
                    int minObs = (int) p[7];
                    String band = p.length > 9 ? bandFromCode((int) p[9]) : null;

                    long startNs = (GAIA_EPOCH_UNIX_MS + (long)(startTime * MS_PER_DAY)) * 1_000_000L;
                    long endNs = (GAIA_EPOCH_UNIX_MS + (long)(endTime * MS_PER_DAY)) * 1_000_000L;

                    Set<Long> hpids = new HashSet<>();
                    try {
                        new HealpixUtil().setNside(HEALPIX_LEVEL);
                        RangeSet rangeSet = HealpixUtil.queryDisc(ra, dec, radius, HEALPIX_LEVEL);
                        if (rangeSet != null) for (int i = 0; i < rangeSet.nranges(); i++)
                            for (long px = rangeSet.ivbegin(i); px <= rangeSet.ivend(i); px++) hpids.add(px);
                    } catch (Exception e) {}
                    if (hpids.isEmpty()) { results.add(new int[]{0}); return; }

                    StringBuilder mFilter = new StringBuilder();
                    boolean first = true;
                    for (Long hpid : hpids) {
                        if (!first) mFilter.append(" or ");
                        mFilter.append("r._measurement == \"hp_").append(hpid).append("\"");
                        first = false;
                    }
                    String bandFilter = (band != null && !band.isEmpty())
                            ? String.format(" |> filter(fn: (r) => r.band == \"%s\")", band) : "";

                    String flux = String.format(Locale.US,
                            "from(bucket: \"%s\") " +
                                    "|> range(start: %d, stop: %d) " +
                                    "|> filter(fn: (r) => (%s) and r._field == \"mag\" and r._value >= %f and r._value <= %f)%s " +
                                    "|> group(columns: [\"source_id\", \"ra\", \"dec\"]) " +
                                    "|> count() " +
                                    "|> filter(fn: (r) => r._value >= %d)",
                            bucket, startNs, endNs, mFilter.toString(), magMin, magMax, bandFilter, minObs);

                    int matched = 0;
                    HttpURLConnection conn = openFluxConnection(600000);
                    try (OutputStream os = conn.getOutputStream()) { os.write(flux.getBytes()); }
                    if (conn.getResponseCode() == 200) {
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                            String line; int sidCol = -1, raCol = -1, decCol = -1;
                            while ((line = reader.readLine()) != null) {
                                if (line.isEmpty()) { sidCol = -1; raCol = -1; decCol = -1; continue; }
                                if (line.startsWith("#")) continue;
                                String[] parts = line.split(",");
                                if (sidCol < 0) {
                                    for (int j = 0; j < parts.length; j++) {
                                        String col = parts[j].trim();
                                        if ("source_id".equals(col)) sidCol = j;
                                        else if ("ra".equals(col)) raCol = j;
                                        else if ("dec".equals(col)) decCol = j;
                                    }
                                    continue;
                                }
                                if (parts.length > Math.max(sidCol, Math.max(raCol, decCol))) {
                                    try {
                                        if (raCol >= 0 && decCol >= 0) {
                                            double sra = Double.parseDouble(parts[raCol].trim());
                                            double sdec = Double.parseDouble(parts[decCol].trim());
                                            if (isInCone(sra, sdec, ra, dec, radius)) matched++;
                                        } else { matched++; }
                                    } catch (NumberFormatException e) {}
                                }
                            }
                        }
                    }
                    results.add(new int[]{matched});
                } catch (Exception e) {
                    results.add(new int[]{0});
                } finally { latch.countDown(); }
            });
        }
        try { latch.await(600, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return results;
    }

    /** 批量并行时间范围查询（自包含），返回 int[]（每个查询的行数） */
    public List<int[]> executeBatchTimeRangeQueries(List<long[]> sourceIdsWithCoords, List<double[]> timeWindows) {
        List<int[]> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(sourceIdsWithCoords.size());
        for (int idx = 0; idx < sourceIdsWithCoords.size(); idx++) {
            final long[] q = sourceIdsWithCoords.get(idx);
            final double[] tw = timeWindows.get(idx);
            queryPool.submit(() -> {
                try {
                    long sourceId = q[0];
                    double ra = Double.longBitsToDouble(q[1]);
                    double dec = Double.longBitsToDouble(q[2]);
                    long hpid = HealpixUtil.raDecToHealpix(ra, dec, HEALPIX_LEVEL);
                    long startNs = (GAIA_EPOCH_UNIX_MS + (long)(tw[0] * MS_PER_DAY)) * 1_000_000L;
                    long endNs = (GAIA_EPOCH_UNIX_MS + (long)(tw[1] * MS_PER_DAY)) * 1_000_000L;
                    String flux = String.format(
                            "from(bucket: \"%s\") " +
                                    "|> range(start: %d, stop: %d) " +
                                    "|> filter(fn: (r) => r._measurement == \"hp_%d\" and r.source_id == \"%d\" and r._field == \"mag\")",
                            bucket, startNs, endNs, hpid, sourceId);
                    results.add(new int[]{executeFluxQueryHttp(flux)});
                } catch (Exception e) {
                    results.add(new int[]{0});
                } finally { latch.countDown(); }
            });
        }
        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return results;
    }

    /** 批量并行聚合统计查询（自包含），返回 double[][]{count, avgMag} */
    public List<double[]> executeBatchAggregationQueries(List<long[]> sourceIdsWithCoords) {
        List<double[]> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(sourceIdsWithCoords.size());
        for (long[] q : sourceIdsWithCoords) {
            queryPool.submit(() -> {
                try {
                    long sourceId = q[0];
                    double ra = Double.longBitsToDouble(q[1]);
                    double dec = Double.longBitsToDouble(q[2]);
                    long hpid = HealpixUtil.raDecToHealpix(ra, dec, HEALPIX_LEVEL);
                    long startNs = GAIA_EPOCH_UNIX_MS * 1_000_000L;
                    long endNs = (GAIA_EPOCH_UNIX_MS + 4000L * MS_PER_DAY) * 1_000_000L;
                    String flux = String.format(
                            "from(bucket: \"%s\") " +
                                    "|> range(start: %d, stop: %d) " +
                                    "|> filter(fn: (r) => r._measurement == \"hp_%d\" and r.source_id == \"%d\" and r._field == \"mag\") " +
                                    "|> group() " +
                                    "|> mean()",
                            bucket, startNs, endNs, hpid, sourceId);
                    double avgMag = 0;
                    List<String> rows = executeFluxQueryReturnRows(flux);
                    if (!rows.isEmpty()) {
                        String[] parts = rows.get(0).split(",");
                        if (parts.length > 0) {
                            try { avgMag = Double.parseDouble(parts[parts.length - 1].trim()); } catch (Exception e) {}
                        }
                    }
                    results.add(new double[]{rows.size(), avgMag});
                } catch (Exception e) {
                    results.add(new double[]{0, 0});
                } finally { latch.countDown(); }
            });
        }
        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return results;
    }

    /** 批量并行单波段查询（自包含），返回 int[]（每个查询的行数） */
    public List<int[]> executeBatchBandQueries(List<long[]> sourceIdsWithCoords, List<String> bands) {
        List<int[]> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(sourceIdsWithCoords.size());
        for (int idx = 0; idx < sourceIdsWithCoords.size(); idx++) {
            final long[] q = sourceIdsWithCoords.get(idx);
            final String band = bands.get(idx);
            queryPool.submit(() -> {
                try {
                    long sourceId = q[0];
                    double ra = Double.longBitsToDouble(q[1]);
                    double dec = Double.longBitsToDouble(q[2]);
                    long hpid = HealpixUtil.raDecToHealpix(ra, dec, HEALPIX_LEVEL);
                    long startNs = GAIA_EPOCH_UNIX_MS * 1_000_000L;
                    long endNs = (GAIA_EPOCH_UNIX_MS + 4000L * MS_PER_DAY) * 1_000_000L;
                    String flux = String.format(
                            "from(bucket: \"%s\") " +
                                    "|> range(start: %d, stop: %d) " +
                                    "|> filter(fn: (r) => r._measurement == \"hp_%d\" and r.source_id == \"%d\" and r.band == \"%s\" and r._field == \"mag\")",
                            bucket, startNs, endNs, hpid, sourceId, band);
                    results.add(new int[]{executeFluxQueryHttp(flux)});
                } catch (Exception e) {
                    results.add(new int[]{0});
                } finally { latch.countDown(); }
            });
        }
        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return results;
    }

    private static String bandFromCode(int code) {
        switch (code) { case 1: return "G"; case 2: return "BP"; case 3: return "RP"; default: return null; }
    }

    private static int bandToCode(String band) {
        if (band == null) return 0;
        switch (band) { case "G": return 1; case "BP": return 2; case "RP": return 3; default: return 0; }
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
    /**
     * 确保 bucket 存在，不存在则创建
     */
    public void ensureBucketExists() {
        try {
            // 检查是否已存在
            Bucket existing = clients[0].getBucketsApi().findBucketByName(bucket);
            if (existing != null) {
                System.out.println("[InfluxDB] Bucket already exists: " + bucket);
                return;
            }

            // 查找 orgID
            String orgID = null;
            for (Organization o : clients[0].getOrganizationsApi().findOrganizations()) {
                if (o.getName().equals(org)) {
                    orgID = o.getId();
                    break;
                }
            }
            if (orgID == null) {
                System.err.println("[InfluxDB] Cannot find org: " + org);
                return;
            }

            // 创建 bucket（retention=0 永不过期）
            BucketRetentionRules retention = new BucketRetentionRules();
            retention.setEverySeconds(0);

            Bucket newBucket = new Bucket();
            newBucket.setName(bucket);
            newBucket.setOrgID(orgID);
            newBucket.setRetentionRules(Collections.singletonList(retention));

            clients[0].getBucketsApi().createBucket(newBucket);
            System.out.println("[InfluxDB] Created bucket: " + bucket);

        } catch (Exception e) {
            System.err.println("[InfluxDB] ensureBucketExists failed: " + e.getMessage());
        }
    }

    /**
     * 删除 bucket（实验结束清理用）
     */
    public void deleteBucket() {
        try {
            Bucket b = clients[0].getBucketsApi().findBucketByName(bucket);
            if (b != null) {
                clients[0].getBucketsApi().deleteBucket(b);
                System.out.println("[InfluxDB] Deleted bucket: " + bucket);
            }
        } catch (Exception e) {
            System.err.println("[InfluxDB] deleteBucket failed: " + e.getMessage());
        }
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