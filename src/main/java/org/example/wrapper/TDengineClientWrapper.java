package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.HealpixUtil;
import org.example.utils.StorageUtils;
import healpix.RangeSet;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class TDengineClientWrapper implements AutoCloseable {
    private static final int HEALPIX_LEVEL = 1;
    private static final int WRITE_THREADS = 16;
    private static final int QUERY_THREADS = 32;
    private static final int BATCH_SIZE = 10_000;
    private static final int VGROUPS = 32;
    private final String database;
    private final String host;
    private final int port;
    private final Connection[] threadConnections;
    private final Connection[] queryConnections;
    private final ExecutorService writePool;
    private final ExecutorService queryPool;
    private static final long GAIA_EPOCH_UNIX_US = 1420070400000000L;
    private static final long US_PER_DAY = 86400000000L;
    private static final long GAIA_EPOCH_UNIX_MS = 1420070400000L;
    private static final long MS_PER_DAY = 86400000L;
    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private long initialDataDirSize;
    private String dataDirPath;
    private final AtomicLong insertedRows = new AtomicLong(0);
    private final AtomicLong failedRows = new AtomicLong(0);
    private final Set<Long> createdStables = ConcurrentHashMap.newKeySet();
    private final Set<String> createdSubtables = ConcurrentHashMap.newKeySet();

    public TDengineClientWrapper() { this("astro_db"); }
    public TDengineClientWrapper(String database) {
        this.database = database;
        this.host = System.getenv().getOrDefault("TDENGINE_HOST", "127.0.0.1");
        this.port = Integer.parseInt(System.getenv().getOrDefault("TDENGINE_PORT", "6030"));
        this.threadConnections = new Connection[WRITE_THREADS];
        this.queryConnections = new Connection[QUERY_THREADS];
        try {
            initDatabase();
            for (int i = 0; i < WRITE_THREADS; i++) { threadConnections[i] = createConn(); threadConnections[i].createStatement().execute("USE " + database); }
            for (int i = 0; i < QUERY_THREADS; i++) { queryConnections[i] = createConn(); queryConnections[i].createStatement().execute("USE " + database); }
        } catch (Exception e) { throw new RuntimeException("TDengine init failed", e); }
        this.writePool = Executors.newFixedThreadPool(WRITE_THREADS, r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        this.queryPool = Executors.newFixedThreadPool(QUERY_THREADS, r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        this.dataDirPath = System.getenv().getOrDefault("TAOS_DIR", System.getProperty("user.home") + "/tdengine") + "/data/vnode";
        this.initialDataDirSize = getDataDirSize();
        System.out.printf("[OK] TDengine init (db=%s, hp_level=%d)%n", database, HEALPIX_LEVEL);
    }
    private void initDatabase() throws SQLException {
        try (Connection c = createConn(); Statement s = c.createStatement()) {
            s.execute(String.format("CREATE DATABASE IF NOT EXISTS %s VGROUPS %d BUFFER 256 WAL_LEVEL 1 WAL_FSYNC_PERIOD 3000 PAGES 1024 PAGESIZE 16 CACHESIZE 128 CACHEMODEL 'both' PRECISION 'us'", database, VGROUPS));
        }
    }
    private void ensureStable(Statement stmt, long hpid) throws SQLException {
        if (createdStables.add(hpid)) {
            stmt.execute("CREATE STABLE IF NOT EXISTS " + database + ".hp_" + hpid + " (" +
                    "ts TIMESTAMP, transit_id BIGINT, obs_time DOUBLE, mag DOUBLE, flux DOUBLE, " +
                    "flux_error DOUBLE, flux_over_error DOUBLE, rejected_by_photometry BOOL, " +
                    "rejected_by_variability BOOL, other_flags INT, solution_id BIGINT" +
                    ") TAGS (source_id BIGINT, band NCHAR(4), ra DOUBLE, dec_val DOUBLE)");
        }
    }

    public void putBatch(List<LightCurvePoint> points) {
        if (points == null || points.isEmpty()) return;
        logicalBytesWritten.addAndGet(points.size() * 90L);
        int cs = Math.max(1, (points.size() + WRITE_THREADS - 1) / WRITE_THREADS);
        List<List<LightCurvePoint>> chunks = new ArrayList<>();
        for (int i = 0; i < points.size(); i += cs) chunks.add(points.subList(i, Math.min(i + cs, points.size())));
        CountDownLatch latch = new CountDownLatch(chunks.size());
        for (int idx = 0; idx < chunks.size(); idx++) {
            final int ti = idx; final List<LightCurvePoint> chunk = chunks.get(idx);
            writePool.submit(() -> { try { writeChunk(ti, chunk); insertedRows.addAndGet(chunk.size()); } catch (Exception e) { failedRows.addAndGet(chunk.size()); } finally { latch.countDown(); } });
        }
        try { latch.await(120, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
    private void writeChunk(int ti, List<LightCurvePoint> points) throws SQLException {
        Connection conn = threadConnections[ti % WRITE_THREADS];
        Map<String, List<LightCurvePoint>> groups = new HashMap<>();
        Map<String, long[]> meta = new HashMap<>();
        for (LightCurvePoint p : points) {
            long hpid = HealpixUtil.raDecToHealpix(p.ra, p.dec, HEALPIX_LEVEL);
            String key = hpid + "_" + p.sourceId + "_" + p.band;
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(p);
            meta.putIfAbsent(key, new long[]{hpid});
        }
        Set<Long> hpids = new HashSet<>(); for (long[] m : meta.values()) hpids.add(m[0]);
        try (Statement stmt = conn.createStatement()) { for (long hpid : hpids) ensureStable(stmt, hpid); }
        for (Map.Entry<String, List<LightCurvePoint>> entry : groups.entrySet()) {
            List<LightCurvePoint> gp = entry.getValue(); long hpid = meta.get(entry.getKey())[0];
            LightCurvePoint f = gp.get(0);
            String sub = "hp" + hpid + "_s" + f.sourceId + "_" + f.band.toLowerCase();
            if (createdSubtables.add(sub)) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(String.format(Locale.US, "CREATE TABLE IF NOT EXISTS %s.%s USING %s.hp_%d TAGS(%d,'%s',%.6f,%.6f)",
                            database, sub, database, hpid, f.sourceId, f.band, f.ra, f.dec));
                }
            }
            for (int s = 0; s < gp.size(); s += BATCH_SIZE) {
                int e = Math.min(s + BATCH_SIZE, gp.size());
                StringBuilder sql = new StringBuilder("INSERT INTO ").append(database).append(".").append(sub).append(" VALUES ");
                for (int i = s; i < e; i++) {
                    LightCurvePoint p = gp.get(i);
                    long tsUs = GAIA_EPOCH_UNIX_US + (long)(p.time * US_PER_DAY) + Math.abs(p.transitId % 999_000);
                    if (i > s) sql.append(" ");
                    sql.append(String.format(Locale.US, "(%d,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%s,%s,%d,%d)",
                            tsUs, p.transitId, p.time, p.mag, p.flux, p.fluxError, p.fluxOverError,
                            p.rejectedByPhotometry?"true":"false", p.rejectedByVariability?"true":"false", p.otherFlags, p.solutionId));
                }
                try (Statement stmt = conn.createStatement()) { stmt.execute(sql.toString()); }
            }
        }
    }

    // ==================== 单条直接查询接口（无线程池开销） ====================

    /** 单条简单查询，返回行数 */
    public int querySingle(long sourceId, double ra, double dec) {
        long hpid = HealpixUtil.raDecToHealpix(ra, dec, HEALPIX_LEVEL);
        String sql = String.format("SELECT ts FROM %s.hp_%d WHERE source_id=%d ORDER BY ts", database, hpid, sourceId);
        int rowCount = 0;
        try (Statement stmt = queryConnections[0].createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) rowCount++;
        } catch (Exception e) {}
        return rowCount;
    }

    /** 单条时间范围查询 */
    public int queryTimeRange(long sourceId, double ra, double dec, double startTime, double endTime) {
        long hpid = HealpixUtil.raDecToHealpix(ra, dec, HEALPIX_LEVEL);
        long startUs = (GAIA_EPOCH_UNIX_MS + (long)(startTime * MS_PER_DAY)) * 1000;
        long endUs = (GAIA_EPOCH_UNIX_MS + (long)(endTime * MS_PER_DAY)) * 1000;
        String sql = String.format("SELECT ts FROM %s.hp_%d WHERE source_id=%d AND ts>=%d AND ts<=%d",
                database, hpid, sourceId, startUs, endUs);
        int rowCount = 0;
        try (Statement stmt = queryConnections[0].createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) rowCount++;
        } catch (Exception e) {}
        return rowCount;
    }

    /** 单条聚合查询 */
    public double[] queryAggregation(long sourceId, double ra, double dec) {
        long hpid = HealpixUtil.raDecToHealpix(ra, dec, HEALPIX_LEVEL);
        String sql = String.format("SELECT COUNT(*),AVG(mag),MIN(mag),MAX(mag) FROM %s.hp_%d WHERE source_id=%d",
                database, hpid, sourceId);
        try (Statement stmt = queryConnections[0].createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) return new double[]{rs.getDouble(1), rs.getDouble(2)};
        } catch (Exception e) {}
        return new double[]{0, 0};
    }

    /** 单条波段查询 */
    public int queryBand(long sourceId, double ra, double dec, String band) {
        long hpid = HealpixUtil.raDecToHealpix(ra, dec, HEALPIX_LEVEL);
        String sql = String.format("SELECT ts FROM %s.hp_%d WHERE source_id=%d AND band='%s'",
                database, hpid, sourceId, band);
        int rowCount = 0;
        try (Statement stmt = queryConnections[0].createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) rowCount++;
        } catch (Exception e) {}
        return rowCount;
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
                    String sql = String.format("SELECT ts FROM %s.hp_%d WHERE source_id=%d ORDER BY ts", database, hpid, sourceId);
                    int rowCount = 0;
                    Connection c = queryConnections[(int)(Thread.currentThread().getId() % QUERY_THREADS)];
                    try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) rowCount++;
                    }
                    results.add(new int[]{rowCount});
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

                    long startUs = (GAIA_EPOCH_UNIX_MS + (long)(startTime * MS_PER_DAY)) * 1000;
                    long endUs = (GAIA_EPOCH_UNIX_MS + (long)(endTime * MS_PER_DAY)) * 1000;

                    Set<Long> hpids = new HashSet<>();
                    try {
                        new HealpixUtil().setNside(HEALPIX_LEVEL);
                        RangeSet rangeSet = HealpixUtil.queryDisc(ra, dec, radius, HEALPIX_LEVEL);
                        if (rangeSet != null) for (int i = 0; i < rangeSet.nranges(); i++)
                            for (long px = rangeSet.ivbegin(i); px <= rangeSet.ivend(i); px++) hpids.add(px);
                    } catch (Exception e) {}

                    int matched = 0;
                    for (long hpid : hpids) {
                        String bandFilter = (band != null && !band.isEmpty()) ? String.format(" AND band='%s'", band) : "";
                        String sql = String.format(Locale.US,
                                "SELECT source_id,FIRST(ra) as sra,FIRST(dec_val) as sdec,COUNT(*) as cnt FROM %s.hp_%d WHERE ts>=%d AND ts<=%d AND mag>=%f AND mag<=%f%s GROUP BY source_id HAVING COUNT(*)>=%d",
                                database, hpid, startUs, endUs, magMin, magMax, bandFilter, minObs);
                        Connection c = queryConnections[(int)(hpid % QUERY_THREADS)];
                        try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                            while (rs.next()) {
                                if (isInCone(rs.getDouble("sra"), rs.getDouble("sdec"), ra, dec, radius)) matched++;
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
                    long startUs = (GAIA_EPOCH_UNIX_MS + (long)(tw[0] * MS_PER_DAY)) * 1000;
                    long endUs = (GAIA_EPOCH_UNIX_MS + (long)(tw[1] * MS_PER_DAY)) * 1000;
                    String sql = String.format("SELECT ts FROM %s.hp_%d WHERE source_id=%d AND ts>=%d AND ts<=%d",
                            database, hpid, sourceId, startUs, endUs);
                    int rowCount = 0;
                    Connection c = queryConnections[(int)(Thread.currentThread().getId() % QUERY_THREADS)];
                    try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) rowCount++;
                    }
                    results.add(new int[]{rowCount});
                } catch (Exception e) {
                    results.add(new int[]{0});
                } finally { latch.countDown(); }
            });
        }
        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return results;
    }

    /** 批量并行聚合统计查询（自包含），返回 double[]（count, avgMag） */
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
                    String sql = String.format(
                            "SELECT COUNT(*),AVG(mag),MIN(mag),MAX(mag) FROM %s.hp_%d WHERE source_id=%d",
                            database, hpid, sourceId);
                    double count = 0, avgMag = 0;
                    Connection c = queryConnections[(int)(Thread.currentThread().getId() % QUERY_THREADS)];
                    try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                        if (rs.next()) { count = rs.getDouble(1); avgMag = rs.getDouble(2); }
                    }
                    results.add(new double[]{count, avgMag});
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
                    String sql = String.format("SELECT ts FROM %s.hp_%d WHERE source_id=%d AND band='%s'",
                            database, hpid, sourceId, band);
                    int rowCount = 0;
                    Connection c = queryConnections[(int)(Thread.currentThread().getId() % QUERY_THREADS)];
                    try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) rowCount++;
                    }
                    results.add(new int[]{rowCount});
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
    private static boolean isInCone(double ra1, double dec1, double ra2, double dec2, double radius) {
        double ra1R=Math.toRadians(ra1),dec1R=Math.toRadians(dec1),ra2R=Math.toRadians(ra2),dec2R=Math.toRadians(dec2);
        double a=Math.sin((dec2R-dec1R)/2)*Math.sin((dec2R-dec1R)/2)+Math.cos(dec1R)*Math.cos(dec2R)*Math.sin((ra2R-ra1R)/2)*Math.sin((ra2R-ra1R)/2);
        return Math.toDegrees(2*Math.atan2(Math.sqrt(a),Math.sqrt(1-a)))<=radius;
    }
    private Connection createConn() throws SQLException {
        Properties p=new Properties(); p.setProperty("user",System.getenv().getOrDefault("TDENGINE_USER","root")); p.setProperty("password",System.getenv().getOrDefault("TDENGINE_PASSWORD","taosdata"));
        return DriverManager.getConnection(String.format("jdbc:TAOS://%s:%d/?charset=UTF-8",host,port),p);
    }
    public void forceFlush(){try(Connection c=createConn();Statement s=c.createStatement()){s.execute("FLUSH DATABASE "+database);}catch(Exception e){}}
    public double getWriteAmplification(){long l=logicalBytesWritten.get();if(l<=0)return 1.0;long p=getDataDirSize()-initialDataDirSize;return p>0?(double)p/l:1.0;}
    private long getDataDirSize(){try{File d=new File(dataDirPath);return d.exists()?StorageUtils.getDirectorySize(dataDirPath):0;}catch(Exception e){return 0;}}
    public String getDatabase(){return database;}
    public String getFailureSummary(){return String.format("insertedRows=%d, failedRows=%d",insertedRows.get(),failedRows.get());}
    public boolean hasWriteFailures(){return failedRows.get()>0;}
    @Override public void close(){
        writePool.shutdown();queryPool.shutdown();
        try{writePool.awaitTermination(30,TimeUnit.SECONDS);}catch(InterruptedException e){}
        try{queryPool.awaitTermination(30,TimeUnit.SECONDS);}catch(InterruptedException e){}
        for(Connection c:threadConnections){try{if(c!=null)c.close();}catch(Exception e){}}
        for(Connection c:queryConnections){try{if(c!=null)c.close();}catch(Exception e){}}
    }
}