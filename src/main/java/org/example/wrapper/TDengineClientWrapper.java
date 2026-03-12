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
    private static final int QUERY_THREADS = 8;
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
            s.execute("DROP DATABASE IF EXISTS " + database);
            s.execute(String.format("CREATE DATABASE IF NOT EXISTS %s VGROUPS %d BUFFER 256 WAL_LEVEL 1 WAL_FSYNC_PERIOD 3000 PAGES 256 PRECISION 'us'", database, VGROUPS));
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

    public void clearQueryCache() {
        try {
            for (int i = 0; i < QUERY_THREADS; i++) { if (queryConnections[i]!=null) try{queryConnections[i].close();}catch(Exception e){} queryConnections[i]=createConn(); queryConnections[i].createStatement().execute("USE "+database); }
            try(Connection c=createConn();Statement s=c.createStatement()){s.execute("RESET QUERY CACHE");}catch(Exception e){}
        } catch (Exception e) {}
    }

    public List<LightCurvePoint> executeSimpleQuery(long sourceId) {
        List<LightCurvePoint> rows = new ArrayList<>();
        for (long hpid : createdStables) {
            String sql = String.format("SELECT ts,source_id,ra,dec_val,band,transit_id,obs_time,mag,flux,flux_error,flux_over_error,rejected_by_photometry,rejected_by_variability,other_flags,solution_id FROM %s.hp_%d WHERE source_id=%d ORDER BY ts", database, hpid, sourceId);
            try (Statement stmt = queryConnections[0].createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) rows.add(new LightCurvePoint(rs.getLong("source_id"),rs.getDouble("ra"),rs.getDouble("dec_val"),rs.getLong("transit_id"),rs.getString("band").trim(),rs.getDouble("obs_time"),rs.getDouble("mag"),rs.getDouble("flux"),rs.getDouble("flux_error"),rs.getDouble("flux_over_error"),rs.getBoolean("rejected_by_photometry"),rs.getBoolean("rejected_by_variability"),rs.getInt("other_flags"),rs.getLong("solution_id")));
            } catch (SQLException e) {}
            if (!rows.isEmpty()) break;
        }
        return rows;
    }

    public List<Long> executeSTMKQuery(double ra, double dec, double radius, double startTime, double endTime, double magMin, double magMax, int minObs) {
        long startUs = (GAIA_EPOCH_UNIX_MS + (long)(startTime * MS_PER_DAY)) * 1000;
        long endUs = (GAIA_EPOCH_UNIX_MS + (long)(endTime * MS_PER_DAY)) * 1000;
        Set<Long> targetHpids = calcHealpixCone(ra, dec, radius);
        if (targetHpids.isEmpty()) return Collections.emptyList();
        List<Long> matched = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(targetHpids.size());
        for (long hpid : targetHpids) {
            queryPool.submit(() -> {
                try {
                    Connection c = queryConnections[(int)(hpid % QUERY_THREADS)];
                    String sql = String.format(Locale.US, "SELECT source_id,FIRST(ra) as sra,FIRST(dec_val) as sdec,COUNT(*) as cnt FROM %s.hp_%d WHERE ts>=%d AND ts<=%d AND mag>=%f AND mag<=%f GROUP BY source_id HAVING COUNT(*)>=%d", database, hpid, startUs, endUs, magMin, magMax, minObs);
                    try (Statement stmt = c.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
                        while (rs.next()) { if (isInCone(rs.getDouble("sra"),rs.getDouble("sdec"),ra,dec,radius)) matched.add(rs.getLong("source_id")); }
                    }
                } catch (Exception e) {} finally { latch.countDown(); }
            });
        }
        try { latch.await(300, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return matched;
    }

    private Set<Long> calcHealpixCone(double ra, double dec, double radius) {
        Set<Long> ids = new HashSet<>();
        try { new HealpixUtil().setNside(HEALPIX_LEVEL); RangeSet rs = HealpixUtil.queryDisc(ra, dec, radius, HEALPIX_LEVEL);
            if (rs != null) for (int i = 0; i < rs.nranges(); i++) for (long p = rs.ivbegin(i); p <= rs.ivend(i); p++) ids.add(p);
        } catch (Exception e) {} return ids;
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
        try(Connection c=createConn();Statement s=c.createStatement()){s.execute("DROP DATABASE IF EXISTS "+database);}catch(Exception e){}
    }
}