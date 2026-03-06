package org.example;

import org.rocksdb.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RocksDB Global Resource Manager
 *
 * Aligned to TDengine C++ configuration:
 *   NUM_THREADS  = 16  -> WRITE_THREADS (client-side parallel writers)
 *   NUM_VGROUPS  = 32  -> DISTRIBUTION_PARALLELISM (healpix dispatch threads)
 *   BATCH_SIZE   = 10000 -> BATCH_SIZE (points per write batch)
 *   BUFFER_SIZE  = 256MB -> MEMTABLE_FLUSH_THRESHOLD per partition
 *
 * Total memory budget: managed by writeBufferManager (shared across all partitions)
 */
public class RocksDBGlobalResourceManager implements AutoCloseable {
    private static volatile RocksDBGlobalResourceManager instance;

    // ==================== Config constants (aligned to TDengine) ====================

    /** Client-side parallel write threads (TDengine NUM_THREADS = 16) */
    public static final int WRITE_THREADS = 16;

    /** Distribution parallelism for healpix dispatch (TDengine NUM_VGROUPS = 32) */
    public static final int DISTRIBUTION_PARALLELISM = 32;

    /** Points per write batch (TDengine BATCH_SIZE = 10000) */
    public static final int BATCH_SIZE = 10_000;

    /** Per-partition memtable flush threshold (TDengine BUFFER = 256MB per vgroup) */
    public static final long MEMTABLE_FLUSH_THRESHOLD = 256L * 1024 * 1024;

    // ==================== Shared JNI resources ====================

    private final Cache blockCache;
    private final WriteBufferManager writeBufferManager;
    private final Env env;

    /** Write thread pool: 16 threads for parallel putLightCurveBatch calls */
    private final ExecutorService writeThreadPool;

    /** Distribution thread pool: 32 threads for healpix dispatch */
    private final ExecutorService distributionThreadPool;

    /** Custom async flush pool for background SST ingest */
    private final ExecutorService customAsyncFlushPool;
    private final Semaphore flushConcurrencyLimiter;

    private RocksDBGlobalResourceManager(long cacheCapacity, long writeBufferCapacity) {
        RocksDB.loadLibrary();

        this.blockCache = new LRUCache(cacheCapacity, -1, false, 0.0);
        this.writeBufferManager = new WriteBufferManager(writeBufferCapacity, blockCache);

        this.env = Env.getDefault();
        this.env.setBackgroundThreads(8, Priority.HIGH);   // native flush threads
        this.env.setBackgroundThreads(WRITE_THREADS, Priority.LOW); // native compaction threads

        // Write thread pool: 16 threads (aligned to TDengine NUM_THREADS)
        this.writeThreadPool = Executors.newFixedThreadPool(WRITE_THREADS,
                new DaemonThreadFactory("RocksDB-Writer"));

        // Distribution thread pool: 32 threads (aligned to TDengine NUM_VGROUPS)
        this.distributionThreadPool = Executors.newFixedThreadPool(DISTRIBUTION_PARALLELISM,
                new DaemonThreadFactory("RocksDB-Distributor"));

        // Async flush pool for background SST generation
        this.customAsyncFlushPool = Executors.newFixedThreadPool(WRITE_THREADS,
                new DaemonThreadFactory("RocksDB-Custom-Flush"));
        this.flushConcurrencyLimiter = new Semaphore(8, true);
    }

    public static RocksDBGlobalResourceManager getInstance() {
        if (instance == null) {
            synchronized (RocksDBGlobalResourceManager.class) {
                if (instance == null) {
                    instance = new RocksDBGlobalResourceManager(
                            100L * 1024 * 1024 * 1024,  // 100GB block cache
                            20L * 1024 * 1024 * 1024     // 20GB write buffer budget
                    );
                }
            }
        }
        return instance;
    }

    public Cache getBlockCache() { return blockCache; }
    public WriteBufferManager getWriteBufferManager() { return writeBufferManager; }
    public Env getEnv() { return env; }
    public ExecutorService getWriteThreadPool() { return writeThreadPool; }
    public ExecutorService getDistributionThreadPool() { return distributionThreadPool; }
    public ExecutorService getCustomAsyncFlushPool() { return customAsyncFlushPool; }
    public Semaphore getFlushConcurrencyLimiter() { return flushConcurrencyLimiter; }

    @Override
    public void close() {
        shutdownPool(writeThreadPool, "WriteThreadPool");
        shutdownPool(distributionThreadPool, "DistributionThreadPool");
        shutdownPool(customAsyncFlushPool, "CustomAsyncFlushPool");
        if (writeBufferManager != null) writeBufferManager.close();
        if (blockCache != null) blockCache.close();
        if (env != null) env.close();
    }

    private void shutdownPool(ExecutorService pool, String name) {
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException e) {
                pool.shutdownNow();
            }
        }
    }

    private static class DaemonThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(0);

        DaemonThreadFactory(String prefix) { this.prefix = prefix; }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName(prefix + "-" + counter.getAndIncrement());
            return t;
        }
    }
}