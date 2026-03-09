package org.example;

import org.rocksdb.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RocksDB 全局资源管理器
 *
 * 配置对齐（所有系统统一）：
 *   WRITE_THREADS           = 16  （并行写入线程数）
 *   DISTRIBUTION_PARALLELISM = 32  （分发并行度）
 *   BATCH_SIZE              = 10000（每批写入行数）
 *   MEMTABLE_FLUSH_THRESHOLD = 256MB（由 RocksDB 原生 memtable 管理）
 */
public class RocksDBGlobalResourceManager implements AutoCloseable {
    private static volatile RocksDBGlobalResourceManager instance;

    // ==================== 全局配置常量 ====================

    public static final int WRITE_THREADS = 16;
    public static final int DISTRIBUTION_PARALLELISM = 32;
    public static final int BATCH_SIZE = 10_000;
    public static final long MEMTABLE_FLUSH_THRESHOLD = 256L * 1024 * 1024;

    // ==================== 共享 JNI 资源 ====================

    private final Cache blockCache;
    private final WriteBufferManager writeBufferManager;
    private final Env env;

    /** 写入线程池：16 线程并行写入 */
    private final ExecutorService writeThreadPool;

    /** 分发线程池：32 线程并行分发 */
    private final ExecutorService distributionThreadPool;

    private RocksDBGlobalResourceManager(long cacheCapacity, long writeBufferCapacity) {
        RocksDB.loadLibrary();

        this.blockCache = new LRUCache(cacheCapacity, -1, false, 0.0);
        this.writeBufferManager = new WriteBufferManager(writeBufferCapacity, blockCache);

        this.env = Env.getDefault();
        this.env.setBackgroundThreads(WRITE_THREADS, Priority.HIGH);
        this.env.setBackgroundThreads(WRITE_THREADS, Priority.LOW);

        this.writeThreadPool = Executors.newFixedThreadPool(WRITE_THREADS,
                new DaemonThreadFactory("RocksDB-Writer"));

        this.distributionThreadPool = Executors.newFixedThreadPool(DISTRIBUTION_PARALLELISM,
                new DaemonThreadFactory("RocksDB-Distributor"));
    }

    public static RocksDBGlobalResourceManager getInstance() {
        if (instance == null) {
            synchronized (RocksDBGlobalResourceManager.class) {
                if (instance == null) {
                    instance = new RocksDBGlobalResourceManager(
                            100L * 1024 * 1024 * 1024,
                            20L * 1024 * 1024 * 1024
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

    @Override
    public void close() {
        shutdownPool(writeThreadPool, "WriteThreadPool");
        shutdownPool(distributionThreadPool, "DistributionThreadPool");
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