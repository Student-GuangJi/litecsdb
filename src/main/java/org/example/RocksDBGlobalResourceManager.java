package org.example;

import org.rocksdb.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * RocksDB 全局资源管理器
 * 统筹管理所有 HEALPix 分区实例共享的资源。
 */
public class RocksDBGlobalResourceManager implements AutoCloseable {
    private static volatile RocksDBGlobalResourceManager instance;

    // 共享的核心 JNI 资源
    private final Cache blockCache;
    private final WriteBufferManager writeBufferManager;
    private final Env env;

    // 🚀 新增：应用层定制的异步 Flush 线程池 (专门用于后台切分 SST 并 Ingest)
    private final ExecutorService customAsyncFlushPool;

    private RocksDBGlobalResourceManager(long cacheCapacity, long writeBufferCapacity) {
        RocksDB.loadLibrary();

        this.blockCache = new LRUCache(cacheCapacity, -1, false, 0.0);
        this.writeBufferManager = new WriteBufferManager(writeBufferCapacity, blockCache);

        this.env = Env.getDefault();
        this.env.setBackgroundThreads(8, Priority.HIGH);       // 原生 Flush 线程池
        this.env.setBackgroundThreads(16, Priority.LOW);       // 原生 Compaction 线程池

        // 🚀 初始化应用层定制 Flush 线程池 (榨干多核 CPU)
        this.customAsyncFlushPool = Executors.newFixedThreadPool(16, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("RocksDB-Custom-Flush-" + t.getId());
                return t;
            }
        });
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

    // 🚀 暴露定制的线程池给 RocksDBServer 使用
    public ExecutorService getCustomAsyncFlushPool() { return customAsyncFlushPool; }

    @Override
    public void close() {
        if (customAsyncFlushPool != null) {
            customAsyncFlushPool.shutdown();
            try {
                customAsyncFlushPool.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {}
        }
        if (writeBufferManager != null) writeBufferManager.close();
        if (blockCache != null) blockCache.close();
        if (env != null) env.close();
    }
}