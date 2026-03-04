package org.example.wrapper;

import org.example.RocksDBServer.LightCurvePoint;
import org.example.utils.StorageUtils;

import org.rocksdb.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class NativeRocksDBWrapper implements AutoCloseable {
    private final String dbPath = "/mnt/nvme/home/wangxc/litecsdb/nativeRocksDB/";
    private RocksDB db;
    private Statistics statistics;
    private final AtomicLong logicalBytesWritten = new AtomicLong(0);
    private long initialDirSize = 0;

    public NativeRocksDBWrapper() {
        RocksDB.loadLibrary();
        this.statistics = new Statistics();

        try (final Options options = new Options()
                .setCreateIfMissing(true)
                .setWriteBufferSize(64 * 1024 * 1024) // 64MB 写入缓存
                .setMaxWriteBufferNumber(4)
                .setStatistics(statistics)) {
            this.db = RocksDB.open(options, dbPath);
            // 记录原生 RocksDB 启动时的目录初始大小
            this.initialDirSize = StorageUtils.getDirectorySize(dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("初始化原生 RocksDB 失败", e);
        }
    }

    public void putBatch(List<LightCurvePoint> points) {
        try (final WriteBatch batch = new WriteBatch();
             final WriteOptions writeOpts = new WriteOptions().setDisableWAL(true)) {

            long batchLogicalSize = points.size() * 90L;
            for (LightCurvePoint p : points) {
                // Key: sourceId|band|time
                byte[] key = String.format("%d|%s|%.6f", p.sourceId, p.band, p.time).getBytes();
                byte[] value = String.format("%.6f,%.6f,%.6f,%.6f,%d",
                        p.mag, p.flux, p.fluxError, p.fluxOverError, p.solutionId).getBytes();
                batch.put(key, value);
            }
            db.write(writeOpts, batch);
            logicalBytesWritten.addAndGet(batchLogicalSize);
        } catch (RocksDBException e) {
            System.err.println("原生 RocksDB 批量写入失败: " + e.getMessage());
        }
    }

    public void forceFlush() {
        try {
            db.flush(new FlushOptions().setWaitForFlush(true));
        } catch (RocksDBException e) {
            System.err.println("原生 RocksDB 刷盘失败: " + e.getMessage());
        }
    }

    public double getWriteAmplification() {
        long logical = logicalBytesWritten.get();
        if (logical == 0) return 1.0;

        // 🚀 使用初末目录大小的差值，作为真实的物理占用增量！
        long currentDirSize = StorageUtils.getDirectorySize(dbPath);
        long physical = currentDirSize - initialDirSize;

        return physical > 0 ? (double) physical / logical : 1.0;
    }

    @Override
    public void close() {
        if (db != null) db.close();
        if (statistics != null) statistics.close();
    }
}