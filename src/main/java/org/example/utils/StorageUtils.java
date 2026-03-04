package org.example.utils;

import java.io.File;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class StorageUtils {
    /**
     * 获取指定目录的物理占用大小（字节）
     */
    public static long getDirectorySize(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("目录不存在或无权限访问: " + dirPath);
            return 0;
        }

        AtomicLong size = new AtomicLong(0);
        try {
            Files.walkFileTree(Paths.get(dirPath), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    size.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            System.err.println("计算目录大小出错: " + e.getMessage());
        }
        return size.get();
    }
}