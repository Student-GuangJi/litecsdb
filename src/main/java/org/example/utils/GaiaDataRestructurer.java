package org.example.utils;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GaiaDataRestructurer {

    // 基础路径配置
    private static final String BASE_DIR = "gaiadr2";
    private static final String COORD_FILE = BASE_DIR + "/source_coordinates.csv";
    private static final String LC_DIR = BASE_DIR + "/individual_lightcurves";
    private static final String OUT_DIR = BASE_DIR + "/observation_records_by_time";

    // 批处理大小：每处理多少个文件落盘一次。可根据需要调整。
    private static final int BATCH_SIZE = 50000;

    // 输出文件的完整表头
    private static final String OUTPUT_HEADER = "source_id,ra,dec,transit_id,band,time,mag,flux,flux_error,flux_over_error,rejected_by_photometry,rejected_by_variability,other_flags,solution_id";

    public static void main(String[] args) {
        System.out.println("开始执行数据重构任务...");
        long startTime = System.currentTimeMillis();

        // 1. 加载坐标数据到内存
        Map<String, String> coordinatesMap = loadCoordinates();
        if (coordinatesMap.isEmpty()) {
            System.err.println("坐标文件读取失败或为空，程序终止。");
            return;
        }

        // 2. 创建输出目录
        File outDirFile = new File(OUT_DIR);
        if (!outDirFile.exists() && !outDirFile.mkdirs()) {
            System.err.println("无法创建输出目录: " + OUT_DIR);
            return;
        }

        // 3. 处理光变曲线文件
        processLightcurves(coordinatesMap);

        long endTime = System.currentTimeMillis();
        System.out.println("数据重构完成！总耗时: " + (endTime - startTime) / 1000 + " 秒");
    }

    /**
     * 读取 source_coordinates.csv 并缓存为 Map
     */
    private static Map<String, String> loadCoordinates() {
        System.out.println("正在加载坐标数据...");
        Map<String, String> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(COORD_FILE))) {
            String line = br.readLine(); // 跳过表头
            while ((line = br.readLine()) != null) {
                // line 格式: source_id,ra,dec
                int firstCommaIndex = line.indexOf(',');
                if (firstCommaIndex > 0) {
                    String sourceId = line.substring(0, firstCommaIndex);
                    String raDec = line.substring(firstCommaIndex + 1);
                    map.put(sourceId, raDec);
                }
            }
            System.out.println("成功加载 " + map.size() + " 个天体的坐标。");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    /**
     * 遍历并处理所有光变曲线文件
     */
    private static void processLightcurves(Map<String, String> coordinatesMap) {
        System.out.println("开始处理光变曲线文件...");
        File dir = new File(LC_DIR);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".csv"));

        if (files == null || files.length == 0) {
            System.out.println("未找到任何光变曲线文件！");
            return;
        }

        System.out.println("共发现 " + files.length + " 个文件待处理。");

        // 内存缓冲区：按整数时间分类存储组装好的CSV行
        Map<Integer, List<String>> timeBuffer = new HashMap<>();
        int processedCount = 0;

        for (File file : files) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line = br.readLine(); // 跳过表头
                while ((line = br.readLine()) != null) {
                    // 解析行数据
                    String[] parts = line.split(",");
                    if (parts.length < 12) continue; // 格式不完整的数据跳过

                    String sourceId = parts[0];
                    String timeStr = parts[3];

                    // 获取时间的整数部分
                    int integerTime = (int) Math.floor(Double.parseDouble(timeStr));

                    // 获取对应的 ra, dec
                    String raDec = coordinatesMap.getOrDefault(sourceId, ",");

                    // 组装新的行: source_id, ra, dec, transit_id, band, time, ...
                    StringBuilder newLine = new StringBuilder();
                    newLine.append(sourceId).append(",").append(raDec);
                    for (int i = 1; i < parts.length; i++) {
                        newLine.append(",").append(parts[i]);
                    }

                    // 加入对应的缓冲区
                    timeBuffer.computeIfAbsent(integerTime, k -> new ArrayList<>()).add(newLine.toString());
                }
            } catch (IOException | NumberFormatException e) {
                System.err.println("处理文件 " + file.getName() + " 时出错: " + e.getMessage());
            }

            processedCount++;

            // 达到批次大小，落盘以释放内存
            if (processedCount % BATCH_SIZE == 0) {
                flushBufferToDisk(timeBuffer);
                System.out.println("已处理 " + processedCount + " 个文件...");
            }
        }

        // 将最后剩余的缓冲数据落盘
        if (!timeBuffer.isEmpty()) {
            flushBufferToDisk(timeBuffer);
            System.out.println("已处理全部 " + processedCount + " 个文件。");
        }
    }

    /**
     * 将缓冲区内的数据写入对应时间的 CSV 文件中（追加模式）
     */
    private static void flushBufferToDisk(Map<Integer, List<String>> timeBuffer) {
        for (Map.Entry<Integer, List<String>> entry : timeBuffer.entrySet()) {
            int timeKey = entry.getKey();
            List<String> records = entry.getValue();

            File outFile = new File(OUT_DIR, "time_" + timeKey + ".csv");
            boolean isNewFile = !outFile.exists();

            // 使用追加模式写入 (true)
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(outFile, true))) {
                // 如果是新创建的文件，先写入表头
                if (isNewFile) {
                    bw.write(OUTPUT_HEADER);
                    bw.newLine();
                }

                // 批量写入数据
                for (String record : records) {
                    bw.write(record);
                    bw.newLine();
                }
            } catch (IOException e) {
                System.err.println("写入文件 " + outFile.getName() + " 时出错: " + e.getMessage());
            }
        }
        // 落盘后清空缓冲区，释放内存
        timeBuffer.clear();
    }
}