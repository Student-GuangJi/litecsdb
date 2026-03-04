package org.example.utils;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class DatasetGenerator {

    // 数据量阶梯设置
    private static final long[] TARGET_SIZES = {
            2_000_000L, 4_000_000L, 8_000_000L, 16_000_000L,
            32_000_000L, 64_000_000L, 128_000_000L
    };

    private static final int TOTAL_SOURCES = 549371;
    private static final Random RANDOM = new Random();

    // 内部类：代表一行观测记录
    static class Observation {
        String sourceId, ra, dec, transitId, band;
        double time, mag, flux, fluxError, fluxOverError;
        String rejectedPhot, rejectedVar, otherFlags, solutionId;

        // 生成 CSV 行
        public String toCsvLine(boolean includeRaDec) {
            if (includeRaDec) {
                return String.format(Locale.US, "%s,%s,%s,%s,%s,%.6f,%.6f,%.6f,%.6f,%.6f,%s,%s,%s,%s",
                        sourceId, ra, dec, transitId, band, time, mag, flux, fluxError, fluxOverError,
                        rejectedPhot, rejectedVar, otherFlags, solutionId);
            } else {
                return String.format(Locale.US, "%s,%s,%s,%.6f,%.6f,%.6f,%.6f,%.6f,%s,%s,%s,%s",
                        sourceId, transitId, band, time, mag, flux, fluxError, fluxOverError,
                        rejectedPhot, rejectedVar, otherFlags, solutionId);
            }
        }
    }

    // 内存数据池
    private static final Map<String, List<Observation>> memoryPool = new HashMap<>();

    public static void main(String[] args) throws Exception {
        System.out.println("====== 开始初始化超大内存数据池 ======");
        long startTime = System.currentTimeMillis();

        // 调用加载方法，从之前重构好的目录读取数据
        loadAllDataToMemory("gaiadr2/observation_records_by_time");

        long loadTime = System.currentTimeMillis();
        System.out.println("真实数据加载完毕！耗时: " + (loadTime - startTime) / 1000 + " 秒");
        System.out.println("内存池中共有 " + memoryPool.size() + " 个天体的数据。开始按梯度生成数据集...");

        for (long targetSize : TARGET_SIZES) {
            System.out.println("\n正在生成量级: " + targetSize + " 条数据...");
            generateBatchDataset(targetSize);
            generateIncrementalDataset(targetSize);
        }
    }

    /**
     * 将重构后的时序文件加载到超大内存池中
     * * @param inputDir 之前重构输出的 observation_records_by_time 文件夹路径
     */
    private static void loadAllDataToMemory(String inputDir) {
        File dir = new File(inputDir);
        File[] files = dir.listFiles((d, name) -> name.endsWith(".csv"));

        if (files == null || files.length == 0) {
            System.err.println("未找到重构后的数据文件，请检查路径：" + inputDir);
            return;
        }

        // 按照文件名（即时间）进行排序，确保存入 memoryPool 的 List 是按时间升序的
        Arrays.sort(files, Comparator.comparing(File::getName));

        int fileCount = 0;
        for (File file : files) {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line = br.readLine(); // 跳过表头
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",", -1);
                    if (parts.length < 14) continue;

                    Observation obs = new Observation();
                    obs.sourceId = parts[0];
                    obs.ra = parts[1];
                    obs.dec = parts[2];
                    obs.transitId = parts[3];
                    obs.band = parts[4];
                    obs.time = Double.parseDouble(parts[5]);
                    obs.mag = Double.parseDouble(parts[6]);
                    obs.flux = Double.parseDouble(parts[7]);
                    obs.fluxError = Double.parseDouble(parts[8]);
                    obs.fluxOverError = Double.parseDouble(parts[9]);
                    obs.rejectedPhot = parts[10];
                    obs.rejectedVar = parts[11];
                    obs.otherFlags = parts[12];
                    obs.solutionId = parts[13];

                    // 将对象加入内存池。computeIfAbsent 会在键不存在时自动创建一个新的 ArrayList
                    memoryPool.computeIfAbsent(obs.sourceId, k -> new ArrayList<>()).add(obs);
                }
            } catch (IOException | NumberFormatException e) {
                System.err.println("读取文件 " + file.getName() + " 时出错：" + e.getMessage());
            }

            fileCount++;
            if (fileCount % 10 == 0) {
                System.out.println("已加载 " + fileCount + " 个时间分区文件...");
            }
        }
    }

    /**
     * 场景 1：保持 54万个文件不变，根据时间长短截取/扩展数据
     */
    private static void generateBatchDataset(long targetSize) {
        String baseDir = "generated_datasets/batch_" + targetSize + "/individual_lightcurves";
        new File(baseDir).mkdirs();

        // 计算每个文件需要多少行数据
        long rowsPerFile = (long) Math.ceil((double) targetSize / TOTAL_SOURCES);
        long currentTotal = 0;

        for (Map.Entry<String, List<Observation>> entry : memoryPool.entrySet()) {
            if (currentTotal >= targetSize) break;

            String sourceId = entry.getKey();
            List<Observation> realObs = entry.getValue(); // 这里的数据应该是按 time 升序的

            long rowsToWrite = Math.min(rowsPerFile, targetSize - currentTotal);

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(baseDir + "/lightcurve_" + sourceId + ".csv"))) {
                bw.write("source_id,transit_id,band,time,mag,flux,flux_error,flux_over_error,rejected_by_photometry,rejected_by_variability,other_flags,solution_id\n");

                Observation lastObs = null;
                for (int i = 0; i < rowsToWrite; i++) {
                    if (i < realObs.size()) {
                        // 写入真实数据
                        lastObs = realObs.get(i);
                        bw.write(lastObs.toCsvLine(false) + "\n");
                    } else {
                        // 真实数据耗尽，科学模拟未来数据
                        lastObs = simulateNextObservation(lastObs);
                        bw.write(lastObs.toCsvLine(false) + "\n");
                    }
                    currentTotal++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 场景 2：按时间点生成，同一个 time 文件内先塞满 54万天体，不够再增加 time
     */
    private static void generateIncrementalDataset(long targetSize) {
        String baseDir = "generated_datasets/incremental_" + targetSize + "/observation_records_by_time";
        new File(baseDir).mkdirs();

        long currentTotal = 0;
        int currentTimePoint = 1800; // 假设数据从 1800 开始

        while (currentTotal < targetSize) {
            String fileName = baseDir + "/time_" + currentTimePoint + ".csv";

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileName))) {
                bw.write("source_id,ra,dec,transit_id,band,time,mag,flux,flux_error,flux_over_error,rejected_by_photometry,rejected_by_variability,other_flags,solution_id\n");

                for (String sourceId : memoryPool.keySet()) {
                    if (currentTotal >= targetSize) break;

                    // 寻找该天体在当前时间点是否有真实数据
                    Observation obs = findObservationByTime(sourceId, currentTimePoint);
                    if (obs == null) {
                        // 如果没有，基于它最近的数据模拟出一个此刻的观测值
                        obs = simulateObservationForSpecificTime(sourceId, currentTimePoint);
                    }

                    bw.write(obs.toCsvLine(true) + "\n");
                    currentTotal++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            // 当前时间点填满（或包含所有天体后），进入下一个时间周期
            currentTimePoint++;
        }
    }

    /**
     * 科学模拟算法：基于上一条数据，生成下一条合理的观测记录
     */
    private static Observation simulateNextObservation(Observation lastObs) {
        Observation next = new Observation();
        next.sourceId = lastObs.sourceId;
        next.ra = lastObs.ra;
        next.dec = lastObs.dec;
        next.transitId = String.valueOf(Long.parseLong(lastObs.transitId) + RANDOM.nextInt(1000));
        next.band = lastObs.band;

        // 1. 时间推演：Gaia 观测间隔增加 0.5 到 3.5 天
        next.time = lastObs.time + 0.5 + (RANDOM.nextDouble() * 3.0);

        // 2. Flux 波动：正态分布波动 1% (0.01)
        double noiseFactor = 1.0 + RANDOM.nextGaussian() * 0.01;
        next.flux = lastObs.flux * noiseFactor;

        // 3. Mag 计算：物理对数公式 Mag_new = Mag_old - 2.5 * log10(Flux_new / Flux_old)
        next.mag = lastObs.mag - 2.5 * Math.log10(next.flux / lastObs.flux);

        // 其他字段保持或微调
        next.fluxError = lastObs.fluxError * (1.0 + RANDOM.nextGaussian() * 0.05);
        next.fluxOverError = next.flux / next.fluxError;
        next.rejectedPhot = lastObs.rejectedPhot;
        next.rejectedVar = lastObs.rejectedVar;
        next.otherFlags = lastObs.otherFlags;
        next.solutionId = lastObs.solutionId;

        return next;
    }

    /**
     * 使用二分查找在天体的历史记录中快速定位指定时间（整数部分匹配）的观测数据
     * * @param sourceId 天体唯一标识
     * @param integerTime 目标时间的整数部分
     * @return 匹配的 Observation 对象，如果没有则返回 null
     */
    private static Observation findObservationByTime(String sourceId, int integerTime) {
        List<Observation> obsList = memoryPool.get(sourceId);

        // 如果这个天体没有任何数据，直接返回 null
        if (obsList == null || obsList.isEmpty()) {
            return null;
        }

        int low = 0;
        int high = obsList.size() - 1;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            Observation midObs = obsList.get(mid);

            // 获取中间元素的整数时间
            int midIntegerTime = (int) Math.floor(midObs.time);

            if (midIntegerTime == integerTime) {
                return midObs; // 找到了完全匹配的时间段数据
            } else if (midIntegerTime < integerTime) {
                low = mid + 1; // 目标在右半侧
            } else {
                high = mid - 1; // 目标在左半侧
            }
        }

        // 如果二分查找结束依然没找到，说明该天体在这一时间整数段内没有真实观测记录
        return null;
    }

    // 辅助方法：模拟特定时间的数据
    private static Observation simulateObservationForSpecificTime(String sourceId, int integerTime) {
        Observation baseObs = memoryPool.get(sourceId).get(0); // 取第一条作基准
        Observation fakeObs = simulateNextObservation(baseObs);
        fakeObs.time = integerTime + RANDOM.nextDouble(); // 强行落在该时间区间
        return fakeObs;
    }
}