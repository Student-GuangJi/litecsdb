package org.example.exp;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 消融实验结果分析器
 *
 * 读取 FlushAblationExperiment 输出的 CSV，生成：
 *   1. 按策略×错序率汇总的统计表（均值±标准差）
 *   2. 论文 LaTeX 表格（可直接粘贴）
 *   3. gnuplot/matplotlib 绘图数据文件
 *
 * 使用：
 *   java -cp ... org.example.FlushAblationAnalyzer \
 *       --input results/flush_ablation_xxx.csv \
 *       --output results/
 */
public class FlushAblationAnalyzer {

    public static void main(String[] args) throws Exception {
        String inputCsv = null, outputDir = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--input":  inputCsv = args[++i]; break;
                case "--output": outputDir = args[++i]; break;
            }
        }
        if (inputCsv == null) {
            System.err.println("Usage: --input <csv> [--output <dir>]");
            System.exit(1);
        }
        if (outputDir == null) outputDir = new File(inputCsv).getParent();

        List<Record> records = loadCsv(inputCsv);
        System.out.printf("Loaded %d records%n", records.size());

        // 按 (strategy, disorderRate) 分组
        Map<String, List<Record>> groups = records.stream()
                .collect(Collectors.groupingBy(r -> r.strategy + "|" + r.disorderRate));

        // 输出汇总表
        printSummaryTable(groups);

        // 输出 LaTeX 表格
        generateLatexWATable(groups, outputDir);
        generateLatexThroughputTable(groups, outputDir);
        generateLatexReadTable(groups, outputDir);

        // 输出绘图数据
        generatePlotData(groups, outputDir);
    }

    // ==================== CSV 加载 ====================

    static List<Record> loadCsv(String path) throws IOException {
        List<Record> records = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String header = br.readLine(); // skip header
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length < 14) continue;
                Record r = new Record();
                r.strategy = parts[0];
                r.disorderRate = Double.parseDouble(parts[1]);
                r.round = Integer.parseInt(parts[2]);
                r.writeTimeMs = Long.parseLong(parts[3]);
                r.throughput = Long.parseLong(parts[4]);
                r.waRatio = Double.parseDouble(parts[5]);
                r.saRatio = Double.parseDouble(parts[6]);
                r.compactionBytesWritten = Long.parseLong(parts[7]);
                r.flushBytesWritten = Long.parseLong(parts[8]);
                r.readLatencyMedianUs = Double.parseDouble(parts[9]);
                r.readLatencyP99Us = Double.parseDouble(parts[10]);
                r.readAvgSstHits = Double.parseDouble(parts[11]);
                r.dirSizeBytes = Long.parseLong(parts[12]);
                r.logicalBytes = Long.parseLong(parts[13]);
                records.add(r);
            }
        }
        return records;
    }

    // ==================== 汇总打印 ====================

    static void printSummaryTable(Map<String, List<Record>> groups) {
        System.out.println("\n===== Summary Table =====");
        System.out.printf("%-10s %6s %10s %12s %8s %8s %12s %12s%n",
                "Strategy", "r", "WA", "Throughput", "SA",
                "WriteMs", "ReadMed_us", "ReadP99_us");
        System.out.println("-".repeat(90));

        String[] strategies = {"DEFAULT", "FRAG", "FRAG_BA"};
        double[] rates = {0.0, 0.05, 0.10, 0.20, 0.50};

        for (String s : strategies) {
            for (double r : rates) {
                String key = s + "|" + r;
                List<Record> recs = groups.get(key);
                if (recs == null || recs.isEmpty()) continue;

                double avgWA = recs.stream().mapToDouble(x -> x.waRatio).average().orElse(0);
                double avgTP = recs.stream().mapToLong(x -> x.throughput).average().orElse(0);
                double avgSA = recs.stream().mapToDouble(x -> x.saRatio).average().orElse(0);
                double avgWT = recs.stream().mapToLong(x -> x.writeTimeMs).average().orElse(0);
                double avgRdMed = recs.stream().mapToDouble(x -> x.readLatencyMedianUs).average().orElse(0);
                double avgRdP99 = recs.stream().mapToDouble(x -> x.readLatencyP99Us).average().orElse(0);

                System.out.printf("%-10s %6.2f %10.3f %,12.0f %8.3f %,8.0f %12.1f %12.1f%n",
                        s, r, avgWA, avgTP, avgSA, avgWT, avgRdMed, avgRdP99);
            }
            System.out.println();
        }
    }

    // ==================== LaTeX 表格生成 ====================

    /**
     * 表1：不同 Flush 策略在各错序率下的写放大
     */
    static void generateLatexWATable(Map<String, List<Record>> groups,
                                     String outputDir) throws IOException {
        try (PrintWriter pw = new PrintWriter(
                new FileWriter(outputDir + "/table_wa.tex"))) {
            pw.println("\\begin{table}[htbp]");
            pw.println("\\centering");
            pw.println("\\caption{不同Flush策略在各错序率下的写放大}");
            pw.println("\\label{tab:ablation-wa}");
            pw.println("\\begin{tabular}{crrrrrr}");
            pw.println("\\toprule");
            pw.println("\\multirow{2}{*}{错序率} "
                    + "& \\multicolumn{2}{c}{\\textbf{DEFAULT}} "
                    + "& \\multicolumn{2}{c}{\\textbf{FRAG}} "
                    + "& \\multicolumn{2}{c}{\\textbf{FRAG+BA}} \\\\");
            pw.println("\\cmidrule(lr){2-3} \\cmidrule(lr){4-5} \\cmidrule(lr){6-7}");
            pw.println(" & WA & $\\sigma$ & WA & $\\sigma$ & WA & $\\sigma$ \\\\");
            pw.println("\\midrule");

            double[] rates = {0.0, 0.05, 0.10, 0.20, 0.50};
            String[] strategies = {"DEFAULT", "FRAG", "FRAG_BA"};

            for (double r : rates) {
                pw.printf("%.2f", r);
                for (String s : strategies) {
                    String key = s + "|" + r;
                    List<Record> recs = groups.getOrDefault(key, Collections.emptyList());
                    if (recs.isEmpty()) {
                        pw.print(" & -- & --");
                    } else {
                        double avg = recs.stream().mapToDouble(x -> x.waRatio).average().orElse(0);
                        double std = computeStd(recs.stream().mapToDouble(x -> x.waRatio).toArray());
                        pw.printf(" & %.3f & %.3f", avg, std);
                    }
                }
                pw.println(" \\\\");
            }

            pw.println("\\bottomrule");
            pw.println("\\end{tabular}");
            pw.println("\\end{table}");
        }
        System.out.println("Generated: " + outputDir + "/table_wa.tex");
    }

    /**
     * 表2：不同 Flush 策略在各错序率下的写入吞吐量
     */
    static void generateLatexThroughputTable(Map<String, List<Record>> groups,
                                             String outputDir) throws IOException {
        try (PrintWriter pw = new PrintWriter(
                new FileWriter(outputDir + "/table_throughput.tex"))) {
            pw.println("\\begin{table}[htbp]");
            pw.println("\\centering");
            pw.println("\\caption{不同Flush策略在各错序率下的写入吞吐量（rec/s）}");
            pw.println("\\label{tab:ablation-throughput}");
            pw.println("\\begin{tabular}{crrrrrr}");
            pw.println("\\toprule");
            pw.println("\\multirow{2}{*}{错序率} "
                    + "& \\multicolumn{2}{c}{\\textbf{DEFAULT}} "
                    + "& \\multicolumn{2}{c}{\\textbf{FRAG}} "
                    + "& \\multicolumn{2}{c}{\\textbf{FRAG+BA}} \\\\");
            pw.println("\\cmidrule(lr){2-3} \\cmidrule(lr){4-5} \\cmidrule(lr){6-7}");
            pw.println(" & 均值 & $\\sigma$ & 均值 & $\\sigma$ & 均值 & $\\sigma$ \\\\");
            pw.println("\\midrule");

            double[] rates = {0.0, 0.05, 0.10, 0.20, 0.50};
            String[] strategies = {"DEFAULT", "FRAG", "FRAG_BA"};

            for (double r : rates) {
                pw.printf("%.2f", r);
                for (String s : strategies) {
                    String key = s + "|" + r;
                    List<Record> recs = groups.getOrDefault(key, Collections.emptyList());
                    if (recs.isEmpty()) {
                        pw.print(" & -- & --");
                    } else {
                        double avg = recs.stream().mapToLong(x -> x.throughput).average().orElse(0);
                        double std = computeStd(recs.stream()
                                .mapToDouble(x -> (double) x.throughput).toArray());
                        pw.printf(" & %,.0f & %,.0f", avg, std);
                    }
                }
                pw.println(" \\\\");
            }

            pw.println("\\bottomrule");
            pw.println("\\end{tabular}");
            pw.println("\\end{table}");
        }
        System.out.println("Generated: " + outputDir + "/table_throughput.tex");
    }

    /**
     * 表3：读取性能对比（固定 r=0.20）
     */
    static void generateLatexReadTable(Map<String, List<Record>> groups,
                                       String outputDir) throws IOException {
        try (PrintWriter pw = new PrintWriter(
                new FileWriter(outputDir + "/table_read.tex"))) {
            pw.println("\\begin{table}[htbp]");
            pw.println("\\centering");
            pw.println("\\caption{不同Flush策略下的单天体读取性能（错序率$r=0.20$）}");
            pw.println("\\label{tab:ablation-read}");
            pw.println("\\begin{tabular}{lrrr}");
            pw.println("\\toprule");
            pw.println("策略 & 读取延迟中位数 ($\\mu$s) & P99 ($\\mu$s) & 平均触达SST数 \\\\");
            pw.println("\\midrule");

            String[] strategies = {"DEFAULT", "FRAG", "FRAG_BA"};
            String[] labels = {"DEFAULT", "FRAG", "FRAG+BA"};

            for (int i = 0; i < strategies.length; i++) {
                String key = strategies[i] + "|0.2";
                List<Record> recs = groups.getOrDefault(key, Collections.emptyList());
                if (recs.isEmpty()) {
                    pw.printf("%s & -- & -- & -- \\\\%n", labels[i]);
                } else {
                    double med = recs.stream().mapToDouble(x -> x.readLatencyMedianUs).average().orElse(0);
                    double p99 = recs.stream().mapToDouble(x -> x.readLatencyP99Us).average().orElse(0);
                    double sst = recs.stream().mapToDouble(x -> x.readAvgSstHits).average().orElse(0);
                    pw.printf("%s & %.1f & %.1f & %.1f \\\\%n", labels[i], med, p99, sst);
                }
            }

            pw.println("\\bottomrule");
            pw.println("\\end{tabular}");
            pw.println("\\end{table}");
        }
        System.out.println("Generated: " + outputDir + "/table_read.tex");
    }

    // ==================== 绘图数据 ====================

    /**
     * 输出 WA vs disorder_rate 绘图数据（三条线）
     * 可用 gnuplot 或 matplotlib 绘制
     */
    static void generatePlotData(Map<String, List<Record>> groups,
                                 String outputDir) throws IOException {
        try (PrintWriter pw = new PrintWriter(
                new FileWriter(outputDir + "/plot_wa_vs_disorder.dat"))) {
            pw.println("# disorder_rate  DEFAULT_WA  DEFAULT_std  FRAG_WA  FRAG_std  FRAG_BA_WA  FRAG_BA_std");

            double[] rates = {0.0, 0.05, 0.10, 0.20, 0.50};
            String[] strategies = {"DEFAULT", "FRAG", "FRAG_BA"};

            for (double r : rates) {
                pw.printf("%.2f", r);
                for (String s : strategies) {
                    String key = s + "|" + r;
                    List<Record> recs = groups.getOrDefault(key, Collections.emptyList());
                    double avg = recs.stream().mapToDouble(x -> x.waRatio).average().orElse(0);
                    double std = computeStd(recs.stream().mapToDouble(x -> x.waRatio).toArray());
                    pw.printf("  %.4f  %.4f", avg, std);
                }
                pw.println();
            }
        }
        System.out.println("Generated: " + outputDir + "/plot_wa_vs_disorder.dat");

        // 同时输出 Python 绘图脚本
        try (PrintWriter pw = new PrintWriter(
                new FileWriter(outputDir + "/plot_wa.py"))) {
            pw.println("import matplotlib.pyplot as plt");
            pw.println("import numpy as np");
            pw.println();
            pw.println("data = np.loadtxt('plot_wa_vs_disorder.dat', skiprows=1)");
            pw.println("r = data[:, 0]");
            pw.println();
            pw.println("fig, ax = plt.subplots(figsize=(8, 5))");
            pw.println("ax.errorbar(r, data[:,1], yerr=data[:,2], marker='o', label='DEFAULT', capsize=3)");
            pw.println("ax.errorbar(r, data[:,3], yerr=data[:,4], marker='s', label='FRAG', capsize=3)");
            pw.println("ax.errorbar(r, data[:,5], yerr=data[:,6], marker='^', label='FRAG+BA', capsize=3)");
            pw.println("ax.set_xlabel('Disorder Rate')");
            pw.println("ax.set_ylabel('Write Amplification')");
            pw.println("ax.legend()");
            pw.println("ax.grid(True, alpha=0.3)");
            pw.println("plt.tight_layout()");
            pw.println("plt.savefig('fig_wa_vs_disorder.pdf')");
            pw.println("plt.show()");
        }
        System.out.println("Generated: " + outputDir + "/plot_wa.py");
    }

    // ==================== 工具方法 ====================

    static double computeStd(double[] values) {
        if (values.length <= 1) return 0;
        double mean = Arrays.stream(values).average().orElse(0);
        double sumSq = Arrays.stream(values).map(v -> (v - mean) * (v - mean)).sum();
        return Math.sqrt(sumSq / (values.length - 1));
    }

    // ==================== 数据类 ====================

    static class Record {
        String strategy;
        double disorderRate;
        int round;
        long writeTimeMs;
        long throughput;
        double waRatio;
        double saRatio;
        long compactionBytesWritten;
        long flushBytesWritten;
        double readLatencyMedianUs;
        double readLatencyP99Us;
        double readAvgSstHits;
        long dirSizeBytes;
        long logicalBytes;
    }
}