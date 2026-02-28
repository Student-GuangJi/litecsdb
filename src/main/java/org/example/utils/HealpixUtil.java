package org.example.utils;

import healpix.HealpixBase;
import healpix.Pointing;
import healpix.RangeSet;
import healpix.Scheme;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class HealpixUtil {
    private static final int BASE_NSIDE = 1; // 基础分辨率
    private static final HealpixBase baseHealpix;

    static {
        try {
            baseHealpix = new HealpixBase(BASE_NSIDE, Scheme.NESTED);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setNside(int level) {
        try {
            baseHealpix.setNside(BASE_NSIDE * (1 << level));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    // 支持不同level的坐标转换
    public static long raDecToHealpix(double ra, double dec, int level) {
//        level为1时，npix=48，level为2时，npix=192，level为3时，npix=768，level为4时，npix=3072，level为5时，npix=12288，level为6时，npix=49152，每个像素对应面积公式为：4π/(npix)
        int nside = BASE_NSIDE * (1 << level); // nside = BASE_NSIDE * 2^level, npix = 12 * nside^2 = 12 * 4^level, level = log2(npix/12)
        double phi = Math.toRadians(ra);
        double theta = Math.toRadians(90.0 - dec);
        Pointing pointing = new Pointing(theta, phi);

        try {
            if (nside == BASE_NSIDE) {
                return baseHealpix.ang2pix(pointing);
            }
            return new HealpixBase(nside, Scheme.NESTED).ang2pix(pointing);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Pointing healpixToPointing(long healpixId, int level) {
        int nside = BASE_NSIDE * (1 << level); // nside = BASE_NSIDE * 2^level
        try {
            if (nside == BASE_NSIDE) {
                return baseHealpix.pix2ang(healpixId);
            }
            return new HealpixBase(nside, Scheme.NESTED).pix2ang(healpixId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static double angularDistance(double ra1, double dec1, double ra2, double dec2) {
        double dRa = Math.toRadians(ra2 - ra1);
        double dDec = Math.toRadians(dec2 - dec1);
        double a = Math.sin(dDec/2) * Math.sin(dDec/2) +
                Math.cos(Math.toRadians(dec1)) * Math.cos(Math.toRadians(dec2)) *
                        Math.sin(dRa/2) * Math.sin(dRa/2);
        return Math.toDegrees(2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)));
    }

    public static RangeSet queryDisc(double ra, double dec, double radiusDegrees, int level) throws Exception {
        double phi = Math.toRadians(ra);
        double theta = Math.toRadians(90.0 - dec);
        Pointing pointing = new Pointing(theta, phi);
        return baseHealpix.queryDiscInclusive(pointing, Math.toRadians(radiusDegrees), level);
    }

    public static Set<Integer> queryRectangle(double minRa, double maxRa,
                                              double minDec, double maxDec, int level) {
        Set<Integer> pixels = new HashSet<>();
        double step = 0.1; // 采样步长(度)

        for (double ra = minRa; ra <= maxRa; ra += step) {
            pixels.add((int)raDecToHealpix(ra, minDec, level));
            pixels.add((int)raDecToHealpix(ra, maxDec, level));
        }
        for (double dec = minDec; dec <= maxDec; dec += step) {
            pixels.add((int)raDecToHealpix(minRa, dec, level));
            pixels.add((int)raDecToHealpix(maxRa, dec, level));
        }

        for (double ra = minRa + step; ra < maxRa; ra += step) {
            for (double dec = minDec + step; dec < maxDec; dec += step) {
                pixels.add((int)raDecToHealpix(ra, dec, level));
            }
        }
        return pixels;
    }

    public static Set<Integer> queryPolygon(List<Double> raList, List<Double> decList, int level) {
        Set<Integer> pixels = new HashSet<>();
        if (raList.size() != decList.size() || raList.size() < 3) {
            return pixels;
        }

        for (int i = 0; i < raList.size(); i++) {
            int j = (i + 1) % raList.size();
            pixels.addAll(queryLine(raList.get(i), decList.get(i),
                    raList.get(j), decList.get(j), level));
        }

        double minRa = Collections.min(raList);
        double maxRa = Collections.max(raList);
        double minDec = Collections.min(decList);
        double maxDec = Collections.max(decList);

        double step = 0.1;
        for (double ra = minRa; ra <= maxRa; ra += step) {
            for (double dec = minDec; dec <= maxDec; dec += step) {
                if (isPointInPolygon(ra, dec, raList, decList)) {
                    pixels.add((int)raDecToHealpix(ra, dec, level));
                }
            }
        }
        return pixels;
    }

    private static boolean isPointInPolygon(double ra, double dec,
                                            List<Double> raList, List<Double> decList) {
        boolean result = false;
        for (int i = 0, j = raList.size() - 1; i < raList.size(); j = i++) {
            if ((decList.get(i) > dec) != (decList.get(j) > dec) &&
                    (ra < (raList.get(j) - raList.get(i)) * (dec - decList.get(i)) /
                            (decList.get(j) - decList.get(i)) + raList.get(i))) {
                result = !result;
            }
        }
        return result;
    }

    private static Set<Integer> queryLine(double ra1, double dec1,
                                          double ra2, double dec2, int level) {
        Set<Integer> pixels = new HashSet<>();
        double steps = 100;
        for (int i = 0; i <= steps; i++) {
            double t = i / steps;
            double ra = ra1 + t * (ra2 - ra1);
            double dec = dec1 + t * (dec2 - dec1);
            pixels.add((int)raDecToHealpix(ra, dec, level));
        }
        return pixels;
    }
}