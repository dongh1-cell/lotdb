/**
 * Native Java TsFile benchmark — single-thread, process-level CPU timing.
 *
 * Supports two modes:
 *   EAGER (default): Standard TsFileReader.query() API.
 *   LAZY  (-Dtsfile.lazy.page.load=true):
 *          Uses LazyTsFileQuerier with per-chunk I/O tracking.
 *          This gives accurate read-amplification by summing compressed
 *          sizes of chunks whose time range overlaps the query, rather
 *          than using the coarse file_size/n approximation.
 *
 * Usage:
 *   java -cp <classpath> [-Dtsfile.lazy.page.load=true] TsFileNativeRunner <tsfile_dir>
 */

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.ChunkMetadata;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.util.*;

public class TsFileNativeRunner {

    // Lazy page-load mode (controlled by system property)
    static final boolean LAZY_MODE = Boolean.parseBoolean(
            System.getProperty("tsfile.lazy.page.load", "false"));

    // Per-device LazyTsFileQuerier cache (lazy mode only)
    static Map<String, LazyTsFileQuerier> lazyReaders = new HashMap<>();

    // ── Benchmark config (matches config.py) ──
    static final long BASE_START = 1704038400L;
    static final int DURATION_DAYS = 10;
    static final long TOTAL_SECONDS = DURATION_DAYS * 86400L;
    static final int INTERVAL_SECONDS = 2;
    static final int POINTS_PER_SERIES = (int)(TOTAL_SECONDS / INTERVAL_SECONDS);

    static final int[] SAMPLING_RATES = {1, 10, 100, 500};
    static final double[] COLUMN_SELECTIVITIES = {0.07, 0.20, 0.50, 1.0};
    static final int N_RUNS = 5;
    static final int WARMUP_RUNS = 3;
    static final int RANDOM_WINDOW_COUNT = 500;
    static final int RANDOM_WINDOW_LENGTH = 512;
    static final int TARGET_DEVICES_FOR_RANDOM = 5;

    static String DEVICE_PATH;
    static List<String> ALL_MEASUREMENTS = new ArrayList<>();
    static Map<String, String> deviceFiles = new LinkedHashMap<>();
    static OperatingSystemMXBean osBean;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java TsFileNativeRunner <tsfile_dir>");
            System.exit(1);
        }

        osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        String tsDir = args[0];
        discoverFiles(tsDir);
        if (deviceFiles.isEmpty()) {
            System.err.println("No .tsfile files in " + tsDir);
            System.exit(1);
        }

        // Discover measurements
        String firstFile = deviceFiles.values().iterator().next();
        TsFileSequenceReader r = new TsFileSequenceReader(firstFile);
        List<IDeviceID> devs = r.getAllDevices();
        DEVICE_PATH = devs.get(0).toString();
        Map<String, List<ChunkMetadata>> chunks = r.readChunkMetadataInDevice(devs.get(0));
        for (String key : chunks.keySet()) {
            if (!key.isEmpty()) ALL_MEASUREMENTS.add(key);
        }
        Collections.sort(ALL_MEASUREMENTS);
        r.close();

        List<String> devices = new ArrayList<>(deviceFiles.keySet());

        // ── Warmup phase (JIT compilation, not measured) ──
        warmup(devices);

        // ── Measured phase ──
        List<Map<String, Object>> allResults = new ArrayList<>();
        allResults.addAll(runPattern1(devices));
        allResults.addAll(runPattern2(devices));
        allResults.addAll(runPattern3(devices));
        allResults.addAll(runPattern4(devices));

        // Output JSON (Python reads this)
        System.out.println(toJson(allResults));
    }

    // ── Warmup ──

    static void warmup(List<String> devices) throws Exception {
        Random rng = new Random(42);
        for (int i = 0; i < WARMUP_RUNS; i++) {
            String dev = devices.get(rng.nextInt(devices.size()));
            String meas = ALL_MEASUREMENTS.get(rng.nextInt(ALL_MEASUREMENTS.size()));
            if (LAZY_MODE) {
                LazyTsFileQuerier lq = openLazyReader(dev);
                lq.countRows(DEVICE_PATH, meas, BASE_START, BASE_START + TOTAL_SECONDS);
            } else {
                TsFileReader ts = openReader(dev);
                queryCount(ts, meas, BASE_START, BASE_START + TOTAL_SECONDS);
                ts.close();
            }
        }
        // Clean up lazy readers after warmup so measurement phase starts fresh
        if (LAZY_MODE) {
            closeAllLazyReaders();
        }
    }

    // ── File discovery ──

    static void discoverFiles(String dir) {
        File d = new File(dir);
        File[] files = d.listFiles((f, name) -> name.endsWith(".tsfile"));
        if (files == null) return;
        Arrays.sort(files);
        for (File f : files) {
            String name = f.getName().replace(".tsfile", "");
            deviceFiles.put(name, f.getAbsolutePath());
        }
    }

    // ── Query helpers ──

    static int queryCount(TsFileReader tsReader, String measurement,
                          long timeStart, long timeEnd) throws Exception {
        Path path = new Path(DEVICE_PATH, measurement, true);
        QueryExpression expr;
        if (timeStart >= 0 && timeEnd >= 0) {
            org.apache.tsfile.read.filter.basic.Filter f1 = TimeFilterApi.gtEq(timeStart);
            org.apache.tsfile.read.filter.basic.Filter f2 = TimeFilterApi.ltEq(timeEnd);
            org.apache.tsfile.read.filter.basic.Filter tf = FilterFactory.and(f1, f2);
            expr = QueryExpression.create(Collections.singletonList(path), new GlobalTimeExpression(tf));
        } else {
            expr = QueryExpression.create(Collections.singletonList(path), null);
        }
        var ds = tsReader.query(expr);
        int count = 0;
        while (ds.hasNext()) {
            ds.next();
            count++;
        }
        return count;
    }

    static LazyTsFileQuerier openLazyReader(String device) throws IOException {
        if (!lazyReaders.containsKey(device)) {
            String path = deviceFiles.get(device);
            lazyReaders.put(device, new LazyTsFileQuerier(path));
        }
        return lazyReaders.get(device);
    }

    static void closeAllLazyReaders() {
        for (LazyTsFileQuerier lq : lazyReaders.values()) {
            try { lq.close(); } catch (Exception ignored) {}
        }
        lazyReaders.clear();
    }

    // ── Timing helpers ──

    static long getProcessCpuNs() {
        // Full JVM process CPU time (user + system), in nanoseconds
        return osBean.getProcessCpuTime();
    }

    static Map<String, Object> makeResult(String pattern, int run, String device,
                                           String measurement, double wallS, long cpuNs,
                                           int pointsReturned, long bytesRead) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("pattern", pattern);
        r.put("run", run);
        r.put("format", "tsfile");
        r.put("device", device);
        if (measurement != null) r.put("measurement", measurement);
        r.put("points_returned", pointsReturned);
        r.put("wall_time_s", wallS);
        r.put("cpu_user_s", cpuNs / 1_000_000_000.0);   // process-level CPU
        r.put("cpu_sys_s", 0.0);
        r.put("bytes_read", bytesRead);
        r.put("bytes_useful", (long) pointsReturned * 16);
        r.put("read_amplification",
              pointsReturned > 0 ? (double) bytesRead / (pointsReturned * 16.0) : 0.0);
        r.put("mem_rss_before_kb", 0);
        r.put("mem_rss_peak_kb", 0);
        r.put("mem_rss_after_kb", 0);
        r.put("mem_delta_kb", 0);
        r.put("throughput_mbps", 0.0);
        return r;
    }

    static TsFileReader openReader(String device) throws Exception {
        String path = deviceFiles.get(device);
        TsFileSequenceReader seq = new TsFileSequenceReader(path);
        return new TsFileReader(seq);
    }

    // ── Pattern 1: Sequential Scan ──

    static List<Map<String, Object>> runPattern1(List<String> devices) throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();
        Random rng = new Random(42 + 100);
        long tStart = BASE_START;
        long tEnd = BASE_START + TOTAL_SECONDS;

        for (int run = 0; run < N_RUNS; run++) {
            String device = devices.get(rng.nextInt(devices.size()));
            String measurement = ALL_MEASUREMENTS.get(rng.nextInt(ALL_MEASUREMENTS.size()));

            int n;
            long bytesRead;
            long cpu0, cpu1, t0, t1;

            if (LAZY_MODE) {
                LazyTsFileQuerier lq = openLazyReader(device);
                cpu0 = getProcessCpuNs();
                t0 = System.nanoTime();
                n = lq.countRows(DEVICE_PATH, measurement, tStart, tEnd);
                t1 = System.nanoTime();
                cpu1 = getProcessCpuNs();
                bytesRead = lq.getLastBytesRead();
            } else {
                TsFileReader tsReader = openReader(device);
                cpu0 = getProcessCpuNs();
                t0 = System.nanoTime();
                n = queryCount(tsReader, measurement, tStart, tEnd);
                t1 = System.nanoTime();
                cpu1 = getProcessCpuNs();
                tsReader.close();
                bytesRead = fileSize(device) / (ALL_MEASUREMENTS.size() + 1);
            }

            results.add(makeResult("sequential_scan", run, device, measurement,
                                   (t1 - t0) / 1e9, cpu1 - cpu0, n, bytesRead));
        }
        return results;
    }

    // ── Pattern 2: Column Subset ──

    static List<Map<String, Object>> runPattern2(List<String> devices) throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();
        Random rng = new Random(42 + 200);
        long tStart = BASE_START;
        long tEnd = BASE_START + TOTAL_SECONDS;

        for (int run = 0; run < N_RUNS; run++) {
            String device = devices.get(rng.nextInt(devices.size()));

            for (double sel : COLUMN_SELECTIVITIES) {
                int nCols = Math.max(1, (int)(ALL_MEASUREMENTS.size() * sel));
                List<String> selected = new ArrayList<>(ALL_MEASUREMENTS);
                Collections.shuffle(selected, new Random(rng.nextLong()));
                selected = selected.subList(0, nCols);

                int total = 0;
                long bytesRead = 0;
                long cpu0, cpu1, t0, t1;

                if (LAZY_MODE) {
                    LazyTsFileQuerier lq = openLazyReader(device);
                    cpu0 = getProcessCpuNs();
                    t0 = System.nanoTime();
                    for (String meas : selected) {
                        total += lq.countRows(DEVICE_PATH, meas, tStart, tEnd);
                        bytesRead += lq.getLastBytesRead();
                    }
                    t1 = System.nanoTime();
                    cpu1 = getProcessCpuNs();
                } else {
                    TsFileReader tsReader = openReader(device);
                    cpu0 = getProcessCpuNs();
                    t0 = System.nanoTime();
                    for (String meas : selected) {
                        total += queryCount(tsReader, meas, tStart, tEnd);
                    }
                    t1 = System.nanoTime();
                    cpu1 = getProcessCpuNs();
                    tsReader.close();
                    bytesRead = (fileSize(device) / (ALL_MEASUREMENTS.size() + 1)) * nCols;
                }

                Map<String, Object> r = makeResult("column_subset", run, device, null,
                                                    (t1 - t0) / 1e9, cpu1 - cpu0, total, bytesRead);
                r.put("selectivity", sel);
                r.put("n_cols_requested", nCols);
                r.put("n_cols_total", ALL_MEASUREMENTS.size());
                r.put("time_range_days", DURATION_DAYS);
                results.add(r);
            }
        }
        return results;
    }

    // ── Pattern 3: Downsampling ──

    static List<Map<String, Object>> runPattern3(List<String> devices) throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();
        Random rng = new Random(42 + 300);
        long tStart = BASE_START;
        long tEnd = BASE_START + TOTAL_SECONDS;

        for (int run = 0; run < N_RUNS; run++) {
            String device = devices.get(rng.nextInt(devices.size()));
            String measurement = ALL_MEASUREMENTS.get(rng.nextInt(ALL_MEASUREMENTS.size()));

            for (int step : SAMPLING_RATES) {
                int total;
                long bytesRead;
                long cpu0, cpu1, t0, t1;

                if (LAZY_MODE) {
                    LazyTsFileQuerier lq = openLazyReader(device);
                    cpu0 = getProcessCpuNs();
                    t0 = System.nanoTime();
                    total = lq.countRows(DEVICE_PATH, measurement, tStart, tEnd);
                    t1 = System.nanoTime();
                    cpu1 = getProcessCpuNs();
                    bytesRead = lq.getLastBytesRead();
                } else {
                    TsFileReader tsReader = openReader(device);
                    cpu0 = getProcessCpuNs();
                    t0 = System.nanoTime();
                    total = queryCount(tsReader, measurement, tStart, tEnd);
                    t1 = System.nanoTime();
                    cpu1 = getProcessCpuNs();
                    tsReader.close();
                    bytesRead = fileSize(device) / (ALL_MEASUREMENTS.size() + 1);
                }

                int useful = total / step;
                Map<String, Object> r = makeResult("downsampling", run, device, measurement,
                                                    (t1 - t0) / 1e9, cpu1 - cpu0, useful, bytesRead);
                r.put("sample_step", step);
                results.add(r);
            }
        }
        return results;
    }

    // ── Pattern 4: Random Windows ──

    static List<Map<String, Object>> runPattern4(List<String> devices) throws Exception {
        List<Map<String, Object>> results = new ArrayList<>();
        Random rng = new Random(42 + 400);

        int nTargetMeas = Math.max(1, (int)(ALL_MEASUREMENTS.size() * 0.2));
        List<String> shuffledMeas = new ArrayList<>(ALL_MEASUREMENTS);
        Collections.shuffle(shuffledMeas, new Random(42 + 400));
        List<String> targetMeas = shuffledMeas.subList(0, nTargetMeas);

        long windowSpan = RANDOM_WINDOW_LENGTH * INTERVAL_SECONDS;
        long[][] windows = new long[RANDOM_WINDOW_COUNT][2];
        for (int w = 0; w < RANDOM_WINDOW_COUNT; w++) {
            long ws = BASE_START + (long) rng.nextInt((int)(TOTAL_SECONDS - windowSpan));
            windows[w][0] = ws;
            windows[w][1] = ws + windowSpan;
        }

        List<String> shuffledDevs = new ArrayList<>(devices);
        Collections.shuffle(shuffledDevs, new Random(42 + 400));
        int nDevs = Math.min(TARGET_DEVICES_FOR_RANDOM, devices.size());
        List<String> testDevices = shuffledDevs.subList(0, nDevs);

        for (String device : testDevices) {
            int total = 0;
            long bytesRead = 0;
            long cpu0, cpu1, t0, t1;

            if (LAZY_MODE) {
                LazyTsFileQuerier lq = openLazyReader(device);
                cpu0 = getProcessCpuNs();
                t0 = System.nanoTime();
                for (String meas : targetMeas) {
                    for (long[] win : windows) {
                        total += lq.countRows(DEVICE_PATH, meas, win[0], win[1]);
                    }
                }
                t1 = System.nanoTime();
                cpu1 = getProcessCpuNs();
                // bytesRead: use same fraction-based estimate as eager,
                // but with lazy's more accurate per-measurement base cost
                double fraction = Math.min(1.0,
                        (double)(windowSpan * RANDOM_WINDOW_COUNT) / TOTAL_SECONDS);
                long perMeasCost = lq.getLastBytesRead() > 0
                        ? lq.getLastBytesRead()
                        : fileSize(device) / (ALL_MEASUREMENTS.size() + 1);
                bytesRead = (long)(perMeasCost * nTargetMeas * fraction);
            } else {
                TsFileReader tsReader = openReader(device);
                cpu0 = getProcessCpuNs();
                t0 = System.nanoTime();
                for (String meas : targetMeas) {
                    for (long[] win : windows) {
                        total += queryCount(tsReader, meas, win[0], win[1]);
                    }
                }
                t1 = System.nanoTime();
                cpu1 = getProcessCpuNs();
                tsReader.close();

                double fraction = Math.min(1.0,
                        (double)(windowSpan * RANDOM_WINDOW_COUNT) / TOTAL_SECONDS);
                bytesRead = (long)((fileSize(device) / (ALL_MEASUREMENTS.size() + 1))
                                    * nTargetMeas * fraction);
            }

            Map<String, Object> r = makeResult("random_windows", 0, device, null,
                                                (t1 - t0) / 1e9, cpu1 - cpu0, total, bytesRead);
            r.put("n_windows", RANDOM_WINDOW_COUNT);
            r.put("n_measurements", nTargetMeas);
            r.put("window_length", RANDOM_WINDOW_LENGTH);
            results.add(r);
        }
        return results;
    }

    // ── Helpers ──

    static long fileSize(String device) {
        return new File(deviceFiles.get(device)).length();
    }

    static String toJson(Object obj) {
        if (obj == null) return "null";
        if (obj instanceof String) return "\"" + escape((String) obj) + "\"";
        if (obj instanceof Number) return obj.toString();
        if (obj instanceof Boolean) return obj.toString();
        if (obj instanceof List) {
            StringBuilder sb = new StringBuilder("[");
            List<?> list = (List<?>) obj;
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(toJson(list.get(i)));
            }
            sb.append("]");
            return sb.toString();
        }
        if (obj instanceof Map) {
            StringBuilder sb = new StringBuilder("{");
            Map<?, ?> map = (Map<?, ?>) obj;
            boolean first = true;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                if (!first) sb.append(",");
                sb.append("\"").append(escape(e.getKey().toString())).append("\":");
                sb.append(toJson(e.getValue()));
                first = false;
            }
            sb.append("}");
            return sb.toString();
        }
        return "\"" + escape(obj.toString()) + "\"";
    }

    static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
