/**
 * Resync marker cache benchmark.
 *
 * This variant does not persist markers in the file. It models a practical
 * cache design: the first page access performs normal sequential decode and
 * builds an in-memory segmented GORILLA cache; repeated accesses to the same
 * page can then use resync decode without increasing on-disk storage.
 */

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;

import java.io.File;
import java.util.*;

public class ResyncCacheBenchmarkRunner {
    static final int[] STEP_SIZES = {10, 100, 500};
    static final int MARKER_INTERVAL = Integer.getInteger("bench.marker.interval", 64);
    static final int MAX_VALUES = Integer.getInteger("bench.max.values", 20000);
    static final int N_MEASURE = Integer.getInteger("bench.n.measure", 20);

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: ResyncCacheBenchmarkRunner <tsfile_dir>");
            System.exit(1);
        }

        File dir = new File(args[0]);
        File[] files = dir.listFiles((f, n) -> n.endsWith(".tsfile"));
        if (files == null || files.length == 0) {
            System.err.println("No files");
            return;
        }
        Arrays.sort(files);

        double[] values = readValues(files, MAX_VALUES);
        byte[] standard = GorillaResyncCodec.encode(values, values.length + 100);

        long buildT0 = System.nanoTime();
        byte[] cache = GorillaResyncCodec.encode(values, MARKER_INTERVAL);
        long buildNs = System.nanoTime() - buildT0;

        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"source\":\"").append("C-MAPSS TsFile sequence concat").append("\",");
        json.append("\"total_values\":").append(values.length).append(",");
        json.append("\"marker_interval\":").append(MARKER_INTERVAL).append(",");
        json.append("\"disk_overhead_pct\":0.0,");
        json.append("\"cache_bytes\":").append(cache.length).append(",");
        json.append("\"standard_encoded_bytes\":").append(standard.length).append(",");
        json.append("\"cache_vs_standard_pct\":").append(String.format("%.1f",
                100.0 * (cache.length - standard.length) / Math.max(standard.length, 1))).append(",");
        json.append("\"cache_build_ns\":").append(buildNs).append(",");
        json.append("\"results\": [");

        for (int si = 0; si < STEP_SIZES.length; si++) {
            int step = STEP_SIZES[si];
            int[] targets = new int[values.length / step];
            for (int i = 0; i < targets.length; i++) targets[i] = i * step;

            long fullNs = benchFullDecode(standard);
            long cachedNs = benchCachedDecode(cache, targets);
            int cachedDecoded = GorillaResyncCodec.countDecodedValues(cache, targets);

            if (si > 0) json.append(",");
            json.append("{");
            json.append("\"step\":").append(step).append(",");
            json.append("\"targets\":").append(targets.length).append(",");
            json.append("\"epoch1_full_decode_ns\":").append(fullNs).append(",");
            json.append("\"cache_hit_decode_ns\":").append(cachedNs).append(",");
            json.append("\"cache_hit_speedup\":").append(String.format("%.1f",
                    cachedNs > 0 ? (double) fullNs / cachedNs : 0)).append(",");
            json.append("\"decoded_with_cache\":").append(cachedDecoded).append(",");
            json.append("\"decode_reduction_pct\":").append(String.format("%.1f",
                    100.0 * (values.length - cachedDecoded) / values.length));
            json.append("}");
        }
        json.append("]}");
        System.out.println(json);
    }

    static double[] readValues(File[] files, int maxValues) throws Exception {
        List<Double> vals = new ArrayList<>();
        for (File file : files) {
            if (vals.size() >= maxValues) break;
            readValuesFromFile(file, maxValues, vals);
        }
        double[] out = new double[vals.size()];
        for (int i = 0; i < vals.size(); i++) out[i] = vals.get(i);
        return out;
    }

    static void readValuesFromFile(File file, int maxValues, List<Double> vals) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(file.getAbsolutePath());
        TsFileReader reader = new TsFileReader(seq);
        IDeviceID device = seq.getAllDevices().get(0);
        String meas = null;
        for (String k : seq.getMeasurement(device).keySet()) {
            if (k != null && !k.isEmpty()) { meas = k; break; }
        }
        if (meas == null) throw new IllegalStateException("No measurement");

        Path p = new Path(device.toString(), meas, true);
        var ds = reader.query(QueryExpression.create(Collections.singletonList(p), null));
        while (ds.hasNext() && vals.size() < maxValues) {
            var row = ds.next();
            for (var field : row.getFields()) {
                if (field == null) continue;
                try {
                    vals.add(field.getDoubleV());
                    break;
                } catch (Exception ignored) {
                }
            }
        }
        reader.close();
        seq.close();
    }

    static long benchFullDecode(byte[] standard) throws Exception {
        long t0 = System.nanoTime();
        for (int i = 0; i < N_MEASURE; i++) {
            GorillaResyncCodec.decodeAll(standard);
        }
        return (System.nanoTime() - t0) / N_MEASURE;
    }

    static long benchCachedDecode(byte[] cache, int[] targets) throws Exception {
        long t0 = System.nanoTime();
        for (int i = 0; i < N_MEASURE; i++) {
            GorillaResyncCodec.decodeSampled(cache, targets);
        }
        return (System.nanoTime() - t0) / N_MEASURE;
    }
}
