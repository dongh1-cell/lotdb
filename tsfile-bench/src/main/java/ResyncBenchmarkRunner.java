/**
 * GORILLA Resync Marker Benchmark.
 *
 * Reads real TsFile page data, re-encodes with standard & resync GORILLA,
 * measures decode speedup for AI training access patterns.
 *
 * Usage: java -Xmx256m -cp <classpath> ResyncBenchmarkRunner <tsfile_dir>
 */

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.read.TsFileSequenceReader;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class ResyncBenchmarkRunner {

    static final int[] STEP_SIZES = {1, 10, 100, 500};
    static final int N_WARMUP = 10;
    static final int N_MEASURE = 20;
    static final int MARKER_INTERVAL = 64;
    static final int WINDOW_LENGTH = 512;
    static final int MAX_VALUES = 8000;

    public static void main(String[] args) throws Exception {
        File dir = new File(args[0]);
        File[] files = dir.listFiles((f, n) -> n.endsWith(".tsfile"));
        if (files == null || files.length == 0) { System.err.println("No files"); return; }
        Arrays.sort(files);

        // ---- Step 1: extract values from real TsFile ----
        TsFileSequenceReader reader = new TsFileSequenceReader(files[0].getAbsolutePath());
        IDeviceID device = reader.getAllDevices().get(0);
        Map<String, TSDataType> measMap = reader.getMeasurement(device);
        String meas = null;
        for (String k : measMap.keySet()) { if (k != null && !k.isEmpty()) { meas = k; break; } }

        List<Double> vals = new ArrayList<>();
        for (ChunkMetadata cm : reader.getChunkMetadataList(device, meas, true)) {
            if (vals.size() >= MAX_VALUES) break;
            try {
                org.apache.tsfile.read.common.Chunk ch = reader.readMemChunk(cm);
                ByteBuffer cb = ch.getData().duplicate();
                while (cb.hasRemaining() && vals.size() < MAX_VALUES) {
                    try {
                        PageHeader ph = PageHeader.deserializeFrom(cb, TSDataType.DOUBLE);
                        byte[] comp = new byte[ph.getCompressedSize()];
                        cb.get(comp);
                        ByteBuffer raw = ByteBuffer.wrap(org.xerial.snappy.Snappy.uncompress(comp));
                        var dec = new org.apache.tsfile.encoding.decoder.DoublePrecisionDecoderV1();
                        for (int i = 0; i < (int) ph.getNumOfValues() && raw.hasRemaining(); i++)
                            vals.add(dec.readDouble(raw));
                    } catch (Exception e) { break; }
                }
            } catch (Exception e) { continue; }
        }
        reader.close();

        int N = vals.size();
        double[] all = new double[N];
        for (int i = 0; i < N; i++) all[i] = vals.get(i);
        vals.clear(); vals = null; System.gc();

        // ---- Step 2: encode ----
        byte[] stdEnc = GorillaResyncCodec.encode(all, N + 100);
        byte[] resEnc = GorillaResyncCodec.encode(all, MARKER_INTERVAL);
        long rawBytes = (long) N * 8;

        // ---- Step 3: benchmark downsampling ----
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"total_values\":").append(N).append(",");
        json.append("\"marker_interval\":").append(MARKER_INTERVAL).append(",");
        json.append("\"compression\":{");
        json.append("\"raw_bytes\":").append(rawBytes).append(",");
        json.append("\"standard_bytes\":").append(stdEnc.length).append(",");
        json.append("\"resync_bytes\":").append(resEnc.length).append(",");
        json.append("\"overhead_pct\":").append(String.format("%.1f",
                100.0 * (resEnc.length - stdEnc.length) / stdEnc.length));
        json.append("},\"downsampling\":{");

        for (int si = 0; si < STEP_SIZES.length; si++) {
            int step = STEP_SIZES[si];
            int[] targets = new int[N / step];
            for (int i = 0; i < targets.length; i++) targets[i] = i * step;

            // Warmup
            for (int w = 0; w < N_WARMUP; w++) {
                GorillaResyncCodec.decodeAll(stdEnc);
                GorillaResyncCodec.decodeSampled(resEnc, targets);
                if (w % 5 == 0) System.gc();
            }
            System.gc();

            // Measure full decode
            long t0 = System.nanoTime();
            for (int m = 0; m < N_MEASURE; m++) GorillaResyncCodec.decodeAll(stdEnc);
            long fullNs = (System.nanoTime() - t0) / N_MEASURE;
            System.gc();

            // Measure skip decode
            long t1 = System.nanoTime();
            for (int m = 0; m < N_MEASURE; m++) GorillaResyncCodec.decodeSampled(resEnc, targets);
            long skipNs = (System.nanoTime() - t1) / N_MEASURE;

            int skipDec = GorillaResyncCodec.countDecodedValues(resEnc, targets);
            double speedup = skipNs > 0 ? (double) fullNs / skipNs : 0;

            if (si > 0) json.append(",");
            json.append("\"").append(step).append("\":{");
            json.append("\"full_ns\":").append(fullNs).append(",");
            json.append("\"skip_ns\":").append(skipNs).append(",");
            json.append("\"speedup\":").append(String.format("%.1f", speedup)).append(",");
            json.append("\"full_dec\":").append(N).append(",");
            json.append("\"skip_dec\":").append(skipDec).append(",");
            json.append("\"reduction\":").append(String.format("%.1f",
                    100.0 * (N - skipDec) / N));
            json.append("}");
        }
        json.append("}}");
        System.out.println(json.toString());
    }
}
