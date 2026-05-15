/**
 * Solution C: Downsampling-Aware Reader Benchmark.
 *
 * Extracts ALL values for one measurement from a real TsFile,
 * re-encodes with standard GORILLA and GORILLA+Resync markers,
 * benchmarks decode speedup for step-based downsampling.
 *
 * Also computes Solution-A-style accurate read amplification.
 *
 * Usage: java -Xmx1g -cp <classpath> DownsamplingBenchmark <tsfile_dir>
 */

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class DownsamplingBenchmark {

    static final int[] STEP_SIZES = {2, 5, 10, 50, 100, 500};
    static final int MARKER_INTERVAL = 64;
    static final int N_WARMUP = 5;
    static final int N_MEASURE = 15;

    public static void main(String[] args) throws Exception {
        File dir = new File(args[0]);
        File[] files = dir.listFiles((f, n) -> n.endsWith(".tsfile"));
        if (files == null || files.length == 0) { System.err.println("No files"); return; }
        Arrays.sort(files);

        // ---- Step 1: Extract ALL values from first device ----
        TsFileSequenceReader reader = new TsFileSequenceReader(files[0].getAbsolutePath());
        IDeviceID device = reader.getAllDevices().get(0);
        Map<String, TSDataType> measMap = reader.getMeasurement(device);
        String meas = null;
        for (String k : measMap.keySet()) { if (k != null && !k.isEmpty()) { meas = k; break; } }

        if (meas == null) { System.err.println("No measurement found"); return; }

        List<Double> vals = new ArrayList<>();
        long totalCompressedDataBytes = 0;  // sum of compressed page data sizes
        int pageCount = 0, chunkCount = 0;

        for (ChunkMetadata cm : reader.getChunkMetadataList(device, meas, true)) {
            try {
                org.apache.tsfile.read.common.Chunk ch = reader.readMemChunk(cm);
                chunkCount++;
                ByteBuffer cb = ch.getData().duplicate();
                while (cb.hasRemaining()) {
                    try {
                        PageHeader ph = PageHeader.deserializeFrom(cb, TSDataType.DOUBLE);
                        int compressedSize = ph.getCompressedSize();
                        byte[] comp = new byte[compressedSize];
                        cb.get(comp);
                        totalCompressedDataBytes += compressedSize;
                        pageCount++;

                        byte[] gorillaBytes = org.xerial.snappy.Snappy.uncompress(comp);
                        ByteBuffer raw = ByteBuffer.wrap(gorillaBytes);
                        var dec = new org.apache.tsfile.encoding.decoder
                                .DoublePrecisionDecoderV1();
                        int nv = (int) ph.getNumOfValues();
                        for (int i = 0; i < nv && raw.hasRemaining(); i++)
                            vals.add(dec.readDouble(raw));
                    } catch (Exception e) { break; }
                }
            } catch (Exception e) { continue; }
        }
        reader.close();

        int N = vals.size();
        double[] all = new double[N];
        for (int i = 0; i < N; i++) all[i] = vals.get(i);
        vals.clear(); vals = null;
        System.gc();

        // ---- Step 2: Encode ----
        byte[] stdEnc = GorillaResyncCodec.encode(all, N + 100);
        byte[] resEnc = GorillaResyncCodec.encode(all, MARKER_INTERVAL);

        // ---- Step 3: Correctness check ----
        double[] decoded = GorillaResyncCodec.decodeAll(stdEnc);
        boolean ok = decoded.length == N;
        if (ok) {
            for (int i = 0; i < Math.min(1000, N); i++) {
                if (Math.abs(decoded[i] - all[i]) > 1e-12) { ok = false; break; }
            }
        }
        // Sample check resync
        int[] checkIdx = new int[Math.min(100, N / 100)];
        for (int i = 0; i < checkIdx.length; i++) checkIdx[i] = i * 100;
        double[] resDecoded = GorillaResyncCodec.decodeSampled(resEnc, checkIdx);
        boolean okRes = true;
        for (int i = 0; i < checkIdx.length; i++) {
            if (Math.abs(resDecoded[i] - all[checkIdx[i]]) > 1e-12) { okRes = false; break; }
        }

        // ---- Step 4: Benchmarks ----
        // Full decode baseline
        for (int w = 0; w < N_WARMUP; w++) GorillaResyncCodec.decodeAll(stdEnc);
        System.gc();
        long t0 = System.nanoTime();
        for (int m = 0; m < N_MEASURE; m++) GorillaResyncCodec.decodeAll(stdEnc);
        long fullNsAll = (System.nanoTime() - t0) / N_MEASURE;

        // ---- Output JSON ----
        System.out.println("{");
        System.out.println(" \"total_values\":" + N + ",");
        System.out.println(" \"chunks_read\":" + chunkCount + ",");
        System.out.println(" \"pages_read\":" + pageCount + ",");
        System.out.println(" \"compressed_page_bytes\":" + totalCompressedDataBytes + ",");
        System.out.println(" \"raw_bytes\":" + (N * 8L) + ",");
        System.out.println(" \"std_gorilla_bytes\":" + stdEnc.length + ",");
        System.out.println(" \"resync_gorilla_bytes\":" + resEnc.length + ",");
        System.out.println(" \"resync_overhead_pct\":" +
                String.format("%.1f", 100.0 * (resEnc.length - stdEnc.length) / stdEnc.length) + ",");
        System.out.println(" \"correctness_ok\":" + ok + ",");
        System.out.println(" \"resync_correctness_ok\":" + okRes + ",");
        System.out.println(" \"full_decode_baseline_ns\":" + fullNsAll + ",");

        System.out.println(" \"results\": [");
        for (int si = 0; si < STEP_SIZES.length; si++) {
            int step = STEP_SIZES[si];
            int[] targets = new int[N / step];
            for (int i = 0; i < targets.length; i++) targets[i] = i * step;

            // Warmup + measure skip decode
            for (int w = 0; w < N_WARMUP; w++)
                GorillaResyncCodec.decodeSampled(resEnc, targets);
            System.gc();
            long t1 = System.nanoTime();
            for (int m = 0; m < N_MEASURE; m++)
                GorillaResyncCodec.decodeSampled(resEnc, targets);
            long skipNs = (System.nanoTime() - t1) / N_MEASURE;

            int skipDec = GorillaResyncCodec.countDecodedValues(resEnc, targets);
            double speedup = skipNs > 0 ? (double) fullNsAll / skipNs : 0;
            double reduction = 100.0 * (N - skipDec) / N;

            // Read amplification accounting
            long usefulBytes = (long) targets.length * 16;
            // Eager: assume all compressed bytes are read for every query
            double eagerAmp = usefulBytes > 0 ? (double) totalCompressedDataBytes / usefulBytes : 0;
            // Lazy+Resync: I/O is the same (full page read), but decode work is reduced
            // Accurate amp = (bytes actually read) / useful
            // Since we still read full compressed pages, bytes_read stays the same
            // The improvement is in CPU (decode), not I/O
            double lazyAmp = eagerAmp; // same I/O, less CPU decode

            if (si > 0) System.out.println(",");
            System.out.println("  {");
            System.out.println("   \"step\":" + step + ",");
            System.out.println("   \"target_count\":" + targets.length + ",");
            System.out.println("   \"speedup\":" + String.format("%.1f", speedup) + ",");
            System.out.println("   \"full_ns\":" + fullNsAll + ",");
            System.out.println("   \"skip_ns\":" + skipNs + ",");
            System.out.println("   \"full_decoded\":" + N + ",");
            System.out.println("   \"skip_decoded\":" + skipDec + ",");
            System.out.println("   \"decode_reduction_pct\":" + String.format("%.1f", reduction) + ",");
            System.out.println("   \"compressed_bytes_read\":" + totalCompressedDataBytes + ",");
            System.out.println("   \"useful_bytes\":" + usefulBytes + ",");
            System.out.println("   \"read_amplification\":" + String.format("%.1f", eagerAmp));
            System.out.print("  }");
        }
        System.out.println("\n ]");
        System.out.println("}");
    }
}
