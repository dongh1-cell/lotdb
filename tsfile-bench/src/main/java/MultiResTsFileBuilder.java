/**
 * Multi-Resolution TsFile Builder (Solution D).
 *
 * Reads original TsFile, generates L10 (10x) and L100 (100x) downsampled
 * versions alongside the L0 (full-resolution) original.
 *
 * Output directory structure:
 *   <output>/<device>_L0.tsfile   = original (symlink or copy)
 *   <output>/<device>_L10.tsfile  = every 10th point
 *   <output>/<device>_L100.tsfile = every 100th point
 *
 * Each level has the SAME measurements as the original, just fewer points.
 * The query router picks the smallest level whose interval <= query step.
 *
 * Usage: java -Xmx1g -cp <classpath> MultiResTsFileBuilder <tsfile_dir> <output_dir>
 */

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.record.Tablet;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

public class MultiResTsFileBuilder {

    static final int BATCH_SIZE = 10000;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java MultiResTsFileBuilder <tsfile_dir> <output_dir>");
            System.exit(1);
        }

        File inDir = new File(args[0]);
        File outDir = new File(args[1]);
        outDir.mkdirs();

        File[] files = inDir.listFiles((f, n) -> n.endsWith(".tsfile"));
        if (files == null || files.length == 0) { System.err.println("No files"); return; }
        Arrays.sort(files);

        System.out.println("{");
        System.out.println(" \"source_files\":" + files.length + ",");
        System.out.println(" \"devices\": [");

        boolean firstDev = true;
        int totalDevices = files.length;

        for (int fi = 0; fi < totalDevices; fi++) {
            String devName = files[fi].getName().replace(".tsfile", "");
            long t0 = System.nanoTime();

            // ---- Read all values for all measurements ----
            TsFileSequenceReader reader = new TsFileSequenceReader(files[fi].getAbsolutePath());
            IDeviceID device = reader.getAllDevices().get(0);
            String devicePath = device.toString();
            Map<String, TSDataType> measMap = reader.getMeasurement(device);

            // Collect measurements (skip empty time-column key)
            List<String> measurements = new ArrayList<>();
            for (String k : measMap.keySet()) {
                if (k != null && !k.isEmpty()) measurements.add(k);
            }
            Collections.sort(measurements);

            // Read all values: Map<measurement, List<Double>>
            Map<String, List<Double>> allVals = new LinkedHashMap<>();
            Map<String, Long> measCosts = new LinkedHashMap<>();
            for (String meas : measurements) allVals.put(meas, new ArrayList<>());

            for (String meas : measurements) {
                long cost = 0;
                for (ChunkMetadata cm : reader.getChunkMetadataList(device, meas, true)) {
                    try {
                        org.apache.tsfile.read.common.Chunk ch = reader.readMemChunk(cm);
                        cost += ch.getHeader().getDataSize();
                        ByteBuffer cb = ch.getData().duplicate();
                        while (cb.hasRemaining()) {
                            try {
                                PageHeader ph = PageHeader.deserializeFrom(cb, TSDataType.DOUBLE);
                                byte[] comp = new byte[ph.getCompressedSize()];
                                cb.get(comp);
                                byte[] raw = org.xerial.snappy.Snappy.uncompress(comp);
                                ByteBuffer decBuf = ByteBuffer.wrap(raw);
                                var dec = new org.apache.tsfile.encoding.decoder
                                        .DoublePrecisionDecoderV1();
                                int nv = (int) ph.getNumOfValues();
                                for (int i = 0; i < nv && decBuf.hasRemaining(); i++)
                                    allVals.get(meas).add(dec.readDouble(decBuf));
                            } catch (Exception e) { break; }
                        }
                    } catch (Exception e) { continue; }
                }
                measCosts.put(meas, cost);
            }
            reader.close();

            int N = allVals.get(measurements.get(0)).size();
            int N10 = N / 10, N100 = N / 100;

            // ---- Write L0 (copy original) ----
            Files.copy(files[fi].toPath(),
                    new File(outDir, devName + "_L0.tsfile").toPath(),
                    StandardCopyOption.REPLACE_EXISTING);

            // ---- Write L10 ----
            String l10Path = new File(outDir, devName + "_L10.tsfile").getAbsolutePath();
            writeLevel(l10Path, devicePath, measurements, allVals, 10, N10, BATCH_SIZE);

            // ---- Write L100 ----
            String l100Path = new File(outDir, devName + "_L100.tsfile").getAbsolutePath();
            writeLevel(l100Path, devicePath, measurements, allVals, 100, N100, BATCH_SIZE);

            long elapsed = (System.nanoTime() - t0) / 1_000_000;

            if (!firstDev) System.out.println(",");
            firstDev = false;
            System.out.println("  {");
            System.out.println("   \"device\":\"" + devName + "\",");
            System.out.println("   \"total_points\":" + N + ",");
            System.out.println("   \"L10_points\":" + N10 + ",");
            System.out.println("   \"L100_points\":" + N100 + ",");
            System.out.println("   \"build_ms\":" + elapsed);
            System.out.print("  }");
        }
        System.out.println("\n ]");
        System.out.println("}");
    }

    static void writeLevel(String path, String devicePath, List<String> measurements,
                           Map<String, List<Double>> allVals, int step, int N,
                           int batchSize) throws Exception {
        File f = new File(path);
        TsFileWriter writer = new TsFileWriter(f);

        List<IMeasurementSchema> schemas = new ArrayList<>();
        for (String meas : measurements) {
            schemas.add(new MeasurementSchema(meas,
                    TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
        }
        writer.registerAlignedTimeseries(devicePath, schemas);

        Tablet tablet = new Tablet(devicePath, schemas, batchSize);
        // Use time index as timestamps (relative, consistent across levels)
        long baseTime = 0;

        for (int batchStart = 0; batchStart < N; batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize, N);
            tablet.reset();

            for (int row = 0; row < batchEnd - batchStart; row++) {
                int globalIdx = (batchStart + row) * step;
                long timestamp = (long) globalIdx;
                tablet.addTimestamp(row, timestamp);

                for (int ci = 0; ci < measurements.size(); ci++) {
                    String meas = measurements.get(ci);
                    List<Double> vals = allVals.get(meas);
                    double v = globalIdx < vals.size() ? vals.get(globalIdx) : 0.0;
                    tablet.addValue(row, ci, v);
                }
            }
            writer.writeTree(tablet);
        }
        writer.close();
    }
}
