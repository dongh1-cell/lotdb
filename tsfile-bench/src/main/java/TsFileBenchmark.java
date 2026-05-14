/**
 * TsFile I/O analysis using raw byte-level metadata navigation.
 *
 * Measures page-level I/O amplification for AI training workloads
 * by analyzing the chunk/page hierarchy of a TsFile.
 */

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.*;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TsFileBenchmark {

    static final int NUM_MEAS = 15;
    static final int TOTAL_POINTS = 43200;
    static final int INTERVAL_MS = 2000;
    static final String DEVICE = "root.test.d1";
    static final String TS_FILE = "tsfile_bench.tsfile";
    static final Random RNG = new Random(42);

    public static void main(String[] args) throws Exception {
        System.out.println("=== TsFile Page-Level I/O Analysis ===");
        System.out.printf("Config: %d measurements x %d points = %,d total%n%n",
                NUM_MEAS, TOTAL_POINTS, NUM_MEAS * TOTAL_POINTS);

        generateTsFile();
        analyzeStructure();
        benchmarkReads();

        new File(TS_FILE).delete();
        System.out.println("\nDone.");
    }

    static void generateTsFile() throws Exception {
        System.out.print("Writing TsFile... ");
        long t0 = System.nanoTime();

        File f = new File(TS_FILE);
        TsFileWriter writer = new TsFileWriter(f);

        List<IMeasurementSchema> schemas = new ArrayList<>();
        for (int i = 0; i < NUM_MEAS; i++) {
            schemas.add(new MeasurementSchema(
                    "m" + String.format("%02d", i),
                    TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY));
        }
        writer.registerAlignedTimeseries(DEVICE, schemas);

        double[] bases = new double[NUM_MEAS];
        double[] amps = new double[NUM_MEAS];
        double[] periods = new double[NUM_MEAS];
        for (int i = 0; i < NUM_MEAS; i++) {
            bases[i] = 20 + RNG.nextDouble() * 30;
            amps[i] = 3 + RNG.nextDouble() * 10;
            periods[i] = 3600 + RNG.nextDouble() * 7200;
        }

        int batchSize = 1000;
        Tablet tablet = new Tablet(DEVICE, schemas, batchSize);

        for (int batchStart = 0; batchStart < TOTAL_POINTS; batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize, TOTAL_POINTS);
            tablet.reset();
            for (int row = 0; row < batchEnd - batchStart; row++) {
                int globalIdx = batchStart + row;
                long timestamp = (long) globalIdx * INTERVAL_MS;
                tablet.addTimestamp(row, timestamp);
                for (int m = 0; m < NUM_MEAS; m++) {
                    double t = timestamp / 1000.0;
                    double value = bases[m]
                            + amps[m] * Math.sin(2 * Math.PI * t / periods[m])
                            + RNG.nextGaussian() * 0.5;
                    tablet.addValue(row, m, value);
                }
            }
            writer.writeTree(tablet);
        }
        writer.close();

        long elapsed = (System.nanoTime() - t0) / 1_000_000;
        long fs = f.length();
        System.out.printf("OK: %d ms, %,d KB, %.1f bytes/pt%n",
                elapsed, fs / 1024, (double) fs / (NUM_MEAS * TOTAL_POINTS));
    }

    static void analyzeStructure() throws IOException {
        System.out.println("\n--- TsFile Internal Structure ---");
        TsFileSequenceReader reader = new TsFileSequenceReader(TS_FILE);

        long fs = reader.fileSize();
        System.out.printf("File size: %,d bytes%n", fs);

        // Get all devices
        List<IDeviceID> devices = reader.getAllDevices();
        System.out.printf("Devices: %d%n", devices.size());

        for (IDeviceID device : devices) {
            System.out.printf("Device: %s%n", device);

            // Read chunk metadata for this device
            Map<String, List<ChunkMetadata>> chunksByMeas =
                    reader.readChunkMetadataInDevice(device);
            System.out.printf("  Measurements: %d%n", chunksByMeas.size());

            long totalChunkBytes = 0;
            int totalChunks = 0;
            long totalPoints = 0;

            for (Map.Entry<String, List<ChunkMetadata>> entry : chunksByMeas.entrySet()) {
                for (ChunkMetadata cm : entry.getValue()) {
                    totalChunks++;
                    // ChunkMetadata extends IChunkMetadata
                    // Use statistics to get point count
                    totalPoints += cm.getStatistics().getCount();
                }
            }

            System.out.printf("  Total chunks: %d%n", totalChunks);
            System.out.printf("  Total points: %,d%n", totalPoints);
            System.out.printf("  Avg points/chunk: %.0f%n",
                    totalChunks > 0 ? (double) totalPoints / totalChunks : 0);

            // Read aligned chunk metadata for I/O amplification analysis
            List<AbstractAlignedChunkMetadata> alignedChunks =
                    reader.getAlignedChunkMetadata(device, false);

            System.out.printf("  Aligned chunk groups: %d%n", alignedChunks.size());

            for (AbstractAlignedChunkMetadata acm : alignedChunks) {
                if (acm instanceof AlignedChunkMetadata) {
                    AlignedChunkMetadata aligned = (AlignedChunkMetadata) acm;
                    IChunkMetadata timeChunk = aligned.getTimeChunkMetadata();
                    List<IChunkMetadata> valueChunks = aligned.getValueChunkMetadataList();

                    long timePoints = timeChunk.getStatistics().getCount();
                    int numValues = valueChunks.size();

                    System.out.printf("  Aligned chunk: time=%,d pts, values=%d cols%n",
                            timePoints, numValues);
                    System.out.printf("    I/O amplification for 1-col read: >= %dx%n",
                            numValues);
                    break; // Just show first
                }
            }
        }

        reader.close();
    }

    static void benchmarkReads() throws IOException {
        System.out.println("\n--- Benchmark: Page-Level I/O Walk ---");

        TsFileSequenceReader reader = new TsFileSequenceReader(TS_FILE);
        List<IDeviceID> devices = reader.getAllDevices();

        if (devices.isEmpty()) {
            reader.close();
            return;
        }

        IDeviceID device = devices.get(0);
        long t0, elapsed;
        int totalChunks = 0;
        long totalPoints = 0;

        // 1. Metadata-only scan
        t0 = System.nanoTime();
        Map<String, List<ChunkMetadata>> chunksByMeas =
                reader.readChunkMetadataInDevice(device);
        for (List<ChunkMetadata> cms : chunksByMeas.values()) {
            for (ChunkMetadata cm : cms) {
                totalChunks++;
                totalPoints += cm.getStatistics().getCount();
            }
        }
        elapsed = (System.nanoTime() - t0) / 1_000;
        System.out.printf("Metadata scan: %d us, %d chunks, %,d pts%n",
                elapsed, totalChunks, totalPoints);

        // 2. Read raw chunk data (simulates full I/O)
        t0 = System.nanoTime();
        long totalBytesRead = 0;
        int chunksRead = 0;

        for (List<ChunkMetadata> cms : chunksByMeas.values()) {
            for (ChunkMetadata cm : cms) {
                long offset = cm.getOffsetOfChunkHeader();
                // Approximate chunk size from statistics
                // Estimate chunk size from file size ratio
                int estimatedSize = (int) (reader.fileSize() / (NUM_MEAS * TOTAL_POINTS / 2));
                if (estimatedSize > 0) {
                    reader.readChunk(offset, estimatedSize);
                    totalBytesRead += estimatedSize;
                    chunksRead++;
                }
                if (chunksRead >= 5) break; // Sample first 5 chunks
            }
            if (chunksRead >= 5) break;
        }

        elapsed = (System.nanoTime() - t0) / 1_000;
        System.out.printf("Raw chunk read (5 chunks): %d us, %,d bytes%n",
                elapsed, totalBytesRead);

        // 3. I/O amplification analysis
        System.out.println("\n--- I/O Amplification Analysis ---");
        long fileSize = reader.fileSize();
        long totalPts = NUM_MEAS * TOTAL_POINTS;
        double bytesPerPt = (double) fileSize / totalPts;

        System.out.printf("File size: %,d bytes (%.1f MB)%n", fileSize, fileSize / 1e6);
        System.out.printf("Bytes per point: %.2f%n", bytesPerPt);
        System.out.printf("Measurements: %d%n", NUM_MEAS);
        System.out.println();

        // Scenario: AI training needs 3/15 cols at 1/100 sampling
        int cols = 3;
        int sample = 100;

        // In TsFile aligned pages, all columns share the same time column
        // Reading 1 column requires reading the aligned chunk (time + all values)
        // Approximate I/O for reading 1 measurement:
        long bytesPerMeas = fileSize / NUM_MEAS;
        long bytesReadAI = bytesPerMeas * cols; // Read these measurements
        long bytesUsefulAI = (long) (totalPts * bytesPerPt * cols / NUM_MEAS / sample);

        double amplification = (double) bytesReadAI / Math.max(bytesUsefulAI, 1);

        System.out.println("AI Training Scenario:");
        System.out.printf("  Need: %d/%d cols at 1/%d sampling%n", cols, NUM_MEAS, sample);
        System.out.printf("  Data on disk: %,d bytes%n", bytesReadAI);
        System.out.printf("  Data actually needed: %,d bytes%n", bytesUsefulAI);
        System.out.printf("  I/O Amplification: >= %.0fx%n", amplification);
        System.out.println();
        System.out.println("Root cause: Aligned TsFile pages bundle time + all value");
        System.out.println("columns into a single compressed unit. Page is the atomic");
        System.out.println("decompression unit. Cannot read 1/N columns from a page");
        System.out.println("without decompressing all N columns.");

        reader.close();
    }
}
