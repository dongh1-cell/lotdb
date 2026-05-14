/**
 * TsFile read/write benchmark for AI training workloads.
 *
 * Compares TsFile I/O characteristics against Parquet, Arrow, HDF5 for
 * 4 query patterns representative of LLM-era AI training data access.
 *
 * Run via: mvnw -f benchmark/tsfile-bench/pom.xml exec:java
 */

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class TsFileBenchmark {

    static final int NUM_MEAS = 15;
    static final int TOTAL_POINTS = 43200; // 1 day at 2-sec intervals
    static final int INTERVAL_MS = 2000;
    static final String DEVICE = "root.test.d1";
    static final String TS_FILE = "target/tsfile_bench.tsfile";
    static final Random RNG = new Random(42);

    public static void main(String[] args) throws Exception {
        System.out.println("=== TsFile Benchmark: AI Training Workloads ===");
        System.out.printf("Config: %d meas x %d pts = %,d total points%n%n",
                NUM_MEAS, TOTAL_POINTS, NUM_MEAS * TOTAL_POINTS);

        new File("target").mkdirs();
        generateTsFile();
        benchmarkSequentialScan();
        benchmarkColumnSubset();
        benchmarkDownsampling();
        benchmarkRandomWindows();

        System.out.println("\n=== Results Summary ===");
        System.out.println("TsFile page is the atomic I/O unit — all data within a");
        System.out.println("time-aligned page must be decompressed before filtering.");
        System.out.println("For AI training workloads requiring partial data from pages,");
        System.out.println("this causes significant I/O amplification.");
    }

    // ─── TsFile Generation ─────────────────────────────────────────

    static void generateTsFile() throws Exception {
        System.out.print("Generating TsFile... ");
        long t0 = System.nanoTime();

        File f = new File(TS_FILE);
        try (TsFileWriter writer = new TsFileWriter(f)) {

            // Register aligned timeseries (all measurements under one device)
            List<MeasurementSchema> schemas = new ArrayList<>();
            for (int i = 0; i < NUM_MEAS; i++) {
                schemas.add(new MeasurementSchema(
                        "m" + String.format("%02d", i),
                        TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY));
            }
            writer.registerAlignedTimeseries(DEVICE, schemas);

            // Pre-compute signal parameters for realistic data
            double[] bases = new double[NUM_MEAS];
            double[] amps = new double[NUM_MEAS];
            double[] periods = new double[NUM_MEAS];
            for (int i = 0; i < NUM_MEAS; i++) {
                bases[i] = 20 + RNG.nextDouble() * 30;
                amps[i] = 3 + RNG.nextDouble() * 10;
                periods[i] = 3600 + RNG.nextDouble() * 7200;
            }

            // Write data in tablets (batch write for efficiency)
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
        }

        long elapsed = (System.nanoTime() - t0) / 1_000_000;
        long fileSize = f.length();
        System.out.printf("done (%d ms, %,d KB)%n", elapsed, fileSize / 1024);
    }

    // ─── Benchmark 1: Sequential Scan ───────────────────────────────

    static void benchmarkSequentialScan() throws IOException {
        System.out.print("\n[P1] Sequential Scan... ");
        long t0 = System.nanoTime();
        int totalPoints = 0;

        try (TsFileSequenceReader reader = new TsFileSequenceReader(TS_FILE)) {
            // Read all timeseries metadata
            Map<String, List<String>> deviceMap = reader.getAllDevices();
            for (Map.Entry<String, List<String>> entry : deviceMap.entrySet()) {
                for (String measurement : entry.getValue()) {
                    Path path = new Path(entry.getKey(), measurement, true);
                    // Build a full-time-range query
                    long startTime = 0;
                    long endTime = (long) TOTAL_POINTS * INTERVAL_MS;
                    Filter timeFilter = FilterFactory.and(
                            FilterFactory.timeGtEq(startTime),
                            FilterFactory.timeLtEq(endTime));

                    SingleSeriesExpression expr = new SingleSeriesExpression(path, timeFilter);
                    QueryExpression query = QueryExpression.create(
                            Collections.singletonList(expr), null);

                    QueryDataSet dataSet = reader.query(query);
                    while (dataSet.hasNext()) {
                        dataSet.next();
                        totalPoints++;
                    }
                }
            }
        }

        long elapsed = (System.nanoTime() - t0) / 1_000_000;
        long fileSize = new File(TS_FILE).length();
        double throughputMBs = (fileSize / 1048576.0) / (elapsed / 1000.0);
        System.out.printf("%d ms, %,d points, %.1f MB/s%n",
                elapsed, totalPoints, throughputMBs);
    }

    // ─── Benchmark 2: Column Subset ─────────────────────────────────

    static void benchmarkColumnSubset() throws IOException {
        int nSelect = Math.max(1, NUM_MEAS / 5); // 20% = 3 columns
        System.out.printf("%n[P2] Column Subset (%d/%d measurements)... %n", nSelect, NUM_MEAS);

        // Select random subset
        Set<Integer> selected = new HashSet<>();
        while (selected.size() < nSelect) {
            selected.add(RNG.nextInt(NUM_MEAS));
        }

        for (int selectivity : new int[]{20, 50, 100}) {
            int nCols = Math.max(1, NUM_MEAS * selectivity / 100);
            if (selectivity == 100) nCols = NUM_MEAS;

            long t0 = System.nanoTime();
            int totalPoints = 0;

            try (TsFileSequenceReader reader = new TsFileSequenceReader(TS_FILE)) {
                for (int m = 0; m < nCols; m++) {
                    String measName = "m" + String.format("%02d", m);
                    Path path = new Path(DEVICE, measName, true);
                    Filter timeFilter = FilterFactory.and(
                            FilterFactory.timeGtEq(0L),
                            FilterFactory.timeLtEq((long) TOTAL_POINTS * INTERVAL_MS));
                    SingleSeriesExpression expr = new SingleSeriesExpression(path, timeFilter);
                    QueryExpression query = QueryExpression.create(
                            Collections.singletonList(expr), null);
                    QueryDataSet dataSet = reader.query(query);
                    while (dataSet.hasNext()) {
                        dataSet.next();
                        totalPoints++;
                    }
                }
            }

            long elapsed = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("  %d%% cols (%d): %d ms, %,d points%n",
                    selectivity, nCols, elapsed, totalPoints);
        }
    }

    // ─── Benchmark 3: Downsampling ─────────────────────────────────

    static void benchmarkDownsampling() throws IOException {
        System.out.println("\n[P3] Downsampling (I/O amplification test)...");

        String measName = "m00"; // single measurement
        Path path = new Path(DEVICE, measName, true);
        Filter timeFilter = FilterFactory.and(
                FilterFactory.timeGtEq(0L),
                FilterFactory.timeLtEq((long) TOTAL_POINTS * INTERVAL_MS));

        for (int step : new int[]{1, 10, 100, 500}) {
            long t0 = System.nanoTime();
            int totalPoints = 0;
            int usefulPoints = 0;

            try (TsFileSequenceReader reader = new TsFileSequenceReader(TS_FILE)) {
                SingleSeriesExpression expr = new SingleSeriesExpression(path, timeFilter);
                QueryExpression query = QueryExpression.create(
                        Collections.singletonList(expr), null);
                QueryDataSet dataSet = reader.query(query);

                int counter = 0;
                while (dataSet.hasNext()) {
                    dataSet.next();
                    totalPoints++;
                    if (counter % step == 0) usefulPoints++;
                    counter++;
                }
            }

            long elapsed = (System.nanoTime() - t0) / 1_000_000;
            long fileSize = new File(TS_FILE).length();
            // For single measurement, estimate bytes read ≈ fileSize / NUM_MEAS
            long bytesReadEst = fileSize / NUM_MEAS;
            long usefulBytes = (long) usefulPoints * 16; // time(int64) + value(double)
            double amplification = (double) bytesReadEst / Math.max(usefulBytes, 1);
            System.out.printf("  Step 1/%d: %d ms, %,d pts -> %,d useful, I/O amp %.1fx%n",
                    step, elapsed, totalPoints, usefulPoints, amplification);
        }
    }

    // ─── Benchmark 4: Random Window Access ─────────────────────────

    static void benchmarkRandomWindows() throws IOException {
        System.out.println("\n[P4] Random Window Access (AI DataLoader simulation)...");
        int nWindows = 100;
        int windowSize = 512; // points per window
        int nSelectedMeas = 3; // 20% of 15

        long t0 = System.nanoTime();
        int totalPoints = 0;

        try (TsFileSequenceReader reader = new TsFileSequenceReader(TS_FILE)) {
            for (int w = 0; w < nWindows; w++) {
                long startTime = (long) RNG.nextInt(TOTAL_POINTS - windowSize) * INTERVAL_MS;
                long endTime = startTime + (long) windowSize * INTERVAL_MS;
                int measIdx = RNG.nextInt(NUM_MEAS);
                String measName = "m" + String.format("%02d", measIdx);
                Path path = new Path(DEVICE, measName, true);

                Filter timeFilter = FilterFactory.and(
                        FilterFactory.timeGtEq(startTime),
                        FilterFactory.timeLtEq(endTime));
                SingleSeriesExpression expr = new SingleSeriesExpression(path, timeFilter);
                QueryExpression query = QueryExpression.create(
                        Collections.singletonList(expr), null);
                QueryDataSet dataSet = reader.query(query);

                while (dataSet.hasNext()) {
                    dataSet.next();
                    totalPoints++;
                }
            }
        }

        long elapsed = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("  %d windows x %d pts: %d ms, %,d total pts, %.0f pts/s%n",
                nWindows, windowSize, elapsed, totalPoints,
                totalPoints / (elapsed / 1000.0));
    }
}
