import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.file.metadata.*;
import org.apache.tsfile.enums.TSDataType;
import java.util.*;

public class VerifyMultiResV2 {
    public static void main(String[] args) throws Exception {
        String l0Path  = "data/tsfile_multi/d_000_L0.tsfile";
        String l10Path = "data/tsfile_multi/d_000_L10.tsfile";
        String l100Path= "data/tsfile_multi/d_000_L100.tsfile";

        // Discover device and first measurement from L0
        TsFileSequenceReader r0 = new TsFileSequenceReader(l0Path);
        IDeviceID dev = r0.getAllDevices().get(0);
        String devicePath = dev.toString();
        Map<String, TSDataType> measMap = r0.getMeasurement(dev);
        List<String> measurements = new ArrayList<>();
        for (String k : measMap.keySet()) {
            if (k != null && !k.isEmpty()) measurements.add(k);
        }
        Collections.sort(measurements);
        r0.close();
        String meas = measurements.get(0); // first measurement

        System.out.println("Device: " + devicePath);
        System.out.println("Measurement: " + meas);
        System.out.println("Total measurements: " + measurements.size());
        System.out.println();

        // Read all values from L0 (full resolution)
        double[] l0Vals = readAllValues(l0Path, devicePath, meas);
        System.out.println("L0 values: " + l0Vals.length);

        // Read all values from L10
        double[] l10Vals = readAllValues(l10Path, devicePath, meas);
        System.out.println("L10 values: " + l10Vals.length);

        // Read all values from L100
        double[] l100Vals = readAllValues(l100Path, devicePath, meas);
        System.out.println("L100 values: " + l100Vals.length);
        System.out.println();

        // L10[i] should equal L0[i*10] (every 10th point from L0)
        System.out.println("=== L0 vs L10: every 10th point comparison ===");
        int mismatchCount = 0;
        double maxAbsDiff = 0;
        for (int i = 0; i < Math.min(l10Vals.length, l0Vals.length / 10); i++) {
            double expected = l0Vals[i * 10];
            double actual = l10Vals[i];
            double diff = Math.abs(expected - actual);
            if (diff > maxAbsDiff) maxAbsDiff = diff;
            if (diff > 1e-12) {
                if (mismatchCount < 5) {
                    System.out.println("  MISMATCH at i=" + i + " (t=" + (i*10) + "): L0[" + (i*10) + "]=" + expected + "  L10[" + i + "]=" + actual + "  diff=" + diff);
                }
                mismatchCount++;
            }
        }
        System.out.println("  Total mismatches: " + mismatchCount + " / " + Math.min(l10Vals.length, l0Vals.length / 10));
        System.out.println("  Max absolute diff: " + maxAbsDiff);
        System.out.println();

        // L100[i] should equal L0[i*100]
        System.out.println("=== L0 vs L100: every 100th point comparison ===");
        mismatchCount = 0;
        maxAbsDiff = 0;
        for (int i = 0; i < Math.min(l100Vals.length, l0Vals.length / 100); i++) {
            double expected = l0Vals[i * 100];
            double actual = l100Vals[i];
            double diff = Math.abs(expected - actual);
            if (diff > maxAbsDiff) maxAbsDiff = diff;
            if (diff > 1e-12) {
                if (mismatchCount < 5) {
                    System.out.println("  MISMATCH at i=" + i + " (t=" + (i*100) + "): L0[" + (i*100) + "]=" + expected + "  L100[" + i + "]=" + actual + "  diff=" + diff);
                }
                mismatchCount++;
            }
        }
        System.out.println("  Total mismatches: " + mismatchCount + " / " + Math.min(l100Vals.length, l0Vals.length / 100));
        System.out.println("  Max absolute diff: " + maxAbsDiff);
        System.out.println();

        // Print first 10 values from each for visual inspection
        System.out.println("=== Sample values (first 10) ===");
        System.out.println("L0:   " + Arrays.toString(Arrays.copyOf(l0Vals, 10)));
        System.out.println("L10:  " + Arrays.toString(Arrays.copyOf(l10Vals, 10)));
        System.out.println("L100: " + Arrays.toString(Arrays.copyOf(l100Vals, 10)));
        System.out.println();

        // Check timestamps
        System.out.println("=== Timestamp verification ===");
        printTimestamps(l10Path, devicePath, meas, "L10");
        printTimestamps(l100Path, devicePath, meas, "L100");
    }

    static double[] readAllValues(String fpath, String devicePath, String meas) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(fpath);
        TsFileReader reader = new TsFileReader(seq);
        Path p = new Path(devicePath, meas, true);
        var tf = FilterFactory.and(
                TimeFilterApi.gtEq(0L), TimeFilterApi.ltEq(Long.MAX_VALUE));
        var expr = QueryExpression.create(
                Collections.singletonList(p), new GlobalTimeExpression(tf));
        var ds = reader.query(expr);
        List<Double> vals = new ArrayList<>();
        while (ds.hasNext()) {
            var row = ds.next();
            var fields = row.getFields();
            // fields[0] = timestamp, fields[1] = value
            if (fields.size() > 1 && fields.get(1) != null) {
                vals.add(fields.get(1).getDoubleV());
            }
        }
        reader.close();
        seq.close();
        double[] result = new double[vals.size()];
        for (int i = 0; i < vals.size(); i++) result[i] = vals.get(i);
        return result;
    }

    static void printTimestamps(String fpath, String devicePath, String meas, String label) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(fpath);
        TsFileReader reader = new TsFileReader(seq);
        Path p = new Path(devicePath, meas, true);
        var tf = FilterFactory.and(
                TimeFilterApi.gtEq(0L), TimeFilterApi.ltEq(Long.MAX_VALUE));
        var expr = QueryExpression.create(
                Collections.singletonList(p), new GlobalTimeExpression(tf));
        var ds = reader.query(expr);
        long[] timestamps = new long[10];
        int count = 0;
        while (ds.hasNext() && count < 10) {
            timestamps[count++] = ds.next().getTimestamp();
        }
        System.out.println("  " + label + " first 10 timestamps: " + Arrays.toString(timestamps));
        reader.close();
        seq.close();
    }
}
