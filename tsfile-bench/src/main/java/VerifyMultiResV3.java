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

public class VerifyMultiResV3 {
    public static void main(String[] args) throws Exception {
        // Use the ORIGINAL files (not multi_res copies) for L0
        String l0Path   = "data/tsfile/d_000.tsfile";
        String l10Path  = "data/tsfile_multi/d_000_L10.tsfile";
        String l100Path = "data/tsfile_multi/d_000_L100.tsfile";

        // Discover
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
        String meas = measurements.get(0);
        System.out.println("Measurement: " + meas);

        // Read and compare
        compareEveryNth(l0Path, l10Path, devicePath, meas, 10, "L0_vs_L10");
        compareEveryNth(l0Path, l100Path, devicePath, meas, 100, "L0_vs_L100");

        // Check what the original L0 file size is vs multi_res L0
        System.out.println("=== File sizes ===");
        System.out.println("Original L0: " + new java.io.File(l0Path).length() + " bytes");
        System.out.println("MultiRes L10: " + new java.io.File(l10Path).length() + " bytes");
        System.out.println("MultiRes L100: " + new java.io.File(l100Path).length() + " bytes");

        // Also verify L10/L100 measurement count and points
        System.out.println("\n=== L10 structure ===");
        printStructure(l10Path, dev, measurements);
        System.out.println("\n=== L100 structure ===");
        printStructure(l100Path, dev, measurements);
    }

    static void compareEveryNth(String l0Path, String downPath, String devicePath,
                                 String meas, int step, String label) throws Exception {
        // Read L0 (full res) — just record every Nth value
        double[] l0Sampled = readValuesAtStride(l0Path, devicePath, meas, step);
        // Read downsampled file — all values
        double[] downVals = readAllValuesTS(downPath, devicePath, meas);

        System.out.println("\n=== " + label + ": step=" + step + " ===");
        System.out.println("  L0 sampled (every " + step + "): " + l0Sampled.length + " values");
        System.out.println("  Downsampled file: " + downVals.length + " values");

        int n = Math.min(l0Sampled.length, downVals.length);
        int mismatches = 0;
        double maxDiff = 0;
        for (int i = 0; i < n; i++) {
            double diff = Math.abs(l0Sampled[i] - downVals[i]);
            if (diff > maxDiff) maxDiff = diff;
            if (diff > 1e-12) {
                if (mismatches < 3) {
                    System.out.println("  MISMATCH[" + i + "]: L0=" + l0Sampled[i] + " down=" + downVals[i] + " diff=" + diff);
                }
                mismatches++;
            }
        }
        if (l0Sampled.length != downVals.length) {
            System.out.println("  LENGTH MISMATCH: L0_sampled=" + l0Sampled.length + " down=" + downVals.length);
            mismatches += Math.abs(l0Sampled.length - downVals.length);
        }
        System.out.println("  Mismatches: " + mismatches + " / " + n + "  MaxDiff: " + maxDiff);
        System.out.println("  First 5 L0:   " + Arrays.toString(Arrays.copyOf(l0Sampled, Math.min(5, l0Sampled.length))));
        System.out.println("  First 5 Down: " + Arrays.toString(Arrays.copyOf(downVals, Math.min(5, downVals.length))));
    }

    static double[] readAllValuesTS(String fpath, String devicePath, String meas) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(fpath);
        TsFileReader reader = new TsFileReader(seq);
        // Use same query pattern as TsFileNativeRunner
        Path p = new Path(devicePath, meas, true);
        var tf = FilterFactory.and(TimeFilterApi.gtEq(0L), TimeFilterApi.ltEq(Long.MAX_VALUE));
        var expr = QueryExpression.create(Collections.singletonList(p), new GlobalTimeExpression(tf));
        var ds = reader.query(expr);

        List<Double> vals = new ArrayList<>();
        while (ds.hasNext()) {
            var row = ds.next();
            int fieldCount = row.getFields().size();
            // In table model, fields[0]=time, fields[1]=value
            if (fieldCount > 1) {
                var field = row.getFields().get(1);
                if (field != null && field.getDataType() == TSDataType.DOUBLE) {
                    vals.add(field.getDoubleV());
                }
            }
        }
        reader.close();
        seq.close();
        double[] result = new double[vals.size()];
        for (int i = 0; i < vals.size(); i++) result[i] = vals.get(i);
        return result;
    }

    static double[] readValuesAtStride(String fpath, String devicePath, String meas, int step) throws Exception {
        double[] all = readAllValuesTS(fpath, devicePath, meas);
        int n = all.length / step;
        double[] sampled = new double[n];
        for (int i = 0; i < n; i++) sampled[i] = all[i * step];
        return sampled;
    }

    static void printStructure(String fpath, IDeviceID dev, List<String> measurements) throws Exception {
        TsFileSequenceReader r = new TsFileSequenceReader(fpath);
        long fsize = new java.io.File(fpath).length();
        System.out.println("  File size: " + fsize + " bytes (" + String.format("%.1f", fsize/1024.0) + " KB)");

        IDeviceID firstDev = r.getAllDevices().get(0);
        for (String meas : measurements) {
            List<ChunkMetadata> metas = r.getChunkMetadataList(firstDev, meas, true);
            for (ChunkMetadata cm : metas) {
                int numPoints = (int)cm.getNumOfPoints();
                System.out.println("  " + meas + ": " + numPoints + " pts"
                        + ", startTime=" + cm.getStartTime() + ", endTime=" + cm.getEndTime()
                        + ", offset=" + cm.getOffsetOfChunkHeader());
            }
        }
        r.close();
    }
}
