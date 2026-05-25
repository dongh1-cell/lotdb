import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.file.metadata.*;
import org.apache.tsfile.enums.TSDataType;
import java.util.*;

public class VerifyMultiResV4 {
    public static void main(String[] args) throws Exception {
        String l0Path   = "data/tsfile/d_000.tsfile";
        String l10Path  = "data/tsfile_multi/d_000_L10.tsfile";
        String l100Path = "data/tsfile_multi/d_000_L100.tsfile";

        // Discover device and measurement from L0
        TsFileSequenceReader r0 = new TsFileSequenceReader(l0Path);
        IDeviceID dev = r0.getAllDevices().get(0);
        String devicePath = dev.toString();
        Map<String, TSDataType> measMap = r0.getMeasurement(dev);
        List<String> measurements = new ArrayList<>();
        for (String k : measMap.keySet()) {
            if (k != null && !k.isEmpty()) measurements.add(k);
        }
        Collections.sort(measurements);
        String meas = measurements.get(0);
        r0.close();

        System.out.println("Device: " + devicePath);
        System.out.println("Measurement: " + meas);
        System.out.println("All measurements: " + measurements.size());

        // ── Try both Path modes ──
        System.out.println("\n=== Trying different Path constructors ===");

        // Mode 1: tree model path (isTable=false)
        System.out.println("\n--- Path(device, meas, false) - tree model ---");
        double[] vals1 = readValues(l0Path, devicePath, meas, false);
        System.out.println("  Got " + vals1.length + " values");

        // Mode 2: table model path (isTable=true)
        System.out.println("\n--- Path(device, meas, true) - table model ---");
        double[] vals2 = readValues(l0Path, devicePath, meas, true);
        System.out.println("  Got " + vals2.length + " values");

        // Determine which mode works
        boolean useTableModel = vals2.length > vals1.length;
        System.out.println("\nUsing " + (useTableModel ? "table model" : "tree model") + " path");

        // ── Now do the actual comparison ──
        System.out.println("\n=== Actual comparison ===");
        double[] l0All = useTableModel ? vals2 : vals1;

        if (l0All.length == 0) {
            System.out.println("ERROR: Cannot read L0 values!");
            return;
        }
        System.out.println("L0 total values: " + l0All.length);

        // L10
        double[] l10Vals = readValues(l10Path, devicePath, meas, useTableModel);
        System.out.println("L10 values: " + l10Vals.length);

        // L100
        double[] l100Vals = readValues(l100Path, devicePath, meas, useTableModel);
        System.out.println("L100 values: " + l100Vals.length);

        // Compare L0[10*i] vs L10[i]
        int n10 = Math.min(l10Vals.length, l0All.length / 10);
        int mismatches10 = 0;
        double maxDiff10 = 0;
        for (int i = 0; i < n10; i++) {
            double diff = Math.abs(l0All[i * 10] - l10Vals[i]);
            if (diff > maxDiff10) maxDiff10 = diff;
            if (diff > 1e-12) {
                if (mismatches10 < 3) System.out.println("  L10 MISMATCH[" + i + "]: L0=" + l0All[i*10] + " L10=" + l10Vals[i] + " diff=" + diff);
                mismatches10++;
            }
        }
        System.out.println("L10 correctness: " + mismatches10 + " mismatches / " + n10 + " checked, maxDiff=" + maxDiff10);

        // Compare L0[100*i] vs L100[i]
        int n100 = Math.min(l100Vals.length, l0All.length / 100);
        int mismatches100 = 0;
        double maxDiff100 = 0;
        for (int i = 0; i < n100; i++) {
            double diff = Math.abs(l0All[i * 100] - l100Vals[i]);
            if (diff > maxDiff100) maxDiff100 = diff;
            if (diff > 1e-12) {
                if (mismatches100 < 3) System.out.println("  L100 MISMATCH[" + i + "]: L0=" + l0All[i*100] + " L100=" + l100Vals[i] + " diff=" + diff);
                mismatches100++;
            }
        }
        System.out.println("L100 correctness: " + mismatches100 + " mismatches / " + n100 + " checked, maxDiff=" + maxDiff100);

        // Print samples
        System.out.println("\n=== Samples ===");
        System.out.println("L0[0:10]:   " + Arrays.toString(Arrays.copyOf(l0All, 10)));
        System.out.println("L10[0:10]:  " + Arrays.toString(Arrays.copyOf(l10Vals, Math.min(10, l10Vals.length))));
        System.out.println("L100[0:10]: " + Arrays.toString(Arrays.copyOf(l100Vals, Math.min(10, l100Vals.length))));
    }

    static double[] readValues(String fpath, String devicePath, String meas, boolean useTableModel) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(fpath);
        TsFileReader reader = new TsFileReader(seq);
        Path p = new Path(devicePath, meas, useTableModel);
        var expr = QueryExpression.create(Collections.singletonList(p), null);
        var ds = reader.query(expr);

        List<Double> vals = new ArrayList<>();
        while (ds.hasNext()) {
            var row = ds.next();
            var fields = row.getFields();
            if (fields.size() > 1 && fields.get(1) != null
                    && fields.get(1).getDataType() == TSDataType.DOUBLE) {
                vals.add(fields.get(1).getDoubleV());
            }
        }
        reader.close();
        seq.close();
        double[] result = new double[vals.size()];
        for (int i = 0; i < vals.size(); i++) result[i] = vals.get(i);
        return result;
    }
}
