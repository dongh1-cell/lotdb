import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.file.metadata.*;
import org.apache.tsfile.enums.TSDataType;
import java.util.*;

public class VerifyMultiResV5 {
    public static void main(String[] args) throws Exception {
        String l0Path   = "data/tsfile/d_000.tsfile";
        String l10Path  = "data/tsfile_multi/d_000_L10.tsfile";
        String l100Path = "data/tsfile_multi/d_000_L100.tsfile";

        // Discover EXACTLY like TsFileNativeRunner
        TsFileSequenceReader r0 = new TsFileSequenceReader(l0Path);
        List<IDeviceID> devs = r0.getAllDevices();
        String devicePath = devs.get(0).toString();
        // Use readChunkMetadataInDevice — same as TsFileNativeRunner
        Map<String, List<ChunkMetadata>> chunks = r0.readChunkMetadataInDevice(devs.get(0));
        List<String> measurements = new ArrayList<>();
        for (String key : chunks.keySet()) {
            if (key != null && !key.isEmpty()) measurements.add(key);
        }
        Collections.sort(measurements);
        r0.close();
        String meas = measurements.get(0);

        System.out.println("Device: " + devicePath);
        System.out.println("Measurement: " + meas);
        System.out.println("All: " + measurements.size());

        // Read L0
        double[] l0Vals = readAll(l0Path, devicePath, meas);
        System.out.println("L0: " + l0Vals.length + " values");
        if (l0Vals.length == 0) {
            System.out.println("FATAL: cannot read L0");
            return;
        }

        // Read L10
        double[] l10Vals = readAll(l10Path, devicePath, meas);
        System.out.println("L10: " + l10Vals.length + " values");

        // Read L100
        double[] l100Vals = readAll(l100Path, devicePath, meas);
        System.out.println("L100: " + l100Vals.length + " values");

        // Compare
        compare("L10", l0Vals, l10Vals, 10);
        compare("L100", l0Vals, l100Vals, 100);

        // Samples
        System.out.println("\nSamples:");
        for (int i = 0; i < 5; i++) {
            System.out.printf("  L0[%d]=%.6f  L0[%d*%d]=%.6f  L10[%d]=%.6f  L100[%d]=%.6f%n",
                    i, l0Vals[i], i*10+9, 10, l0Vals[i*10+9], i, l10Vals[i], i, l100Vals[i]);
        }
    }

    static double[] readAll(String fpath, String devicePath, String meas) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(fpath);
        TsFileReader reader = new TsFileReader(seq);
        Path path = new Path(devicePath, meas, true);
        // Use time filter like TsFileNativeRunner warmup (0 to Long.MAX_VALUE)
        org.apache.tsfile.read.filter.basic.Filter f1 = TimeFilterApi.gtEq(0L);
        org.apache.tsfile.read.filter.basic.Filter f2 = TimeFilterApi.ltEq(Long.MAX_VALUE);
        org.apache.tsfile.read.filter.basic.Filter tf = FilterFactory.and(f1, f2);
        QueryExpression expr = QueryExpression.create(
                Collections.singletonList(path), new GlobalTimeExpression(tf));
        var ds = reader.query(expr);

        List<Double> vals = new ArrayList<>();
        while (ds.hasNext()) {
            RowRecord row = ds.next();
            List<org.apache.tsfile.read.common.Field> fields = row.getFields();
            // In table model aligned tsfile: fields[0]=timestamp, fields[1]=value
            if (fields.size() >= 2) {
                var f = fields.get(1);
                if (f != null) {
                    try {
                        vals.add(f.getDoubleV());
                    } catch (Exception e) {
                        // skip null/invalid
                    }
                }
            }
        }
        reader.close();
        seq.close();
        double[] result = new double[vals.size()];
        for (int i = 0; i < vals.size(); i++) result[i] = vals.get(i);
        return result;
    }

    static void compare(String label, double[] l0, double[] down, int step) {
        int n = Math.min(down.length, l0.length / step);
        int mismatches = 0;
        double maxDiff = 0;
        for (int i = 0; i < n; i++) {
            double diff = Math.abs(l0[i * step] - down[i]);
            if (diff > maxDiff) maxDiff = diff;
            if (diff > 1e-12) {
                if (mismatches < 3) System.out.printf("  %s MISMATCH[%d]: L0=%.10f down=%.10f diff=%.2e%n",
                        label, i, l0[i*step], down[i], diff);
                mismatches++;
            }
        }
        System.out.printf("%s: %d mismatches / %d (%.2f%%), maxDiff=%.2e%n",
                label, mismatches, n, 100.0*mismatches/n, maxDiff);
        if (l0.length / step != down.length) {
            System.out.printf("  LENGTH DIFF: L0_sampled=%d down=%d%n", l0.length/step, down.length);
        }
    }
}
