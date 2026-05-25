import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;

import java.io.File;
import java.util.Collections;
import java.util.Map;

public class ValidateMultiRes {
    static class FileInfo {
        String device;
        String measurement;
        int count;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ValidateMultiRes <orig_tsfile> <multi_dir>");
            System.exit(1);
        }
        File orig = new File(args[0]);
        File dir = new File(args[1]);
        String dev = orig.getName().replace(".tsfile", "");
        File l0 = new File(dir, dev + "_L0.tsfile");
        File l10 = new File(dir, dev + "_L10.tsfile");
        File l100 = new File(dir, dev + "_L100.tsfile");

        FileInfo info = info(orig);
        String meas = info.measurement;
        System.out.println("measurement=" + meas);
        printInfo("orig", orig, meas);
        printInfo("L0", l0, meas);
        printInfo("L10", l10, meas);
        printInfo("L100", l100, meas);
        printFirstRows("orig", orig, meas, 5);
        printFirstRows("L10", l10, meas, 5);
        printFirstRows("L100", l100, meas, 5);

        int[] idxs = {0, 1, 2, 10, 100, 1000, 4319};
        long baseStart = 1704038400L;
        for (int idx : idxs) {
            double orig10 = queryValue(orig, meas, baseStart + idx * 10L * 2L);
            double l10v = queryValue(l10, meas, baseStart + idx * 10L * 2L);
            double orig100 = queryValue(orig, meas, baseStart + idx * 100L * 2L);
            double l100v = queryValue(l100, meas, baseStart + idx * 100L * 2L);
            System.out.printf("idx=%d L10 orig=%f l10=%f diff=%g | L100 orig=%f l100=%f diff=%g%n",
                    idx, orig10, l10v, Math.abs(orig10 - l10v),
                    orig100, l100v, Math.abs(orig100 - l100v));
        }
    }

    static FileInfo info(File f) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(f.getAbsolutePath());
        IDeviceID device = seq.getAllDevices().get(0);
        Map<String, TSDataType> measMap = seq.getMeasurement(device);
        String meas = null;
        for (String k : measMap.keySet()) {
            if (k != null && !k.isEmpty()) {
                meas = k;
                break;
            }
        }
        int total = 0;
        for (var cm : seq.getChunkMetadataList(device, meas, true)) {
            total += (int) cm.getNumOfPoints();
        }
        FileInfo info = new FileInfo();
        info.device = device.toString();
        info.measurement = meas;
        info.count = total;
        seq.close();
        return info;
    }

    static void printInfo(String label, File f, String meas) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(f.getAbsolutePath());
        IDeviceID device = seq.getAllDevices().get(0);
        int total = 0;
        for (var cm : seq.getChunkMetadataList(device, meas, true)) {
            total += (int) cm.getNumOfPoints();
        }
        System.out.printf("%s size=%d count=%d device=%s%n", label, f.length(), total, device);
        seq.close();
    }

    static double queryValue(File f, String meas, long time) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(f.getAbsolutePath());
        TsFileReader reader = new TsFileReader(seq);
        String device = seq.getAllDevices().get(0).toString();
        Path p = new Path(device, meas, true);
        var tf = FilterFactory.and(TimeFilterApi.gtEq(time), TimeFilterApi.ltEq(time));
        var expr = QueryExpression.create(Collections.singletonList(p), new GlobalTimeExpression(tf));
        var ds = reader.query(expr);
        double val = Double.NaN;
        if (ds.hasNext()) {
            var row = ds.next();
            var fields = row.getFields();
            String fieldText = fields.toString();
            for (int i = 0; i < fields.size(); i++) {
                try {
                    val = fields.get(i).getDoubleV();
                    break;
                } catch (Exception ignored) {
                }
            }
        }
        reader.close();
        seq.close();
        return val;
    }

    static void printFirstRows(String label, File f, String meas, int limit) throws Exception {
        TsFileSequenceReader seq = new TsFileSequenceReader(f.getAbsolutePath());
        TsFileReader reader = new TsFileReader(seq);
        String device = seq.getAllDevices().get(0).toString();
        Path p = new Path(device, meas, true);
        var expr = QueryExpression.create(Collections.singletonList(p), null);
        var ds = reader.query(expr);
        System.out.print(label + " firstRows:");
        int n = 0;
        while (ds.hasNext() && n < limit) {
            var row = ds.next();
            double val = Double.NaN;
            var fields = row.getFields();
            String fieldText = fields.toString();
            for (int i = 0; i < fields.size(); i++) {
                try {
                    val = fields.get(i).getDoubleV();
                    break;
                } catch (Exception ignored) {
                }
            }
            System.out.print(" (" + row.getTimestamp() + "," + val + ",fields=" + fieldText + ")");
            n++;
        }
        System.out.println();
        reader.close();
        seq.close();
    }
}
