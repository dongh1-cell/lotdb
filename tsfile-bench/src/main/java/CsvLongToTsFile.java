import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class CsvLongToTsFile {
    static final int BATCH_SIZE = 10000;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: CsvLongToTsFile <long_csv> <output_dir>");
            System.exit(1);
        }
        File csv = new File(args[0]);
        File outDir = new File(args[1]);
        outDir.mkdirs();

        Map<String, Map<String, TreeMap<Long, Double>>> data = readLongCsv(csv);
        int fileCount = 0;
        long totalRows = 0;
        for (var devEntry : data.entrySet()) {
            String deviceId = devEntry.getKey();
            Map<String, TreeMap<Long, Double>> byMeasurement = devEntry.getValue();
            File out = new File(outDir, deviceId + ".tsfile");
            if (out.exists() && !out.delete()) {
                throw new RuntimeException("Cannot replace " + out);
            }
            totalRows += writeDevice(out, deviceId, byMeasurement);
            fileCount++;
            if (fileCount % 10 == 0) {
                System.out.println("converted " + fileCount + " devices");
            }
        }
        System.out.println("{\"devices\":" + fileCount + ",\"rows\":" + totalRows + "}");
    }

    static Map<String, Map<String, TreeMap<Long, Double>>> readLongCsv(File csv) throws Exception {
        Map<String, Map<String, TreeMap<Long, Double>>> data = new LinkedHashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csv))) {
            String line = br.readLine();
            if (line == null || !line.startsWith("device_id,")) {
                throw new IllegalArgumentException("Expected header: device_id,measurement,time,value");
            }
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",", -1);
                if (parts.length < 4) continue;
                String device = parts[0];
                String measurement = parts[1];
                long time = Long.parseLong(parts[2]);
                double value = Double.parseDouble(parts[3]);
                data.computeIfAbsent(device, k -> new LinkedHashMap<>())
                        .computeIfAbsent(measurement, k -> new TreeMap<>())
                        .put(time, value);
            }
        }
        return data;
    }

    static long writeDevice(File out, String deviceId,
                            Map<String, TreeMap<Long, Double>> byMeasurement) throws Exception {
        List<String> measurements = new ArrayList<>(byMeasurement.keySet());
        Collections.sort(measurements);

        TreeSet<Long> times = new TreeSet<>();
        for (String meas : measurements) {
            times.addAll(byMeasurement.get(meas).keySet());
        }
        List<Long> timeList = new ArrayList<>(times);

        List<IMeasurementSchema> schemas = new ArrayList<>();
        for (String meas : measurements) {
            schemas.add(new MeasurementSchema(
                    meas, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY));
        }

        String devicePath = "root.cmapss." + deviceId;
        TsFileWriter writer = new TsFileWriter(out);
        writer.registerAlignedTimeseries(devicePath, schemas);
        Tablet tablet = new Tablet(devicePath, schemas, BATCH_SIZE);

        for (int batchStart = 0; batchStart < timeList.size(); batchStart += BATCH_SIZE) {
            int batchEnd = Math.min(batchStart + BATCH_SIZE, timeList.size());
            tablet.reset();
            tablet.setRowSize(batchEnd - batchStart);
            for (int row = 0; row < batchEnd - batchStart; row++) {
                long t = timeList.get(batchStart + row);
                tablet.addTimestamp(row, t);
                for (int col = 0; col < measurements.size(); col++) {
                    Double v = byMeasurement.get(measurements.get(col)).get(t);
                    tablet.addValue(row, col, v == null ? 0.0 : v);
                }
            }
            writer.writeTree(tablet);
        }
        writer.close();
        return (long) timeList.size() * measurements.size();
    }
}
