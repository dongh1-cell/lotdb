import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.file.metadata.*;
import org.apache.tsfile.enums.TSDataType;
import java.util.*;

public class VerifyMultiRes {
    public static void main(String[] args) throws Exception {
        String[] files = {"data/tsfile_multi/d_000_L0.tsfile",
                          "data/tsfile_multi/d_000_L10.tsfile",
                          "data/tsfile_multi/d_000_L100.tsfile"};

        for (String fpath : files) {
            java.io.File f = new java.io.File(fpath);
            long fsize = f.length();
            TsFileSequenceReader r = new TsFileSequenceReader(fpath);
            IDeviceID dev = r.getAllDevices().get(0);
            Map<String, TSDataType> measMap = r.getMeasurement(dev);

            List<String> measurements = new ArrayList<>();
            for (String k : measMap.keySet()) {
                if (k != null && !k.isEmpty()) measurements.add(k);
            }
            Collections.sort(measurements);

            System.out.println("=== " + f.getName() + " ===");
            System.out.println("  Size: " + fsize + " bytes (" + String.format("%.1f", fsize/1024.0) + " KB)");
            System.out.println("  Device: " + dev);
            System.out.println("  Measurement count: " + measurements.size());
            System.out.println("  Measurements: " + measurements);

            long totalPoints = 0;
            long totalCompressed = 0;
            for (String meas : measurements) {
                List<ChunkMetadata> metas = r.getChunkMetadataList(dev, meas, true);
                int measPoints = 0;
                long measCompressed = 0;
                for (ChunkMetadata cm : metas) {
                    measPoints += (int) cm.getNumOfPoints();
                    measCompressed += cm.getStatistics().getCount() > 0
                            ? (long)(cm.getNumOfPoints() * 2.5) : 0;
                }
                System.out.println("    " + meas + ": " + measPoints + " points, " + metas.size() + " chunks");
                totalPoints += measPoints;
                totalCompressed += measCompressed;
            }
            System.out.println("  Total points (all meas): " + totalPoints);
            System.out.println();
            r.close();
        }
    }
}
