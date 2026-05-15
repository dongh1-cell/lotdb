/**
 * GORILLA Resync Codec: standard GORILLA encoding punctuated with periodic
 * uncompressed "resync markers" that serve as independent decode starting
 * points.
 *
 * Format (all multi-byte integers big-endian):
 *   [header]
 *      total_values : int32           total number of double values
 *      num_markers  : int32           count of resync markers
 *      marker_dir   : MarkerEntry[]   one entry per marker (sorted by value_index)
 *   [data]
 *      for each segment:
 *        [first_value: 64 bits raw]    <- resync marker (uncompressed)
 *        [remaining values: GORILLA-encoded variable bits]
 *
 * Each MarkerEntry = (value_index: int32, byte_offset: int32)
 *
 * The first_value of every segment serves as a resync point: the decoder
 * can skip to any value index by binary-searching the marker directory,
 * seeking to the corresponding byte offset, reading the uncompressed value,
 * and decoding forward from there.
 *
 * Segment boundaries are created by re-initializing the GORILLA encoder
 * every markerInterval values. This means the standard TsFile
 * DoublePrecisionEncoderV1 / DoublePrecisionDecoderV1 are reused
 * per-segment, and correctness is maintained.
 */

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GorillaResyncCodec {

    /** Default: insert a resync marker every 64 values (storage overhead ~12%) */
    public static final int DEFAULT_MARKER_INTERVAL = 64;

    // ---- Encoder ----

    /**
     * Encode an array of double values with periodic resync markers.
     *
     * @param values   input values
     * @param interval resync marker interval (values per segment)
     * @return encoded bytes (marker directory + segmented GORILLA data)
     */
    public static byte[] encode(double[] values, int interval) throws IOException {
        if (interval <= 0) interval = DEFAULT_MARKER_INTERVAL;
        int n = values.length;
        int numMarkers = (n + interval - 1) / interval;

        // ---- Pass 1: encode segments, record marker offsets ----
        List<Integer> markerOffsets = new ArrayList<>();
        ByteArrayOutputStream dataStream = new ByteArrayOutputStream();

        for (int seg = 0; seg < numMarkers; seg++) {
            int segStart = seg * interval;
            int segEnd = Math.min(segStart + interval, n);
            int segLen = segEnd - segStart;

            // Record byte offset of this segment's first byte
            markerOffsets.add(dataStream.size());

            // Create a new GORILLA encoder for this segment
            org.apache.tsfile.encoding.encoder.DoublePrecisionEncoderV1 encoder =
                    new org.apache.tsfile.encoding.encoder.DoublePrecisionEncoderV1();

            ByteArrayOutputStream segStream = new ByteArrayOutputStream();
            for (int i = 0; i < segLen; i++) {
                encoder.encode(values[segStart + i], segStream);
            }
            encoder.flush(segStream);
            dataStream.write(segStream.toByteArray());
        }

        byte[] dataBytes = dataStream.toByteArray();

        // ---- Build marker directory (includes total_values) ----
        ByteBuffer header = ByteBuffer.allocate(8 + numMarkers * 8);
        header.order(ByteOrder.BIG_ENDIAN);
        header.putInt(n);   // total_values
        header.putInt(numMarkers);
        for (int i = 0; i < numMarkers; i++) {
            header.putInt(i * interval);           // value_index of this marker
            header.putInt(markerOffsets.get(i));    // byte_offset of this segment
        }

        // ---- Concatenate: header + data ----
        byte[] result = new byte[header.position() + dataBytes.length];
        System.arraycopy(header.array(), 0, result, 0, header.position());
        System.arraycopy(dataBytes, 0, result, header.position(), dataBytes.length);
        return result;
    }

    /**
     * Convenience: encode with default interval.
     */
    public static byte[] encode(double[] values) throws IOException {
        return encode(values, DEFAULT_MARKER_INTERVAL);
    }

    // ---- Decoder ----

    /**
     * Decode all values from a resync-encoded buffer.
     * Decodes until all bytes are consumed (works for any segment count).
     */
    public static double[] decodeAll(byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        int totalValues = buf.getInt();
        int numMarkers = buf.getInt();

        int[] markerOffsets = new int[numMarkers];
        for (int i = 0; i < numMarkers; i++) {
            buf.getInt(); // skip marker value_index
            markerOffsets[i] = buf.getInt();
        }

        int headerEnd = buf.position();
        double[] result = new double[totalValues];
        int outIdx = 0;

        for (int seg = 0; seg < numMarkers && outIdx < totalValues; seg++) {
            int segDataStart = headerEnd + markerOffsets[seg];
            int segDataEnd = (seg + 1 < numMarkers)
                    ? headerEnd + markerOffsets[seg + 1]
                    : data.length;
            if (segDataStart >= data.length) break;

            int segLen = Math.min(segDataEnd - segDataStart, data.length - segDataStart);
            byte[] segBytes = new byte[segLen];
            System.arraycopy(data, segDataStart, segBytes, 0, segLen);

            var decoder = new org.apache.tsfile.encoding.decoder.DoublePrecisionDecoderV1();
            ByteBuffer segBuf = ByteBuffer.wrap(segBytes);
            int maxSegValues = totalValues - outIdx;
            for (int i = 0; i < maxSegValues && segBuf.hasRemaining(); i++) {
                try {
                    result[outIdx++] = decoder.readDouble(segBuf);
                } catch (Exception e) { break; }
            }
        }

        if (outIdx < totalValues) {
            double[] trimmed = new double[outIdx];
            System.arraycopy(result, 0, trimmed, 0, outIdx);
            return trimmed;
        }
        return result;
    }

    /**
     * Decode a subset of values using resync markers for skip-to-point.
     *
     * @param data        encoded bytes
     * @param targetIndices  indices of values to decode (must be sorted)
     * @return decoded values in the same order as targetIndices
     */
    public static double[] decodeSampled(byte[] data, int[] targetIndices) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        int totalValues = buf.getInt();  // skip total_values
        int numMarkers = buf.getInt();

        int[] markerIndices = new int[numMarkers];
        int[] markerOffsets = new int[numMarkers];
        for (int i = 0; i < numMarkers; i++) {
            markerIndices[i] = buf.getInt();
            markerOffsets[i] = buf.getInt();
        }

        int headerEnd = buf.position();
        double[] result = new double[targetIndices.length];

        for (int t = 0; t < targetIndices.length; t++) {
            int targetIdx = targetIndices[t];

            // Binary search for nearest preceding marker
            int markerIdx = findMarker(markerIndices, targetIdx);

            int segDataStart = headerEnd + markerOffsets[markerIdx];
            int segDataEnd = (markerIdx + 1 < numMarkers)
                    ? headerEnd + markerOffsets[markerIdx + 1]
                    : data.length;

            byte[] segBytes = new byte[segDataEnd - segDataStart];
            System.arraycopy(data, segDataStart, segBytes, 0, segBytes.length);

            // Decode segment from marker to target
            org.apache.tsfile.encoding.decoder.DoublePrecisionDecoderV1 decoder =
                    new org.apache.tsfile.encoding.decoder.DoublePrecisionDecoderV1();

            ByteBuffer segBuf = ByteBuffer.wrap(segBytes);
            int skipCount = targetIdx - markerIndices[markerIdx];
            double value = Double.NaN;
            for (int i = 0; i <= skipCount; i++) {
                try {
                    value = decoder.readDouble(segBuf);
                } catch (Exception e) {
                    break;
                }
            }
            result[t] = value;
        }

        return result;
    }

    /** Binary search for largest marker index <= targetIdx */
    private static int findMarker(int[] markerIndices, int targetIdx) {
        int lo = 0, hi = markerIndices.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) / 2;
            if (markerIndices[mid] <= targetIdx) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    /** Count how many double values were decoded (for I/O accounting). */
    public static int countDecodedValues(byte[] data, int[] targetIndices) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
        int totalValues = buf.getInt(); // skip
        int numMarkers = buf.getInt();
        int[] markerIndices = new int[numMarkers];
        for (int i = 0; i < numMarkers; i++) {
            markerIndices[i] = buf.getInt();
            buf.getInt(); // skip offset
        }

        int totalDecoded = 0;
        for (int targetIdx : targetIndices) {
            int markerIdx = findMarker(markerIndices, targetIdx);
            int skipCount = targetIdx - markerIndices[markerIdx] + 1;
            totalDecoded += skipCount;
        }
        return totalDecoded;
    }
}
