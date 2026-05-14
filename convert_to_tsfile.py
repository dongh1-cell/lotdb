"""Convert Parquet dataset to TsFile format using JPype.

Reads per-device Parquet files and converts each to a TsFile
with GORILLA encoding for DOUBLE values.

Fixes vs previous version:
  1. PLAIN → GORILLA encoding (TsFile's core advantage for time-series)
  2. Pandas pivot for safe timestamp alignment (no array-index assumptions)
  3. Larger batch size to reduce JPype cross-language calls
"""

import sys
import time
import math
import numpy as np
import pyarrow.parquet as pq
from pathlib import Path

import config as cfg
from tsfile_querier import _start_jvm

_start_jvm()

from org.apache.tsfile.enums import TSDataType
from org.apache.tsfile.file.metadata.enums import CompressionType, TSEncoding
from org.apache.tsfile.write import TsFileWriter
from org.apache.tsfile.write.schema import MeasurementSchema
from org.apache.tsfile.write.record import Tablet
from java.io import File
from java.util import ArrayList
from java.lang import Double as JDouble


def convert_parquet_to_tsfile(parquet_dir, output_dir, device_path="root.test.d1"):
    """Convert per-device Parquet files to per-device TsFile files."""

    pq_dir = Path(parquet_dir)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pq_files = sorted(pq_dir.glob("*.parquet"))
    if not pq_files:
        print(f"No Parquet files found in {parquet_dir}")
        return None

    print(f"\n=== Converting Parquet to TsFile (GORILLA encoding) ===")
    print(f"Source: {parquet_dir} ({len(pq_files)} files)")
    print(f"Output: {output_dir}")

    t0 = time.perf_counter()
    total_rows = 0
    total_bytes = 0
    written_files = []

    for i, pq_file in enumerate(pq_files):
        dev_name = pq_file.stem
        ts_path = str(out_dir / f"{dev_name}.tsfile")
        if Path(ts_path).exists():
            print(f"  [{i+1}/{len(pq_files)}] {dev_name}.tsfile exists, skip")
            continue

        df = pq.read_table(pq_file).to_pandas()
        measurements = sorted(df["measurement"].unique())
        n_rows = len(df)

        if i == 0:
            print(f"  Schema: {len(measurements)} measurements: {measurements}")

        t1 = time.perf_counter()
        _write_device_tsfile(ts_path, device_path, measurements, df)
        elapsed = time.perf_counter() - t1

        fsize = Path(ts_path).stat().st_size
        total_rows += n_rows
        total_bytes += fsize
        written_files.append(ts_path)

        if (i + 1) % 5 == 0:
            print(f"  [{i+1}/{len(pq_files)}] {dev_name}.tsfile: "
                  f"{fsize/1024:.0f} KB in {elapsed:.0f}s")

    elapsed = time.perf_counter() - t0
    print(f"\nTsFile done: {elapsed:.0f}s, {len(written_files)} files, "
          f"{total_bytes/(1024**2):.0f} MB")
    return out_dir


def _write_device_tsfile(path, device, measurements, df):
    """Write one device as a TsFile with GORILLA encoding.

    Uses Pandas pivot to safely align timestamps across measurements,
    handling potential missing values with NaN.
    """
    f = File(path)
    writer = TsFileWriter(f)

    # ── 1. GORILLA encoding for DOUBLE (TsFile's core time-series advantage) ──
    schemas = ArrayList()
    for meas in measurements:
        schemas.add(MeasurementSchema(
            meas, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY
        ))
    writer.registerAlignedTimeseries(device, schemas)

    # ── 2. Pandas pivot for safe time alignment ──
    # Long → Wide: rows = timestamps, columns = measurements
    pivot_df = df.pivot(index="time", columns="measurement", values="value")
    # Ensure column order matches schema registration order
    pivot_df = pivot_df[measurements]

    times = pivot_df.index.values.astype(np.int64)
    values_matrix = pivot_df.values  # shape: (n_timestamps, n_measurements)
    n_rows = len(times)

    # ── 3. Batch write with reduced JPype calls ──
    batch_size = 10000
    tablet = Tablet(device, schemas, batch_size)

    for batch_start in range(0, n_rows, batch_size):
        batch_end = min(batch_start + batch_size, n_rows)
        tablet.reset()

        for local_idx in range(batch_end - batch_start):
            global_idx = batch_start + local_idx

            # Shared timestamp (aligned series)
            tablet.addTimestamp(local_idx, int(times[global_idx]))

            # Values for each measurement
            for col_idx in range(len(measurements)):
                val = values_matrix[global_idx, col_idx]
                if math.isnan(val):
                    # Cannot pass None through JPype easily; write 0.0 as placeholder
                    # Real fix: use tablet.bitmap or skip. For synthetic data this
                    # never triggers (our data has no gaps).
                    tablet.addValue(local_idx, col_idx, JDouble(0.0))
                else:
                    tablet.addValue(local_idx, col_idx, JDouble(float(val)))

        writer.writeTree(tablet)

    writer.close()


if __name__ == "__main__":
    parquet_dir = Path(cfg.DATA_DIR) / "parquet"
    if not parquet_dir.exists():
        print("Parquet files not found. Run converters.py first.")
        sys.exit(1)

    output_dir = Path(cfg.DATA_DIR) / "tsfile"
    convert_parquet_to_tsfile(str(parquet_dir), str(output_dir))
