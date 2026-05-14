"""Convert CSV dataset to Parquet, Arrow IPC, and HDF5 formats.

Strategy: read CSV once into per-device Parquet files, then convert
Parquet -> Arrow and Parquet -> HDF5 for efficiency.
"""

import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather
import h5py
from pathlib import Path
import shutil

import config as cfg


def convert_to_parquet(csv_path):
    """Single-pass CSV -> per-device Parquet files."""
    print("\n=== Converting CSV to Parquet ===")
    output_dir = Path(cfg.DATA_DIR) / "parquet"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(exist_ok=True)

    t0 = time.perf_counter()
    total_rows = 0

    chunk_iter = pd.read_csv(
        csv_path,
        chunksize=2_000_000,
        dtype={"device_id": str, "measurement": str, "time": np.int64, "value": np.float64},
    )

    device_file_handles = {}  # device -> (writer, row_count)

    for chunk_idx, chunk in enumerate(chunk_iter):
        for dev_id, group in chunk.groupby("device_id"):
            # Sort by measurement, time for TsFile-like layout
            group = group.sort_values(["measurement", "time"]).reset_index(drop=True)
            table = pa.Table.from_pandas(group, preserve_index=False)

            if dev_id not in device_file_handles:
                out_path = output_dir / f"{dev_id}.parquet"
                writer = pq.ParquetWriter(
                    out_path, table.schema,
                    compression="snappy",
                    max_rows_per_page=500_000,
                )
                device_file_handles[dev_id] = (writer, 0)

            writer, count = device_file_handles[dev_id]
            writer.write_table(table)
            device_file_handles[dev_id] = (writer, count + len(group))

        total_rows += len(chunk)
        if (chunk_idx + 1) % 5 == 0:
            pct = 100 * total_rows / (cfg.NUM_DEVICES * cfg.MEASUREMENTS_PER_DEVICE * cfg.DURATION_DAYS * 86400 / cfg.INTERVAL_SECONDS)
            print(f"  Processed {total_rows:,} rows ({pct:.0f}%)")

    # Close all writers
    for dev_id, (writer, count) in device_file_handles.items():
        writer.close()
        print(f"  {dev_id}.parquet: {count:,} rows")

    elapsed = time.perf_counter() - t0
    total_size = sum(f.stat().st_size for f in output_dir.glob("*.parquet"))
    print(f"Parquet done: {elapsed:.0f}s, {total_size/(1024**2):.0f} MB total")
    return output_dir


def convert_to_arrow(parquet_dir):
    """Convert per-device Parquet -> per-device Arrow IPC files."""
    print("\n=== Converting Parquet to Arrow IPC ===")
    output_dir = Path(cfg.DATA_DIR) / "arrow"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(exist_ok=True)

    t0 = time.perf_counter()
    total_rows = 0

    for pq_file in sorted(parquet_dir.glob("*.parquet")):
        df = pq.read_table(pq_file).to_pandas()
        out_path = output_dir / f"{pq_file.stem}.arrow"
        feather.write_feather(df, out_path, compression="lz4")
        total_rows += len(df)
        print(f"  {out_path.name}: {len(df):,} rows")

    elapsed = time.perf_counter() - t0
    total_size = sum(f.stat().st_size for f in output_dir.glob("*.arrow"))
    print(f"Arrow IPC done: {elapsed:.0f}s, {total_size/(1024**2):.0f} MB total")
    return output_dir


def convert_to_hdf5(parquet_dir):
    """Convert per-device Parquet -> HDF5 with chunked storage.

    HDF5 chunk size is set to 100K points — analogous to TsFile page size.
    """
    print("\n=== Converting Parquet to HDF5 ===")
    output_path = Path(cfg.DATA_DIR) / "hdf5" / "iot_dataset.h5"
    output_path.parent.mkdir(exist_ok=True)

    t0 = time.perf_counter()
    total_rows = 0

    with h5py.File(output_path, "w") as f:
        for pq_file in sorted(parquet_dir.glob("*.parquet")):
            device_id = pq_file.stem
            df = pq.read_table(pq_file).to_pandas()

            dev_group = f.require_group(device_id)

            for meas_name, group in df.groupby("measurement"):
                meas_group = dev_group.require_group(meas_name)

                times = group["time"].values
                values = group["value"].values
                n = len(times)
                chunks = (min(100_000, n),)

                # Time array
                ds_t = meas_group.require_dataset(
                    "time", shape=(n,), dtype=np.int64,
                    chunks=chunks, compression="gzip", compression_opts=4,
                )
                ds_t[:] = times

                # Value array
                ds_v = meas_group.require_dataset(
                    "value", shape=(n,), dtype=np.float64,
                    chunks=chunks, compression="gzip", compression_opts=4,
                )
                ds_v[:] = values

                total_rows += n

            n_meas = len(df["measurement"].unique())
            print(f"  {device_id}: {n_meas} measurements, {len(df):,} rows")

    elapsed = time.perf_counter() - t0
    total_size = output_path.stat().st_size
    print(f"HDF5 done: {elapsed:.0f}s, {total_size/(1024**2):.0f} MB total")
    return output_path


def verify_outputs(parquet_dir, arrow_dir, hdf5_path):
    """Verify all converted formats have correct data counts."""
    import numpy as np

    print("\n=== Output Verification ===")
    expected_rows_per_device = cfg.MEASUREMENTS_PER_DEVICE * cfg.DURATION_DAYS * 86400 // cfg.INTERVAL_SECONDS
    expected_total = cfg.NUM_DEVICES * expected_rows_per_device

    # Verify Parquet: count rows from metadata (fast), sample measurements from one file
    pq_total = 0
    pq_meas_set = set()
    first_pf = None
    for pq_file in sorted(parquet_dir.glob("*.parquet")):
        pf = pq.ParquetFile(pq_file)
        pq_total += pf.metadata.num_rows
        if first_pf is None:
            first_pf = pf
    # Read all row groups from first file to collect all measurement names
    if first_pf:
        for i in range(first_pf.metadata.num_row_groups):
            df_sample = first_pf.read_row_group(i).to_pandas()
            pq_meas_set.update(df_sample["measurement"].unique())
    print(f"Parquet: {len(list(parquet_dir.glob('*.parquet')))} files, "
          f"{pq_total:,} rows, {len(pq_meas_set)} measurement names")
    print(f"  Expected: {cfg.NUM_DEVICES} files, {expected_total:,} rows, "
          f"{cfg.MEASUREMENTS_PER_DEVICE} measurements")
    ok_pq = (len(list(parquet_dir.glob("*.parquet"))) == cfg.NUM_DEVICES
             and pq_total == expected_total
             and len(pq_meas_set) == cfg.MEASUREMENTS_PER_DEVICE)

    # Verify Arrow
    arrow_total = 0
    for af in sorted(arrow_dir.glob("*.arrow")):
        df = feather.read_feather(af)
        arrow_total += len(df)
    print(f"Arrow: {len(list(arrow_dir.glob('*.arrow')))} files, {arrow_total:,} rows")
    ok_arrow = (len(list(arrow_dir.glob("*.arrow"))) == cfg.NUM_DEVICES
                and arrow_total == expected_total)

    # Verify HDF5
    with h5py.File(hdf5_path, "r") as f:
        n_dev = len(list(f.keys()))
        d0 = list(f.keys())[0]
        n_meas = len(list(f[d0].keys()))
        m0 = list(f[d0].keys())[0]
        n_pts = len(f[f"{d0}/{m0}/time"])
    print(f"HDF5: {n_dev} devices, {n_meas} measurements/device, "
          f"{n_pts} points/measurement")
    ok_hdf5 = (n_dev == cfg.NUM_DEVICES
               and n_meas == cfg.MEASUREMENTS_PER_DEVICE
               and n_pts == cfg.DURATION_DAYS * 86400 // cfg.INTERVAL_SECONDS)

    all_ok = ok_pq and ok_arrow and ok_hdf5
    print(f"\nOverall: {'ALL OK' if all_ok else 'ISSUES FOUND'}")
    if not all_ok:
        if not ok_pq: print("  Parquet: MISMATCH")
        if not ok_arrow: print("  Arrow: MISMATCH")
        if not ok_hdf5: print("  HDF5: MISMATCH")
    return all_ok


if __name__ == "__main__":
    csv_path = Path(cfg.DATA_DIR) / "iot_dataset.csv"
    if not csv_path.exists():
        print(f"CSV not found: {csv_path}. Run data_gen.py first.")
        exit(1)

    pq_dir = convert_to_parquet(csv_path)
    arrow_dir = convert_to_arrow(pq_dir)
    hdf5_path = convert_to_hdf5(pq_dir)
    print("\nAll format conversions complete.")
    verify_outputs(pq_dir, arrow_dir, hdf5_path)
