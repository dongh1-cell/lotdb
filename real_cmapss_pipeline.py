"""Prepare and smoke-test a real NASA C-MAPSS workload.

This script intentionally avoids importing the full ``rul-datasets`` package
because that package pulls training dependencies that are not required for file
format benchmarking. It uses the same C-MAPSS raw files and default sensor
selection documented in ``rul-datasets``.
"""

from __future__ import annotations

import json
import shutil
import time
from datetime import datetime
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq

from benchmark_runner import (
    ArrowFeatherPandasQuerier,
    ArrowIPCProjectedQuerier,
    HDF5Querier,
    ParquetDatasetQuerier,
    ParquetFileQuerier,
    ParquetNaiveQuerier,
    _run_query_with_metrics,
)


ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data_real" / "raw" / "CMAPSS"
OUT_DIR = ROOT / "data_real" / "cmapss_fd001"
RESULT_DIR = ROOT / "results" / "raw_json"

# Same default channels used by rul-datasets CmapssReader.
DEFAULT_CHANNELS = [4, 5, 6, 9, 10, 11, 13, 14, 15, 16, 17, 19, 22, 23]


def load_cmapss_fd001_long() -> pd.DataFrame:
    raw_path = RAW_DIR / "train_FD001.txt"
    if not raw_path.exists():
        raise FileNotFoundError(
            f"{raw_path} not found. Download/extract CMAPSSData.zip first."
        )

    names = ["unit", "cycle"]
    names += [f"setting_{i}" for i in range(1, 4)]
    names += [f"sensor_{i:02d}" for i in range(1, 22)]
    raw = pd.read_csv(raw_path, sep=r"\s+", header=None, names=names)

    # feature_select indexes the 24 channels after unit/cycle:
    # 3 operation settings + 21 sensors.
    channel_names = names[2:]
    selected = [channel_names[idx] for idx in DEFAULT_CHANNELS]

    frames = []
    for unit, unit_df in raw.groupby("unit", sort=True):
        device_id = f"fd001_unit_{int(unit):03d}"
        melted = unit_df.melt(
            id_vars=["cycle"],
            value_vars=selected,
            var_name="measurement",
            value_name="value",
        )
        melted.insert(0, "device_id", device_id)
        melted = melted.rename(columns={"cycle": "time"})
        melted["time"] = melted["time"].astype(np.int64)
        melted["value"] = melted["value"].astype(np.float64)
        frames.append(melted[["device_id", "measurement", "time", "value"]])

    return pd.concat(frames, ignore_index=True)


def write_formats(df: pd.DataFrame) -> dict[str, str]:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    (OUT_DIR / "parquet").mkdir(parents=True)
    (OUT_DIR / "arrow").mkdir(parents=True)
    (OUT_DIR / "hdf5").mkdir(parents=True)

    csv_path = OUT_DIR / "cmapss_fd001_long.csv"
    df.to_csv(csv_path, index=False)

    for dev, group in df.groupby("device_id", sort=True):
        group = group.sort_values(["measurement", "time"]).reset_index(drop=True)
        table = pa.Table.from_pandas(group, preserve_index=False)
        pq.write_table(
            table,
            OUT_DIR / "parquet" / f"{dev}.parquet",
            compression="snappy",
            row_group_size=100_000,
        )
        feather.write_feather(
            group,
            OUT_DIR / "arrow" / f"{dev}.arrow",
            compression="lz4",
        )

    h5_path = OUT_DIR / "hdf5" / "cmapss_fd001.h5"
    with h5py.File(h5_path, "w") as h5:
        for dev, dev_df in df.groupby("device_id", sort=True):
            dev_group = h5.require_group(dev)
            for meas, meas_df in dev_df.groupby("measurement", sort=True):
                meas_group = dev_group.require_group(meas)
                times = meas_df["time"].to_numpy(np.int64)
                values = meas_df["value"].to_numpy(np.float64)
                chunks = (min(4096, len(times)),)
                meas_group.create_dataset(
                    "time",
                    data=times,
                    chunks=chunks,
                    compression="gzip",
                    compression_opts=4,
                )
                meas_group.create_dataset(
                    "value",
                    data=values,
                    chunks=chunks,
                    compression="gzip",
                    compression_opts=4,
                )

    return {
        "csv": str(csv_path),
        "parquet": str(OUT_DIR / "parquet"),
        "arrow": str(OUT_DIR / "arrow"),
        "hdf5": str(h5_path),
    }


def run_p4_real_smoke() -> list[dict]:
    parquet_dir = OUT_DIR / "parquet"
    arrow_dir = OUT_DIR / "arrow"
    hdf5_path = OUT_DIR / "hdf5" / "cmapss_fd001.h5"
    queriers = {
        "parquet_naive": ParquetNaiveQuerier(parquet_dir),
        "parquet_file": ParquetFileQuerier(parquet_dir),
        "parquet_dataset": ParquetDatasetQuerier(parquet_dir),
        "arrow_feather_pandas": ArrowFeatherPandasQuerier(arrow_dir),
        "arrow_ipc_projected": ArrowIPCProjectedQuerier(arrow_dir),
        "hdf5": HDF5Querier(hdf5_path),
    }

    devices = sorted([p.stem for p in parquet_dir.glob("*.parquet")])[:5]
    measurements = ["sensor_02", "sensor_03", "sensor_04"]
    rng = np.random.default_rng(20260525)
    results = []

    for device in devices:
        table = pq.read_table(parquet_dir / f"{device}.parquet", columns=["time"])
        max_time = int(table["time"].to_numpy().max())
        if max_time <= 31:
            continue
        windows = []
        for _ in range(100):
            start = int(rng.integers(1, max_time - 30))
            windows.append((start, start + 30))

        for fmt, q in queriers.items():
            query_windows = windows[:20] if fmt == "parquet_naive" else windows
            scale = len(windows) / len(query_windows)
            byte_cost = 0
            if hasattr(q, "_cost"):
                if fmt.startswith("arrow"):
                    byte_cost = q._cost(device, measurements[0])
                elif fmt == "hdf5":
                    byte_cost = sum(q._cost(device, m) for m in measurements)
                else:
                    fraction = min(1.0, (30 * len(windows)) / max_time)
                    byte_cost = sum(int(q._cost(device, m) * fraction) for m in measurements)

            metrics = _run_query_with_metrics(
                q, "random_windows", (device, measurements, query_windows), byte_cost
            )
            if fmt == "parquet_naive":
                metrics["wall_time_s"] *= scale
                metrics["cpu_user_s"] *= scale
                metrics["cpu_sys_s"] *= scale
                metrics["points_returned"] = int(round(metrics["points_returned"] * scale))
                metrics["bytes_useful"] = metrics["points_returned"] * 16
                metrics["read_amplification"] = (
                    metrics["bytes_read"] / metrics["bytes_useful"]
                    if metrics["bytes_useful"]
                    else 0.0
                )
                metrics["estimated"] = True
                metrics["sample_windows"] = len(query_windows)
                metrics["scale_factor"] = scale

            results.append(
                {
                    "dataset": "NASA C-MAPSS FD001 train",
                    "pattern": "real_random_windows",
                    "format": fmt,
                    "device": device,
                    "n_windows": len(windows),
                    "window_length": 30,
                    "n_measurements": len(measurements),
                    "measurements": measurements,
                    **metrics,
                }
            )

    return results


def main() -> None:
    t0 = time.perf_counter()
    df = load_cmapss_fd001_long()
    paths = write_formats(df)
    results = run_p4_real_smoke()
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULT_DIR / f"real_cmapss_fd001_p4_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")

    summary = {
        "rows": int(len(df)),
        "devices": int(df["device_id"].nunique()),
        "measurements": int(df["measurement"].nunique()),
        "paths": paths,
        "result_json": str(out),
        "elapsed_s": round(time.perf_counter() - t0, 3),
    }
    print(json.dumps(summary, indent=2))
    by_fmt = {}
    for row in results:
        by_fmt.setdefault(row["format"], []).append(row)
    for fmt, rows in sorted(by_fmt.items()):
        avg_wall = sum(r["wall_time_s"] for r in rows) / len(rows)
        print(fmt, "avg_wall_s=", round(avg_wall, 4))


if __name__ == "__main__":
    main()
