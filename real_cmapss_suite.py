"""C-MAPSS real-data benchmark suite.

This suite addresses the teacher feedback that the benchmark should use real
industrial data, compare multiple Arrow layouts, and include training-style
window workloads in addition to uniform random windows.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.feather as feather
import pyarrow.parquet as pq

from benchmark_runner import (
    ArrowFeatherPandasQuerier,
    ArrowIPCProjectedQuerier,
    HDF5Querier,
    ParquetDatasetQuerier,
    ParquetFileQuerier,
    ParquetNaiveQuerier,
    _count_window_rows_from_df,
    _filter_long_df,
    _run_query_with_metrics,
)


ROOT = Path(__file__).resolve().parent
RAW_DIR = ROOT / "data_real" / "raw" / "CMAPSS"
DATA_ROOT = ROOT / "data_real"
RESULT_DIR = ROOT / "results" / "raw_json"

# Same default channel indexes used by rul-datasets CmapssReader. These are
# indexes over operation settings + sensors, not over the whole raw row.
DEFAULT_CHANNELS = [4, 5, 6, 9, 10, 11, 13, 14, 15, 16, 17, 19, 22, 23]


def subset_tag(subset: str) -> str:
    return subset.lower()


def output_dir(subset: str) -> Path:
    return DATA_ROOT / f"cmapss_{subset_tag(subset)}"


def load_cmapss_long(subset: str) -> pd.DataFrame:
    raw_path = RAW_DIR / f"train_{subset}.txt"
    if not raw_path.exists():
        raise FileNotFoundError(f"missing C-MAPSS raw file: {raw_path}")

    names = ["unit", "cycle"]
    names += [f"setting_{i}" for i in range(1, 4)]
    names += [f"sensor_{i:02d}" for i in range(1, 22)]
    raw = pd.read_csv(raw_path, sep=r"\s+", header=None, names=names)

    channel_names = names[2:]
    selected = [channel_names[idx] for idx in DEFAULT_CHANNELS]

    frames = []
    prefix = subset_tag(subset)
    for unit, unit_df in raw.groupby("unit", sort=True):
        device_id = f"{prefix}_unit_{int(unit):03d}"
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


def _wide_device_frame(dev_df: pd.DataFrame) -> pd.DataFrame:
    wide = dev_df.pivot(index="time", columns="measurement", values="value")
    wide = wide.sort_index().reset_index()
    wide.columns.name = None
    return wide


def write_formats(subset: str, df: pd.DataFrame, rebuild: bool) -> dict[str, str]:
    out_dir = output_dir(subset)
    if rebuild and out_dir.exists():
        shutil.rmtree(out_dir)

    for name in ["parquet", "arrow", "arrow_wide", "arrow_series", "hdf5"]:
        (out_dir / name).mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / f"cmapss_{subset_tag(subset)}_long.csv"
    if rebuild or not csv_path.exists():
        df.to_csv(csv_path, index=False)

    for dev, group in df.groupby("device_id", sort=True):
        group = group.sort_values(["measurement", "time"]).reset_index(drop=True)
        table = pa.Table.from_pandas(group, preserve_index=False)
        pq.write_table(
            table,
            out_dir / "parquet" / f"{dev}.parquet",
            compression="snappy",
            row_group_size=100_000,
        )
        feather.write_feather(
            group,
            out_dir / "arrow" / f"{dev}.arrow",
            compression="lz4",
        )

        wide = _wide_device_frame(group)
        feather.write_feather(
            wide,
            out_dir / "arrow_wide" / f"{dev}.arrow",
            compression="lz4",
        )

        rows = []
        for meas, meas_df in group.groupby("measurement", sort=True):
            rows.append(
                {
                    "measurement": meas,
                    "time": meas_df["time"].to_numpy(np.int64).tolist(),
                    "value": meas_df["value"].to_numpy(np.float64).tolist(),
                }
            )
        series_table = pa.table(
            {
                "measurement": pa.array([r["measurement"] for r in rows]),
                "time": pa.array([r["time"] for r in rows], type=pa.list_(pa.int64())),
                "value": pa.array([r["value"] for r in rows], type=pa.list_(pa.float64())),
            }
        )
        feather.write_feather(
            series_table,
            out_dir / "arrow_series" / f"{dev}.arrow",
            compression="lz4",
        )

    h5_path = out_dir / "hdf5" / f"cmapss_{subset_tag(subset)}.h5"
    if h5_path.exists():
        h5_path.unlink()
    with h5py.File(h5_path, "w") as h5:
        for dev, dev_df in df.groupby("device_id", sort=True):
            dev_group = h5.require_group(dev)
            for meas, meas_df in dev_df.groupby("measurement", sort=True):
                meas_group = dev_group.require_group(meas)
                times = meas_df["time"].to_numpy(np.int64)
                values = meas_df["value"].to_numpy(np.float64)
                chunks = (min(4096, len(times)),)
                meas_group.create_dataset(
                    "time", data=times, chunks=chunks, compression="gzip", compression_opts=4
                )
                meas_group.create_dataset(
                    "value", data=values, chunks=chunks, compression="gzip", compression_opts=4
                )

    return {
        "csv": str(csv_path),
        "parquet": str(out_dir / "parquet"),
        "arrow": str(out_dir / "arrow"),
        "arrow_wide": str(out_dir / "arrow_wide"),
        "arrow_series": str(out_dir / "arrow_series"),
        "hdf5": str(h5_path),
    }


class ArrowDatasetQuerier(ArrowIPCProjectedQuerier):
    """Arrow IPC files read through pyarrow.dataset scanner."""

    def __init__(self, data_dir: Path):
        super().__init__(data_dir)
        self._datasets = {
            dev: ds.dataset(path, format="ipc") for dev, path in self.files.items()
        }

    def _scan_df(self, device, measurements, time_start=None, time_end=None):
        if isinstance(measurements, str):
            measurements = [measurements]
        expr = None
        for measurement in measurements:
            cur = ds.field("measurement") == measurement
            expr = cur if expr is None else expr | cur
        if time_start is not None:
            expr = expr & (ds.field("time") >= time_start)
        if time_end is not None:
            expr = expr & (ds.field("time") <= time_end)
        return self._datasets[device].to_table(
            columns=["measurement", "time", "value"], filter=expr
        ).to_pandas()

    def sequential_scan(self, device, measurement, time_start, time_end):
        return len(self._scan_df(device, measurement, time_start, time_end))

    def column_subset(self, device, target_measurements, time_start, time_end):
        return len(self._scan_df(device, target_measurements, time_start, time_end))

    def downsampling(self, device, measurement, time_start, time_end, step_n):
        return len(self._scan_df(device, measurement, time_start, time_end).iloc[::step_n])

    def random_windows(self, device, target_measurements, windows):
        min_start = min(w[0] for w in windows)
        max_end = max(w[1] for w in windows)
        df = self._scan_df(device, target_measurements, min_start, max_end)
        return _count_window_rows_from_df(df, target_measurements, windows)


class ArrowWideQuerier:
    """One Arrow IPC file per device, one column per measurement."""

    def __init__(self, data_dir: Path):
        self.files = {f.stem: str(f) for f in data_dir.glob("*.arrow")}
        self._file_sizes = {dev: Path(p).stat().st_size for dev, p in self.files.items()}

    def _cost(self, device, measurement):
        return self._file_sizes.get(device, 0)

    def _read_df(self, device, columns):
        cols = ["time"] + [c for c in columns if c != "time"]
        return feather.read_table(self.files[device], columns=cols, memory_map=True).to_pandas()

    def sequential_scan(self, device, measurement, time_start, time_end):
        df = self._read_df(device, [measurement])
        mask = (df["time"] >= time_start) & (df["time"] <= time_end)
        return int(mask.sum())

    def column_subset(self, device, target_measurements, time_start, time_end):
        df = self._read_df(device, target_measurements)
        mask = (df["time"] >= time_start) & (df["time"] <= time_end)
        return int(mask.sum()) * len(target_measurements)

    def downsampling(self, device, measurement, time_start, time_end, step_n):
        df = self._read_df(device, [measurement])
        mask = (df["time"] >= time_start) & (df["time"] <= time_end)
        return len(df.loc[mask].iloc[::step_n])

    def random_windows(self, device, target_measurements, windows):
        df = self._read_df(device, target_measurements)
        times = df["time"]
        total = 0
        for start, end in windows:
            total += int(((times >= start) & (times <= end)).sum()) * len(target_measurements)
        return total


class ArrowSeriesRowQuerier:
    """One Arrow IPC file per device, one row per measurement with list values."""

    def __init__(self, data_dir: Path):
        self.files = {f.stem: str(f) for f in data_dir.glob("*.arrow")}
        self._file_sizes = {dev: Path(p).stat().st_size for dev, p in self.files.items()}

    def _cost(self, device, measurement):
        return self._file_sizes.get(device, 0)

    def _read_df(self, device):
        return feather.read_table(self.files[device], memory_map=True).to_pandas()

    def _series(self, device, measurement):
        df = self._read_df(device)
        row = df.loc[df["measurement"] == measurement].iloc[0]
        return np.asarray(row["time"], dtype=np.int64), np.asarray(row["value"], dtype=np.float64)

    def sequential_scan(self, device, measurement, time_start, time_end):
        times, values = self._series(device, measurement)
        mask = (times >= time_start) & (times <= time_end)
        return int(len(values[mask]))

    def column_subset(self, device, target_measurements, time_start, time_end):
        return sum(self.sequential_scan(device, m, time_start, time_end) for m in target_measurements)

    def downsampling(self, device, measurement, time_start, time_end, step_n):
        times, values = self._series(device, measurement)
        mask = (times >= time_start) & (times <= time_end)
        return int(len(values[mask][::step_n]))

    def random_windows(self, device, target_measurements, windows):
        total = 0
        df = self._read_df(device)
        for measurement in target_measurements:
            row = df.loc[df["measurement"] == measurement].iloc[0]
            times = np.asarray(row["time"], dtype=np.int64)
            for start, end in windows:
                total += int(((times >= start) & (times <= end)).sum())
        return total


def make_queriers(out_dir: Path, include_naive: bool) -> dict:
    queriers = {
        "parquet_file": ParquetFileQuerier(out_dir / "parquet"),
        "parquet_dataset": ParquetDatasetQuerier(out_dir / "parquet"),
        "arrow_feather_pandas": ArrowFeatherPandasQuerier(out_dir / "arrow"),
        "arrow_ipc_projected": ArrowIPCProjectedQuerier(out_dir / "arrow"),
        "arrow_dataset": ArrowDatasetQuerier(out_dir / "arrow"),
        "arrow_wide": ArrowWideQuerier(out_dir / "arrow_wide"),
        "arrow_series_row": ArrowSeriesRowQuerier(out_dir / "arrow_series"),
        "hdf5": HDF5Querier(out_dir / "hdf5" / f"{out_dir.name}.h5"),
    }
    h5_files = list((out_dir / "hdf5").glob("*.h5"))
    if h5_files:
        queriers["hdf5"] = HDF5Querier(h5_files[0])
    if include_naive:
        queriers = {"parquet_naive": ParquetNaiveQuerier(out_dir / "parquet"), **queriers}
    return queriers


def device_measurements(out_dir: Path) -> tuple[list[str], list[str]]:
    devices = sorted(p.stem for p in (out_dir / "parquet").glob("*.parquet"))
    first = pq.read_table(out_dir / "parquet" / f"{devices[0]}.parquet", columns=["measurement"])
    measurements = sorted(first["measurement"].to_pandas().unique().tolist())
    return devices, measurements


def device_time_bounds(out_dir: Path, device: str) -> tuple[int, int]:
    table = pq.read_table(out_dir / "parquet" / f"{device}.parquet", columns=["time"])
    arr = table["time"].to_numpy()
    return int(arr.min()), int(arr.max())


def build_windows(kind: str, start: int, end: int, window_length: int, count: int,
                  stride: int, rng: np.random.Generator) -> list[tuple[int, int]]:
    if end <= start:
        return [(start, end)]
    span = min(window_length, end - start + 1)
    max_start = max(start, end - span + 1)
    if kind == "random":
        return [
            (s := int(rng.integers(start, max_start + 1)), min(end, s + span - 1))
            for _ in range(count)
        ]
    windows = []
    cur = start
    while cur <= max_start and len(windows) < count:
        windows.append((cur, min(end, cur + span - 1)))
        cur += max(1, stride)
    return windows


def byte_cost_for(q, fmt: str, device: str, measurements: list[str],
                  fraction: float = 1.0) -> int:
    if not hasattr(q, "_cost"):
        return 0
    if fmt.startswith("arrow"):
        return int(q._cost(device, measurements[0]) * fraction)
    return sum(int(q._cost(device, m) * fraction) for m in measurements)


def run_real_benchmarks(subset: str, workloads: list[str], include_naive: bool,
                        device_limit: int, window_count: int) -> list[dict]:
    out_dir = output_dir(subset)
    queriers = make_queriers(out_dir, include_naive=include_naive)
    devices, measurements = device_measurements(out_dir)
    devices = devices[: min(device_limit, len(devices))]
    rng = np.random.default_rng(20260525 + int(subset[-3:]))
    target_measurements = measurements[: min(3, len(measurements))]
    results = []

    for device in devices:
        t_start, t_end = device_time_bounds(out_dir, device)
        full_range = (t_start, t_end)
        measurement = target_measurements[0]

        # P1-P3 are kept compact but complete across the real formats.
        for fmt, q in queriers.items():
            metrics = _run_query_with_metrics(
                q,
                "sequential_scan",
                (device, measurement, *full_range),
                byte_cost_for(q, fmt, device, [measurement]),
            )
            results.append({
                "dataset": f"NASA C-MAPSS {subset} train",
                "pattern": "sequential_scan",
                "format": fmt,
                "device": device,
                "measurement": measurement,
                **metrics,
            })

            metrics = _run_query_with_metrics(
                q,
                "column_subset",
                (device, target_measurements, *full_range),
                byte_cost_for(q, fmt, device, target_measurements),
            )
            results.append({
                "dataset": f"NASA C-MAPSS {subset} train",
                "pattern": "column_subset",
                "format": fmt,
                "device": device,
                "n_measurements": len(target_measurements),
                **metrics,
            })

            for step in [1, 10, 100]:
                metrics = _run_query_with_metrics(
                    q,
                    "downsampling",
                    (device, measurement, *full_range, step),
                    byte_cost_for(q, fmt, device, [measurement]),
                )
                results.append({
                    "dataset": f"NASA C-MAPSS {subset} train",
                    "pattern": "downsampling",
                    "format": fmt,
                    "device": device,
                    "measurement": measurement,
                    "sample_step": step,
                    **metrics,
                })

        for workload in workloads:
            if workload == "random":
                window_length, stride, source = 30, 1, "GluonTS/PyTorch-style randomized instances; RUL-sized context"
                windows = build_windows("random", t_start, t_end, window_length, window_count, stride, rng)
            elif workload == "sliding":
                window_length, stride, source = 30, 1, "RUL Datasets C-MAPSS sliding windows"
                windows = build_windows("sliding", t_start, t_end, window_length, window_count, stride, rng)
            elif workload == "epoch_reuse":
                window_length, stride, source = 30, 1, "Repeated epoch access over a fixed training-window index"
                base = build_windows("sliding", t_start, t_end, window_length, max(1, window_count // 2), stride, rng)
                windows = base + base
            else:
                raise ValueError(f"unknown workload: {workload}")

            fraction = min(1.0, (window_length * len(windows)) / max(1, t_end - t_start + 1))
            for fmt, q in queriers.items():
                is_estimate = fmt == "parquet_naive" and len(windows) > 20
                query_windows = windows[:20] if is_estimate else windows
                scale = len(windows) / len(query_windows) if is_estimate else 1.0
                metrics = _run_query_with_metrics(
                    q,
                    "random_windows",
                    (device, target_measurements, query_windows),
                    byte_cost_for(q, fmt, device, target_measurements, fraction),
                )
                if is_estimate:
                    metrics["wall_time_s"] *= scale
                    metrics["cpu_user_s"] *= scale
                    metrics["cpu_sys_s"] *= scale
                    metrics["points_returned"] = int(round(metrics["points_returned"] * scale))
                    metrics["bytes_useful"] = metrics["points_returned"] * 16
                    metrics["read_amplification"] = (
                        metrics["bytes_read"] / metrics["bytes_useful"]
                        if metrics["bytes_useful"] else 0.0
                    )
                    metrics["estimated"] = True
                    metrics["sample_windows"] = len(query_windows)
                    metrics["scale_factor"] = scale
                results.append({
                    "dataset": f"NASA C-MAPSS {subset} train",
                    "pattern": f"training_windows_{workload}",
                    "format": fmt,
                    "device": device,
                    "n_windows": len(windows),
                    "window_length": window_length,
                    "stride": stride,
                    "n_measurements": len(target_measurements),
                    "workload_source": source,
                    **metrics,
                })
    return results


def compile_and_convert_tsfile(subset: str) -> None:
    out_dir = output_dir(subset)
    csv_path = out_dir / f"cmapss_{subset_tag(subset)}_long.csv"
    tsfile_dir = out_dir / "tsfile"
    m2 = Path.home() / ".m2" / "repository"
    classes = ROOT / "tsfile-bench" / "target" / "classes"
    jars = [
        m2 / "org/apache/tsfile/tsfile/2.3.0-260422-SNAPSHOT/tsfile-2.3.0-260422-SNAPSHOT.jar",
        m2 / "org/apache/tsfile/common/2.3.0-260422-SNAPSHOT/common-2.3.0-260422-SNAPSHOT.jar",
        m2 / "org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar",
        m2 / "org/slf4j/slf4j-simple/1.7.25/slf4j-simple-1.7.25.jar",
        m2 / "org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
        m2 / "at/yawk/lz4/lz4-java/1.10.1/lz4-java-1.10.1.jar",
        m2 / "org/tukaani/xz/1.8/xz-1.8.jar",
        m2 / "com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar",
        m2 / "org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar",
    ]
    cp = ";".join([str(classes)] + [str(j) for j in jars])
    classes.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "javac", "-encoding", "UTF-8", "-cp", cp, "-d", str(classes),
            str(ROOT / "tsfile-bench" / "src" / "main" / "java" / "CsvLongToTsFile.java"),
        ],
        cwd=ROOT,
        check=False,
    )
    subprocess.run(
        ["java", "-Xmx2g", "-cp", cp, "CsvLongToTsFile", str(csv_path), str(tsfile_dir)],
        cwd=ROOT,
        check=True,
    )


def summarize_sizes(subset: str, df: pd.DataFrame) -> dict:
    out_dir = output_dir(subset)

    def size_of(path: Path) -> int:
        if path.is_file():
            return path.stat().st_size
        return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())

    return {
        "subset": subset,
        "rows": int(len(df)),
        "devices": int(df["device_id"].nunique()),
        "measurements": int(df["measurement"].nunique()),
        "sizes_bytes": {
            "csv": size_of(out_dir / f"cmapss_{subset_tag(subset)}_long.csv"),
            "parquet": size_of(out_dir / "parquet"),
            "arrow_long": size_of(out_dir / "arrow"),
            "arrow_wide": size_of(out_dir / "arrow_wide"),
            "arrow_series": size_of(out_dir / "arrow_series"),
            "hdf5": size_of(out_dir / "hdf5"),
            "tsfile": size_of(out_dir / "tsfile") if (out_dir / "tsfile").exists() else 0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subsets", nargs="+", default=["FD001", "FD002", "FD003", "FD004"])
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--skip-tsfile", action="store_true")
    parser.add_argument("--include-naive", action="store_true")
    parser.add_argument("--device-limit", type=int, default=5)
    parser.add_argument("--window-count", type=int, default=100)
    parser.add_argument(
        "--workloads", nargs="+", default=["random", "sliding", "epoch_reuse"],
        choices=["random", "sliding", "epoch_reuse"],
    )
    args = parser.parse_args()

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []
    summaries = []
    started = time.perf_counter()

    for subset in args.subsets:
        print(f"[{subset}] loading and writing formats")
        df = load_cmapss_long(subset)
        write_formats(subset, df, rebuild=args.rebuild)
        if not args.skip_tsfile:
            print(f"[{subset}] converting long CSV to TsFile")
            compile_and_convert_tsfile(subset)
        summaries.append(summarize_sizes(subset, df))

        print(f"[{subset}] running real-data benchmark")
        all_results.extend(
            run_real_benchmarks(
                subset,
                workloads=args.workloads,
                include_naive=args.include_naive,
                device_limit=args.device_limit,
                window_count=args.window_count,
            )
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_path = RESULT_DIR / f"real_cmapss_suite_{timestamp}.json"
    summary_path = RESULT_DIR / f"real_cmapss_suite_summary_{timestamp}.json"
    result_path.write_text(json.dumps(all_results, indent=2, default=str), encoding="utf-8")
    summary_path.write_text(json.dumps({
        "summaries": summaries,
        "result_json": str(result_path),
        "elapsed_s": round(time.perf_counter() - started, 3),
        "args": vars(args),
    }, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"result_json": str(result_path), "summary_json": str(summary_path)}, indent=2))


if __name__ == "__main__":
    main()
