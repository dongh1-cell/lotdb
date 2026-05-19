"""Benchmark runner for file format comparison.

Tests 4 query patterns against Parquet, Arrow IPC, HDF5, TsFile.
Collects: wall time, CPU time (user/sys), time breakdown (IO/decompress/filter),
          read amplification (bytes_read/bytes_useful), peak memory (RSS).
"""

import os
import gc
import json
import sys
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.feather as feather
import h5py
import psutil
from pathlib import Path
from datetime import datetime

import os

# ── Single-thread enforcement for fair CPU comparison ──
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
try:
    import pyarrow as pa
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
except Exception:
    pass

import config as cfg
from metrics import QueryMeasurer

PARQUET_P4_SAMPLE_WINDOWS = 50

# TsFile support via native Java subprocess (no JPype overhead)
try:
    from tsfile_native import run_benchmark as tsfile_run
    HAS_TSFILE = True
except (ImportError, ModuleNotFoundError):
    HAS_TSFILE = False


# ─── Measurement discovery ─────────────────────────────────────────────

def discover_measurements(parquet_dir):
    first_file = sorted(parquet_dir.glob("*.parquet"))[0]
    pf = pq.ParquetFile(first_file)
    names = set()
    for i in range(pf.metadata.num_row_groups):
        df = pf.read_row_group(i).to_pandas()
        names.update(df["measurement"].unique())
    return sorted(names)


def discover_devices(parquet_dir):
    return sorted([f.stem for f in parquet_dir.glob("*.parquet")])


# ─── Byte-cost precomputation for read amplification ───────────────────

def _precompute_arrow_costs(querier):
    """Arrow reads the entire file for every query."""
    costs = {}
    for dev, path in querier.files.items():
        fsize = Path(path).stat().st_size
        costs[(dev, "any")] = fsize
    return costs


def _precompute_hdf5_costs(querier):
    """HDF5: each dataset has known compressed chunk sizes."""
    costs = {}
    with h5py.File(querier.data_path, "r") as f:
        for dev in f.keys():
            for meas in f[dev].keys():
                # Sum of chunk sizes for time + value datasets
                total = 0
                for key in ["time", "value"]:
                    ds = f[f"{dev}/{meas}/{key}"]
                    if ds.chunks:
                        n_chunks = (ds.shape[0] + ds.chunks[0] - 1) // ds.chunks[0]
                        chunk_size = ds.chunks[0] * ds.dtype.itemsize
                        total += n_chunks * chunk_size
                    else:
                        total += ds.shape[0] * ds.dtype.itemsize
                costs[(dev, meas)] = total
    return costs


def _precompute_tsfile_costs(querier):
    """TsFile: read chunk metadata to get compressed sizes."""
    costs = {}
    for dev, path in querier.files.items():
        _, ts_reader = querier._get_reader(dev)
        # Approximate: file size / number of measurements
        fsize = Path(path).stat().st_size
        n_meas = len(querier.measurements)
        for meas in querier.measurements:
            costs[(dev, meas)] = fsize // (n_meas + 1)  # +1 for time column
    return costs


# ─── Query pattern implementations with measurement ────────────────────

class IOMeasure:
    """Legacy I/O measurement wrapper."""

    def __init__(self):
        self.proc = psutil.Process()
        try:
            self.before = self.proc.io_counters()
            self.can_measure = True
        except (AttributeError, psutil.AccessDenied):
            self.before = None
            self.can_measure = False
        self.t0 = os.times()  # keep for reference

    def read(self):
        t1 = os.times()
        result = {
            "wall_time_s": 0.0,  # filled later
            "cpu_user_s": t1.user - self.t0.user,
            "cpu_sys_s": t1.system - self.t0.system,
        }
        if self.can_measure:
            try:
                after = self.proc.io_counters()
                result["io_bytes"] = after.read_bytes - self.before.read_bytes
                result["io_ops"] = after.read_count - self.before.read_count
            except (AttributeError, psutil.AccessDenied):
                pass
        return result


def _run_query_with_metrics(querier, method_name, args, byte_cost):
    """Run a querier method with full instrumentation.

    Returns dict with all metrics.
    """
    m = QueryMeasurer()
    m.start()

    # Call the querier method
    method = getattr(querier, method_name)
    n_points = method(*args)

    # Set byte accounting
    m.set_bytes_read(byte_cost)
    m.set_bytes_useful(n_points * 16)  # time(int64) + value(float64) = 16 bytes

    metrics = m.finish()

    return {
        "points_returned": n_points,
        "wall_time_s": metrics.wall_time_s,
        "cpu_user_s": metrics.cpu_user_s,
        "cpu_sys_s": metrics.cpu_sys_s,
        "bytes_read": metrics.bytes_read,
        "bytes_useful": metrics.bytes_useful,
        "read_amplification": metrics.read_amplification,
        "throughput_mbps": metrics.throughput_mbps,
        "mem_rss_before_kb": metrics.mem_rss_before_kb,
        "mem_rss_peak_kb": metrics.mem_rss_peak_kb,
        "mem_rss_after_kb": metrics.mem_rss_after_kb,
        "mem_delta_kb": metrics.mem_delta_kb,
    }


# ─── Parquet Querier ───────────────────────────────────────────────────

class ParquetQuerier:
    def __init__(self, data_dir):
        self.files = {f.stem: str(f) for f in data_dir.glob("*.parquet")}
        # Per-file cost: each file = one device, all measurements bundled
        self._file_cost = {dev: Path(p).stat().st_size for dev, p in self.files.items()}

    def _cost(self, device, measurement):
        # Compressed bytes ≈ file_size / n_measurements (15 in our dataset)
        # Measurement count discovered at runtime from benchmark patterns
        fsize = self._file_cost.get(device, 0)
        return fsize // 15 if fsize > 0 else 0

    def sequential_scan(self, device, measurement, time_start, time_end):
        table = pq.read_table(
            self.files[device],
            filters=[
                ("measurement", "==", measurement),
                ("time", ">=", time_start),
                ("time", "<=", time_end),
            ],
        )
        return table.num_rows

    def column_subset(self, device, target_measurements, time_start, time_end):
        total = 0
        for meas in target_measurements:
            total += self.sequential_scan(device, meas, time_start, time_end)
        return total

    def downsampling(self, device, measurement, time_start, time_end, step_n):
        total = self.sequential_scan(device, measurement, time_start, time_end)
        return total // step_n

    def random_windows(self, device, target_measurements, windows):
        total = 0
        for meas in target_measurements:
            for (t_start, t_end) in windows:
                total += self.sequential_scan(device, meas, t_start, t_end)
        return total


class ArrowQuerier:
    def __init__(self, data_dir):
        self.files = {f.stem: str(f) for f in data_dir.glob("*.arrow")}
        self._file_sizes = {dev: Path(p).stat().st_size for dev, p in self.files.items()}
        self._costs = _precompute_arrow_costs(self)

    def _cost(self, device, measurement):
        return self._costs.get((device, "any"), self._file_sizes.get(device, 0))

    def _filter_df(self, df, measurement, time_start, time_end):
        mask = df["measurement"] == measurement
        if time_start is not None:
            mask &= (df["time"] >= time_start)
        if time_end is not None:
            mask &= (df["time"] <= time_end)
        return df[mask]

    def sequential_scan(self, device, measurement, time_start, time_end):
        df = feather.read_feather(self.files[device])
        return len(self._filter_df(df, measurement, time_start, time_end))

    def column_subset(self, device, target_measurements, time_start, time_end):
        df = feather.read_feather(self.files[device])
        mask = df["measurement"].isin(target_measurements)
        if time_start is not None:
            mask &= (df["time"] >= time_start)
        if time_end is not None:
            mask &= (df["time"] <= time_end)
        return len(df[mask])

    def downsampling(self, device, measurement, time_start, time_end, step_n):
        df = feather.read_feather(self.files[device])
        subset = self._filter_df(df, measurement, time_start, time_end)
        return len(subset.iloc[::step_n])

    def random_windows(self, device, target_measurements, windows):
        df = feather.read_feather(self.files[device])
        total = 0
        for meas in target_measurements:
            meas_mask = df["measurement"] == meas
            meas_df = df.loc[meas_mask]
            for (t_start, t_end) in windows:
                mask = (meas_df["time"] >= t_start) & (meas_df["time"] <= t_end)
                total += int(mask.sum())
        return total


class HDF5Querier:
    def __init__(self, data_path):
        self.data_path = data_path
        self._costs = _precompute_hdf5_costs(self)

    def _cost(self, device, measurement):
        return self._costs.get((device, measurement), 0)

    def sequential_scan(self, device, measurement, time_start, time_end):
        with h5py.File(self.data_path, "r") as f:
            times = f[f"{device}/{measurement}/time"][:]
            values = f[f"{device}/{measurement}/value"][:]
        mask = np.ones(len(times), dtype=bool)
        if time_start is not None:
            mask &= (times >= time_start)
        if time_end is not None:
            mask &= (times <= time_end)
        return len(values[mask])

    def column_subset(self, device, target_measurements, time_start, time_end):
        total = 0
        with h5py.File(self.data_path, "r") as f:
            for meas in target_measurements:
                times = f[f"{device}/{meas}/time"][:]
                values = f[f"{device}/{meas}/value"][:]
                mask = np.ones(len(times), dtype=bool)
                if time_start is not None:
                    mask &= (times >= time_start)
                if time_end is not None:
                    mask &= (times <= time_end)
                total += len(values[mask])
        return total

    def downsampling(self, device, measurement, time_start, time_end, step_n):
        with h5py.File(self.data_path, "r") as f:
            times = f[f"{device}/{measurement}/time"][:]
            values = f[f"{device}/{measurement}/value"][:]
        mask = np.ones(len(times), dtype=bool)
        if time_start is not None:
            mask &= (times >= time_start)
        if time_end is not None:
            mask &= (times <= time_end)
        idx = np.where(mask)[0]
        return len(values[idx[::step_n]])

    def random_windows(self, device, target_measurements, windows):
        total = 0
        with h5py.File(self.data_path, "r") as f:
            for meas in target_measurements:
                times = f[f"{device}/{meas}/time"][:]
                values = f[f"{device}/{meas}/value"][:]
                for (t_start, t_end) in windows:
                    mask = (times >= t_start) & (times <= t_end)
                    total += len(values[mask])
        return total


# ─── Pattern functions ─────────────────────────────────────────────────

def get_timestamps_for_range(duration_days):
    total_seconds = cfg.DURATION_DAYS * 86400
    base_start = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
    if duration_days >= cfg.DURATION_DAYS:
        return base_start, base_start + total_seconds
    rng = np.random.default_rng(cfg.SEED + 999)
    max_offset = total_seconds - int(duration_days * 86400)
    offset = rng.integers(0, max_offset) if max_offset > 0 else 0
    return base_start + int(offset), base_start + int(offset) + int(duration_days * 86400)


def run_pattern_1_sequential(queriers, devices, measurements, n_runs=5):
    results = []
    rng = np.random.default_rng(cfg.SEED + 100)
    t_start, t_end = get_timestamps_for_range(cfg.DURATION_DAYS)

    for run in range(n_runs):
        device = devices[rng.integers(0, len(devices))]
        measurement = measurements[rng.integers(0, len(measurements))]

        for format_name, q in queriers.items():
            byte_cost = q._cost(device, measurement) if hasattr(q, '_cost') else 0
            args = (device, measurement, t_start, t_end)
            metrics = _run_query_with_metrics(q, "sequential_scan", args, byte_cost)

            results.append({
                "pattern": "sequential_scan",
                "run": run,
                "format": format_name,
                "device": device,
                "measurement": measurement,
                "time_range_days": cfg.DURATION_DAYS,
                **metrics,
            })
            gc.collect()

        if (run + 1) % 3 == 0:
            print(f"  [sequential_scan] {run + 1}/{n_runs} done")

    return results


def run_pattern_2_column_subset(queriers, devices, measurements, n_runs=5):
    results = []
    rng = np.random.default_rng(cfg.SEED + 200)
    t_start, t_end = get_timestamps_for_range(cfg.DURATION_DAYS)

    for run in range(n_runs):
        device = devices[rng.integers(0, len(devices))]

        for selectivity in cfg.COLUMN_SELECTIVITIES:
            n_cols = max(1, int(len(measurements) * selectivity))
            selected = rng.choice(measurements, n_cols, replace=False).tolist()

            for format_name, q in queriers.items():
                # Byte cost: Arrow reads file once; others per-column
                byte_cost = 0
                if hasattr(q, '_cost'):
                    if format_name == 'arrow':
                        byte_cost = q._cost(device, selected[0]) if selected else 0
                    else:
                        for m in selected:
                            byte_cost += q._cost(device, m)

                args = (device, selected, t_start, t_end)
                metrics = _run_query_with_metrics(q, "column_subset", args, byte_cost)

                results.append({
                    "pattern": "column_subset",
                    "run": run,
                    "format": format_name,
                    "device": device,
                    "selectivity": selectivity,
                    "n_cols_requested": n_cols,
                    "n_cols_total": len(measurements),
                    "time_range_days": cfg.DURATION_DAYS,
                    **metrics,
                })
                gc.collect()

        if (run + 1) % 3 == 0:
            print(f"  [column_subset] {run + 1}/{n_runs} done")

    return results


def run_pattern_3_downsampling(queriers, devices, measurements, n_runs=5):
    results = []
    rng = np.random.default_rng(cfg.SEED + 300)
    t_start, t_end = get_timestamps_for_range(cfg.DURATION_DAYS)

    for run in range(n_runs):
        device = devices[rng.integers(0, len(devices))]
        measurement = measurements[rng.integers(0, len(measurements))]

        for step in cfg.SAMPLING_RATES:
            for format_name, q in queriers.items():
                byte_cost = q._cost(device, measurement) if hasattr(q, '_cost') else 0
                args = (device, measurement, t_start, t_end, step)
                metrics = _run_query_with_metrics(q, "downsampling", args, byte_cost)

                results.append({
                    "pattern": "downsampling",
                    "run": run,
                    "format": format_name,
                    "device": device,
                    "measurement": measurement,
                    "sample_step": step,
                    **metrics,
                })
                gc.collect()

        if (run + 1) % 3 == 0:
            print(f"  [downsampling] {run + 1}/{n_runs} done")

    return results


def run_pattern_4_random_windows(queriers, devices, measurements):
    results = []
    rng = np.random.default_rng(cfg.SEED + 400)

    total_seconds = cfg.DURATION_DAYS * 86400
    base_start = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
    base_end = base_start + total_seconds
    window_span_s = cfg.RANDOM_WINDOW_LENGTH * cfg.INTERVAL_SECONDS

    windows = [
        (t_s := rng.integers(base_start, base_end - window_span_s),
         t_s + window_span_s)
        for _ in range(cfg.RANDOM_WINDOW_COUNT)
    ]

    n_target_meas = max(1, int(len(measurements) * 0.2))
    target_measurements = rng.choice(measurements, n_target_meas, replace=False).tolist()

    print(f"  [random_windows] {cfg.RANDOM_WINDOW_COUNT} windows, "
          f"{len(target_measurements)}/{len(measurements)} measurements")

    test_devices = rng.choice(devices, min(5, len(devices)), replace=False)

    for dev_idx, device in enumerate(test_devices):
        for format_name, q in queriers.items():
            is_parquet_estimate = format_name == "parquet"
            query_windows = windows[:PARQUET_P4_SAMPLE_WINDOWS] if is_parquet_estimate else windows
            scale = (len(windows) / len(query_windows)) if is_parquet_estimate else 1.0

            # Byte cost: each window touches a subset of the measurement's data
            # Estimate: windows cover (window_span_s * n_windows / total_span) fraction
            fraction = min(1.0, (window_span_s * cfg.RANDOM_WINDOW_COUNT)
                           / total_seconds)
            byte_cost = 0
            if hasattr(q, '_cost'):
                if format_name == 'arrow':
                    byte_cost = q._cost(device, target_measurements[0]) if target_measurements else 0
                elif format_name == 'hdf5':
                    for m in target_measurements:
                        byte_cost += q._cost(device, m)
                else:
                    for m in target_measurements:
                        byte_cost += int(q._cost(device, m) * fraction)

            args = (device, target_measurements, query_windows)
            metrics = _run_query_with_metrics(q, "random_windows", args, byte_cost)
            if is_parquet_estimate:
                metrics["wall_time_s"] *= scale
                metrics["cpu_user_s"] *= scale
                metrics["cpu_sys_s"] *= scale
                metrics["points_returned"] = int(round(metrics["points_returned"] * scale))
                metrics["bytes_useful"] = metrics["points_returned"] * 16
                metrics["read_amplification"] = (
                    metrics["bytes_read"] / metrics["bytes_useful"]
                    if metrics["bytes_useful"] > 0 else 0.0
                )
                metrics["throughput_mbps"] = (
                    metrics["bytes_useful"] / 1e6 / metrics["wall_time_s"]
                    if metrics["wall_time_s"] > 0 else 0.0
                )

            row = {
                "pattern": "random_windows",
                "format": format_name,
                "device": str(device),
                "n_windows": cfg.RANDOM_WINDOW_COUNT,
                "n_measurements": len(target_measurements),
                "window_length": cfg.RANDOM_WINDOW_LENGTH,
                **metrics,
            }
            if is_parquet_estimate:
                row["estimated"] = True
                row["sample_windows"] = len(query_windows)
                row["scale_factor"] = scale
                row["estimate_method"] = "linear extrapolation from sampled random windows"
            results.append(row)
            gc.collect()

        print(f"  [random_windows] device {dev_idx + 1}/{len(test_devices)}: {device}")

    return results


# ─── Main ──────────────────────────────────────────────────────────────

def run_all_benchmarks(lazy=False):
    """Run all benchmarks.

    Args:
        lazy: If True, enable TsFile lazy page loading mode
              (-Dtsfile.lazy.page.load=true).
    """
    print("=" * 60)
    print("IoTDB File Format Benchmark: AI Training Workloads")
    if lazy:
        print("[LAZY MODE] TsFile: -Dtsfile.lazy.page.load=true")
    print("=" * 60)

    parquet_dir = Path(cfg.DATA_DIR) / "parquet"
    arrow_dir = Path(cfg.DATA_DIR) / "arrow"
    hdf5_path = Path(cfg.DATA_DIR) / "hdf5" / "iot_dataset.h5"
    tsfile_dir = Path(cfg.DATA_DIR) / "tsfile"

    if not parquet_dir.exists() or not list(parquet_dir.glob("*.parquet")):
        print("No Parquet files found. Run converters.py first.")
        return

    # Run native TsFile benchmark first (separate Java process, no JPype overhead)
    tsfile_results = []
    tsfile_list = []
    if HAS_TSFILE and tsfile_dir.exists() and list(tsfile_dir.glob("*.tsfile")):
        tsfile_list = sorted(tsfile_dir.glob("*.tsfile"))
        print("[OK] TsFile native runner starting...")
        tsfile_results = tsfile_run(str(tsfile_dir), lazy=lazy)
        print(f"[OK] TsFile: {len(tsfile_results)} native results collected")

    # Python queriers for Parquet, Arrow, HDF5
    queriers = {}
    queriers["parquet"] = ParquetQuerier(parquet_dir)
    print("[OK] Parquet querier ready")

    if arrow_dir.exists() and list(arrow_dir.glob("*.arrow")):
        queriers["arrow"] = ArrowQuerier(arrow_dir)
        print("[OK] Arrow IPC querier ready")

    if hdf5_path.exists():
        queriers["hdf5"] = HDF5Querier(hdf5_path)
        print("[OK] HDF5 querier ready")

    all_pq_devices = discover_devices(parquet_dir)
    measurements = discover_measurements(parquet_dir)

    if tsfile_list:
        tsfile_devices = sorted([f.stem for f in tsfile_list])
        devices = [d for d in sorted(all_pq_devices) if d in tsfile_devices]
    else:
        devices = sorted(all_pq_devices)

    print(f"\nSchema: {len(devices)} devices, {len(measurements)} measurements")
    fmt_list = list(queriers.keys()) + (["tsfile"] if tsfile_results else [])
    print(f"Formats: {fmt_list}")
    print(f"Metrics: wall_time, cpu_user, cpu_sys, bytes_read, bytes_useful, "
          f"read_amplification, mem_rss_peak\n")

    all_results = []

    print("--- Pattern 1: Sequential Scan ---")
    r1 = run_pattern_1_sequential(queriers, devices, measurements, n_runs=5)
    all_results.extend(r1)
    _print_summary(r1, "Sequential Scan")

    print("\n--- Pattern 2: Column Subset ---")
    r2 = run_pattern_2_column_subset(queriers, devices, measurements, n_runs=5)
    all_results.extend(r2)
    _print_summary(r2, "Column Subset")

    print("\n--- Pattern 3: Downsampling (Key Test) ---")
    r3 = run_pattern_3_downsampling(queriers, devices, measurements, n_runs=5)
    all_results.extend(r3)
    _print_summary(r3, "Downsampling")

    print("\n--- Pattern 4: AI Training Simulation ---")
    r4 = run_pattern_4_random_windows(queriers, devices, measurements)
    all_results.extend(r4)
    _print_summary(r4, "AI Training Simulation")

    # Add native TsFile results (per-pattern summary)
    if tsfile_results:
        all_results.extend(tsfile_results)
        for pat in ["sequential_scan", "column_subset", "downsampling", "random_windows"]:
            pat_results = [r for r in tsfile_results if r["pattern"] == pat]
            if pat_results:
                _print_summary(pat_results, f"TsFile {pat}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_path = Path(cfg.RESULT_DIR) / f"benchmark_results_{timestamp}.json"
    with open(result_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n[OK] Results saved to {result_path}")

    return all_results


def _print_summary(results, name):
    by_format = {}
    for r in results:
        fmt = r["format"]
        if fmt not in by_format:
            by_format[fmt] = []
        by_format[fmt].append(r)

    print(f"  --- {name} Summary ---")
    for fmt, items in by_format.items():
        avg_t = sum(r["wall_time_s"] for r in items) / len(items)
        avg_pts = sum(r["points_returned"] for r in items) / len(items)
        avg_cpu = sum(r["cpu_user_s"] for r in items) / len(items)
        avg_read = sum(r.get("bytes_read", 0) for r in items) / len(items)
        avg_amp = sum(r.get("read_amplification", 0) for r in items) / len(items)
        avg_mem = sum(r.get("mem_delta_kb", 0) for r in items) / len(items)
        print(f"  {fmt:>10s}: {avg_t:8.3f}s wall, {avg_cpu:6.3f}s cpu, "
              f"{avg_read/(1024**2):7.1f}MB read, {avg_amp:8.1f}x amp, "
              f"{avg_mem:7.0f}KB mem")


if __name__ == "__main__":
    lazy_mode = "--lazy" in sys.argv
    run_all_benchmarks(lazy=lazy_mode)
