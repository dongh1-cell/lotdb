"""Generate synthetic industrial IoT dataset.

Creates multi-device, multi-measurement time-series data with realistic
patterns: temperature, vibration, pressure, humidity, energy, flow rate, etc.

Output: one combined CSV file for import to other formats.

Usage:
  python data_gen.py                # full dataset
  python data_gen.py --validate     # small dataset + self-check
"""

import sys
import numpy as np
from pathlib import Path
from datetime import datetime

import config as cfg

# Pattern types with physical constraints
# (base_min, base_max, amp_min, amp_max, noise_min, noise_max,
#  period_h_min, period_h_max, trend_per_day_max, min_value)
PATTERN_SPECS = {
    "temperature": (20, 35, 3, 10, 0.1, 0.5, 20, 28, 0.05, None),     # deg C
    "vibration":   (0.5, 2.0, 0.3, 1.5, 0.05, 0.2, 0.5, 4, 0.01, 0),  # mm/s, min >= 0
    "pressure":    (100, 200, 10, 30, 0.5, 2.0, 4, 12, 0.2, 0),        # kPa, min >= 0
    "humidity":    (40, 70, 5, 20, 0.3, 1.0, 22, 26, 0.1, 0),          # %RH, min >= 0
    "energy":      (10, 50, 5, 20, 0.2, 1.0, 12, 24, 0.02, 0),         # kW, min >= 0
    "flow_rate":   (5, 15, 2, 6, 0.1, 0.3, 2, 8, 0.01, 0),             # L/min, min >= 0
    "speed":       (100, 300, 20, 80, 1, 5, 1, 6, 0.5, 0),             # RPM, min >= 0
    "level":       (30, 80, 5, 15, 0.1, 0.5, 8, 16, 0.3, 0),           # %, min >= 0
}

PATTERN_ORDER = list(PATTERN_SPECS.keys())  # 8 types


def _meas_name(pattern, idx):
    """Generate measurement name: pattern + round number if >1."""
    n_types = len(PATTERN_ORDER)
    if idx < n_types:
        return pattern
    round_num = idx // n_types + 1
    return f"{pattern}_{round_num}"


def generate_measurement_configs(n_measurements):
    """Create measurement profiles with deterministic variation."""
    rng = np.random.default_rng(cfg.SEED)
    configs = []

    for i in range(n_measurements):
        pattern = PATTERN_ORDER[i % len(PATTERN_ORDER)]
        b_min, b_max, a_min, a_max, n_min, n_max, p_min, p_max, t_max, min_val = \
            PATTERN_SPECS[pattern]

        base = rng.uniform(b_min, b_max)
        amplitude = rng.uniform(a_min, a_max)
        noise = rng.uniform(n_min, n_max)
        period_h = rng.uniform(p_min, p_max)
        trend_per_day = rng.uniform(-t_max, t_max)

        # Round num determines within-pattern variation
        round_num = i // len(PATTERN_ORDER)
        if round_num > 0:
            base += rng.uniform(-5, 5) * round_num
            amplitude *= (0.7 + 0.6 * round_num / 3)

        configs.append({
            "name": _meas_name(pattern, i),
            "pattern": pattern,
            "base": base,
            "amplitude": amplitude,
            "noise": noise,
            "period_h": period_h,
            "trend_per_day": trend_per_day,
            "min_value": min_val,
        })

    return configs


def generate_device_signal(m_config, device_id, timestamps_abs, timestamps_rel):
    """Generate a time series for one device-measurement pair.

    Uses relative time (seconds since dataset start) for all slow-varying
    components to avoid floating-point issues with Unix epoch times.

    Each device gets independent parameter variations (base shift, amplitude
    scale, period stretch, phase offset) to ensure realistic cross-device
    diversity. The seed is constructed to guarantee different devices produce
    different signals for the same measurement type.
    """
    # Per-device + per-measurement deterministic random state.
    # hash() on the measurement name ensures same measurement type
    # across devices gets different seeds via device_id * large_prime.
    meas_hash = hash(m_config["name"]) % 100000
    seed = (cfg.SEED * 1000000
            + device_id * 7919   # prime
            + meas_hash * 31)    # spread within measurement type
    rng = np.random.default_rng(seed)

    n_points = len(timestamps_abs)
    t_rel = timestamps_rel.astype(np.float64)  # seconds since dataset start

    # --- Per-device parameter randomization ---
    # Each device gets its own base offset, amplitude scale, period stretch,
    # and phase shift, drawn from the device's RNG stream.
    base = m_config["base"] + rng.uniform(-5, 5)
    amp_scale = 0.6 + rng.uniform(0.0, 0.8)  # 0.6x to 1.4x of nominal amplitude
    amp = m_config["amplitude"] * amp_scale
    noise = m_config["noise"] * (0.7 + rng.uniform(0.0, 0.6))

    # Period varies by up to ±25% per device
    period_s = m_config["period_h"] * 3600.0 * (0.75 + rng.uniform(0.0, 0.5))

    # Random phase offset so devices are not synchronized
    phase_offset = rng.uniform(0.0, 2.0 * np.pi)

    trend_per_second = m_config["trend_per_day"] / 86400.0

    # --- Signal components ---

    # Primary oscillation with per-device phase offset
    primary = amp * np.sin(2.0 * np.pi * t_rel / period_s + phase_offset)

    # Sub-harmonic
    sub = amp * 0.3 * np.sin(2.0 * np.pi * t_rel / (period_s * 0.5) + phase_offset * 0.7)

    # Long-term trend: linear drift + slow weekly oscillation
    trend = trend_per_second * t_rel + amp * 0.15 * np.sin(
        2.0 * np.pi * t_rel / (7.0 * 86400.0) + phase_offset * 0.3
    )

    # Anomaly spikes: rare, high-amplitude (0.05% of points)
    anomaly_mask = rng.random(n_points) < 0.0005
    anomaly_vals = rng.normal(0.0, amp * 3.0, n_points) * anomaly_mask

    # Gaussian sensor noise
    noise_component = rng.normal(0.0, noise, n_points)

    # Combine
    signal = base + primary + sub + trend + anomaly_vals + noise_component

    # Apply physical constraints (e.g., flow_rate >= 0)
    if m_config.get("min_value") is not None:
        signal = np.maximum(signal, m_config["min_value"])

    return np.round(signal, 4)


def _validate_signal(values, measurement_name, expected_points):
    """Quick sanity check on generated signal."""
    issues = []
    if len(values) != expected_points:
        issues.append(f"length {len(values)} != {expected_points}")
    if values is None or len(values) == 0:
        issues.append("empty signal")
    if np.any(np.isnan(values)):
        issues.append(f"contains NaN ({np.isnan(values).sum()} values)")
    if np.any(np.isinf(values)):
        issues.append("contains Inf")

    if not issues:
        return None
    return f"{measurement_name}: " + ", ".join(issues)


def generate_dataset(validate_only=False):
    """Generate the full dataset and save as CSV.

    Args:
        validate_only: If True, generate a small subset for validation only.
    """
    if validate_only:
        # Override config for quick validation
        orig = (cfg.NUM_DEVICES, cfg.DURATION_DAYS, cfg.MEASUREMENTS_PER_DEVICE)
        cfg.NUM_DEVICES = 3
        cfg.DURATION_DAYS = 1
        cfg.MEASUREMENTS_PER_DEVICE = 8  # one of each type

    m_configs = generate_measurement_configs(cfg.MEASUREMENTS_PER_DEVICE)

    total_seconds = cfg.DURATION_DAYS * 86400
    n_points = total_seconds // cfg.INTERVAL_SECONDS

    start_time = int(datetime(2024, 1, 1, 0, 0, 0).timestamp())
    timestamps_abs = np.arange(
        start_time, start_time + n_points * cfg.INTERVAL_SECONDS,
        cfg.INTERVAL_SECONDS, dtype=np.int64
    )
    # Relative time (seconds since dataset start) for trend/oscillation computation
    timestamps_rel = timestamps_abs - timestamps_abs[0].astype(np.float64)

    total_stored = cfg.NUM_DEVICES * cfg.MEASUREMENTS_PER_DEVICE * n_points
    est_size_mb = total_stored * 50 / (1024 * 1024)  # ~50 bytes per CSV row

    print(f"Dataset config:")
    print(f"  Devices: {cfg.NUM_DEVICES}")
    print(f"  Measurements per device: {cfg.MEASUREMENTS_PER_DEVICE}")
    print(f"  Duration: {cfg.DURATION_DAYS} days")
    print(f"  Interval: {cfg.INTERVAL_SECONDS}s")
    print(f"  Points per series: {n_points:,}")
    print(f"  Total rows: {total_stored:,}")
    print(f"  Estimated CSV size: {est_size_mb:.0f} MB")
    print(f"  Measurements: {[m['name'] for m in m_configs]}")
    print()

    combined_path = Path(cfg.DATA_DIR) / "iot_dataset.csv"

    # Pre-generate all signals to avoid per-row computation
    # Store as list of (device_name, meas_name, timestamps, signal_values)
    print("Generating signals...")
    all_series = []
    validation_errors = []

    for dev_id in range(cfg.NUM_DEVICES):
        device_name = f"d_{dev_id:03d}"
        for m_cfg in m_configs:
            signal = generate_device_signal(m_cfg, dev_id, timestamps_abs, timestamps_rel)
            err = _validate_signal(signal, f"{device_name}/{m_cfg['name']}", n_points)
            if err:
                validation_errors.append(err)
            all_series.append({
                "device": device_name,
                "measurement": m_cfg["name"],
                "timestamps": timestamps_abs,
                "values": signal,
            })
        if (dev_id + 1) % 10 == 0:
            print(f"  Generated device {dev_id + 1}/{cfg.NUM_DEVICES}")

    if validation_errors:
        print(f"\nVALIDATION ERRORS ({len(validation_errors)}):")
        for e in validation_errors[:10]:
            print(f"  {e}")
        if not validate_only:
            print("  Aborting. Fix issues before full generation.")
            return None
    else:
        print("  All signals passed validation.")

    # Write CSV in batches
    print(f"\nWriting CSV to {combined_path}...")
    rows_written = 0
    batch_lines = []
    batch_target = 500_000  # lines per batch write

    with open(combined_path, "w") as f:
        f.write("device_id,measurement,time,value\n")

        for series in all_series:
            dev = series["device"]
            meas = series["measurement"]
            ts = series["timestamps"]
            vals = series["values"]

            # Build lines in batches
            for i in range(0, n_points, cfg.CHUNK_SIZE):
                end = min(i + cfg.CHUNK_SIZE, n_points)
                for j in range(i, end):
                    batch_lines.append(f"{dev},{meas},{ts[j]},{vals[j]}")
                    if len(batch_lines) >= batch_target:
                        f.write("\n".join(batch_lines) + "\n")
                        rows_written += len(batch_lines)
                        batch_lines = []
                        if rows_written % 10_000_000 == 0:
                            pct = 100.0 * rows_written / total_stored
                            print(f"  {rows_written:,} / {total_stored:,} rows ({pct:.1f}%)")

        # Flush remaining
        if batch_lines:
            f.write("\n".join(batch_lines) + "\n")
            rows_written += len(batch_lines)

    print(f"Done. {rows_written:,} rows written.")
    file_size_mb = combined_path.stat().st_size / (1024 * 1024)
    print(f"File size: {file_size_mb:.1f} MB")

    # Final data quality report
    if not validate_only:
        _print_quality_report(all_series, cfg.NUM_DEVICES)

    # Restore config if validating
    if validate_only:
        cfg.NUM_DEVICES, cfg.DURATION_DAYS, cfg.MEASUREMENTS_PER_DEVICE = orig
        # Delete the small validation file
        combined_path.unlink(missing_ok=True)

    return combined_path


def _print_quality_report(all_series, num_devices):
    """Print data quality metrics."""
    print("\n--- Data Quality Report ---")
    # Sample a few series for detailed checks
    import collections
    by_meas = collections.defaultdict(list)
    for s in all_series:
        by_meas[s["measurement"]].append(s)

    for meas, series_list in sorted(by_meas.items())[:5]:
        s = series_list[0]
        vals = s["values"]
        print(f"\n  {meas}:")
        print(f"    Range: [{vals.min():.2f}, {vals.max():.2f}]")
        print(f"    Mean={vals.mean():.2f}, Std={vals.std():.2f}")
        # Check time intervals
        ts = s["timestamps"]
        diffs = np.diff(ts)
        bad = (diffs != cfg.INTERVAL_SECONDS).sum()
        print(f"    Time monotonic: {np.all(ts[1:] > ts[:-1])}")
        print(f"    Interval mismatches: {bad}/{len(diffs)}")

    # Cross-device variation check
    if "temperature" in by_meas and len(by_meas["temperature"]) >= 2:
        t0 = by_meas["temperature"][0]["values"]
        t1 = by_meas["temperature"][1]["values"]
        corr = np.corrcoef(t0, t1)[0, 1]
        print(f"\n  Cross-device: d_000 vs d_001 temperature correlation = {corr:.4f}")
        if abs(corr) > 0.95:
            print(f"    WARNING: devices nearly identical (should be <0.95)")
        else:
            print(f"    OK: devices are sufficiently different")


if __name__ == "__main__":
    if "--validate" in sys.argv:
        print("=== VALIDATION MODE (small dataset) ===\n")
        generate_dataset(validate_only=True)
        print("\nValidation complete. If all OK, run 'python data_gen.py' for full dataset.")
    else:
        generate_dataset()
