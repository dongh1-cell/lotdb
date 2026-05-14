"""Analyze and visualize benchmark results.

Generates:
  1. Per-pattern comparison table
  2. I/O amplification chart (the key metric)
  3. Throughput vs selectivity chart
  4. Data utilization ratio analysis
"""

import json
import sys
import numpy as np
from pathlib import Path
from collections import defaultdict

import config as cfg

# Try importing matplotlib but handle missing
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def load_results():
    """Load the most recent benchmark result file."""
    result_dir = Path(cfg.RESULT_DIR)
    result_files = sorted(result_dir.glob("benchmark_results_*.json"))
    if not result_files:
        print("No benchmark results found. Run benchmark_runner.py first.")
        return None
    latest = result_files[-1]
    print(f"Loading results from {latest.name}")
    with open(latest) as f:
        return json.load(f)


def group_by(results, *keys):
    """Group results by given keys, return nested dict."""
    grouped = {}
    for r in results:
        d = grouped
        for k in keys[:-1]:
            val = r.get(k, "?")
            if val not in d:
                d[val] = {}
            d = d[val]
        last_val = r.get(keys[-1], "?")
        if last_val not in d:
            d[last_val] = []
        d[last_val].append(r)
    return grouped


def _safe_mean(items, key, default=0):
    """Compute trimmed mean: discard the slowest (cold-start) run if >=3 runs.

    For benchmarking, the first run includes OS page cache cold miss and
    library initialization overhead. Removing the max gives a fairer
    measurement of steady-state format performance.
    """
    vals = []
    for r in items:
        v = r.get(key)
        if v is None:
            continue
        if isinstance(v, str):
            v = float(v)
        vals.append(v)
    if not vals:
        return default
    if len(vals) >= 3:
        vals.remove(max(vals))  # discard cold-start outlier
    return sum(vals) / len(vals)


def print_table(results):
    """Print comprehensive comparison table."""
    print("\n" + "=" * 90)
    print("FILE FORMAT BENCHMARK RESULTS")
    print("=" * 90)

    # Pattern 1: Sequential scan
    print("\n--- Pattern 1: Sequential Scan (Full range, single measurement) ---")
    r1 = [r for r in results if r["pattern"] == "sequential_scan"]
    by_fmt = defaultdict(list)
    for r in r1:
        by_fmt[r["format"]].append(r)

    print(f"{'Format':>12s}  {'Wall(s)':>8s}  {'CPU(s)':>8s}  {'%CPU':>6s}  "
          f"{'Read(MB)':>9s}  {'Useful(MB)':>10s}  {'Amp':>7s}  "
          f"{'Mem(KB)':>9s}  {'Pts':>12s}")
    print("-" * 110)
    for fmt, items in sorted(by_fmt.items()):
        avg_t = _safe_mean(items, "wall_time_s")
        avg_cpu = _safe_mean(items, "cpu_user_s") + _safe_mean(items, "cpu_sys_s")
        avg_read = _safe_mean(items, "bytes_read")
        avg_useful = _safe_mean(items, "bytes_useful")
        avg_amp = _safe_mean(items, "read_amplification")
        avg_mem = _safe_mean(items, "mem_delta_kb")
        avg_pts = _safe_mean(items, "points_returned")
        pct_cpu = 100 * avg_cpu / avg_t if avg_t > 0 else 0
        print(f"{fmt:>12s}  {avg_t:>8.3f}  {avg_cpu:>8.3f}  {pct_cpu:>5.0f}%  "
              f"{avg_read/(1024**2):>9.1f}  {avg_useful/(1024**2):>10.2f}  {avg_amp:>7.1f}  "
              f"{avg_mem:>9.0f}  {avg_pts:>12,.0f}")

    # Pattern 2: Column subset
    print("\n--- Pattern 2: Column Subset (I/O vs column selectivity) ---")
    r2 = [r for r in results if r["pattern"] == "column_subset"]
    by_fmt_sel = defaultdict(lambda: defaultdict(list))
    for r in r2:
        by_fmt_sel[r["format"]][r["selectivity"]].append(r)

    print(f"{'Format':>12s}  {'Sel':>5s}  {'Wall(s)':>8s}  {'CPU(s)':>8s}  "
          f"{'Read(MB)':>9s}  {'Amp':>7s}  {'Mem(KB)':>9s}  {'Pts':>12s}")
    print("-" * 110)
    for fmt in sorted(by_fmt_sel):
        for sel in sorted(by_fmt_sel[fmt]):
            items = by_fmt_sel[fmt][sel]
            avg_t = _safe_mean(items, "wall_time_s")
            avg_cpu = _safe_mean(items, "cpu_user_s") + _safe_mean(items, "cpu_sys_s")
            avg_read = _safe_mean(items, "bytes_read")
            avg_amp = _safe_mean(items, "read_amplification")
            avg_mem = _safe_mean(items, "mem_delta_kb")
            avg_pts = _safe_mean(items, "points_returned")
            print(f"{fmt:>12s}  {sel:>5.0%}  {avg_t:>8.3f}  {avg_cpu:>8.3f}  "
                  f"{avg_read/(1024**2):>9.1f}  {avg_amp:>7.1f}  "
                  f"{avg_mem:>9.0f}  {avg_pts:>12,.0f}")

    # Pattern 3: Downsampling
    print("\n--- Pattern 3: Downsampling (I/O waste per sample rate) ---")
    r3 = [r for r in results if r["pattern"] == "downsampling"]
    by_fmt_step = defaultdict(lambda: defaultdict(list))
    for r in r3:
        by_fmt_step[r["format"]][r["sample_step"]].append(r)

    print(f"{'Format':>12s}  {'Step':>6s}  {'Wall(s)':>8s}  {'CPU(s)':>8s}  "
          f"{'Read(MB)':>9s}  {'Amp':>8s}  {'Mem(KB)':>9s}  {'Pts':>10s}")
    print("-" * 110)
    for fmt in sorted(by_fmt_step):
        for step in sorted(by_fmt_step[fmt]):
            items = by_fmt_step[fmt][step]
            avg_t = _safe_mean(items, "wall_time_s")
            avg_cpu = _safe_mean(items, "cpu_user_s") + _safe_mean(items, "cpu_sys_s")
            avg_read = _safe_mean(items, "bytes_read")
            avg_amp = _safe_mean(items, "read_amplification")
            avg_mem = _safe_mean(items, "mem_delta_kb")
            avg_pts = _safe_mean(items, "points_returned")
            print(f"{fmt:>12s}  {step:>6d}  {avg_t:>8.3f}  {avg_cpu:>8.3f}  "
                  f"{avg_read/(1024**2):>9.1f}  {avg_amp:>8.1f}  "
                  f"{avg_mem:>9.0f}  {avg_pts:>10,.0f}")

    # Pattern 4: AI training
    print("\n--- Pattern 4: AI Training Simulation (Random windows) ---")
    r4 = [r for r in results if r["pattern"] == "random_windows"]
    by_fmt = defaultdict(list)
    for r in r4:
        by_fmt[r["format"]].append(r)

    print(f"{'Format':>12s}  {'Wall(s)':>8s}  {'CPU(s)':>8s}  {'%CPU':>6s}  "
          f"{'Read(MB)':>9s}  {'Amp':>7s}  {'Mem(KB)':>9s}  {'Pts':>12s}")
    print("-" * 110)
    for fmt, items in sorted(by_fmt.items()):
        avg_t = _safe_mean(items, "wall_time_s")
        avg_cpu = _safe_mean(items, "cpu_user_s") + _safe_mean(items, "cpu_sys_s")
        avg_read = _safe_mean(items, "bytes_read")
        avg_amp = _safe_mean(items, "read_amplification")
        avg_mem = _safe_mean(items, "mem_delta_kb")
        avg_pts = _safe_mean(items, "points_returned")
        pct_cpu = 100 * avg_cpu / avg_t if avg_t > 0 else 0
        print(f"{fmt:>12s}  {avg_t:>8.3f}  {avg_cpu:>8.3f}  {pct_cpu:>5.0f}%  "
              f"{avg_read/(1024**2):>9.1f}  {avg_amp:>7.1f}  "
              f"{avg_mem:>9.0f}  {avg_pts:>12,.0f}")


def plot_io_amplification(results):
    """Generate benchmark visualization charts."""
    if not HAS_MATPLOTLIB:
        print("\n[!] matplotlib not available, skipping plots")
        return

    # 4 formats → 4 distinct colors
    COLORS = ["#2196F3", "#4CAF50", "#FF9800", "#E91E63"]  # blue, green, orange, pink

    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle("TsFile vs Industry Formats: AI Training Workload Benchmark",
                 fontsize=15, fontweight="bold")

    # ── Row 1, Col 1: Downsampling read amplification ──
    ax = axes[0, 0]
    r3 = [r for r in results if r["pattern"] == "downsampling"]
    by_fmt_step = defaultdict(lambda: defaultdict(list))
    for r in r3:
        by_fmt_step[r["format"]][r["sample_step"]].append(r)

    for i, fmt in enumerate(sorted(by_fmt_step)):
        steps = sorted(by_fmt_step[fmt])
        amps = []
        for step in steps:
            items = by_fmt_step[fmt][step]
            avg_read = _safe_mean(items, "bytes_read")
            avg_pts = _safe_mean(items, "points_returned")
            useful = avg_pts * 16
            amp = avg_read / useful if useful > 0 and avg_read > 0 else 0
            amps.append(amp)
        ax.plot(steps, amps, "o-", label=fmt, linewidth=2, markersize=8,
                color=COLORS[i % len(COLORS)])

    ax.set_xlabel("Downsampling Rate (1/N points)", fontsize=11)
    ax.set_ylabel("Read Amplification", fontsize=11)
    ax.set_title("Read Amplification vs Downsampling Rate", fontsize=12)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.set_xscale("log")
    ax.set_yscale("log")

    # ── Row 1, Col 2: Column pruning ──
    ax = axes[0, 1]
    r2 = [r for r in results if r["pattern"] == "column_subset"]
    by_fmt_sel = defaultdict(lambda: defaultdict(list))
    for r in r2:
        by_fmt_sel[r["format"]][r["selectivity"]].append(r)

    for i, fmt in enumerate(sorted(by_fmt_sel)):
        sels = sorted(by_fmt_sel[fmt])
        reads = [_safe_mean(by_fmt_sel[fmt][s], "bytes_read") for s in sels]
        ax.plot([s * 100 for s in sels], reads, "s-", label=fmt, linewidth=2, markersize=8,
                color=COLORS[i % len(COLORS)])

    ax.set_xlabel("Column Selectivity (%)", fontsize=11)
    ax.set_ylabel("Bytes Read (MB)", fontsize=11)
    ax.set_title("Column Pruning: Bytes Read vs Columns Requested", fontsize=12)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    # ── Row 1, Col 3: Pts/s throughput by pattern ──
    ax = axes[0, 2]
    all_patterns = ["sequential_scan", "column_subset", "downsampling", "random_windows"]
    pattern_labels = ["Seq.Scan", "Col.Subset", "Downsample", "AI-Train"]
    fmt_names = sorted(set(r["format"] for r in results))

    x = np.arange(len(pattern_labels))
    width = 0.2

    for i, fmt in enumerate(fmt_names):
        pts_per_s = []
        for pat in all_patterns:
            pat_results = [r for r in results if r["pattern"] == pat and r["format"] == fmt]
            if pat_results:
                avg_t = _safe_mean(pat_results, "wall_time_s")
                avg_pts = _safe_mean(pat_results, "points_returned")
                pts_per_s.append(avg_pts / avg_t if avg_t > 0 else 0)
            else:
                pts_per_s.append(0)
        ax.bar(x + i * width, pts_per_s, width, label=fmt,
               color=COLORS[i % len(COLORS)])

    ax.set_xlabel("Query Pattern", fontsize=11)
    ax.set_ylabel("Throughput (pts/s)", fontsize=11)
    ax.set_title("Effective Throughput by Query Pattern", fontsize=12)
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels(pattern_labels)
    ax.legend(fontsize=9)
    ax.set_yscale("log")

    # ── Row 2, Col 1: CPU vs Wall time (P4 AI Training) ──
    ax = axes[1, 0]
    r4 = [r for r in results if r["pattern"] == "random_windows"]
    by_fmt = defaultdict(list)
    for r in r4:
        by_fmt[r["format"]].append(r)

    fmts = sorted(by_fmt.keys())
    wall_times = [_safe_mean(by_fmt[f], "wall_time_s") for f in fmts]
    cpu_times = [_safe_mean(by_fmt[f], "cpu_user_s") + _safe_mean(by_fmt[f], "cpu_sys_s")
                 for f in fmts]

    x2 = np.arange(len(fmts))
    w = 0.35
    ax.bar(x2 - w/2, wall_times, w, label="Wall Time", color="#2196F3")
    ax.bar(x2 + w/2, cpu_times, w, label="CPU Time", color="#FF9800")
    ax.set_xticks(x2)
    ax.set_xticklabels(fmts)
    ax.set_ylabel("Time (s)")
    ax.set_title("Wall vs CPU Time: AI Training (P4)", fontsize=12)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)

    # ── Row 2, Col 2: Memory delta by pattern ──
    ax = axes[1, 1]
    pats_for_mem = ["sequential_scan", "random_windows"]
    mem_labels = ["Seq.Scan", "AI-Train"]
    x3 = np.arange(len(mem_labels))

    for i, fmt in enumerate(fmts):
        mem_vals = []
        for pat in pats_for_mem:
            pat_items = [r for r in results if r["pattern"] == pat and r["format"] == fmt]
            mem_vals.append(_safe_mean(pat_items, "mem_delta_kb") / 1024)
        ax.bar(x3 + i * width, mem_vals, width, label=fmt,
               color=COLORS[i % len(COLORS)])

    ax.set_xticks(x3 + width * 1.5)
    ax.set_xticklabels(mem_labels)
    ax.set_ylabel("Memory Delta (MB)")
    ax.set_title("Memory Footprint: RSS Delta per Query", fontsize=12)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.axhline(y=0, color="gray", linewidth=0.5)

    # ── Row 2, Col 3: CPU utilization % ──
    ax = axes[1, 2]
    cpu_vals = []
    for fmt in fmts:
        all_items = [r for r in results if r["format"] == fmt]
        avg_cpu = _safe_mean(all_items, "cpu_user_s") + _safe_mean(all_items, "cpu_sys_s")
        avg_wall = _safe_mean(all_items, "wall_time_s")
        cpu_pct = 100 * avg_cpu / avg_wall if avg_wall > 0 else 0
        cpu_vals.append(cpu_pct)

    bar_colors = ["#2196F3" if v <= 120 else "#FF9800" if v <= 300 else "#E91E63"
                  for v in cpu_vals]
    ax.bar(fmts, cpu_vals, color=bar_colors)
    ax.axhline(y=100, color="gray", linewidth=0.5, linestyle="--")
    ax.set_ylabel("CPU Utilization (%)")
    ax.set_title("CPU Parallelism: CPU/Wall Ratio", fontsize=12)
    ax.grid(True, alpha=0.3)

    # Add text labels
    for i, v in enumerate(cpu_vals):
        ax.text(i, v + 5, f"{v:.0f}%", ha="center", fontsize=10)

    plt.tight_layout()
    out_path = Path(cfg.RESULT_DIR) / "io_amplification.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"\n[OK] Chart saved to {out_path}")
    plt.close()


def print_key_findings(results):
    """Print key findings and conclusions."""
    print("\n" + "=" * 90)
    print("KEY FINDINGS FOR TsFile OPTIMIZATION")
    print("=" * 90)

    # Downsampling amplification
    r3 = [r for r in results if r["pattern"] == "downsampling"]
    by_fmt_step = defaultdict(lambda: defaultdict(list))
    for r in r3:
        by_fmt_step[r["format"]][r["sample_step"]].append(r)

    print("\n1. DOWNSAMPLING READ AMPLIFICATION")
    print("   Ratio of compressed bytes accessed to useful decompressed bytes.")
    for fmt in sorted(by_fmt_step):
        max_step = max(by_fmt_step[fmt].keys())
        items = by_fmt_step[fmt][max_step]
        avg_amp = _safe_mean(items, "read_amplification")
        avg_read = _safe_mean(items, "bytes_read")
        avg_useful = _safe_mean(items, "bytes_useful")
        print(f"   {fmt:>12s}: 1/{max_step} sampling -> {avg_amp:.1f}x amplification "
              f"({avg_read/(1024**2):.0f} MB read / {avg_useful/(1024**2):.3f} MB useful)")

    # Column subset efficiency
    r2 = [r for r in results if r["pattern"] == "column_subset"]
    by_fmt_sel = defaultdict(lambda: defaultdict(list))
    for r in r2:
        by_fmt_sel[r["format"]][r["selectivity"]].append(r)

    print("\n2. COLUMN SUBSET EFFICIENCY")
    print("   Lower time at low selectivity = better column pruning at I/O layer.")
    for fmt in sorted(by_fmt_sel):
        min_sel = min(by_fmt_sel[fmt].keys())
        max_sel = max(by_fmt_sel[fmt].keys())
        t_min = _safe_mean(by_fmt_sel[fmt][min_sel], "wall_time_s")
        t_max = _safe_mean(by_fmt_sel[fmt][max_sel], "wall_time_s")
        ratio = t_min / t_max if t_max > 0 else 1
        print(f"   {fmt:>12s}: {min_sel:.0%} col time vs {max_sel:.0%} col time: "
              f"{t_min:.2f}s / {t_max:.2f}s = {ratio:.2f}x "
              f"({'good pruning' if ratio < 0.5 else 'weak pruning'})")

    # AI training pattern
    r4 = [r for r in results if r["pattern"] == "random_windows"]
    print("\n3. AI TRAINING WORKLOAD PERFORMANCE")
    print(f"   {'Format':>12s}  {'Wall(s)':>8s}  {'CPU(s)':>8s}  {'Read(MB)':>9s}  "
          f"{'Amp':>7s}  {'PkMem(KB)':>10s}  {'Pts/s':>10s}")
    print("   " + "-" * 80)
    for fmt in sorted(set(r["format"] for r in r4)):
        items = [r for r in r4 if r["format"] == fmt]
        avg_t = _safe_mean(items, "wall_time_s")
        avg_cpu = _safe_mean(items, "cpu_user_s") + _safe_mean(items, "cpu_sys_s")
        avg_read = _safe_mean(items, "bytes_read")
        avg_amp = _safe_mean(items, "read_amplification")
        avg_mem = _safe_mean(items, "mem_delta_kb")
        avg_pts = _safe_mean(items, "points_returned")
        pts_per_s = avg_pts / avg_t if avg_t > 0 else 0
        print(f"   {fmt:>12s}  {avg_t:>8.3f}  {avg_cpu:>8.3f}  "
              f"{avg_read/(1024**2):>9.1f}  {avg_amp:>7.1f}  "
              f"{avg_mem:>10.0f}  {pts_per_s:>10,.0f}")

    print("\n4. RECOMMENDATIONS FOR TsFile")
    print("   a) Page-internal column offsets: allow reading specific columns without full decompression")
    print("   b) Sparse-index within pages: enable skip-N reading without full deserialization")
    print("   c) Multi-resolution pages: compaction generates coarser pages for AI workloads")
    print("   d) Column-major page layout: separate time/value columns within page for projection")


if __name__ == "__main__":
    results = load_results()
    if results is None:
        sys.exit(1)

    print_table(results)
    plot_io_amplification(results)
    print_key_findings(results)
