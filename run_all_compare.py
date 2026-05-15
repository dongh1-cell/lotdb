"""Comprehensive comparison: Original, Solution A, B, C.
Runs all benchmark entry points and prints a unified comparison table.
"""
import os, sys, json, subprocess, time
from pathlib import Path

BENCH_DIR = Path(__file__).parent
CLASSES = BENCH_DIR / "tsfile-bench" / "target" / "classes"
SRC = BENCH_DIR / "tsfile-bench" / "src" / "main" / "java"
M2 = os.path.expanduser("~/.m2/repository")
TSFILE_DIR = str((BENCH_DIR / "data" / "tsfile").absolute())

JARS = [
    f"{M2}/org/apache/tsfile/tsfile/2.3.0-260422-SNAPSHOT/tsfile-2.3.0-260422-SNAPSHOT.jar",
    f"{M2}/org/apache/tsfile/common/2.3.0-260422-SNAPSHOT/common-2.3.0-260422-SNAPSHOT.jar",
    f"{M2}/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar",
    f"{M2}/org/slf4j/slf4j-simple/1.7.25/slf4j-simple-1.7.25.jar",
    f"{M2}/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
    f"{M2}/at/yawk/lz4/lz4-java/1.10.1/lz4-java-1.10.1.jar",
    f"{M2}/org/tukaani/xz/1.8/xz-1.8.jar",
    f"{M2}/com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar",
    f"{M2}/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar",
]

ALL_CP = os.pathsep.join([str(CLASSES)] + JARS)
ENV = {**os.environ, "OMP_NUM_THREADS": "1"}
JAVA = ["java", "-Xmx1g", "-cp", ALL_CP]

def run(cmd, timeout=180):
    t0 = time.perf_counter()
    r = subprocess.run(cmd, capture_output=True, text=True, env=ENV, timeout=timeout)
    t1 = time.perf_counter()
    r.elapsed = t1 - t0
    return r

# ── Step 1: Compile ──
print("=" * 70)
print("Compiling all Java sources...")
CLASSES.mkdir(parents=True, exist_ok=True)
srcs = [str(SRC / f) for f in [
    "LazyTsFileQuerier.java", "TsFileNativeRunner.java",
    "GorillaResyncCodec.java", "ResyncBenchmarkRunner.java",
    "DownsamplingBenchmark.java",
]]
javac_cp = os.pathsep.join([str(CLASSES)] + JARS)
r = subprocess.run(
    ["javac", "-encoding", "UTF-8", "-cp", javac_cp, "-d", str(CLASSES)] + srcs,
    capture_output=True, text=True
)
if r.returncode != 0:
    print(f"COMPILE ERROR:\n{r.stderr}")
    sys.exit(1)
print("OK\n")

results = {}

# ── Step 2: Original benchmark (TsFile native runner, eager) ──
print("--- [1/3] ORIGINAL (eager, full 4-pattern benchmark) ---")
r = run(JAVA + ["TsFileNativeRunner", TSFILE_DIR])
if r.returncode == 0:
    results["original"] = json.loads(r.stdout.strip())
    print(f"  OK: {len(results['original'])} entries, {r.elapsed:.1f}s")
else:
    print(f"  FAIL: {r.stderr[:200]}")

# ── Step 3: Solution A (lazy mode) ──
print("--- [2/4] SOLUTION A (lazy) ---")
r = run(JAVA + ["-Dtsfile.lazy.page.load=true", "TsFileNativeRunner", TSFILE_DIR])
if r.returncode == 0:
    results["lazy"] = json.loads(r.stdout.strip())
    print(f"  OK: {len(results['lazy'])} entries, {r.elapsed:.1f}s")
else:
    print(f"  FAIL: {r.stderr[:200]}")

# ── Step 4: Solution B+C combined (Resync decode + Read Amp on FULL data) ──
print("--- [3/4] SOLUTION B+C (Resync + Downsampling) ---")
r = run(JAVA + ["DownsamplingBenchmark", TSFILE_DIR])
if r.returncode == 0:
    results["downsample"] = json.loads(r.stdout.strip())
    print(f"  OK: {results['downsample']['total_values']:,} values, {r.elapsed:.1f}s")
    print(f"  Correctness: std={results['downsample']['correctness_ok']}, resync={results['downsample']['resync_correctness_ok']}")
else:
    print(f"  FAIL: {r.stderr[:200]}")

# ── Step 5 (removed - Solution B standalone is now covered by C on full data) ──

# ── Print Unified Comparison ──
print("\n" + "=" * 70)
print("UNIFIED COMPARISON")
print("=" * 70)

# Table 1: Original vs Solution A (TsFile wall_time per pattern)
if "original" in results and "lazy" in results:
    print("\n=== TABLE 1: Original vs Solution A (Wall Time + Bytes Read) ===\n")
    pats = ["sequential_scan", "column_subset", "downsampling", "random_windows"]
    print(f"{'Pattern':>22s}  {'Orig_wall':>10s}  {'Lazy_wall':>10s}  {'Orig_bytes':>12s}  {'Lazy_bytes':>12s}  {'Delta_bytes':>10s}")
    print("-" * 82)
    for pat in pats:
        o_items = [r for r in results["original"] if r["pattern"] == pat]
        l_items = [r for r in results["lazy"] if r["pattern"] == pat]
        if not o_items or not l_items:
            continue
        ow = sum(r["wall_time_s"] for r in o_items) / len(o_items)
        lw = sum(r["wall_time_s"] for r in l_items) / len(l_items)
        ob = sum(r.get("bytes_read", 0) for r in o_items) / len(o_items)
        lb = sum(r.get("bytes_read", 0) for r in l_items) / len(l_items)
        delta = (lb - ob) / ob * 100
        print(f"{pat:>22s}  {ow:10.4f}s  {lw:10.4f}s  {ob/1e6:>10.1f}MB  {lb/1e6:>10.1f}MB  {delta:>+9.1f}%")

# Table 2: Solution B/C — GORILLA Resync Decode Acceleration (from C benchmark)
if "downsample" in results:
    d = results["downsample"]
    print(f"\n=== TABLE 2: GORILLA Resync Decode Acceleration ===")
    print(f"  Data: {d['total_values']:,} values (FULL dataset), marker_interval=64")
    print(f"  Pages: {d.get('pages_read','?')}, Chunks: {d.get('chunks_read','?')}")
    print(f"  Std GORILLA: {d['std_gorilla_bytes']:,}B  Resync: {d['resync_gorilla_bytes']:,}B  Overhead: {d['resync_overhead_pct']}%")
    print(f"  Correctness: std={d['correctness_ok']}, resync={d['resync_correctness_ok']}")
    base = d['full_decode_baseline_ns']
    print(f"  Full decode baseline: {base:,d} ns ({base/1e6:.1f} ms)\n")
    print(f"{'Step':>6s}  {'Tgt':>8s}  {'Speedup':>8s}  {'FullDec':>10s}  {'SkipDec':>10s}  {'Saved':>8s}  {'Full(ns)':>12s}  {'Skip(ns)':>12s}")
    print("-" * 85)
    for r_item in d["results"]:
        s = r_item['step']
        print(f"{s:>6d}  {r_item['target_count']:>8,d}  {r_item['speedup']:>7.1f}x  {r_item['full_decoded']:>10,d}  {r_item['skip_decoded']:>10,d}  {r_item['decode_reduction_pct']:>7.1f}%  {r_item['full_ns']:>12,d}  {r_item['skip_ns']:>12,d}")

    # Table 3: Read Amplification
    print(f"\n=== TABLE 3: Read Amplification (Solution A+C combined view) ===")
    comp_bytes = d['compressed_page_bytes']
    print(f"  Compressed page data: {comp_bytes:,} bytes ({comp_bytes/1e6:.1f} MB)")
    print(f"  Raw (uncompressed):   {d['raw_bytes']:,} bytes ({d['raw_bytes']/1e6:.1f} MB)")
    print(f"")
    print(f"{'Step':>6s}  {'Targets':>8s}  {'UsefulB':>10s}  {'CompRead':>10s}  {'ReadAmp':>10s}  {'Note':>30s}")
    print("-" * 80)
    for r_item in d["results"]:
        s = r_item['step']
        useful = r_item['useful_bytes']
        comp = r_item['compressed_bytes_read']
        amp = r_item['read_amplification']
        note = "(needs Solution D)" if amp > 10 else "OK"
        print(f"{s:>6d}  {r_item['target_count']:>8,d}  {useful:>10,d}  {comp:>10,d}  {amp:>9.1f}x  {note}")

# Note: Solution B's decode acceleration is now fully covered by
# the DownsamplingBenchmark above (TABLE 2, running on 432K values).

print("\n" + "=" * 70)
print("All benchmarks complete.")
print("=" * 70)
