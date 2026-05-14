"""Quick comparison of eager vs lazy TsFile benchmark modes."""
import os, sys, json, subprocess, time, glob
from pathlib import Path

BENCH_DIR = Path(__file__).parent
TSFILE_DIR = BENCH_DIR / "data" / "tsfile"
CLASSES_DIR = BENCH_DIR / "tsfile-bench" / "target" / "classes"
SRC_DIR = BENCH_DIR / "tsfile-bench" / "src" / "main" / "java"
M2 = os.path.expanduser("~/.m2/repository")

# --- Compile ---
cp_jars = [
    f"{M2}/org/apache/tsfile/tsfile/2.3.0-260422-SNAPSHOT/tsfile-2.3.0-260422-SNAPSHOT.jar",
    f"{M2}/org/apache/tsfile/common/2.3.0-260422-SNAPSHOT/common-2.3.0-260422-SNAPSHOT.jar",
    f"{M2}/org/slf4j/slf4j-api/1.5.6/slf4j-api-1.5.6.jar",
    f"{M2}/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar",
    f"{M2}/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
    f"{M2}/at/yawk/lz4/lz4-java/1.10.1/lz4-java-1.10.1.jar",
    f"{M2}/org/tukaani/xz/1.8/xz-1.8.jar",
    f"{M2}/com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar",
    f"{M2}/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar",
]
CP = os.pathsep.join([str(CLASSES_DIR)] + cp_jars)

# Compile
CLASSES_DIR.mkdir(parents=True, exist_ok=True)
src_files = [str(SRC_DIR / "LazyTsFileQuerier.java"), str(SRC_DIR / "TsFileNativeRunner.java")]
print("Compiling...")
r = subprocess.run(
    ["javac", "-encoding", "UTF-8", "-cp", CP, "-d", str(CLASSES_DIR)] + src_files,
    capture_output=True, text=True
)
if r.returncode != 0:
    print(f"Compile error:\n{r.stderr}")
    sys.exit(1)
print("Compiled OK\n")

# --- Run both modes ---
tsfile_dir = str(TSFILE_DIR)
env = os.environ.copy()
env["OMP_NUM_THREADS"] = "1"

results = {}
for mode, lazy_flag in [("EAGER", "false"), ("LAZY", "true")]:
    print(f"=== {mode} MODE (lazy={lazy_flag}) ===")
    t0 = time.perf_counter()
    r = subprocess.run(
        ["java", f"-Dtsfile.lazy.page.load={lazy_flag}", "-cp", CP,
         "TsFileNativeRunner", tsfile_dir],
        capture_output=True, text=True, env=env, timeout=600
    )
    elapsed = time.perf_counter() - t0
    print(f"  Java process exited in {elapsed:.1f}s, rc={r.returncode}")

    if r.returncode != 0:
        print(f"  STDERR: {r.stderr[:500]}")
        continue

    try:
        data = json.loads(r.stdout.strip())
    except json.JSONDecodeError as e:
        print(f"  JSON parse error: {e}")
        print(f"  STDOUT preview: {r.stdout[:300]}")
        continue

    results[mode] = data
    print(f"  Results: {len(data)} entries")

# --- Compare ---
if "EAGER" in results and "LAZY" in results:
    eager = results["EAGER"]
    lazy = results["LAZY"]

    print("\n" + "=" * 80)
    print("COMPARISON: EAGER vs LAZY")
    print("=" * 80)

    pat_order = ["sequential_scan", "column_subset", "downsampling", "random_windows"]

    for pat in pat_order:
        e_items = [r for r in eager if r["pattern"] == pat]
        l_items = [r for r in lazy if r["pattern"] == pat]

        if not e_items or not l_items:
            continue

        n = len(e_items)
        def avg(items, key):
            vals = [item.get(key, 0) for item in items if isinstance(item, dict)]
            return sum(vals) / max(len(vals), 1)

        print(f"\n--- {pat} ({n} entries each) ---")
        print(f"{'Metric':>22s} {'EAGER':>12s} {'LAZY':>12s} {'Delta':>10s}")
        print("-" * 58)

        for key, fmt in [("wall_time_s", ".3f"), ("cpu_user_s", ".3f"),
                          ("bytes_read", ".0f"), ("read_amplification", ".1f"),
                          ("points_returned", ".0f")]:
            e_val = avg(e_items, key)
            l_val = avg(l_items, key)
            delta = l_val - e_val
            pct = (delta / e_val * 100) if e_val != 0 else 0
            print(f"{key:>22s} {e_val:12{fmt}} {l_val:12{fmt}} {delta:+10{fmt}} ({pct:+.1f}%)")

    # Show per-pattern bytes_read improvement
    print("\n--- read_amplification comparison ---")
    for pat in pat_order:
        e_items = [r for r in eager if r["pattern"] == pat]
        l_items = [r for r in lazy if r["pattern"] == pat]
        if not e_items:
            continue
        e_amp = sum(r.get("read_amplification", 0) for r in e_items) / len(e_items)
        l_amp = sum(r.get("read_amplification", 0) for r in l_items) / len(l_items)
        e_bytes = sum(r.get("bytes_read", 0) for r in e_items) / len(e_items)
        l_bytes = sum(r.get("bytes_read", 0) for r in l_items) / len(l_items)
        print(f"  {pat:>22s}: read_amp {e_amp:.1f} -> {l_amp:.1f},  "
              f"bytes_read {e_bytes/1e6:.1f}MB -> {l_bytes/1e6:.1f}MB")
else:
    print("\nCannot compare: one or both modes failed")
    for mode in results:
        print(f"  {mode}: {len(results[mode])} results")
