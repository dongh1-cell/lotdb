"""Run TsFile benchmark natively in Java (subprocess, no JPype overhead).

Memory: monitors Java subprocess RSS via psutil (fair vs Python queriers).
CPU: Java uses process-level OperatingSystemMXBean.getProcessCpuTime().
Warmup: Java does 3 warmup iterations before measurement.
"""

import os
import json
import time
import subprocess
import sys
from pathlib import Path

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


def _get_classpath():
    m2 = os.path.expanduser("~/.m2/repository")
    jars = [
        f"{m2}/org/apache/tsfile/tsfile/2.3.0-260422-SNAPSHOT/tsfile-2.3.0-260422-SNAPSHOT.jar",
        f"{m2}/org/apache/tsfile/common/2.3.0-260422-SNAPSHOT/common-2.3.0-260422-SNAPSHOT.jar",
        f"{m2}/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
        f"{m2}/at/yawk/lz4/lz4-java/1.10.1/lz4-java-1.10.1.jar",
        f"{m2}/org/tukaani/xz/1.8/xz-1.8.jar",
        f"{m2}/com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar",
        f"{m2}/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar",
        f"{m2}/org/slf4j/slf4j-api/1.5.6/slf4j-api-1.5.6.jar",
        f"{m2}/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar",
    ]
    return ";".join(jars)


def _ensure_compiled():
    bench_dir = Path(__file__).parent / "tsfile-bench"
    src_dir = bench_dir / "src" / "main" / "java"
    classes_dir = bench_dir / "target" / "classes"
    main_class = classes_dir / "TsFileNativeRunner.class"
    lazy_class = classes_dir / "LazyTsFileQuerier.class"

    # Check if both classes are up to date
    main_src = src_dir / "TsFileNativeRunner.java"
    lazy_src = src_dir / "LazyTsFileQuerier.java"
    if main_class.exists() and lazy_class.exists():
        if (main_class.stat().st_mtime >= main_src.stat().st_mtime and
            lazy_class.stat().st_mtime >= lazy_src.stat().st_mtime):
            return classes_dir

    classes_dir.mkdir(parents=True, exist_ok=True)
    cp = _get_classpath() + ";" + str(classes_dir)
    src_files = f'"{main_src}" "{lazy_src}"' if lazy_src.exists() else f'"{main_src}"'
    # Use PowerShell to avoid bash ;-as-command-separator issues on Windows
    cmd = [
        "powershell", "-Command",
        f"javac -encoding UTF-8 -cp '{cp}' -d '{classes_dir}' {src_files} 2>&1"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[!] TsFile compilation failed:\n{result.stderr or result.stdout}")
        return None
    return classes_dir


def run_benchmark(tsfile_dir, lazy=False, java_props=None):
    """Run native TsFile benchmark and return results with memory stats.

    Args:
        tsfile_dir: Path to directory containing per-device .tsfile files.
        lazy: If True, enable lazy page loading via -Dtsfile.lazy.page.load=true.
    """
    classes_dir = _ensure_compiled()
    if classes_dir is None:
        return []

    cp = str(classes_dir) + ";" + _get_classpath()
    ts_dir = str(Path(tsfile_dir).absolute())
    cmd = ["java", "-cp", cp]
    if lazy:
        cmd.append("-Dtsfile.lazy.page.load=true")
    if java_props:
        for key, value in java_props.items():
            cmd.append(f"-D{key}={value}")
    cmd.extend(["TsFileNativeRunner", ts_dir])

    # ── Launch with Popen; drain stdout in thread to avoid pipe deadlock ──
    import threading

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    stdout_lines = []
    stderr_lines = []

    def _drain(pipe, sink):
        for line in pipe:
            sink.append(line)

    t_out = threading.Thread(target=_drain, args=(proc.stdout, stdout_lines), daemon=True)
    t_err = threading.Thread(target=_drain, args=(proc.stderr, stderr_lines), daemon=True)
    t_out.start()
    t_err.start()

    max_rss_bytes = 0
    if HAS_PSUTIL:
        try:
            ps_proc = psutil.Process(proc.pid)
            while proc.poll() is None:
                try:
                    rss = ps_proc.memory_info().rss
                    if rss > max_rss_bytes:
                        max_rss_bytes = rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    break
                time.sleep(0.01)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    proc.wait()
    t_out.join(timeout=2)
    t_err.join(timeout=2)
    stdout = "".join(stdout_lines)
    stderr = "".join(stderr_lines)

    if proc.returncode != 0:
        print(f"[!] TsFile benchmark error:\n{stderr}")
        return []

    # Parse JSON
    try:
        results = json.loads(stdout.strip())
    except json.JSONDecodeError as e:
        print(f"[!] JSON parse error: {e}")
        return []

    # ── Inject memory data into results ──
    peak_kb = int(max_rss_bytes / 1024) if max_rss_bytes > 0 else 0
    for r in results:
        r["mem_rss_before_kb"] = 0
        r["mem_rss_peak_kb"] = peak_kb
        r["mem_rss_after_kb"] = 0
        r["mem_delta_kb"] = peak_kb

    return results


if __name__ == "__main__":
    results = run_benchmark("data/tsfile")
    print(f"Got {len(results)} results")
    if results:
        print(f"Peak memory: {results[0].get('mem_rss_peak_kb', 0)} KB")
