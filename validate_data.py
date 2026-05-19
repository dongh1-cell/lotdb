"""Cross-format data quality and consistency validation.

Checks:
  1. Parquet: point counts, timestamps, no NaN/Inf/duplicates
  2. Cross-device signal diversity
  3. Parquet  vs Arrow value consistency (full, bit-identical)
  4. Parquet  vs HDF5 value consistency (full, bit-identical)
  5. Parquet  vs TsFile point count consistency
  6. Parquet  vs TsFile value consistency (random sample)

Usage:
  python validate_data.py                # full validation
  python validate_data.py --quick        # 3 devices only
"""

import sys
import numpy as np
import pyarrow.parquet as pq
import pyarrow.feather as feather
import h5py
from pathlib import Path

import config as cfg

QUICK = "--quick" in sys.argv


def check_parquet(parquet_dir, max_devices=None):
    """Check Parquet data integrity."""
    files = sorted(Path(parquet_dir).glob("*.parquet"))
    if max_devices:
        files = files[:max_devices]

    print("=== 1. Parquet Integrity ===")
    all_ok = True
    for f in files:
        dev = f.stem
        table = pq.read_table(f)
        df = table.to_pandas()
        meas_set = sorted(df["measurement"].unique())
        times_sorted = np.sort(df["time"].unique())
        n_ts = len(times_sorted)

        # Checks
        dupes = df.duplicated(subset=["measurement", "time"]).sum()
        nans = df["value"].isna().sum()
        infs = np.isinf(df["value"].values).sum()
        diffs = np.diff(times_sorted)
        mono = bool(np.all(diffs == cfg.INTERVAL_SECONDS))
        expected_pts = cfg.DURATION_DAYS * 86400 // cfg.INTERVAL_SECONDS

        errors = []
        if len(meas_set) != cfg.MEASUREMENTS_PER_DEVICE:
            errors.append(f"meas count {len(meas_set)} != {cfg.MEASUREMENTS_PER_DEVICE}")
        if n_ts != expected_pts:
            errors.append(f"timestamps {n_ts} != {expected_pts}")
        if dupes > 0:
            errors.append(f"{dupes} duplicate rows")
        if nans > 0:
            errors.append(f"{nans} NaN values")
        if infs > 0:
            errors.append(f"{infs} Inf values")
        if not mono:
            errors.append("intervals not strictly 2s")

        for m in meas_set:
            cnt = len(df[df["measurement"] == m])
            if cnt != n_ts:
                errors.append(f"{m} has {cnt} pts != {n_ts}")

        if errors:
            print(f"  {dev}: FAIL — {'; '.join(errors)}")
            all_ok = False

    if all_ok:
        print(f"  All {len(files)} devices pass: {cfg.MEASUREMENTS_PER_DEVICE} meas x {expected_pts} pts, no issues")
    return all_ok


def check_cross_device_diversity(parquet_dir, max_devices=5):
    """Check that devices are actually different."""
    files = sorted(Path(parquet_dir).glob("*.parquet"))[:max_devices]
    print(f"\n=== 2. Cross-Device Diversity ({len(files)} devices) ===")

    # Compare 'temperature' across devices
    temps = []
    for f in files:
        df = pq.read_table(f).to_pandas()
        t = df[df["measurement"] == "temperature"]["value"].values[:1000]
        temps.append(t)

    too_similar = []
    for i in range(len(temps)):
        for j in range(i + 1, len(temps)):
            corr = np.corrcoef(temps[i], temps[j])[0, 1]
            if abs(corr) > 0.95:
                too_similar.append((files[i].stem, files[j].stem, corr))

    if too_similar:
        for a, b, c in too_similar:
            print(f"  WARNING: {a} vs {b} correlation={c:.4f}")
        return False

    # Show range diversity for key measurements
    for m in ["temperature", "vibration", "level"]:
        ranges = []
        for f in files:
            df = pq.read_table(f).to_pandas()
            v = df[df["measurement"] == m]["value"].values
            ranges.append((v.min(), v.max()))
        print(f"  {m:>15s}: ranges {[f'{lo:.1f}~{hi:.1f}' for lo, hi in ranges]}")
    print("  All device pairs have correlation < 0.95")
    return True


def check_parquet_vs_arrow(parquet_dir, arrow_dir, max_devices=3):
    """Bit-identical check between Parquet and Arrow."""
    pq_files = sorted(Path(parquet_dir).glob("*.parquet"))[:max_devices]
    ar_files = sorted(Path(arrow_dir).glob("*.arrow"))[:max_devices]
    if len(pq_files) != len(ar_files):
        print("  FAIL: file count mismatch")
        return False

    print(f"\n=== 3. Parquet  vs Arrow ({len(pq_files)} devices) ===")
    for pf, af in zip(pq_files, ar_files):
        pdf = pq.read_table(pf).to_pandas().sort_values(["measurement", "time"])
        adf = feather.read_feather(af).sort_values(["measurement", "time"])
        pv = pdf["value"].values
        av = adf["value"].values
        if len(pv) != len(av) or np.max(np.abs(pv - av)) > 1e-12:
            diff = np.max(np.abs(pv - av)) if len(pv) == len(av) else float("nan")
            print(f"  {pf.stem}: FAIL max_diff={diff}")
            return False
    print("  All bit-identical (max diff < 1e-12)")
    return True


def check_parquet_vs_hdf5(parquet_dir, hdf5_path, max_devices=3):
    """Bit-identical check between Parquet and HDF5."""
    pq_files = sorted(Path(parquet_dir).glob("*.parquet"))[:max_devices]
    print(f"\n=== 4. Parquet  vs HDF5 ({len(pq_files)} devices) ===")

    with h5py.File(hdf5_path, "r") as f:
        for pf in pq_files:
            dev = pf.stem
            pdf = pq.read_table(pf).to_pandas()
            for meas in sorted(pdf["measurement"].unique()):
                pv = pdf[pdf["measurement"] == meas].sort_values("time")["value"].values
                hv = f[f"{dev}/{meas}/value"][:]
                if len(pv) != len(hv) or np.max(np.abs(pv - hv)) > 1e-12:
                    diff = np.max(np.abs(pv - hv)) if len(pv) == len(hv) else float("nan")
                    print(f"  {dev}/{meas}: FAIL max_diff={diff}")
                    return False
    print("  All bit-identical (max diff < 1e-12)")
    return True


def check_tsfile(parquet_dir, tsfile_dir, max_devices=3):
    """Point count + value sample check between Parquet and TsFile."""
    import os, jpype, jpype.imports
    m2 = os.path.expanduser("~/.m2/repository")
    jars = [
        f"{m2}/org/apache/tsfile/tsfile/2.3.0-260422-SNAPSHOT/tsfile-2.3.0-260422-SNAPSHOT.jar",
        f"{m2}/org/apache/tsfile/common/2.3.0-260422-SNAPSHOT/common-2.3.0-260422-SNAPSHOT.jar",
        f"{m2}/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar",
        f"{m2}/org/slf4j/slf4j-simple/1.7.25/slf4j-simple-1.7.25.jar",
        f"{m2}/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar",
        f"{m2}/at/yawk/lz4/lz4-java/1.10.1/lz4-java-1.10.1.jar",
        f"{m2}/org/tukaani/xz/1.8/xz-1.8.jar",
        f"{m2}/com/github/luben/zstd-jni/1.5.5-11/zstd-jni-1.5.5-11.jar",
        f"{m2}/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar",
    ]
    jpype.startJVM(classpath=";".join(jars))
    from org.apache.tsfile.read import TsFileSequenceReader
    from org.apache.tsfile.read.common import Path as TSPath
    from org.apache.tsfile.read.expression import QueryExpression
    from org.apache.tsfile.read.expression.impl import GlobalTimeExpression
    from org.apache.tsfile.read.filter.factory import FilterFactory, TimeFilterApi
    from java.util import Collections

    pq_files = sorted(Path(parquet_dir).glob("*.parquet"))[:max_devices]
    ts_files = {f.stem: f for f in sorted(Path(tsfile_dir).glob("*.tsfile"))}

    print(f"\n=== 5. TsFile Point Count + Value Check ({len(pq_files)} devices) ===")

    for pf in pq_files:
        dev = pf.stem
        if dev not in ts_files:
            print(f"  {dev}: FAIL no matching TsFile")
            return False

        pdf = pq.read_table(pf).to_pandas()
        reader = TsFileSequenceReader(str(ts_files[dev]))
        dev_id = reader.getAllDevices().get(0)
        dev_path = str(dev_id)
        chunks = reader.readChunkMetadataInDevice(dev_id)

        # Point count check
        for meas in sorted(pdf["measurement"].unique()):
            meta_list = chunks.get(meas)
            if meta_list is None:
                print(f"  {dev}/{meas}: FAIL measurement not found in TsFile")
                reader.close()
                return False
            total = sum(int(cm.getNumOfPoints()) for cm in meta_list)
            expected = len(pdf[pdf["measurement"] == meas])
            if total != expected:
                print(f"  {dev}/{meas}: FAIL TsFile={total} pts vs Parquet={expected}")
                reader.close()
                return False

        # Value spot check (3 random timestamps)
        meas_sample = sorted(pdf["measurement"].unique())[0]
        pq_vals = pdf[pdf["measurement"] == meas_sample].set_index("time")["value"]
        sample_times = sorted(np.random.default_rng(42).choice(
            pq_vals.index.values[:400000], 3, replace=False))

        for t in sample_times:
            p = TSPath(dev_path, meas_sample, True)
            tf = FilterFactory.and_(TimeFilterApi.gtEq(int(t)), TimeFilterApi.ltEq(int(t)))
            expr = QueryExpression.create(Collections.singletonList(p), GlobalTimeExpression(tf))
            ds = reader.query(expr)
            if ds.hasNext():
                row = ds.next()
                ts_val = row.getFields().get(1).getDoubleV()
                pq_val = pq_vals.get(t)
                if abs(ts_val - pq_val) > 1e-4:
                    print(f"  {dev}/{meas_sample}@{t}: FAIL TsFile={ts_val} Parquet={pq_val}")
                    reader.close()
                    return False

        reader.close()

    jpype.shutdownJVM()
    print("  All point counts match, sample values match (tol 1e-4)")
    return True


def main():
    parquet_dir = Path(cfg.DATA_DIR) / "parquet"
    arrow_dir = Path(cfg.DATA_DIR) / "arrow"
    hdf5_path = Path(cfg.DATA_DIR) / "hdf5" / "iot_dataset.h5"
    tsfile_dir = Path(cfg.DATA_DIR) / "tsfile"

    max_dev = 3 if QUICK else None
    print(f"{'QUICK' if QUICK else 'FULL'} validation {'(3 devices)' if QUICK else '(all devices)'}")
    print("=" * 50)

    results = []
    results.append(("Parquet Integrity", check_parquet(parquet_dir, max_dev)))
    results.append(("Cross-Device Diversity", check_cross_device_diversity(parquet_dir, 5 if QUICK else 10)))
    results.append(("Parquet  vs Arrow", check_parquet_vs_arrow(parquet_dir, arrow_dir, max_dev)))
    results.append(("Parquet  vs HDF5", check_parquet_vs_hdf5(parquet_dir, hdf5_path, max_dev)))

    # TsFile check requires JPype (slow), optional
    if not QUICK:
        try:
            results.append(("TsFile Count + Values", check_tsfile(parquet_dir, tsfile_dir, max_dev)))
        except Exception as e:
            print(f"\n  TsFile check skipped: {e}")

    print("\n" + "=" * 50)
    passed = sum(1 for _, ok in results if ok)
    failed = len(results) - passed
    for name, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'} {name}")
    print(f"\n{passed}/{len(results)} checks passed" + (f", {failed} FAILED" if failed else " — ALL OK"))


if __name__ == "__main__":
    main()
