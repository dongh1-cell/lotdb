"""Run native TsFile benchmark on real C-MAPSS subsets."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from tsfile_native import run_benchmark


ROOT = Path(__file__).resolve().parent
RESULT_DIR = ROOT / "results" / "raw_json"


def run_subset(subset: str, lazy: bool) -> list[dict]:
    tag = subset.lower()
    tsfile_dir = ROOT / "data_real" / f"cmapss_{tag}" / "tsfile"
    if not tsfile_dir.exists():
        raise FileNotFoundError(f"missing TsFile directory: {tsfile_dir}")
    props = {
        "bench.interval.seconds": 1,
        "bench.random.window.length": 30,
        "bench.random.window.count": 100,
        "bench.random.target.devices": 10,
        "bench.random.target.measurements": 3,
        "bench.n.runs": 3,
        "bench.warmup.runs": 1,
    }
    rows = run_benchmark(str(tsfile_dir), lazy=lazy, java_props=props)
    for row in rows:
        row["dataset"] = f"NASA C-MAPSS {subset} train"
        row["real_tsfile_suite"] = True
        row["lazy"] = lazy
    return rows


def main() -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    all_rows = []
    for subset in ["FD001", "FD002", "FD003", "FD004"]:
        for lazy in [False, True]:
            print(f"[{subset}] TsFile lazy={lazy}")
            all_rows.extend(run_subset(subset, lazy=lazy))
    out = RESULT_DIR / f"real_tsfile_suite_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.write_text(json.dumps(all_rows, indent=2, default=str), encoding="utf-8")
    print(json.dumps({"result_json": str(out), "rows": len(all_rows)}, indent=2))


if __name__ == "__main__":
    main()
