"""TsFile querier using JPype to call Java TsFile API from Python.

Implements the same 4 query methods as other queriers.
Uses TsFileReader.query() which supports time filtering and
chunk-level statistics-based pruning.

Supports two modes:
  EAGER (default): Standard TsFileReader.query() API.
  LAZY  (lazy=True):  LazyTsFileQuerier with accurate per-chunk I/O
                      tracking via -Dtsfile.lazy.page.load=true.
                      Decompresses pages on-demand and releases
                      each page after use.
"""

import os
import numpy as np
import jpype
import jpype.imports

_jvm_started = False
_jvm_lazy_mode = False
_LAZY_CLASSES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "tsfile-bench", "target", "classes")


def _start_jvm(lazy=False):
    global _jvm_started, _jvm_lazy_mode
    # If JVM already started but lazy mode changed, we need to restart
    if _jvm_started and _jvm_lazy_mode != lazy:
        jpype.shutdownJVM()
        _jvm_started = False
    if _jvm_started:
        return
    _jvm_lazy_mode = lazy

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
    if lazy and os.path.isdir(_LAZY_CLASSES_DIR):
        jars.append(_LAZY_CLASSES_DIR)

    jvm_args = []
    if lazy:
        jvm_args.append("-Dtsfile.lazy.page.load=true")

    jpype.startJVM(*jvm_args, classpath=";".join(jars))
    _jvm_started = True


class TsFileQuerier:
    """Query TsFile via TsFileReader.query() API.

    Can be initialized with either:
    - A single .tsfile path (for backward compatibility)
    - A directory path containing per-device .tsfile files (like other queriers)

    Parameters:
        data_path:  Path to .tsfile file or directory of per-device .tsfile files.
        device_path: Device path prefix (default "root.test.d1").
        lazy:       If True, enable lazy page loading mode via
                    -Dtsfile.lazy.page.load=true.
                    Uses LazyTsFileQuerier for accurate per-page I/O tracking.
    """

    def __init__(self, data_path, device_path="root.test.d1", lazy=False):
        _start_jvm(lazy=lazy)

        from pathlib import Path as PyPath
        p = PyPath(data_path)

        if p.is_dir():
            self.files = {f.stem: str(f) for f in p.glob("*.tsfile")}
            if not self.files:
                raise ValueError(f"No .tsfile files in {data_path}")
            first_file = list(self.files.values())[0]
        else:
            self.files = {PyPath(data_path).stem: data_path}
            first_file = data_path

        self.device_path = device_path
        self._readers = {}  # lazy-opened per-device queriers
        self.path = first_file
        self.lazy = lazy

        # Discover measurement names from the first file
        from org.apache.tsfile.read import TsFileSequenceReader

        reader = TsFileSequenceReader(first_file)
        try:
            devices = reader.getAllDevices()
            if not devices.isEmpty():
                actual_device = devices.get(0)
                chunks = reader.readChunkMetadataInDevice(actual_device)
                all_keys = [str(k) for k in chunks.keySet()]
                self.measurements = sorted([k for k in all_keys if k])
                self.device_path = str(actual_device)
            else:
                self.measurements = []
        except Exception:
            self.measurements = []
        reader.close()

        # Precompute per-measurement byte costs (eager mode fallback)
        self._costs = {}
        if not lazy:
            for dev, path in self.files.items():
                import os as _os
                fsize = _os.path.getsize(path)
                n_meas = max(len(self.measurements), 1)
                per_meas = fsize // (n_meas + 1)  # +1 for time column
                for meas in self.measurements:
                    self._costs[(dev, meas)] = per_meas

    def _cost(self, device, measurement):
        """Return estimated compressed bytes for this device+measurement."""
        if self.lazy:
            # In lazy mode, get actual bytes from the last query
            r = self._get_reader(device)
            return r.getLastBytesRead() if r else self._fallback_cost(device)
        return self._costs.get((device, measurement), 0)

    def _fallback_cost(self, device):
        import os as _os
        fsize = _os.path.getsize(self.files[device])
        return fsize // max(len(self.measurements) + 1, 1)

    def _get_reader(self, device):
        """Get or open a LazyTsFileQuerier for a device."""
        if device not in self._readers:
            from org.apache.tsfile.read import TsFileSequenceReader, TsFileReader

            if device not in self.files:
                raise KeyError(f"Device {device} not found in TsFile files: {list(self.files.keys())}")

            if self.lazy:
                # Lazy mode: use LazyTsFileQuerier
                from LazyTsFileQuerier import LazyTsFileQuerier as LQ
                self._readers[device] = LQ(self.files[device])
            else:
                # Eager mode: standard TsFileReader
                seq = TsFileSequenceReader(self.files[device])
                self._readers[device] = (seq, TsFileReader(seq))
        return self._readers[device]

    def _query_count(self, device, measurement, time_start, time_end):
        """Run a time-filtered query and return row count."""
        if self.lazy:
            r = self._get_reader(device)
            return r.countRows(device, measurement, time_start, time_end)
        else:
            from org.apache.tsfile.read.common import Path
            from org.apache.tsfile.read.expression import QueryExpression
            from org.apache.tsfile.read.expression.impl import GlobalTimeExpression
            from org.apache.tsfile.read.filter.factory import TimeFilterApi, FilterFactory
            from java.util import Collections

            _, ts_reader = self._get_reader(device)

            path_list = Collections.singletonList(
                Path(self.device_path, measurement, True)
            )

            if time_start is not None and time_end is not None:
                tf = FilterFactory.and_(
                    TimeFilterApi.gtEq(time_start),
                    TimeFilterApi.ltEq(time_end),
                )
                expr = QueryExpression.create(path_list, GlobalTimeExpression(tf))
            else:
                expr = QueryExpression.create(path_list, None)

            ds = ts_reader.query(expr)
            count = 0
            while ds.hasNext():
                ds.next()
                count += 1
            return count

    # ── Public API (matches other queriers) ──

    def sequential_scan(self, device, measurement, time_start, time_end):
        return self._query_count(device, measurement, time_start, time_end)

    def column_subset(self, device, target_measurements, time_start, time_end):
        total = 0
        for meas in target_measurements:
            total += self._query_count(device, meas, time_start, time_end)
        return total

    def downsampling(self, device, measurement, time_start, time_end, step_n):
        total = self._query_count(device, measurement, time_start, time_end)
        return total // step_n

    def random_windows(self, device, target_measurements, windows):
        total = 0
        for meas in target_measurements:
            for (t_start, t_end) in windows:
                total += self._query_count(device, meas, t_start, t_end)
        return total

    def close(self):
        for reader in self._readers.values():
            try:
                if self.lazy:
                    reader.close()
                else:
                    seq_reader, ts_reader = reader
                    try:
                        ts_reader.close()
                    except Exception:
                        pass
                    try:
                        seq_reader.close()
                    except Exception:
                        pass
            except Exception:
                pass
        self._readers.clear()

    def __del__(self):
        self.close()
