"""Benchmark metrics: time breakdown, read amplification, peak memory.

Usage:
    from metrics import QueryMeasurer

    m = QueryMeasurer()
    m.start()

    m.phase("io_read")      # start I/O phase
    raw = read_from_file()
    m.phase("decompress")   # end io_read, start decompress
    data = decompress(raw)
    m.phase("filter")       # end decompress, start filter
    result = filter(data)
    m.phase_end()           # end filter

    # Report read amplification
    m.set_bytes_read(compressed_bytes_accessed)
    m.set_bytes_useful(points_returned * 16)

    metrics = m.finish()    # returns QueryMetrics dataclass
"""

import os
import time
import threading
import psutil
from dataclasses import dataclass, field


@dataclass
class QueryMetrics:
    """All metrics for a single query execution."""

    # Wall clock time
    wall_time_s: float = 0.0

    # Time breakdown (seconds)
    io_read_s: float = 0.0        # physical I/O (read from file)
    decompress_s: float = 0.0      # decompression / decoding
    filter_convert_s: float = 0.0  # application-level filtering + type conversion
    other_s: float = 0.0           # uncategorized time

    # CPU time (process-level, from os.times)
    cpu_user_s: float = 0.0
    cpu_sys_s: float = 0.0

    # Read amplification
    bytes_read: int = 0            # compressed bytes accessed on storage
    bytes_useful: int = 0          # decompressed bytes actually needed
    read_amplification: float = 0.0  # bytes_read / bytes_useful

    # Memory (RSS, KB)
    mem_rss_before_kb: int = 0
    mem_rss_peak_kb: int = 0
    mem_rss_after_kb: int = 0
    mem_delta_kb: int = 0          # after - before

    # Throughput
    throughput_mbps: float = 0.0   # bytes_useful / wall_time_s / 1e6


class MemorySampler:
    """Background thread that samples peak RSS during query execution."""

    def __init__(self, interval=0.005):  # 5ms sampling
        self.interval = interval
        self.peak_rss = 0
        self._stop = threading.Event()
        self._thread = None
        self._proc = psutil.Process(os.getpid())

    def _run(self):
        while not self._stop.is_set():
            try:
                rss = self._proc.memory_info().rss
                if rss > self.peak_rss:
                    self.peak_rss = rss
            except Exception:
                pass
            self._stop.wait(self.interval)

    def start(self):
        self.peak_rss = self._proc.memory_info().rss
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        return self.peak_rss


class QueryMeasurer:
    """Orchestrates measurement of a single query."""

    def __init__(self):
        self._mem = MemorySampler()
        self._proc = psutil.Process(os.getpid())

        # Timing
        self._t_wall_start = 0.0
        self._t_wall_end = 0.0
        self._phase_times = {}     # phase_name -> accumulated seconds
        self._phase_order = []     # ordered phase names
        self._current_phase = None
        self._t_phase_start = 0.0

        # CPU
        self._cpu_user_before = 0.0
        self._cpu_sys_before = 0.0

        # I/O
        self._io_bytes_before = 0
        self._bytes_read = 0
        self._bytes_useful = 0

        # Memory
        self._mem_rss_before = 0

        self._started = False

    def start(self):
        """Begin measuring. Call once before the query."""
        # Memory
        self._mem_rss_before = self._proc.memory_info().rss
        self._mem.start()

        # CPU
        cpu = os.times()
        self._cpu_user_before = cpu.user
        self._cpu_sys_before = cpu.system

        # I/O (process-level, for reference)
        try:
            io = self._proc.io_counters()
            self._io_bytes_before = io.read_bytes
        except Exception:
            self._io_bytes_before = 0

        # Wall clock
        self._t_wall_start = time.perf_counter()
        self._started = True

    def phase(self, name):
        """Start a named phase. Ends the previous phase if any."""
        t_now = time.perf_counter()
        if self._current_phase is not None:
            elapsed = t_now - self._t_phase_start
            self._phase_times[self._current_phase] = \
                self._phase_times.get(self._current_phase, 0.0) + elapsed
        self._current_phase = name
        if name not in self._phase_times:
            self._phase_order.append(name)
        self._t_phase_start = time.perf_counter()

    def phase_end(self):
        """End the current phase."""
        if self._current_phase is not None:
            t_now = time.perf_counter()
            elapsed = t_now - self._t_phase_start
            self._phase_times[self._current_phase] = \
                self._phase_times.get(self._current_phase, 0.0) + elapsed
            self._current_phase = None

    def set_bytes_read(self, n):
        """Set the number of compressed bytes accessed from storage."""
        self._bytes_read = n

    def set_bytes_useful(self, n):
        """Set the number of useful bytes (points_returned * 16)."""
        self._bytes_useful = n

    def set_io_read(self):
        """Estimate bytes read from process I/O counters (fallback)."""
        try:
            io = self._proc.io_counters()
            delta = io.read_bytes - self._io_bytes_before
            if delta > 0:
                self._bytes_read = delta
        except Exception:
            pass

    def finish(self) -> QueryMetrics:
        """End measurement and return metrics."""
        if not self._started:
            raise RuntimeError("start() was not called")

        # End current phase
        self.phase_end()

        # Wall clock
        self._t_wall_end = time.perf_counter()
        wall_time = self._t_wall_end - self._t_wall_start

        # CPU
        cpu = os.times()
        cpu_user = cpu.user - self._cpu_user_before
        cpu_sys = cpu.system - self._cpu_sys_before

        # Memory
        self._mem.stop()
        peak_rss = self._mem.peak_rss
        after_rss = self._proc.memory_info().rss

        # Compute derived metrics
        read_amp = (self._bytes_read / self._bytes_useful
                    if self._bytes_useful > 0 and self._bytes_read > 0 else 0.0)
        throughput = (self._bytes_useful / 1e6 / wall_time
                      if wall_time > 0 and self._bytes_useful > 0 else 0.0)

        # Collect phase times
        io_time = self._phase_times.get("io_read", 0.0)
        decomp_time = self._phase_times.get("decompress", 0.0)
        filter_time = self._phase_times.get("filter", 0.0)
        other_time = wall_time - io_time - decomp_time - filter_time

        return QueryMetrics(
            wall_time_s=wall_time,

            io_read_s=io_time,
            decompress_s=decomp_time,
            filter_convert_s=filter_time,
            other_s=max(0.0, other_time),

            cpu_user_s=cpu_user,
            cpu_sys_s=cpu_sys,

            bytes_read=self._bytes_read,
            bytes_useful=self._bytes_useful,
            read_amplification=read_amp,

            mem_rss_before_kb=self._mem_rss_before // 1024,
            mem_rss_peak_kb=peak_rss // 1024,
            mem_rss_after_kb=after_rss // 1024,
            mem_delta_kb=(after_rss - self._mem_rss_before) // 1024,

            throughput_mbps=throughput,
        )
