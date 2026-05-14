"""Benchmark configuration."""

import os

BENCHMARK_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BENCHMARK_DIR, "data")
RESULT_DIR = os.path.join(BENCHMARK_DIR, "results")

# Data generation parameters
NUM_DEVICES = 30
MEASUREMENTS_PER_DEVICE = 15
DURATION_DAYS = 10
INTERVAL_SECONDS = 2  # 2-second sampling
SEED = 42
CHUNK_SIZE = 500_000  # rows per CSV chunk during generation

# Query benchmark parameters
COLUMN_SELECTIVITIES = [0.07, 0.2, 0.5, 1.0]  # fraction of columns requested
SAMPLING_RATES = [1, 10, 100, 500]  # take 1 every N points
TIME_RANGES_DAYS = [0.01, 1, 10]  # ~15min, 1 day, full range
RANDOM_WINDOW_COUNT = 500
RANDOM_WINDOW_LENGTH = 512

# Output
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)
