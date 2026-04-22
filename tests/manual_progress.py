#!/usr/bin/env python3
"""
Manual test script to demonstrate streaming_multiprocessor and DataPrefetcher.
Run with: uv run tests/manual_progress.py
"""

import time

from data_processing_util.preprocessor import prefetch_data
from data_processing_util.streaming_multiprocessor import execute_data_processing

NUM_ITEMS = 20
NUM_WORKERS = 4


def slow_source():
    """Simulates a slow I/O data source (e.g. reading from disk)."""
    for i in range(NUM_ITEMS):
        time.sleep(0.05)  # slow I/O
        yield i


def process(worker_id: int, data: int) -> int:
    """Simulates CPU/GPU work."""
    time.sleep(0.1)
    return data * 2


def flaky_process(worker_id: int, data: int) -> int:
    if data % 5 == 0:
        raise ValueError(f"Simulated failure on item {data}")
    time.sleep(0.05)
    return data * 2


def main():
    print("=== Example 1: plain list input ===")
    results = list(
        execute_data_processing(
            dataset=list(range(NUM_ITEMS)),
            process_func=process,
            num_workers=NUM_WORKERS,
        )
    )
    print(f"Got {len(results)} results. Sample: {sorted(results)[:5]}\n")

    print("=== Example 2: prefetcher reduces idle time for slow I/O ===")
    # Without prefetch: I/O and processing are sequential
    t0 = time.time()
    for item in slow_source():
        time.sleep(0.05)  # simulate processing
    print(
        f"Without prefetch: {time.time() - t0:.1f}s  (I/O + processing fully sequential)"
    )

    # With prefetch: I/O runs in background thread while processing runs in foreground
    t0 = time.time()
    for item in prefetch_data(slow_source(), buffer_size=4):
        time.sleep(0.05)  # simulate processing
    print(
        f"With prefetch:    {time.time() - t0:.1f}s  (I/O and processing overlapped)\n"
    )

    print("=== Example 3: split (processing only first half) ===")
    results = list(
        execute_data_processing(
            dataset=list(range(NUM_ITEMS)),
            process_func=process,
            num_workers=NUM_WORKERS,
            split="1/2",
        )
    )
    print(
        f"Got {len(results)} results (expected {NUM_ITEMS // 2}). Sample: {sorted(results)[:5]}\n"
    )

    print("=== Example 4: error handling ===")
    results = list(
        execute_data_processing(
            dataset=list(range(NUM_ITEMS)),
            process_func=flaky_process,
            num_workers=NUM_WORKERS,
            error_path="/tmp/manual_progress_errors.log",
        )
    )
    print(
        f"Got {len(results)} results (failures skipped). Check /tmp/manual_progress_errors.log for errors.\n"
    )


if __name__ == "__main__":
    main()
