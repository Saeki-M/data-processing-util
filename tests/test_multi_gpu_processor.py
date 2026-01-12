import os
import tempfile
from dataclasses import dataclass
from functools import partial

from data_processing_util.streaming_multiprocessor import execute_data_processing


@dataclass
class Data:
    id: int


def dataset_generator(data_count: int):
    for i in range(data_count):
        yield Data(i)


def tracked_process_func(worker_data: tuple[int, Data], worker_log_path: str):
    worker_id, data = worker_data

    # Log worker_id and data for testing
    with open(worker_log_path, "a") as f:
        f.write(f"{worker_id},{data.id}\n")

    return data.id


def test_execute_data_processing(data_count: int = 10, num_workers: int = 2):
    dataset = dataset_generator(data_count)
    with tempfile.NamedTemporaryFile(mode="w") as worker_log:
        execute_data_processing(
            dataset=dataset,
            process_func=partial(tracked_process_func, worker_log_path=worker_log.name),
            num_workers=num_workers,
            data_count=data_count,
        )

        # Read worker_id and data_id pairs
        with open(worker_log.name, "r") as f:
            worker_data_pairs = [line.strip().split(",") for line in f if line.strip()]
            worker_ids = set(int(pair[0]) for pair in worker_data_pairs)
            processed_ids = set(int(pair[1]) for pair in worker_data_pairs)

    # verify all worker IDs were used and all data IDs were processed
    assert worker_ids == set(range(1, num_workers + 1)), "unexpected worker IDs"
    assert processed_ids == set(range(data_count)), "unprocessed data IDs"


def faulty_process_func(worker_data: tuple[int, Data]):
    _worker_id, data = worker_data
    if data.id == 3:
        raise ValueError("Dummy error for testing")
    return data.id


def test_execute_data_processing_error_handling(data_count: int = 5):
    dataset = dataset_generator(data_count)
    with tempfile.NamedTemporaryFile(mode="w") as f:
        execute_data_processing(
            dataset=dataset,
            process_func=faulty_process_func,
            num_workers=2,
            data_count=data_count,
            error_path=f.name,
        )

        # Verify that error log was written
        assert os.path.exists(f.name), "Error log file should exist"
        with open(f.name, "r") as f:
            content = f.read()
            assert "Dummy error for testing" in content, "incorrect error log content"
