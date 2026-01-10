import os
import tempfile

from data_processing_util.multi_gpu_processor import execute_data_processing


# Define Data class at module level so it can be pickled for multiprocessing
class Data:
    def __init__(self, id):
        self.id = id


# Define processing function at module level so it can be pickled for multiprocessing
def process_func(data):
    return data.id


def dataset_generator(data_count):
    for i in range(data_count):
        yield Data(i)


def init_worker(worker_id: int):
    pass


# Module-level tracked functions for testing (use environment variables for file paths)
def tracked_init_worker(worker_id: int):
    init_log_path = os.environ.get("TEST_INIT_LOG_PATH")
    if init_log_path is not None:
        with open(init_log_path, "a") as f:
            f.write(f"{worker_id}\n")


def tracked_process_func(data):
    process_log_path = os.environ.get("TEST_PROCESS_LOG_PATH")
    if process_log_path is not None:
        with open(process_log_path, "a") as f:
            f.write(f"{data.id}\n")
    return data.id


def test_execute_data_processing(data_count=10, num_workers=2):
    with (
        tempfile.NamedTemporaryFile(mode="w", suffix=".log") as init_log,
        tempfile.NamedTemporaryFile(mode="w", suffix=".log") as process_log,
    ):
        # Set environment variables for child processes
        os.environ["TEST_INIT_LOG_PATH"] = init_log.name
        os.environ["TEST_PROCESS_LOG_PATH"] = process_log.name

        dataset = dataset_generator(data_count)
        execute_data_processing(
            dataset=dataset,
            process_func=tracked_process_func,
            num_workers=num_workers,
            worker_init_func=tracked_init_worker,
            data_count=data_count,
        )

        # Assert init_worker was called for each worker
        with open(init_log.name, "r") as f:
            worker_ids = set(int(line.strip()) for line in f)
        assert worker_ids == set(range(num_workers))

        # Assert process_func was called for each data item
        with open(process_log.name, "r") as f:
            processed_ids = set(int(line.strip()) for line in f)
        assert processed_ids == set(range(data_count))


def faulty_process_func(data):
    if data.id == 3:
        raise ValueError("Dummy error for testing")
    return data.id


def test_execute_data_processing_error_handling(data_count=5):
    dataset = dataset_generator(data_count)

    # Create a temporary file for error logging
    with tempfile.NamedTemporaryFile(mode="w", suffix=".log") as f:
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
            assert "Dummy error for testing" in f.read()


if __name__ == "__main__":
    test_execute_data_processing_error_handling()
