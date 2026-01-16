import logging
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import Any, Callable, Iterator, TypeVar

from tqdm import tqdm

T = TypeVar("T")

logger = logging.getLogger(__name__)


def split_iterator(dataset: Iterator[T], idx: int, total: int) -> Iterator[T]:
    for i, data in enumerate(dataset):
        if i % total == idx:
            yield data


def execute_data_processing(
    dataset: Iterator[T] | list[T],
    process_func: Callable[[int, T], Any],
    num_workers: int,
    split: str | None = None,
    error_path: str | None = None,
):
    data_count = None
    if isinstance(dataset, list):
        data_count = len(dataset)
        dataset = iter(dataset)

    if split is not None:
        idx, total = map(int, split.split("/"))
        assert 1 <= idx <= total, f"Invalid split format: {split}"
        idx -= 1  # Convert to 0-based index

        if data_count is not None:
            data_count = data_count // total + (1 if idx < data_count % total else 0)

        dataset = split_iterator(dataset, idx, total)

    with (
        ProcessPoolExecutor(max_workers=num_workers) as executor,
        tqdm(total=data_count, desc="Processing data") as pbar,
    ):
        # Track which worker is handling which future
        futures: dict[Future[Any], int] = {}
        # Track next worker_id to assign (round-robin, starting from 1)
        next_worker_id = 1

        # Submit initial jobs for each worker
        for _ in range(num_workers):
            try:
                data = next(dataset)
                future = executor.submit(process_func, next_worker_id, data)
                futures[future] = next_worker_id
                next_worker_id = next_worker_id % num_workers + 1
            except StopIteration:
                break

        # Process results as they complete and submit new jobs
        while futures:
            # Wait for at least one future to complete
            complete_job = as_completed(futures).__next__()
            worker_id = futures[complete_job]

            # Update progress
            pbar.update()

            try:
                complete_job.result()  # Raise exception if any
            except Exception as e:
                logger.exception("Error in worker")
                if error_path is not None:
                    with open(error_path, "a") as f:
                        f.write(f"Error in worker: {e}\n")

            del futures[complete_job]

            try:
                data = next(dataset)
                new_future = executor.submit(process_func, worker_id, data)
                futures[new_future] = worker_id
            except StopIteration:
                # No more data to process
                pass
