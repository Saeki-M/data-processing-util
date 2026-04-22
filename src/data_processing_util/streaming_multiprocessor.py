import logging
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import Callable, Iterator, TypeVar

from tqdm import tqdm

T = TypeVar("T")

logger = logging.getLogger(__name__)


def split_iterator(dataset: Iterator[T], idx: int, total: int) -> Iterator[T]:
    for i, data in enumerate(dataset):
        if i % total == idx:
            yield data


T = TypeVar("T")
R = TypeVar("R")


def execute_data_processing(
    dataset: Iterator[T] | list[T],
    process_func: Callable[[int, T], R],
    num_workers: int,
    split: str | None = None,
    error_path: str | None = None,
) -> Iterator[R]:
    data_count: int | None = None
    if isinstance(dataset, list):
        data_count = len(dataset)
        dataset = iter(dataset)

    if split is not None:
        idx, total = map(int, split.split("/"))
        assert 1 <= idx <= total, f"Invalid split format: {split}"
        idx -= 1  # 0-based

        if data_count is not None:
            data_count = data_count // total + (1 if idx < data_count % total else 0)

        dataset = split_iterator(dataset, idx, total)

    with (
        ProcessPoolExecutor(max_workers=num_workers) as executor,
        tqdm(total=data_count, desc="Processing data") as progress,
    ):
        futures: dict[Future[R], int] = {}
        next_worker_id = 1

        def submit_one(worker_id: int) -> bool:
            try:
                data = next(dataset)
            except StopIteration:
                return False
            fut: Future[R] = executor.submit(process_func, worker_id, data)
            futures[fut] = worker_id
            return True

        # prime
        for _ in range(num_workers):
            if not submit_one(next_worker_id):
                break
            next_worker_id = next_worker_id % num_workers + 1

        # stream
        while futures:
            completed: Future[R] = next(as_completed(futures))
            worker_id = futures.pop(completed)

            progress.update(1)

            try:
                value: R = completed.result()
            except Exception as e:
                logger.exception("Error in worker %s", worker_id)
                if error_path is not None:
                    with open(error_path, "a") as f:
                        f.write(f"Error in worker {worker_id}: {e}\n")
                # IMPORTANT: don't yield on failure
            else:
                yield value

            # refill this worker slot
            submit_one(worker_id)
