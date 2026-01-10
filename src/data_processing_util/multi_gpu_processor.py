import logging
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from enum import Enum
from typing import Any, Callable, Iterator

from tqdm import tqdm

logger = logging.getLogger(__name__)


def _init_worker(worker_id: int):
    """Repalce with actual initialization logic if needed"""
    pass


class JobType(Enum):
    INIT = 1
    PROCESS = 2


def execute_data_processing(
    dataset: Iterator[Any],
    process_func: Callable[[Any], Any],
    num_workers: int,
    worker_init_func: Callable[[int], Any] = _init_worker,
    data_count: int | None = None,
    error_path: str | None = None,
):
    with (
        ProcessPoolExecutor(max_workers=num_workers) as executor,
        tqdm(total=data_count, desc="Processing data") as pbar,
    ):
        # Submit initialization and initial data jobs
        futures: dict[Future[Any], JobType] = {}
        for worker_id in range(num_workers):
            # Submit worker initialization (marked with None)
            init_future = executor.submit(worker_init_func, worker_id)
            futures[init_future] = JobType.INIT
        # Process results as they complete and submit new jobs
        while futures:
            # Wait for at least one future to complete
            complete_job = as_completed(futures).__next__()

            if futures[complete_job] == JobType.INIT:
                # init job must not fail
                complete_job.result()
            else:
                # update progress for data jobs
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
                new_future = executor.submit(process_func, data)
                futures[new_future] = JobType.PROCESS
            except StopIteration:
                # No more data to process
                pass
