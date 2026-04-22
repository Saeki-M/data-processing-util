import logging
import queue
import threading
from typing import Generic, Iterator, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)


class DataPrefetcher(Generic[T]):
    """
    Wraps an iterator and prefetches items into a queue in a background thread,
    so the main thread is never blocked waiting for I/O.

    Can be used as a plain iterator or as a context manager:

        for item in DataPrefetcher(data, buffer_size=5):
            ...

        with DataPrefetcher(data, buffer_size=5) as p:
            for item in p:
                ...
    """

    def __init__(
        self,
        data_source: Iterator[T],
        buffer_size: int = 10,
        timeout: float = 10.0,
    ):
        self.data_source = iter(data_source)
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.queue: queue.Queue[T | None] = queue.Queue(maxsize=buffer_size)
        self.prefetch_thread: threading.Thread | None = None
        self.exception: Exception | None = None
        self.stop_event = threading.Event()

    def _prefetch_worker(self):
        try:
            for item in self.data_source:
                while True:
                    if self.stop_event.is_set():
                        return
                    try:
                        self.queue.put(item, timeout=1.0)
                        break
                    except queue.Full:
                        continue
        except Exception as e:
            logger.exception("Error in prefetch worker")
            self.exception = e
        finally:
            if not self.stop_event.is_set():
                while True:
                    if self.stop_event.is_set():
                        break
                    try:
                        self.queue.put(None, timeout=1.0)
                        break
                    except queue.Full:
                        continue

    def start(self):
        if self.prefetch_thread is not None:
            raise RuntimeError("Prefetcher already started")
        self.prefetch_thread = threading.Thread(
            target=self._prefetch_worker,
            daemon=True,
            name="DataPrefetcher",
        )
        self.prefetch_thread.start()

    def __iter__(self):
        if self.prefetch_thread is None:
            self.start()
        return self

    def __next__(self) -> T:
        if self.prefetch_thread is None:
            self.start()

        try:
            item = self.queue.get(timeout=self.timeout)
        except queue.Empty:
            if self.exception is not None:
                raise self.exception
            raise RuntimeError("Prefetch queue timeout - worker may have stalled")

        if item is None:
            if self.exception is not None:
                raise self.exception
            raise StopIteration

        return item

    def stop(self):
        self.stop_event.set()
        if self.prefetch_thread is not None:
            self.prefetch_thread.join(timeout=self.timeout)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


def prefetch_data(
    data_source: Iterator[T],
    buffer_size: int = 10,
    timeout: float = 10.0,
) -> DataPrefetcher[T]:
    return DataPrefetcher(data_source, buffer_size=buffer_size, timeout=timeout)
