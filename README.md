# Data Processing Util

A Python utility library for efficient multi-worker data processing with streaming results, progress tracking, and error handling.

## Components

- **`execute_data_processing`** — distributes work across a pool of processes and streams results back as a generator.
- **`DataPrefetcher` / `prefetch_data`** — reads from a data source in a background thread to keep workers fed without blocking on I/O.

## Usage

### Basic multi-worker processing

```python
from data_processing_util.streaming_multiprocessor import execute_data_processing

def dataset_generator():
    for i in range(1000):
        yield i

def process(worker_id: int, item: int) -> str:
    return f"worker={worker_id} item={item}"

for result in execute_data_processing(
    dataset=dataset_generator(),
    process_func=process,
    num_workers=4,
):
    print(result)
```

`process_func` receives a 1-based `worker_id` and one item from the dataset. Results are yielded as each worker finishes, so you can pipeline downstream work.

### With I/O prefetching

Wrap a slow data source (disk, network) in `prefetch_data` to read ahead in a background thread while workers are busy:

```python
from data_processing_util.preprocessor import prefetch_data
from data_processing_util.streaming_multiprocessor import execute_data_processing

def load_data():
    for path in file_paths:
        yield read_file(path)  # slow I/O

def process(worker_id: int, item) -> dict:
    return run_model(item)

for result in execute_data_processing(
    dataset=prefetch_data(load_data(), buffer_size=20),
    process_func=process,
    num_workers=8,
):
    save(result)
```

### Processing a subset (`split`)

Run multiple instances in parallel on non-overlapping slices of the dataset:

```python
# instance 0: --split 1/4
# instance 1: --split 2/4  ... etc.
for result in execute_data_processing(
    dataset=dataset_generator(),
    process_func=process,
    num_workers=4,
    split="1/4",
):
    ...
```

### Error logging

Failed items are logged and skipped; processing continues:

```python
for result in execute_data_processing(
    dataset=dataset_generator(),
    process_func=process,
    num_workers=4,
    error_path="errors.log",
):
    ...
```
