# Data Processing Util

A Python utility library for efficient multi-worker data processing with progress tracking and error handling.

## Usage

### Basic Example

```python
from data_processing_util.multi_gpu_processor import execute_data_processing

# Define your data class
class Data:
    def __init__(self, id, content):
        self.id = id
        self.content = content

# Define your processing function
def process_func(data):
    # Your processing logic here
    result = data.content.upper()
    return result

# Create a dataset iterator
def dataset_generator():
    for i in range(100):
        yield Data(i, f"content_{i}")

# Execute processing
execute_data_processing(
    dataset=dataset_generator(),
    process_func=process_func,
    num_workers=4,
)
```

### Advanced Example with Worker Initialization

```python
from data_processing_util.multi_gpu_processor import execute_data_processing

# Define a worker initialization function
def init_worker(worker_id: int):
    # Initialize resources for each worker
    # e.g., load models, set up GPU, etc.
    print(f"Initializing worker {worker_id}")
    # Your initialization logic here

# Define your processing function
def process_func(data):
    # Process data using initialized resources
    return processed_data

# Execute with custom initialization
execute_data_processing(
    dataset=dataset_generator(),
    process_func=process_func,
    num_workers=4,
    worker_init_func=init_worker,
    data_count=1000,
    error_path="errors.log"  # Log errors to a file
)
```
