import pytest

from data_processing_util.preprocessor import DataPrefetcher, prefetch_data


def test_basic_iteration():
    data = list(range(10))
    result = list(DataPrefetcher(iter(data)))
    assert result == data


def test_context_manager_with_for_loop():
    """__enter__ + for loop should not double-call start()."""
    data = list(range(5))
    result = []
    with DataPrefetcher(iter(data)) as p:
        for item in p:
            result.append(item)
    assert result == data


def test_prefetch_data_convenience():
    data = list(range(7))
    result = list(prefetch_data(iter(data)))
    assert result == data


def test_exception_propagates():
    def faulty():
        yield 1
        yield 2
        raise ValueError("bad data")

    with pytest.raises(ValueError, match="bad data"):
        list(DataPrefetcher(faulty()))


def test_stop_early_does_not_hang():
    data = list(range(100))
    p = DataPrefetcher(iter(data), buffer_size=2)
    p.start()
    _ = next(p)
    p.stop()  # should return promptly, not hang
