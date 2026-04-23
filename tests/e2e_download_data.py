import time
from typing import Iterator

import httpx

from data_processing_util.preprocessor import prefetch_data


def download_pokemon(client, pokemon_id) -> dict:
    """Download data for a single Pokémon by ID."""
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_id}"
    response = client.get(url)
    response.raise_for_status()
    return response.json()


def data_generator() -> Iterator[dict]:
    with httpx.Client(timeout=30.0) as client:
        for pokemon_id in range(1, 101):
            try:
                data = download_pokemon(client, pokemon_id)
                yield data
            except httpx.HTTPError:
                pass


def pre_fetcher():
    for item in prefetch_data(data_generator(), buffer_size=10):
        yield item


def main():
    start_time = time.time()
    for d in data_generator():
        time.sleep(0.01)
    elapsed_sync = time.time() - start_time

    start_time = time.time()
    for f in pre_fetcher():
        time.sleep(0.01)
    elapsed_prefetch = time.time() - start_time

    print(f"Elapsed time without prefetching: {elapsed_sync:.2f} seconds")
    print(f"Elapsed time with prefetching: {elapsed_prefetch:.2f} seconds")
    assert elapsed_prefetch < elapsed_sync - 1


if __name__ == "__main__":
    main()
