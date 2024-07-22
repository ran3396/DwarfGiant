import json
from typing import List, Tuple
import random
from multiprocessing import Pool, cpu_count


class DwarfGiant:
    def __init__(self, dwarf_giant_data_path: str = 'data.json'):
        self.data_sanitized: List[dict] = self._remove_duplicates(self._read_json(dwarf_giant_data_path))

    def _read_json(self, path: str) -> List[dict]:
        # Read the JSON file
        try:
            with open(path, "r") as file:
                data = json.load(file)
                return data
        except (OSError, json.JSONDecodeError) as e:
            print(f"Could not handle the file: {path} - {e}")
            return []

    def _remove_duplicates(self, data: List[dict]) -> List[dict]:
        # Remove duplicates from the data
        try:
            set_of_frozensets = {frozenset(d.items()) for d in data}
            unique_dicts = [dict(f) for f in set_of_frozensets]
            return unique_dicts
        except (AttributeError, TypeError) as e:
            print(f"Data is not in the correct format - {e}")
            return []

    def create_pairs(self, multiprocessing: bool = True) -> List[Tuple[str, str]]:
        # Split the data into chunks by the number of CPUs and create pairs for each chunk.
        # Finally, create a list of pairs.
        if not multiprocessing:
            return self._create_pairs_chunk(self.data_sanitized)
        data_chunks = self._split_data(self.data_sanitized, cpu_count())
        with Pool(cpu_count()) as pool:
            result_chunks = pool.map(self._create_pairs_chunk, data_chunks)
        pairs = [pair for chunk in result_chunks for pair in chunk]
        return pairs

    def _create_pairs_chunk(self, data_chunk: List[dict]) -> List[Tuple[str, str]]:
        # Creates a list of tuple pairs of the data for each (dwarf, giant) - (name1, name2), (name3, name1)...
        # Each name should be a dwarf one time and a giant one time. The dwarf and the giant selection is random.
        pairs = []
        random.shuffle(data_chunk)
        for i in range(len(data_chunk)):
            if i == len(data_chunk) - 1:
                pairs.append((data_chunk[i]["name"], data_chunk[0]["name"]))
            else:
                pairs.append((data_chunk[i]["name"], data_chunk[i + 1]["name"]))
        return pairs

    def _split_data(self, data: List[dict], chunks_number: int) -> List[List[dict]]:
        # Split the data into chunks and create a list of chunks.
        if not data:
            return []

        if chunks_number >= len(data):
            return [[item] for item in data]

        chunk_size = len(data) / chunks_number
        chunk_list = []
        last = 0

        while last < len(data):
            chunk_list.append(data[int(last):int(last + chunk_size)])
            last += chunk_size

        return chunk_list


if __name__ == "__main__":

    dwarf_giant = DwarfGiant('data.json')
    pairs = dwarf_giant.create_pairs()
    print(pairs)
