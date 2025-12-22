from typing import List, Tuple, Dict
from .base_counter import BaseCounter


class FrequentCounter(BaseCounter):
    """
    Implements the Frequent-Count algorithm.

    Strategy:
    - Maintains at most 'capacity' counters (k).
    - If a new item arrives and memory is full, ALL counters are decremented.
    - Items with count 0 are removed.

    This identifies heavy hitters in the stream with limited memory.
    """

    def __init__(self, capacity: int = 100, **kwargs) -> None:
        self.capacity = capacity
        self.counts: Dict[str, int] = {}

    def process(self, item: str) -> None:
        # CASE 1: Item already monitored
        if item in self.counts:
            self.counts[item] += 1
            return

        # CASE 2: Item new, but we have space
        if len(self.counts) < self.capacity:
            self.counts[item] = 1
            return

        # CASE 3: Item new and memory full (The Decrement Step)
        # We must decrement all existing counters.
        # We collect keys to remove to avoid 'dictionary changed size during iteration' error.
        items_to_remove = []

        for key in self.counts:
            self.counts[key] -= 1
            if self.counts[key] == 0:
                items_to_remove.append(key)

        for key in items_to_remove:
            del self.counts[key]

    def query(self, item: str) -> int:
        # Returns the count if in map, else 0.
        return self.counts.get(item, 0)

    def get_top_n(self, n: int) -> List[Tuple[str, int]]:
        # Sort current counters by frequency
        sorted_items = sorted(
            self.counts.items(),
            key=lambda x: (-x[1], x[0])
        )
        return sorted_items[:n]

    def name(self) -> str:
        return f"Frequent-Count (Capacity k={self.capacity})"