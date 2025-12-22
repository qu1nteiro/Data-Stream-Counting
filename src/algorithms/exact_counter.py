from typing import List, Tuple, Dict
from .base_counter import BaseCounter


class ExactCounter(BaseCounter):
    """
    Implements a deterministic counter using a Hash Map (Dictionary).

    Memory Complexity: O(N * L), where N is unique items and L is avg string length.
    Time Complexity (Process): O(1).
    Time Complexity (Query): O(1).

    Role in Project: TRUTH worthy results for accuracy benchmarking.
    """

    def __init__(self) -> None:
        # Internal storage: Maps actor name -> integer count
        self.counts: Dict[str, int] = {}

    def process(self, item: str) -> None:
        if item in self.counts:
            self.counts[item] += 1
        else:
            self.counts[item] = 1

    def query(self, item: str) -> int:
        # Returns 0 if the item was never seen
        return self.counts.get(item, 0)

    def get_top_n(self, n: int) -> List[Tuple[str, int]]:
        # Sorting is expensive: O(M log M) where M is unique items.
        # We assume this is only called at the end of the stream for analysis.

        # Sort by value (count) descending, then by key (name) alphabetically for tie-breaking stability
        sorted_items = sorted(
            self.counts.items(),
            key=lambda x: (-x[1], x[0])
        )
        return sorted_items[:n]

    def name(self) -> str:
        return "Exact Counter (Ground Truth)"