import random
import math
from typing import List, Tuple, Dict
from .base_counter import BaseCounter


class MorrisCounter(BaseCounter):
    """
    Implements the Approximate Counter (Morris Counter).
    Logic: Decreasing Probability 1/2^k.

    Stats:
    - Maintains a counter 'k' for each item.
    - Increment 'k' with probability 1 / (2^k).
    - Estimate n = 2^k - 1.

    This simulates extreme memory compression for large counts.
    """

    def __init__(self) -> None:
        # Maps actor name -> k (exponent)
        self.counts: Dict[str, int] = {}

    def process(self, item: str) -> None:
        if item not in self.counts:
            # Initial state: k=0.
            # Exact count 1 corresponds to k=1 (since 2^1 - 1 = 1).
            # So the first update (from 0 to 1) should happen with prob 1/2^0 = 1.
            self.counts[item] = 0

        k = self.counts[item]

        # Calculate probability P = 1 / 2^k
        probability = 1.0 / (2 ** k)

        # Random trial
        if random.random() < probability:
            self.counts[item] += 1

    def query(self, item: str) -> int:
        k = self.counts.get(item, 0)
        # Formula: 2^k - 1
        return (2 ** k) - 1

    def get_top_n(self, n: int) -> List[Tuple[str, int]]:
        # Retrieve estimates for all items
        estimated_counts = [
            (actor, self.query(actor))
            for actor in self.counts
        ]

        # Sort by estimated count descending
        estimated_counts.sort(key=lambda x: (-x[1], x[0]))

        return estimated_counts[:n]

    def name(self) -> str:
        return "Approximate Counter (Morris 1/2^k)"