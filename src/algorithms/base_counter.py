from abc import ABC, abstractmethod
from typing import List, Tuple, Any


class BaseCounter(ABC):
    """
    Interface for all counting algorithms.
    Enforces a strict "contract" for:
    1. Exact Counters
    2. Probabilistic Counters (Morris / 1/2^k)
    3. Frequent Item Sketches (Misra-Gries, etc.)
    """

    @abstractmethod
    def __init__(self, **kwargs: Any) -> None:
        """
        Initialize the counter. Parameters depend on the specific implementation
        (e.g., probability factor, buffer size).
        """
        pass

    @abstractmethod
    def process(self, item: str) -> None:
        """
        Process a single item from the data stream.
        This is the 'update' step in streaming terminology.
        """
        pass

    @abstractmethod
    def query(self, item: str) -> int:
        """
        Return the estimated (or exact) count for a specific item.
        """
        pass

    @abstractmethod
    def get_top_n(self, n: int) -> List[Tuple[str, int]]:
        """
        Return the top N most frequent items as a list of (item, count) tuples.
        Sorted by count in descending order.
        """
        pass

    @abstractmethod
    def name(self) -> str:
        """
        Returns the display name of the algorithm for reporting purposes.
        """
        pass