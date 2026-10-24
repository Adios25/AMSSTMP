from abc import ABC, abstractmethod
from typing import List
from src.ingestion.schemas import RawFeedItem

class BaseFetcher(ABC):
    """
    Abstract Base Class for all external feed producers.
    """
    @abstractmethod
    def fetch(self, ticker: str, limit: int = 10) -> List[RawFeedItem]:
        """Fetch and return a list of validated RawFeedItem objects."""
        pass
