### domain/ports.py
from abc import ABC, abstractmethod
import pandas as pd


class WellRepoPort(ABC):
    @abstractmethod
    def insert_many(self, records: list[dict]): ...

    @abstractmethod
    def search_by_name(self, name: str) -> pd.DataFrame: ...

    @abstractmethod
    def get_by_code_and_period(self, code: str, period: str) -> pd.DataFrame: ...

class CsvExporterPort:
    def export(self, data: list, file_path: str) -> None:
        raise NotImplementedError
