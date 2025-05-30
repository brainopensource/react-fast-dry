### service/wells_service.py
from ..domain.ports import WellRepoPort
from ..domain.models import WellProductionExternal


class WellService:
    def __init__(self, repo: WellRepoPort):
        self.repo = repo

    def insert(self, records: list[WellProductionExternal]):
        self.repo.insert_many([r.model_dump() for r in records])

    def search(self, name: str):
        return self.repo.search_by_name(name)

    def get(self, code: str, period: str):
        return self.repo.get_by_code_and_period(code, period)