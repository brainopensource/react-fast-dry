### service/wells_service.py
from ..domain.ports import WellRepoPort
from ..domain.models import WellProduction

class WellService:
    def __init__(self, repo: WellRepoPort):
        self.repo = repo

    def insert(self, records: list[WellProduction]):
        self.repo.insert_many([r.dict() for r in records])

    def search(self, name: str):
        return self.repo.search_by_name(name)

    def get(self, code: str, period: str):
        return self.repo.get_by_code_and_period(code, period)