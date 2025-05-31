# src/service/base.py
class GenericDatasetService:
    def __init__(self, schema, repo, table):
        self.schema = schema
        self.repo = repo
        self.table = table

    def insert_many(self, items):
        # items: list of schema instances
        dicts = [item.model_dump() for item in items]
        return self.repo.insert_many(self.table, dicts)

    def search_by_name(self, name):
        return self.repo.search_by_name(self.table, name)

    def get_by_code_and_period(self, code, period):
        return self.repo.get_by_code_and_period(self.table, code, period)
