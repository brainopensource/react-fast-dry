### infra/duckdb_repo.py
import duckdb
import pandas as pd
from ..utils.sql_loader import load_sql
from ..domain.ports import WellRepoPort


class DuckDBWellRepo(WellRepoPort):
    def __init__(self, db_path: str, sql_path: str):
        self.db_path = db_path
        self.sql = load_sql(sql_path)
        self._init_table()

    def _init_table(self):
        with duckdb.connect(self.db_path) as con:
            con.execute(self.sql['init'])

    def insert_many(self, records: list[dict]):
        df = pd.DataFrame(records)
        with duckdb.connect(self.db_path) as con:
            con.register("temp_df", df)
            con.execute(self.sql['insert'])

    def search_by_name(self, name: str) -> pd.DataFrame:
        with duckdb.connect(self.db_path) as con:
            return con.execute(self.sql['search'], {'name': f'%{name}%'}).df()

    def get_by_code_and_period(self, code: str, period: str) -> pd.DataFrame:
        with duckdb.connect(self.db_path) as con:
            return con.execute(self.sql['get_by_code_period'], {"code": code, "period": period}).df()
