import duckdb
import pandas as pd
from src.utils.sql_loader import load_sql

class DuckDBRepo:
    def __init__(self, db_path: str, sql_path: str):
        self.conn = duckdb.connect(db_path)
        self.queries = load_sql(sql_path)
        self._init_table()

    def _init_table(self):
        self.conn.execute(self.queries["init_table"])

    def insert(self, df: pd.DataFrame, table: str):
        self.conn.execute(f"INSERT INTO {table} SELECT * FROM df", {'df': df})

    def insert_many(self, table: str, dicts: list):
        df = pd.DataFrame(dicts)
        self.insert(df, table)
        return len(df)

    def search_by_name(self, name: str):
        return self.conn.execute(self.queries["search_by_name"], {'name': name}).fetchdf()

    def get_by_code_and_period(self, code: str, period: str):
        return self.conn.execute(self.queries["get_by_code_and_period"], {'code': code, 'period': period}).fetchdf()
