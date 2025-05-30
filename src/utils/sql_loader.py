### utils/sql_loader.py
def load_sql(path: str):
    from pathlib import Path
    blocks = Path(path).read_text().split("-- name: ")
    return {b.split("\n", 1)[0]: b.split("\n", 1)[1] for b in blocks if b}
