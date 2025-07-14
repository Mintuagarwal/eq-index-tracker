import duckdb
from config import FILE_KEY, FILE_KEY_FUNC

def loadData(dataHandler = "parquet"):
    db = duckdb.connect()
    db.sql(f"CREATE TABLE {FILE_KEY} AS SELECT * FROM '{FILE_KEY_FUNC(dataHandler)}'")
    return db

