from dbconnect import loadData
from config import FILE_KEY, INDEX_SIZE

db = loadData()

def build_index(number_of_tickers=INDEX_SIZE, dataHandler="parquet"):
    sql = f"""
    Create table index_{str(number_of_tickers)} as
        SELECT Date, 
        AVG(Adj_Close) AS IndexValue,
        STRING_AGG(Ticker, '-' ORDER BY Market_Cap DESC) AS TickerList
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY Date ORDER BY Market_Cap DESC) AS rnk
            FROM {FILE_KEY}
        )
        WHERE rnk <= {str(number_of_tickers)}
        GROUP BY Date
        ORDER BY Date;
    """
    db.sql(sql)


def persistIndex(table_name = f"index_{str(INDEX_SIZE)}", dataHandler="parquet"):
    db.sql(f"copy {table_name} to 'data/index_{str(INDEX_SIZE)}.{dataHandler}' (FORMAT {dataHandler});")

def build_day_over_day_index_increase(index_name = f"index_{str(INDEX_SIZE)}", dataHandler="parquet"):
    db.sql(f"""
    CREATE DoD_Index_Increase AS
    SELECT
        Date,
        IndexValue,
        COALESCE(
            ((IndexValue - LAG(IndexValue) OVER (ORDER BY Date)) 
            / LAG(IndexValue) OVER (ORDER BY Date)) * 100,
            0
        ) AS DoDIncreasePct
        FROM {index_name}
        ORDER BY Date;
    """)

if __name__ == "__main__":
    # print(db.sql(f"select Ticker, count(*), sum(Market_Cap) from {FILE_KEY} group by Ticker order by 3 desc").fetchdf())
    build_index()
    persistIndex()
    df = db.sql("select * from index_100").fetchdf()
    print(df)
    