from dbconnect import loadData
from config import FILE_KEY, INDEX_SIZE

db = loadData()

def build_index(number_of_tickers=INDEX_SIZE, dataHandler="parquet"):
    """
    Build an equal-weighted index based on the top N stocks by market capitalization

    Parameters
    ----------
    number_of_tickers : int, optional
        The number of stocks to include in the index. Defaults to INDEX_SIZE.
    dataHandler : str, optional
        The type of data handler to use. Defaults to "parquet".

    Returns
    -------
    None

    Notes
    -----
    The table created will have the following columns:
        Date: The date of the value
        IndexValue: The value of the index
        TickerList: The list of tickers for the index
    """

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

def buildDayOverDayIndexDelta(index_name = f"index_{str(INDEX_SIZE)}"):
    """
    Build a table that shows the day-over-day increase of the index value
    
    The table will have the following columns:
        Date: The date of the value
        IndexValue: The value of the index
        DoDIncreasePct: The percentage increase of the index value from the previous day
        TickerList: The list of tickers for the index
    """
    db.sql(f"""
    CREATE Table DoD_Index_Increase AS
    SELECT
        Date,
        IndexValue,
        COALESCE(
            ((IndexValue - LAG(IndexValue) OVER (ORDER BY Date)) 
            / LAG(IndexValue) OVER (ORDER BY Date)) * 100,
            0
        ) AS DoDIncreasePct,
        TickerList
        FROM {index_name}
        ORDER BY Date;
    """)

if __name__ == "__main__":
    # print(db.sql(f"select Ticker, count(*), sum(Market_Cap) from {FILE_KEY} group by Ticker order by 3 desc").fetchdf())
    build_index()
    persistIndex()
    buildDayOverDayIndexDelta()

    import pandas as pd
    pd.set_option('display.max_columns', None)   # show all columns
    pd.set_option('display.width', None)         # don't wrap columns
    pd.set_option('display.max_colwidth', None)
    df = db.sql("select Date, IndexValue, DoDIncreasePct from DoD_Index_Increase").fetchdf()
    print(df)
