import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
import concurrent.futures
import sys
import os
import requests
from bs4 import BeautifulSoup
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import duckdb
# Constants
from config import NUM_DAYS, NUM_TOP_STOCKS, INDEX_UNIVERSE, US_INDEX, PARTITION_FOLDER, URL_MAP
LIST_OF_COLS = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
db = duckdb.connect()

def getTradingDays(endDate=None, num_days=NUM_DAYS):
    """Get the date index from yfinance API, business days"""
    if endDate is None:
        endDate = datetime.now().date()
    startDate = endDate - timedelta(days=num_days * 2)  # Buffer for weekends/holidays
    df = yf.download(US_INDEX, start=startDate, end=endDate)
    return df.index[-num_days:]

def getTopUsStocksByMarketCap(index="sp500",limit=NUM_TOP_STOCKS):
    """Get the top US stocks by market cap
    Fetch top tickers from Slickcharts (S&P 500 or Nasdaq-100).
    Valid values for `index`: "sp500", "nasdaq100"
    """
    if index not in URL_MAP:
        raise ValueError("Index must be 'sp500' or 'nasdaq100'")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0 Safari/537.36"
    }

    response = requests.get(URL_MAP[index], headers=headers)
    response.raise_for_status()

    # Use BeautifulSoup to parse HTML
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    df = pd.read_html(str(table))[0]
    
    df = df[["Symbol", "Company", "Weight", "Price"]].head(limit)
    return df["Symbol"].tolist()

def fetchStockDataSafe(ticker, startDate, endDate):
    """
    Fetch historical stock data for a given ticker and date range, with error handling.

    Parameters
    ----------
    ticker : str
        The ticker symbol of the stock to fetch.
    startDate : datetime.date
        The start date of the date range to fetch.
    endDate : datetime.date
        The end date of the date range to fetch.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the fetched data, with columns Date, Open, High, Low, Close, Volume, and Ticker.
        If an error occurs while fetching the data, returns None.
    """
    try:
        hist = yf.download(ticker, start=startDate, end=endDate)
        hist.reset_index(inplace=True)
        hist["Ticker"] = ticker
        col_set = set()
        list_of_cols = []
        for col in hist.columns:
            if isinstance(col, tuple):
                col = col[0]
            if col not in col_set:
                col_set.add(col)
                list_of_cols.append(col)
        if set(list_of_cols) != set(LIST_OF_COLS):
            logging.info(f"list_of_cols is {list_of_cols}, expected: {LIST_OF_COLS}")
        hist.columns = list_of_cols
        shares_out = yf.Ticker(ticker).info.get("sharesOutstanding", 0) or 1
        if 'Adj Close' not in hist.columns:
            hist['Adj_Close'] = hist['Close']
        else:
            hist.rename(columns={'Adj Close': 'Adj_Close'}, inplace=True)
        hist["Market_Cap"] = hist["Adj_Close"] * shares_out
        return hist
    except Exception as e:
        logger.info(f"Error fetching {ticker}: {e}")
        return None

def fetchIndexBaseData(tickers, tradingDays, dataHandler="csv"):
    """
    Fetch historical stock data for a given list of tickers and date range, and return a single DataFrame with all the data.

    Parameters
    ----------
    tickers : list of str
        A list of ticker symbols to fetch.
    tradingDays : list of datetime.date
        A list of trading days to fetch data for.
    dataHandler : str, optional
        The type of data handler to use. Defaults to "csv".

    Returns
    -------
    pd.DataFrame or the SQL expression for dataHandler - duckdb to persist as parquet or None
        A DataFrame containing the fetched data, with columns Date, Open, High, Low, Close, Volume, and Ticker.
        If an error occurs while fetching the data, returns None.
    """
    import concurrent.futures
    dataframes = []
    startDate = tradingDays[0]
    endDate = tradingDays[-1] + timedelta(days=1)
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetchStockDataSafe, ticker, startDate, endDate) for ticker in tickers]
        for future in concurrent.futures.as_completed(futures):
            df = future.result()
            if df is not None:
                dataframes.append(df)
    if dataframes:
        col_len = len(dataframes[0].columns)
        for df in dataframes[1:]:
            if col_len != len(df.columns):
                print(df.columns)
                print(dataframes[0].columns)
                print(df.loc[0, "Ticker"])
                raise ValueError("All dataframes must have the same number of columns")
        for num, df in enumerate(dataframes):
            db.sql(f"create table view_{num} as select * from df")
        db.sql("SHOW TABLES;")
        rel_expression = " UNION ALL ".join([f"select * from view_{num}" for num in range(len(dataframes))])
        if dataHandler == "duckDb":
            return rel_expression
        else:
            return db.sql(rel_expression).fetchdf()
    else:
        return None

if __name__ == "__main__":
    tradingDays = getTradingDays(endDate=datetime.now().date()-timedelta(days=2))
    tickers = getTopUsStocksByMarketCap()
    mode = "duckDb" # possible values of ('csv', 'duckDb')
    data = fetchIndexBaseData(tickers, tradingDays, dataHandler=mode)
    if mode == "duckDb":
        db.execute("COPY ({}) TO 'data/stock_market_data.parquet' (FORMAT PARQUET);".format(data))
    else:
        data.to_csv("data/stock_market_data.csv", index=False)
    logger.info("Data saved in data folder")
