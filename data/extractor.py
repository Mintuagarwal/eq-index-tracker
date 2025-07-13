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
    try:
        hist = yf.download(ticker, start=startDate, end=endDate)
        hist.reset_index(inplace=True)
        hist["Ticker"] = ticker
        list_of_cols = list(set([col[0] if isinstance(col, tuple) else col for col in hist.columns]))
        if set(list_of_cols) != set(LIST_OF_COLS):
            logging.info(f"list_of_cols is {list_of_cols}, expected: {LIST_OF_COLS}")
        hist.columns = list_of_cols
        if 'Adj Close' not in hist.columns:
            hist['Adj Close'] = hist['Close']
        return hist
    except Exception as e:
        logger.info(f"Error fetching {ticker}: {e}")
        return None

def fetchIndexBaseData(tickers, tradingDays):
    import concurrent.futures
    dataframes = []
    startDate = tradingDays[0]
    endDate = tradingDays[-1] + timedelta(days=1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
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
        rel = db.execute(" UNION ALL ".join([f"select * from view_{num}" for num in range(len(dataframes))]))
        return rel.fetchdf()
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    tradingDays = getTradingDays(endDate=datetime.now().date()-timedelta(days=2))
    tickers = getTopUsStocksByMarketCap()
    print(tickers)
    df = fetchIndexBaseData(tickers, tradingDays)
    df.to_csv("stock_market_data.csv", index=False)
    logger.info("Data saved to stock_market_data.csv")
