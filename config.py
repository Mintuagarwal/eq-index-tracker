NUM_DAYS = 30
NUM_TOP_STOCKS = 150  # Fetch more to cover any delistings or inactive stocks
INDEX_SIZE = 100
INDEX_UNIVERSE = "^NDX"  # Use Nasdaq-100 for now; replace with larger universe if needed
US_INDEX = "SPY"
DATA_FOLDER = "data"
PARTITION_FOLDER = f"{DATA_FOLDER}/partition"
URL_MAP = {
    "sp500": "https://www.slickcharts.com/sp500",
    "nasdaq100": "https://www.slickcharts.com/nasdaq100"
}
FILE_KEY = "stock_market_data"
from functools import partial
FILE_KEY_FUNC = partial("{}/{}.{}".format, DATA_FOLDER, FILE_KEY)
DEFAULT_OUTPUT_STORE = 'outputs'
