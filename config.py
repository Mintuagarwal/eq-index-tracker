NUM_DAYS = 30
NUM_TOP_STOCKS = 150  # Fetch more to cover any delistings or inactive stocks
INDEX_SIZE = 100
INDEX_UNIVERSE = "^NDX"  # Use Nasdaq-100 for now; replace with larger universe if needed
US_INDEX = "SPY"
PARTITION_FOLDER = "data/partition"
URL_MAP = {
    "sp500": "https://www.slickcharts.com/sp500",
    "nasdaq100": "https://www.slickcharts.com/nasdaq100"
}