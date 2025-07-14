from dbconnect import loadData
from config import FILE_KEY

db = loadData()

if __name__ == "__main__":
    print(db.sql(f"select Ticker, count(*), sum(Market_Cap) from {FILE_KEY} group by Ticker order by 3 desc").fetchdf())