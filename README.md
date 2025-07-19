# eq-index-tracker
Equity Index Tracker

This project is my take‑home assignment for a Data Engineer interview at a SaaS startup.  
It constructs and tracks an **equal‑weighted custom index** comprising the **top 100 US stocks by market cap**, re‑evaluated daily.

---

## ✨ Overview

For each trading day in the past month:
1. **Rank all stocks by Market Cap** (computed as Close × Shares Outstanding).
2. Select the **top 100 tickers**.
3. Construct an **equal‑weighted index** (each stock contributes equally).
4. Export daily index values and compositions to an Excel/Parquet output.

✅ **Data Source:** Yahoo Finance via `yfinance`
✅ **Database Engine:** DuckDB (in‑memory)  
✅ **Output:** Parquet files and easily extendable to Excel sheets. Right now, stored in system storage, could be moved to services like S3 using boto clients

Why **Yahoo Finance**?
Easy to Use APIs with clear objects like `Ticker`, `history`, `download`

Why `DuckDB`?
I have been using duckdb (in-memory Database) a lot in my job. I not longer have the need to use pandas. Pandas's just feels slow.
And I cannot read 12 dataframe with 9 million rows, 200 columns each on a remote kernel even with 200 GBs but with duckDb I can leverage the fact that it can operate on parquets only so the kernel requirement reduces massively.
Also in future, I plan to add a **partitioning scheme** for data storage based on dates so that my index construction is faster over a certain window of days as duckDb can effeciently query the right number of parquets for construction.


A lot of prompt engineering was leveraged with ChatGPT.
Here is link to some nice chatter with our dear GPT :)   https://chatgpt.com/share/687baf6b-ac70-8011-bed2-33d6c2d83909