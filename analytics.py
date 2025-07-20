import pandas as pd
from config import DEFAULT_OUTPUT_STORE, DATA_FOLDER

def readAndPreprocessData(data_path=f'{DATA_FOLDER}/index_100.parquet'):
    # Load the saved parquet
    index_df = pd.read_parquet(data_path)

    # Ensure Date is sorted
    index_df = index_df.sort_values("Date").reset_index(drop=True)
    index_df["DailyReturnPct"] = index_df["IndexValue"].pct_change() * 100
    index_df["DailyReturnPct"].iloc[0] = 0  # first day treated as 0

    index_df["CumulativeReturnPct"] = (index_df["IndexValue"] / index_df["IndexValue"].iloc[0] - 1) * 100
    return index_df


def createCompositionData(index_df):
    daily_composition = index_df[["Date", "TickerList"]].copy()
    composition_changes = []
    prev_set = None
    for i, row in index_df.iterrows():
        date = row["Date"]
        tickers_today = set(row["TickerList"].split('-'))
        if prev_set is None:
            added = tickers_today
            removed = set()
        else:
            added = tickers_today - prev_set
            removed = prev_set - tickers_today
        composition_changes.append({
            "Date": date,
            "TickersAdded": "-".join(sorted(added)) if added else "",
            "TickersRemoved": "-".join(sorted(removed)) if removed else "",
            "Intersection": "-".join(sorted(prev_set & tickers_today)) if prev_set else ""
        })
        prev_set = tickers_today

    composition_changes_df = pd.DataFrame(composition_changes)

    return composition_changes_df

def getAnalytics():
    index_df = readAndPreprocessData()
    index_performance = index_df[["Date", "IndexValue", "DailyReturnPct", "CumulativeReturnPct"]]

    composition_changes_df = createCompositionData(index_df)

    total_added = sum(len(c.split('-')) for c in composition_changes_df["TickersAdded"] if c)
    total_removed = sum(len(c.split('-')) for c in composition_changes_df["TickersRemoved"] if c)

    best_day = index_performance.loc[index_performance["DailyReturnPct"].idxmax()]
    worst_day = index_performance.loc[index_performance["DailyReturnPct"].idxmin()]
    aggregate_return = index_performance["CumulativeReturnPct"].iloc[-1]

    summary_metrics = pd.DataFrame([
        ["Total Tickers Added", total_added],
        ["Total Tickers Removed", total_removed],
        ["Best Day", f"{best_day['Date']} ({best_day['DailyReturnPct']:.2f}%)"],
        ["Worst Day", f"{worst_day['Date']} ({worst_day['DailyReturnPct']:.2f}%)"],
        ["Aggregate Return (%)", f"{aggregate_return:.2f}"]
    ], columns=["Metric", "Value"])

    daily_composition = index_df[["Date", "TickerList"]].copy()

    return index_performance, daily_composition, composition_changes_df, summary_metrics

def plotAnalytics(index_performance):
    import plotly.graph_objects as go

    # index_performance is your DataFrame with Date, IndexValue, DailyReturnPct, CumulativeReturnPct
    perf = index_performance.copy()
    perf["Date"] = pd.to_datetime(perf["Date"])

    # Create figure
    fig = go.Figure()

    # Add cumulative return line
    fig.add_trace(go.Scatter(
        x=perf["Date"],
        y=perf["CumulativeReturnPct"],
        mode='lines+markers',
        name='Cumulative Return (%)',
        line=dict(color='royalblue', width=2),
        hovertemplate=(
            "<b>Date:</b> %{x}<br>" +
            "<b>Cumulative Return:</b> %{y:.2f}%<br>" +
            "<b>Daily Return:</b> %{customdata:.2f}%<extra></extra>"
        ),
        customdata=perf["DailyReturnPct"]
    ))

    # Add daily return bars
    fig.add_trace(go.Bar(
        x=perf["Date"],
        y=perf["DailyReturnPct"],
        name='Daily Return (%)',
        marker_color='orange',
        opacity=0.5,
        hovertemplate=(
            "<b>Date:</b> %{x}<br>" +
            "<b>Daily Return:</b> %{y:.2f}%<br>" +
            "<b>Cumulative Return:</b> %{customdata:.2f}%<extra></extra>"
        ),
        customdata=perf["CumulativeReturnPct"]
    ))

    # Layout tweaks
    fig.update_layout(
        title="Index Performance Over Time",
        xaxis_title="Date",
        yaxis_title="Return (%)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        template="plotly_dark",  # or 'plotly_white'
        hovermode="x unified",
        height=600,
        width=1000
    )

    # Show in browser
    fig.show()

    # Save to HTML (commit this to GitHub if you like)
    fig.write_html(f"{DEFAULT_OUTPUT_STORE}/index_performance_plot.html")
    fig.write_image(f"{DEFAULT_OUTPUT_STORE}/index_performance_plot.png", width=600, height=300, scale=1)
    print(f"Interactive plot saved to {DEFAULT_OUTPUT_STORE}/index_performance_plot.html")

if __name__ == "__main__":
    index_performance, daily_composition, composition_changes_df, summary_metrics = getAnalytics()
    with pd.ExcelWriter(f"{DEFAULT_OUTPUT_STORE}/index_outputs.xlsx", engine="openpyxl") as writer:
        index_performance.to_excel(writer, sheet_name="index_performance", index=False)
        daily_composition.to_excel(writer, sheet_name="daily_composition", index=False)
        composition_changes_df.to_excel(writer, sheet_name="composition_changes", index=False)
        summary_metrics.to_excel(writer, sheet_name="summary_metrics", index=False)
    import os
    if os.path.exists(f"{DEFAULT_OUTPUT_STORE}/index_performance_plot.html"):
        os.remove(f"{DEFAULT_OUTPUT_STORE}/index_performance_plot.html")
    plotAnalytics(index_performance)

    print("index_outputs and plots created successfully!")

