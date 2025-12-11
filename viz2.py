import requests
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

DB = "final_project.db"

CHARTS_TABLE = "charts"

def load_chart_and_weather(db_path):

    conn = sqlite3.connect(db_path)
    # SQL SELECT here to get charts and weather data from DB -----------------------------------
    charts = pd.read_sql_query(
        f"SELECT date, position, song_title, artist FROM {CHARTS_TABLE};",
        conn,
    )
    weather = pd.read_sql_query(
        "SELECT date, temperature FROM weather;",
        conn,
    )

    conn.close()

    
    if "position" in charts.columns:
        charts = charts[charts["position"] == 1].copy()

    charts["date"] = pd.to_datetime(charts["date"])
    weather["date"] = pd.to_datetime(weather["date"])

    return charts, weather

def compute_song_stat(charts,weather):
    merged = pd.merge(charts,weather,on="date",how="left")
    # copmuting days of song at #1 and then sorting -------------------------------------------------
    grouped = (merged.groupby(["song_title","artist"], as_index=False)
               .agg(days_at_one = ("date","count"),
                    avg_temp_at_one=("temperature","mean"),
                    )
                    .sort_values("days_at_one",ascending=False))
    
    return grouped

def make_weather_chart_plot():
    charts,weather = load_chart_and_weather(DB)
    stats_df = compute_song_stat(charts,weather)

    if stats_df.empty:
        print("No chart data found")
        return
    
    top = stats_df.head(10).copy()

    # limiting song names - if more than 25 chars, cut it off at 21 and add ...
    def short_title(row):
        title = row["song_title"]
        artist = row["artist"]
        label = f"{title} - {artist}"
        return label if len(label) <= 25 else label[:22] + "..."
    
    top["label"] = top.apply(short_title,axis=1)

    x= range(len(top))

    # weather v top song saved to csv here -------------------------------------------
    top.to_csv("weather_topsong.csv",index=False)

    fig,ax1 = plt.subplots(figsize=(12,6))

    ax1.bar(x,top["days_at_one"],color="skyblue",label = "Days at #1")
    ax1.set_xlabel("Song (Title + Artist)")
    ax1.set_ylabel("Dys at #1", color="blue")
    ax1.tick_params(axis="y",labelcolor="blue")
    ax1.set_xticks(x)
    ax1.set_xticklabels(top["label"],rotation=45,ha="right")

    ax2 = ax1.twinx()
    ax2.plot(
        x,
        top["avg_temp_at_one"],
        color="red",
        marker="o",
        linewidth=2,
        label="Avg Temperature (°C)",
    )
    ax2.set_ylabel("Avg Temperature (°C)", color="red")
    ax2.tick_params(axis="y", labelcolor="red")

    plt.title("How Long Each #1 Song Stayed on Top vs. Average Temperature")
    fig.tight_layout()

    lines = ax1.patches + ax2.get_lines()
    labels = ["Days at #1"] + [line.get_label() for line in ax2.get_lines()]
    ax1.legend(lines, labels, loc="upper left")

    plt.show()


if __name__ == "__main__":
    make_weather_chart_plot()