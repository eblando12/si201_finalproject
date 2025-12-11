import requests
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

DB = "final_project.db"

# loading weather data
def load_weather_by_month(db_path):
    conn = sqlite3.connect(db_path)

    weather = pd.read_sql_query("SELECT date, temperature FROM weather;", conn)
    conn.close()

    weather["date"] = pd.to_datetime(weather["date"])

    weather["month"] = weather["date"].dt.to_period("M").astype(str)

    weather_monthly = (weather.groupby("month", as_index=False)["temperature"]
                       .mean()
                       .rename(columns={"temperature":"avg_temp"})
                       .sort_values("month")
                       )
    
    return weather_monthly

def load_playlists(db_path):
    conn = sqlite3.connect(db_path)
    playlists = pd.read_sql_query(
        "SELECT name,month_added FROM spotify_playlists_meta "
        "WHERE month_added IS NOT NULL;",
        conn,
    )
    conn.close()

    return playlists

def classify_theme(name):
    if not isinstance(name,str):
        return None
    
    n = name.lower()

    winter_keywords = ["winter","holiday","christmas","snow","cold","fall","christmas","season"]
    summer_keywords = ["summer","hot","beach","sun","hot","warm"]

    if any(k in n for k in winter_keywords):
        return "winter"
    if any(k in n for k in summer_keywords):
        return "summer"
    return None

def aggregate_playlists_by_month(playlists):
    playlists["theme"] = playlists["name"].apply(classify_theme)

    playlists = playlists.dropna(subset=["theme"])

    counts = (playlists.groupby(["month_added","theme"])
              .size()
              .unstack(fill_value=0)
              .reset_index()
              .rename(columns={"month_added":"month"})
              .sort_values("month"))
    
    if 'winter' not in counts.columns:
        counts["winter"] = 0
    if 'summer' not in counts.columns:
        counts["summer"] = 0

    return counts

def make_plot_weathervplaylists():
    weather_monthly = load_weather_by_month(DB)
    playlists = load_playlists(DB)
    playlist_counts = aggregate_playlists_by_month(playlists)

    merged = pd.merge(weather_monthly, playlist_counts, on="month", how="left").fillna(0)

    fig, ax1 = plt.subplots(figsize=(12,6))

    x = merged["month"]

    ax1.plot(x,merged["avg_temp"],marker="o",color="red",label="Average Temperature (C)")
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Temperature (C)",color="red")
    ax1.tick_params(axis="y",labelcolor="red")

    ax2 = ax1.twinx()
    ax2.plot(x,merged["winter"],marker="s",color="blue",label="Winter Playlists")
    ax2.plot(x,merged["summer"],marker="^", color="orange",label="Summer Playlists")
    ax2.set_ylabel("Number of Seasonal Playlists",color="blue")
    ax2.tick_params(axis="y", labelcolor="blue")

    plt.title("Average Temperature vs Seasonal Playlist Creation by Month")

    # rotate x labels for readability
    plt.xticks(rotation=45)

    # combined legend
    lines = ax1.get_lines() + ax2.get_lines()
    labels = [line.get_label() for line in lines]
    ax1.legend(lines, labels, loc="upper left")

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    make_plot_weathervplaylists()