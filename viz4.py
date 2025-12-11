import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DB_NAME = "final_project.db"


# converting days to minutes to figure out time after sunset
def sunset_to_minutes(t):
    if not isinstance(t, str):
        return None
    h, m, s = map(int, t.split(":"))
    return h * 60 + m


# playlist themes
def classify_theme(name: str):
    if not isinstance(name, str):
        return None
    n = name.lower()

    winter_keywords = ["winter", "snow", "cold", "christmas", "holiday","ski","traditional"]
    summer_keywords = ["summer", "beach", "sun", "hot", "warm","house"]
    fall_keywords = ["fall", "autumn", "rain", "chill", "coffee","indie","folk"]

    if any(k in n for k in winter_keywords):
        return "winter"
    if any(k in n for k in summer_keywords):
        return "summer"
    if any(k in n for k in fall_keywords):
        return "fall/chill"
    return "other"


def load_data():
    conn = sqlite3.connect(DB_NAME)

    weather = pd.read_sql_query("SELECT date, sunset FROM weather;", conn)
    playlists = pd.read_sql_query(
        "SELECT name, month_added FROM spotify_playlists_meta WHERE month_added IS NOT NULL;",
        conn,
    )

    conn.close()

    weather["date"] = pd.to_datetime(weather["date"])
    weather["month"] = weather["date"].dt.to_period("M").astype(str)
    weather["sunset_minutes"] = weather["sunset"].apply(sunset_to_minutes)

    # average sunset time per month
    weather_monthly = (
        weather.groupby("month", as_index=False)["sunset_minutes"].mean()
    )

    playlists["theme"] = playlists["name"].apply(classify_theme)

    # count playlists per month per theme
    theme_counts = (
        playlists.groupby(["month_added", "theme"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={"month_added": "month"})
    )

    # combine tables
    merged = pd.merge(weather_monthly, theme_counts, on="month", how="left").fillna(0)

    return merged


def make_visualization_4():

    df = load_data()

    # bins for sunset times
    bins = [0, 1000, 1100, 1400]  # roughly: early, mid, late
    labels = ["Early Sunset (Short Days)", "Medium Sunset", "Late Sunset (Long Days)"]

    df["sunset_bin"] = pd.cut(df["sunset_minutes"], bins=bins, labels=labels)

    # agg themes by sunset group
    grouped = (
        df.groupby("sunset_bin")[["winter", "summer", "fall/chill", "other"]]
        .sum()
        .reset_index()
    )
    
    #theme by month csv written here ---------------------------------------------------
    grouped.to_csv("theme_by_month.csv",index=False)
    plt.figure(figsize=(12, 6))

    colors = {
        "winter":"#84bee7",
        "summer": "#fff4a3",
        "fall/chill": "#FFAE7C",
        "other": "#7AD399",
    }

    # stacked bar chart
    bottom = None
    for theme in ["winter", "summer", "fall/chill", "other"]:
        plt.bar(
            grouped["sunset_bin"],
            grouped[theme],
            bottom=bottom,
            label=theme,
            color=colors[theme],
        )
        if bottom is None:
            bottom = grouped[theme].copy()
        else:
            bottom += grouped[theme]

    plt.title("Playlist Themes vs Sunset Timing/Day",
              fontsize=15)
    plt.xlabel("Day Length", fontsize=12)
    plt.ylabel("Number of Playlists Created", fontsize=12)
    plt.xticks(rotation=15)
    plt.legend(title="Playlist Theme")
    plt.tight_layout()

    plt.show()


if __name__ == "__main__":
    make_visualization_4()
