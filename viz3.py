import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DB = "final_project.db"



def classify_theme(name):
    if not isinstance(name, str):
        return None
    n = name.lower()

    winter_keywords = ["winter", "snow", "cold", "christmas", "holiday","ski","traditional"]
    summer_keywords = ["summer", "beach", "sun", "hot", "warm","house"]
    fall_keywords = ["fall", "autumn", "rain", "chill", "coffee","indie","folk"]
    spring_keywords = ["spring","flowers","rainy","sunshine","bloom","easter","pastel"]

    if any(k in n for k in winter_keywords):
        return "Winter"
    if any(k in n for k in summer_keywords):
        return "Summer"
    if any(k in n for k in fall_keywords):
        return "Fall"
    if any(k in n for k in spring_keywords):
        return "Spring"

    return "other"


def season_from_month_str(month_str):
    if not isinstance(month_str, str) or len(month_str) < 7:
        return None
    try:
        month = int(month_str[5:7])
    except ValueError:
        return None

    if month in (12, 1, 2):
        return "Winter"
    elif month in (3, 4, 5):
        return "Spring"
    elif month in (6, 7, 8):
        return "Summer"
    elif month in (9, 10, 11):
        return "Fall"
    return None

def load_playlist_data(db_path):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT name, month_added FROM spotify_playlists_meta WHERE month_added IS NOT NULL;",
        conn
    )
    conn.close()
    return df


def stacked_bar():
    playlists = load_playlist_data(DB)
    playlists = playlists[playlists["month_added"] >= "2023-01"]
    # assign themes
    playlists["theme"] = playlists["name"].apply(classify_theme)

    # count playlists per month per theme
    counts = (
        playlists.groupby(["month_added", "theme"])
        .size()
        .unstack(fill_value=0)
        .sort_index()
    )


    for theme in ["Winter", "Summer", "Spring", "Fall"]:
        if theme not in counts.columns:
            counts[theme] = 0

    # plot stacked bar chart
    plt.figure(figsize=(12, 6))

    counts[["Winter", "Summer", "Spring", "Fall"]].plot(
        kind="bar",
        stacked=True,
        figsize=(14, 7),
        colormap="tab20"
    )

    plt.title("Seasonal Playlist Themes by Month (Spotify)")
    plt.xlabel("Month Added")
    plt.ylabel("Number of Playlists")
    plt.xticks(rotation=45)
    plt.legend(title="Theme")
    plt.tight_layout()

    plt.show()


if __name__ == "__main__":
    stacked_bar()
