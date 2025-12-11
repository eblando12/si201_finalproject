import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DB = "final_project.db"


def classify_theme(name):
    if not isinstance(name, str):
        return None

    # classifying themes for each playlist using keywords
    n = name.lower()

    winter_keywords = ["winter", "snow", "cold", "christmas", "holiday", "ski", "traditional"]
    summer_keywords = ["summer", "beach", "sun", "hot", "warm", "house"]

    if any(k in n for k in winter_keywords):
        return "winter"
    if any(k in n for k in summer_keywords):
        return "summer"
    return None


def load_joined_weather_playlists(db_path):
    conn = sqlite3.connect(db_path)
    # SQL SELECT and JOIN here ---------------------------------------------------------
    #   selecting the month part of the date string, and the temperature + name
    # joining spotify DB with weather ON the date
    query = """
        SELECT
            substr(w.date, 1, 7) AS month,   
            w.temperature,
            p.name
        FROM weather AS w
        JOIN spotify_playlists_meta AS p 
          ON substr(w.date, 1, 7) = p.month_added
        WHERE p.month_added IS NOT NULL;
    """

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def prepare_monthly_data() -> pd.DataFrame:

    df = load_joined_weather_playlists(DB)

    # computing avg temperature by month here ---------------------------------------------
    temp_monthly = (
        df.groupby("month", as_index=False)["temperature"]
          .mean()
          .rename(columns={"temperature": "avg_temp"})
    )

    # playlist classifications 
    df["theme"] = df["name"].apply(classify_theme)
    themed = df.dropna(subset=["theme"])

    # count winter and summer playlists per month -----------------------------------------
    counts = (
        themed.groupby(["month", "theme"])
              .size()
              .unstack(fill_value=0)
              .reset_index()
    )

    # both columns exist even if one theme didn’t appear
    for col in ["winter", "summer"]:
        if col not in counts.columns:
            counts[col] = 0

    merged = pd.merge(temp_monthly, counts, on="month", how="left").fillna(0)

    # create a real datetime month for sorting
    merged["month_dt"] = pd.to_datetime(merged["month"] + "-01")
    merged = merged.sort_values("month_dt")

    # only getting last 24 months (2 yrs) so plot isnt crowded
    if len(merged) > 24:
        merged = merged.tail(24)

    return merged


def make_plot_weathervplaylists():
    merged = prepare_monthly_data()
 
    x = merged["month"]
    # write to csv here ------------------------------------------------------------
    merged.to_csv("weather_v_playlists.csv",index=False)

    # plotting below 
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # temperature line
    ax1.plot(
        x,
        merged["avg_temp"],
        marker="o",
        color="red",
        label="Average Temperature (°C)",
    )
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Temperature (°C)", color="red")
    ax1.tick_params(axis="y", labelcolor="red")
    plt.xticks(rotation=50, ha="right")

    # second axis: seasonal playlists
    ax2 = ax1.twinx()
    ax2.plot(
        x,
        merged["winter"],
        marker="s",
        color="blue",
        label="Winter Playlists",
    )
    ax2.plot(
        x,
        merged["summer"],
        marker="^",
        color="orange",
        label="Summer Playlists",
    )
    ax2.set_ylabel("Number of Seasonal Playlists", color="blue")
    ax2.tick_params(axis="y", labelcolor="blue")

    # light grid for readability
    ax1.grid(axis="y", alpha=0.3, linestyle="--")

    plt.title("Average Temperature vs Seasonal Playlist Creation by Month")

    # combined legend from both axes
    lines = ax1.get_lines() + ax2.get_lines()
    labels = [line.get_label() for line in lines]
    ax1.legend(lines, labels, loc="upper left")

    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    make_plot_weathervplaylists()
