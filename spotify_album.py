import sqlite3
import requests
import base64
from datetime import datetime

DB_NAME = "final_project.db"

# search queries for playlist topics
SEARCH_QUERIES = [
    "rainy",
    "sunny",
    "cloudy",
    "snowy",
    "windy",
]

# from other python file so I dont have my keys in the script
from spotify_credentials import CLIENT_ID, CLIENT_SECRET


# getting access token
def get_spotify_token():
    # all encoding and data information formatting + content
    # was gotten from Spotify Developer website
    auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_string.encode()).decode()

    headers = {
        "Authorization": f"Basic {b64_auth}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {"grant_type": "client_credentials"}

    res = requests.post(
        "https://accounts.spotify.com/api/token",
        headers=headers,
        data=data,
        timeout=15,
    )
    res.raise_for_status()
    # return string of access token
    return res.json()["access_token"]


# function to ensure that metadata table exists
def ensure_tables():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS spotify_playlists_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            spotify_id TEXT UNIQUE,
            name TEXT,
            description TEXT,
            tracks_total INTEGER,
            owner_name TEXT
        )
        """
    )

    conn.commit()
    conn.close()

# have to create new column for approximate date
def ensure_approx_created_column():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # returns info about db structure
    cur.execute("PRAGMA table_info(spotify_playlists_meta);")
    cols = [row[1] for row in cur.fetchall()]

    if "approx_created_at" not in cols:
        cur.execute(
            "ALTER TABLE spotify_playlists_meta ADD COLUMN approx_created_at TEXT;"
        )
        conn.commit()

    conn.close()

# creating new column for month and year of date added in above func
# similar structure to function above - same function as well
def ensure_month_added_column():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(spotify_playlists_meta)")
    cols = [row[1] for row in cur.fetchall()]

    if "month_added" not in cols:
        cur.execute("ALTER TABLE spotify_playlists_meta ADD COLUMN month_added TEXT;")
        conn.commit()

    conn.close()


# searching playlists using parameters (like query topics)
def search_playlists(token: str, query: str, limit: int = 20):
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "q": query,
        "type": "playlist",
        "limit": limit,
        "market": "US",  # 403 error if you do not define a market
    }

    res = requests.get(url, headers=headers, params=params, timeout=15)

    if res.status_code != 200:
        # optional: print for debugging
        # print("Search error:", res.status_code, res.text[:200])
        return []

    data = res.json()
    # get playlists, empty dict if missing, same with items
    items = data.get("playlists", {}).get("items") or []

    # loop through items and only keeps list of non-null items
    return [pl for pl in items if pl is not None]


# adding a function to try and get some temporal data about the playlists
# this will help supplement our other APIs/websites
def get_playlist_added_date(token: str, playlist_id: str):
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"

    # IMPORTANT: Bearer, not Beared
    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": 1, "market": "US"}  # needs market so it doesnt crash

    res = requests.get(url, headers=headers, params=params, timeout=15)

    # to not crash the script, skipping any playlist that it cant get date for
    if res.status_code != 200:
        # optional debug:
        # print("added_at error:", res.status_code, res.text[:200])
        return None

    items = res.json().get("items") or []
    if not items:
        return None

    # added_at is an ISO timestamp string
    return items[0].get("added_at")

# from the temporal data above, extract month of the year
def extract_month(timestamp):
    if not timestamp:
        return None
    try:
        ts = timestamp.replace("Z","")
        dt = datetime.fromisoformat(ts)
        # returns year and month
        return dt.strftime("%Y-%m")
    except:
        return None
    


# inserting playlist metadata if it does not exist
def upsert_playlist_meta(
    spotify_id, name, description, tracks_total, owner_name,
    approx_created_at) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    month_added = extract_month(approx_created_at)
    # playlist ID, playlist name, description, # of tracks, and name of owner
    cur.execute(
        """
        INSERT OR IGNORE INTO spotify_playlists_meta
            (spotify_id, name, description, tracks_total, owner_name, approx_created_at,month_added)
        VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (spotify_id, name, description, tracks_total, owner_name, approx_created_at, month_added),
    )

    inserted = cur.rowcount > 0

    # if there is a created at date, and the DB row is missing it, update column
    if approx_created_at is not None:
        cur.execute(
            """
            UPDATE spotify_playlists_meta
            SET approx_created_at = ?
            WHERE spotify_id = ?
              AND (approx_created_at IS NULL OR approx_created_at = '');
            """,
            (approx_created_at, spotify_id),
        )
    
    if month_added is not None:
        cur.execute(
            """
            UPDATE spotify_playlists_meta
            SET month_added = ?
            WHERE spotify_id = ?
              AND (month_added IS NULL OR month_added = '');
            """,
            (month_added, spotify_id),
        )

    # returns True if a new row has been inserted
    conn.commit()
    conn.close()
    return inserted


# main
def main(max_new_playlists: int = 25):
    ensure_tables()
    ensure_approx_created_column()
    ensure_month_added_column()
    token = get_spotify_token()

    new_count = 0
    # keeping the limit <= 25 of new playlists
    for query in SEARCH_QUERIES:
        if new_count >= max_new_playlists:
            break

        # getting 20 playlists results for each query
        playlists = search_playlists(token, query, limit=20)

        # loops over each playlist dict in playlists
        for pl in playlists:
            if new_count >= max_new_playlists:
                break

            # get playlist id from json
            spotify_id = pl.get("id")
            if not spotify_id:
                continue

            # getting the playlist characteristics
            name = pl.get("name")
            description = pl.get("description") or ""
            tracks_total = (pl.get("tracks") or {}).get("total")
            owner_name = (pl.get("owner") or {}).get("display_name") or ""

            # approx creation date from first track added
            approx_created_at = get_playlist_added_date(token, spotify_id)

            # inserts all characteristics into DB
            inserted = upsert_playlist_meta(
                spotify_id,
                name,
                description,
                tracks_total,
                owner_name,
                approx_created_at,
            )

            if inserted:
                new_count += 1

    # to check how many new playlists were added
    print(f"New playlists added this run: {new_count}")


if __name__ == "__main__":
    # each run inserts max 25 new playlists
    main(max_new_playlists=25)
