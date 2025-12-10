import sqlite3
import requests
import base64

DB_NAME = "final_project.db"

# search queries for playlist topics
SEARCH_QUERIES = [
    "global",
    "study",
    "summer",
    "holiday",
    "winter",
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


# searching playlists using parameters (like query topics)
def search_playlists(token: str, query: str, limit: int = 20):
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "q": query,
        "type": "playlist",
        "limit": limit,
        "market": "US",
    }

    res = requests.get(url, headers=headers, params=params, timeout=15)

    if res.status_code != 200:
        return []

    data = res.json()
    items = data.get("playlists", {}).get("items") or []
    return [pl for pl in items if pl is not None]


# inserting palylist metadata
def upsert_playlist_meta(spotify_id, name, description, tracks_total, owner_name) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # playlist ID, playlist name, description, # of tracks, and name of owner
    cur.execute(
        """
        INSERT OR IGNORE INTO spotify_playlists_meta
            (spotify_id, name, description, tracks_total, owner_name)
        VALUES (?, ?, ?, ?, ?);
        """,
        (spotify_id, name, description, tracks_total, owner_name),
    )

    conn.commit()
    inserted = cur.rowcount > 0
    conn.close()
    return inserted


# main
def main(max_new_playlists: int = 25):
    ensure_tables()
    token = get_spotify_token()

    new_count = 0

    for query in SEARCH_QUERIES:
        if new_count >= max_new_playlists:
            break

        playlists = search_playlists(token, query, limit=20)

        for pl in playlists:
            if new_count >= max_new_playlists:
                break

            spotify_id = pl.get("id")
            if not spotify_id:
                continue

            name = pl.get("name")
            description = pl.get("description") or ""
            tracks_total = (pl.get("tracks") or {}).get("total")
            owner_name = (pl.get("owner") or {}).get("display_name") or ""

            inserted = upsert_playlist_meta(
                spotify_id,
                name,
                description,
                tracks_total,
                owner_name,
            )

            if inserted:
                new_count += 1
                

    print(f"New playlists added this run: {new_count}")


if __name__ == "__main__":
    main(max_new_playlists=25)
