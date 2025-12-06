# Emma Blando + Vanessa Adan

'''
spotify_data.py

Fetches Spotify playlists and their tracks, gets audio features like valence and energy,
and stores everything in a SQLite DataBase

If ran multiple times, each rune will add at most "max_new_tracks" new tracks to limit 
stored items to 25 or less.

'''

import os
import sqlite3
import requests
import base64
from typing import List, Dict, Tuple

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://apy.spotify.com/v1"

# authorize 

def get_access_token() -> str:
    '''
    Uses credentials to get an access token

    '''

    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")

    # spotify requires id and secret encoded in Base64
    auth_bytes = f"{client_id}:{client_secret}".encode("utf-8")
    auth_header = base64.b64encode(auth_bytes).decode("utf-8")

    # headers for token request
    headers = {
        "Authorization":f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded", 
    }

    # spotify token type
    data = {"grant_type":"client_credentials"}

    # request to spotify token endpoint
    resp = requests.post(SPOTIFY_TOKEN_URL, headers=headers,data=data,timeout=15)
    resp.raise_for_status()

    # return access token
    return resp.json()["access_token"]

# setup database
def get_connection(db_name: str) -> sqlite3.Connection:
    """ open connection to SQLite """
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables(conn: sqlite3.Connection) -> None:
    """ Creates tables to store Spotify data"""

    cur = conn.cursor()

    # playlists table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS spotify_playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT, -- internal integer ID
            spotify_id TEXT UNIQUE,               -- Spotify playlist ID
            name TEXT
            );
        """
    )

    # tracks table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS spotify_tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,   -- internal integer ID
            spotify_id TEXT UNIQUE,                 -- Spotify track ID
            name TEXT,
            popularity INTEGER,
            valence REAL,
            energy REAL
        );
        """
    )

    # table if playlists has many tracks
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS playlist_tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,   -- internal key
            playlist_id INTEGER,                    -- FK to spotify_playlists.id
            track_id INTEGER,                       -- FK to spotify.tracks.id
            UNIQUE (playlist_id, track_id),         -- prevents duplicates
            FOREIGN KEY (playlist_id) REFERENCES spotify_playlists(id),
            FOREIGN KEY (track_id) REFERENCES spotify_tracks(id)
            );
        """
    )
    conn.commit()


# api spotify call
def get_featured_playlists(token: str, limit: int=20) -> List[Dict]:
    """
    Returns list of current Spotify featured playlists
    
    """

    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit": limit, "country":"US"}

    url = f"{SPOTIFY_API_BASE}/browse/featured-playlists"
    resp = requests.get(url,headers=headers,params=params, timeout=15)
    resp.raise_for_status()

    # extract playlis
    return resp.json().get("playlists",{}).get("items",[])

def get_playlist_tracks(token: str, playlist_id: str, limit: int=50, offset: int=0) -> Dict:
    """
    Retrieve tracks inside playlist
    """

    headers = {"Authorization": f"Bearer {token}"}
    params = {"limit":limit, "offset":offset, "market":"US"}


    url = f"{SPOTIFY_API_BASE}/playlists/{playlist_id}/tracks"
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()

    return resp.json()

def get_audio_features(token: str, track_ids: List[str]) -> Dict[str,Dict]:
    """
    Get audio features (valence, energy) for up to 100 tracks
    Returns dict
    """

    if not track_ids:
        return {}
    
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{SPOTIFY_API_BASE}/audio-features"

    # join ids
    resp = requests.get(url,headers=headers,params={"ids":",".join(track_ids)}, timeout=15)
    resp.raise_for_status()

    features_list = resp.json().get("audio_features",[])
    return {f["id"]: f for f in features_list if f}


# db helpers
def upsert_playlist(conn, spotify_id,name):
    """ Insert playlist if new, return existing ID if not"""

    cur = conn.cursor()

    cur.execute(
        """
        INSERT OR IGNORE INTO spotify_playlists (spotify_id, name)
        VALUES (?, ?);
        """,
        (spotify_id, name),
    )

    # get ID
    cur.execute("SELECT id FROM spotify_playlists WHERE spotify_id = ?;",(spotify_id,))
    row = cur.fetchone()
    conn.commit()

    return int(row["id"])

def upsert_track(conn, spotify_id, name,popularity, valence, energy):
    """Insert track if new"""
    cur = conn.cursor()

    cur.execute(
        """
        INSERT OR IGNORE INTO spotify_tracks
            (spotify_id, name, popularity, valence, energy)
        VALUES (?, ?, ?, ?, ?);
        """,
        (spotify_id,name,popularity,valence,energy),
    )

    cur.execute("SELECT id FROM spotify_tracks WHERE spotify_id = ?;",
                (spotify_id,))
    row = cur.fetchone()
    conn.commit()

    # if row count is greater than 0 it means the instert happened
    is_new = cur.rowcount > 0
    return int(row["id"]), is_new

def link_playlist_track(conn, playlist_id, track_id):
    """ Create link row if not already present"""
    cur = conn.cursor()

    cur.execute(
        """
        INSERT OR IGNORE INTO playlist_tracks (playlist_id, track_id)
        VALUES (?, ?);
        """,
        (playlist_id, track_id),
    )

    conn.commit()

def fetch_and_store_spotify(db_name: str, max_new_tracks: int =25) -> None:
    """
    Get Spotify token -> create db tables -> fetch feautured playlists ->
    loop through playlists -> fetch their tracks -> get audior features -> 
    insert into db
    """

    token = get_access_token()
    conn = get_connection(db_name)
    create_tables(conn)

    new_tracks_added = 0

    playlists = get_featured_playlists(token)
    print(f"Got {len(playlists)} featured playlists. \n")

    for pl in playlists:
        if new_tracks_added >= max_new_tracks:
            break

        playlist_spotify_id = pl["id"]
        playlist_name = pl["name"]
        print(f"Processing playlist: {playlist_name}")

        # insert playlist if doesnt exists, or get ID
        playlist_db_id = upsert_playlist(conn,playlist_spotify_id,playlist_name)

        offset = 0
        page_size = 50

        while new_tracks_added < max_new_tracks:
            page = get_playlist_tracks(token,playlist_spotify_id, limit=page_size,offset=offset)
            items = page.get("items", [])

            if not items:
                break

                # get track ids and metadata
            track_ids = []
            track_meta = {} # name and popularity

            for item in items:
                track = item.get("track")
                if track and track.get("id"):
                    tid = track["id"]
                    track_ids.append(tid)
                    track_meta[tid] = (track["name"], track.get("popularity",0))
            
            features = get_audio_features(token, track_ids)

            # insert each track 
            for tid in track_ids:
                if new_tracks_added >= max_new_tracks:
                    break

                name,pop = track_meta[tid]
                feat = features.get(tid, {})

                valence = feat.get("valence")
                energy = feat.get("energy")

                # skip tracks with missing audio features
                if valence is None or energy is None:
                    continue

                track_db_id, is_new =  upsert_track( conn, tid,name,pop,valence,energy)

                # create row of track relationships
                link_playlist_track(conn,playlist_db_id,track_db_id)

                if is_new:
                    new_tracks_added += 1
            
            offset += page_size

            if not page.get("next"):
                break
    conn.close()

    print(f"New tracks added: {new_tracks_added}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Spotify Data")
    parser.add_argument("--db", default="final_project.db", help="SQLite DB")
    parser.add_argument("--max-new-tracks", type=int, default=25,
                        help="Max new tracks per run")
    
    args = parser.parse_args()
    fetch_and_store_spotify(args.db,args.max_new_tracks)
