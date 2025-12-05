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
    resp = requests.posts(SPOTIFY_TOKEN_URL, headers=headers,data=data,timeout=15)
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
            ):
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
    