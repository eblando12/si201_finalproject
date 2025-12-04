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