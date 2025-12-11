import sqlite3
import requests 
from bs4 import BeautifulSoup

def scrape_and_store_offical_charts(db_name):

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.execute('CREATE TABLE IF NOT EXISTS charts (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, position INTEGER, song_title TEXT, artist TEXT)')

    conn. commit()

    cur.execute('SELECT DISTINCT date FROM weather')
    weather_dates = cur.fetchall()

    added_items = 0 

    for date in weather_dates: 

        if added_items >= 25:
            break

        fetching_date = date[0]

        cur.execute('SELECT id FROM charts WHERE date = ?', (fetching_date,))
        data = cur.fetchone()

        if data is not None:
            print(f"Skipping {fetching_date}, already in database.")
            continue

        setting_up_date = fetching_date.replace("-", "")
        url = f"https://www.officialcharts.com/charts/singles-chart/{setting_up_date}/7501/"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        title_tag = soup.find('a', class_ = 'chart-name')
        artist_tag = soup.find('a', class_ = 'chart-artist')

        if artist_tag:
            print(f"DEBUG RAW TAG: {artist_tag}")

        if title_tag and artist_tag:
            song_title = title_tag.text.strip()
            artist = artist_tag.text.strip()
            position = 1

            cur.execute('INSERT INTO charts (date, position, song_title, artist) VALUES (?, ?, ?, ?)', (fetching_date, position, song_title, artist))

            added_items += 1
    
    conn.commit()
    conn.close()


    pass

if __name__ == "__main__":
    scrape_and_store_offical_charts("final_project.db")


