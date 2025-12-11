import sqlite3
import requests 
from bs4 import BeautifulSoup

def scrape_and_store_offical_charts(db_name):


    #connecting the sql database
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    #creating charts table
    cur.execute('CREATE TABLE charts (id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT, position INTEGER, song_title TEXT, artist TEXT)')
    conn. commit()


    #This will make sure that we align the dates from the weather data
    cur.execute('SELECT DISTINCT date FROM weather')
    weather_dates = cur.fetchall()

    added_items = 0 
    #looping through each date in the weather data
    for date in weather_dates: 

        #to not surpassing 25 item per run
        if added_items >= 25:
            break

        #from fetching weather data, we need to get the date
        fetching_date = date[0]

        #checking for no reapts in the charts 
        cur.execute('SELECT id FROM charts WHERE date = ?', (fetching_date,))
        data = cur.fetchone()

        #skipping this iteration to not get a repeat
        if data is not None:
            continue

        #in this part we make the dates are be formatted and get the data
        setting_up_date = fetching_date.replace("-", "")
        url = f"https://www.officialcharts.com/charts/singles-chart/{setting_up_date}/7501/"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')

        #finds titles and artists
        title_tag = soup.find('a', class_ = 'chart-name')
        artist_tag = soup.find('a', class_ = 'chart-artist')


        #once finds the title and arits, it'll insert those into data by getting the number one song
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
    conn = sqlite3.connect("final_project.db")
    cur = conn.cursor()

    #making sure table was crated
    try:
        cur.execute("SELECT count(*) FROM charts")
        count = cur.fetchone()[0]
        print(f"Current rows in 'charts' table: {count}")
        
        if count < 100:
            print("need more rows")
        else:
            print("100+ rows")
    except:
        print("no table")
        
    conn.close()


