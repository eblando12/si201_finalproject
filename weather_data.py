import sqlite3
import requests
import my_info

def fetch_and_store_weather_data(start_date,end_date, db_name,city = "London",country = "United Kingdom"):

    #connecting to sql database
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    #creating the table and making the columns
    #the INTEGER PRIMARY KEY AUTOINCREMENT helps increase a number for each run to make sure each row is individual
    #the REAl means it will save a float number
    cur.execute(
        ''' CREATE TABLE weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        date TEXT, 
        temperature REAL,
        description TEXT,
        sunset TEXT,
        sunrise TEXT)
        ''' )
    conn.commit()

    #making sure location is by london in the uk to set up the API request
    location = f"{city}, {country}"
    #visual crossing URL
    base_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}"
    
    #each parameter meanig: metic helps keep the temperature in celcius, api will return in json, daily weather information, and api key
    params = {
        "unitGroup": "metric",
        "contentType": "json",
        "include": "days",
        "key": my_info.visual_crossing_key
    }
    
    #start_date: "2025-01-01", end_date: "2025-04-30" possibly, still need to see if we want to do the full year

    response = requests.get(base_url,params = params)
    if response.status_code != 200: 
        print("Error")
        return

    data = response.json()

    #it looks at the dictionary and returns the value, if not then it will return an empty list
    list_of_days = data.get('days', [])

    #25 day infromation, works like the counter for each row added
    added_items = 0 

    #this loop with go through each days' weather information
    for day in list_of_days:

        #this make sure it doesn't run past 25 items
        if added_items >= 25:
            break

        datetime_value = day.get('datetime')
        temperature_value = day.get('temp')
        conditions_value = day.get('conditions')
        sunrise_value =day.get('sunrise')
        sunset_value = day.get('sunset')

        #this execution checks for any days that were repeated in the data
        cur.execute("SELECT date FROM weather WHERE date = ?", (datetime_value,))


        #checking if that date it not in the data so it can be added
        if cur.fetchone() is None:
            cur.execute(
                '''
                INSERT INTO weather (date,city, temperature, description, sunrise, sunset) VALUES (?,?,?,?,?,?)
                ''', (datetime_value, city, temperature_value, conditions_value, sunrise_value, sunset_value)
            )

            #after adding the weather date information, it will count 1 row has been added
            added_items += 1

    #saves changes
    conn.commit()
    conn.close()

    pass

   
if __name__ == "__main__":

    db_name = "final_project.db"
    
    start_date = "2024-01-01" 
    end_date = "2024-06-01" 
    
    # Call the function
    fetch_and_store_weather_data(start_date, end_date, db_name)
    
    # checking progress
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM weather")
    count = cur.fetchone()[0]
    conn.close()
    
    print(f"rows in 'weather' table: {count}")

    if count < 100:
        print("need more rows")
    else:
        print("100+ rows")