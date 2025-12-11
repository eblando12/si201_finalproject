import sqlite3
import requests
import my_info

def fetch_and_store_weather_data(start_date,end_date, db_name,city = "London",country = "United Kingdom"):


    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.execute(
        ''' CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        date TEXT, 
        temperature REAL,
        description TEXT,
        sunset TEXT,
        sunrise TEXT)
        ''' )
    conn.commit()

    location = f"{city}, {country}"

    base_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}"
    params = {
        "unitGroup": "metric",
        "contentType": "json",
        "include": "days",
        "key": my_info.visual_crossing_key
    }
    
    #start_date: "2025-01-01", end_date: "2025-04-30"

    response = requests.get(base_url,params = params)
    if response.status_code != 200: 
        print("Error")
        return

    data = response.json()

    list_of_days = data.get('days', [])

    added_items = 0 
    for day in list_of_days:

        if added_items >= 25:
            break

        datetime_value = day.get('datetime')
        temperature_value = day.get('temp')
        conditions_value = day.get('conditions')
        sunrise_value =day.get('sunrise')
        sunset_value = day.get('sunset')

        cur.execute("SELECT date FROM weather WHERE date = ?", (datetime_value,))

        if cur.fetchone() is None:
            cur.execute(
                '''
                INSERT INTO weather (date,city, temperature, description, sunrise, sunset) VALUES (?,?,?,?,?,?)
                ''', (datetime_value, city, temperature_value, conditions_value, sunrise_value, sunset_value)
            )
            added_items += 1

    conn.commit()
    conn.close()

    pass

   
if __name__ == "__main__":
    test_db = "weather_test.db"

    start = "2024-01-01" 
    end = "2024-06-01"
    print("--- TEST RUN 1 ---")
    fetch_and_store_weather_data(start, end, test_db)
    
    # Check the count
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM weather")
    count_1 = cur.fetchone()[0]
    print(f"Rows in DB after Run 1: {count_1} (Should be 25)")
    conn.close()

    print("\n--- TEST RUN 2 ---")
    # We run the EXACT same function call again. 
    # It should skip the first 25 (duplicates) and add the NEXT 25.
    fetch_and_store_weather_data(start, end, test_db)
    
    conn = sqlite3.connect(test_db)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM weather")
    count_2 = cur.fetchone()[0]
    print(f"Rows in DB after Run 2: {count_2} (Should be 50)")
    
    # Check for duplicates
    cur.execute("SELECT count(DISTINCT date) FROM weather")
    unique_dates = cur.fetchone()[0]
    print(f"Unique dates: {unique_dates} (Should match Total Rows: {count_2})")
    
    conn.close()
if __name__ == "__main__":
    # 1. Define your parameters
    start = "2025-01-01"
    end = "2025-02-01"
    db = "final_project.db"
    
    # 2. CALL the function
    print("Starting weather fetch...")
    fetch_and_store_weather_data(start, end, db)
    print("Finished!")

if __name__ == "__main__":
    db_name = "final_project.db" 
    start_date = "2024-01-01" # You can adjust these dates
    end_date = "2024-06-01"

    print(f"Fetching weather data for {db_name}...")
    fetch_and_store_weather_data(start_date, end_date, db_name)
    print("Done!")