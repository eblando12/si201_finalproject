import sqlite3

# Connect to your database file
conn = sqlite3.connect("final_project.db")
cur = conn.cursor()

# Get the first 5 rows from the charts table
try:
    cur.execute("SELECT * FROM charts LIMIT 5")
    rows = cur.fetchall()

    print("\n--- Data currently in 'charts' table ---")
    if not rows:
        print("The table is empty!")
    for row in rows:
        print(row)
except sqlite3.OperationalError:
    print("Error: The 'charts' table does not exist yet.")

conn.close()
