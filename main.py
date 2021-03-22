from datetime import datetime
import datetime
import pandas as pd
import requests
import sqlalchemy
import sqlite3

# --------------------------- PERSONAL PARAMETERS --------------------------- #

DB_LOCATION = "sqlite:///spotify_tracks.sqlite"
USER_ID = ''
TOKEN = ''

# --------------------------------------------------------------------------- #

def is_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print('No songs downloaded.')
        return False

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated.")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null valued found.")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs doesn't come from within the last 24 hours ")

    return True

headers = {
    'Accept': "application/json",
    'Content-Type': "application/json",
    'Authorization': f"Bearer {TOKEN}"
}

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
r = requests.get(f'https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}', headers=headers)
data = r.json()

song_names = []
artist_names = []
played_at_list = []
timestamps = []

for song in data['items']:
    song_names.append(song['track']['name'])
    artist_names.append(song['track']['album']['artists'][0]['name'])
    played_at_list.append(song['played_at'])
    timestamps.append(song['played_at'][0:10])

song_dict = {
    'song_name': song_names,
    'artist_name': artist_names,
    'played_at': played_at_list,
    'timestamp': timestamps
}
df = pd.DataFrame(song_dict, columns=['song_name', 'artist_name', 'played_at', 'timestamp'])

# Validate
if is_valid_data(df):
    print("Data valid, proceed to Load stage")

# Save data to database
engine = sqlalchemy.create_engine(DB_LOCATION)
conn = sqlite3.connect('spotify_tracks.sqlite')
cursor = conn.cursor()

sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

cursor.execute(sql_query)
print("Opened database successfully")

try:
    df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
except:
    print("Data already exists in the database")

conn.close()
print("Close database successfully")


