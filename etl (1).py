import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# Connecting to the sparkify database, set the session to automatically commit. Obtain conn and cur for further executions.
conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
conn.set_session(autocommit=True)
cur = conn.cursor()

# Setup process for tables to be obtained from song_data, i.e. songs and artists tables.
def process_song_file(cur, filepath):
    """Inserts records into songs and artists tables.
    
    Keyword arguments:
    cur -- Uses the cursor object of the Sparkifydb to execute insert statements.
    filepath -- Takes in the path of the json format file of song metadata. Data in the file stored here will be inserted in the tables.
    """
    # open song file
    df = pd.read_json(filepath, typ='series')
    df.to_frame()

    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values
    song_data.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data.tolist()
    cur.execute(artist_table_insert, artist_data)

# Setup process for tables to be obtained from log_data, i.e. users, time, and songplays tables.
def process_log_file(cur, filepath):
    """Inserts records into songplays, users and time tables.
    
    Keyword arguments:
    cur -- Uses the cursor object of the Sparkifydb to execute insert statements.
    filepath -- Takes in the path of the json format log file. Data in the file stored here will be inserted in the tables.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts*1000000)
    
    # insert time data records
    time_data = [t.tolist(), t.dt.hour.tolist(), t.dt.day.tolist(), t.dt.weekofyear.tolist(), t.dt.minute.tolist(), t.dt.year.tolist(), t.dt.dayofweek.tolist()]
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


# For each of the songs and and each of the rows in the log data table, we need to to iterate the process of inserting rows.
def process_data(cur, conn, filepath, func):
    """Iterates selected process from the above two.
    
    cur -- Uses the cursor object of the Sparkifydb to execute insert statements.
    conn -- To commit changes to the db.
    filepath -- Takes the location of the directory where all the json files/logs are located. In order to read one file at a time, then execute the process mentioned by func.
    func -- Takes in either 'process_song_file' or 'process_song_file' and executes that method.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

        
def main():
    """This function execute the process data function. Once on song_data 'process_song_file' and then on log_data 'process_log_file'
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()