import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import warnings
warnings.filterwarnings('ignore')


def process_song_file(cur, filepath):
    '''
    This functions takes a song json file and processes it into a dataframe.
    Then selects columns for each table depending on which columns they need. 
        - Song table requires song_id, title, artist, year and duration
        - Artist table requires artist_id, artist_name, artist_location, artist_latitude and artist_longitude
    Converts the records array into a list
    Then inserts it into their respective table using cur parameter and a query located in sql_queries.py
    '''
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    This functions takes a log json file and processes it into a dataframe. 
    Filters the whole dataframe to the condition that the page column values must be equal to NextSong (page == NextSong)
    Then selects columns for each table depending on which columns they need. 
        - time table requires ts and with that column we create the rest of the needed variables using the dt module
        - user table requires userId, firstName, lastName, gender, level
    Converts the records array into a list
    Then inserts it into their respective table using cur parameter and a query located in sql_queries.py
    
    The last part searches for song_id and artist_id based on the title, artist name, and duration of a song. The artist id and song id are modified depending on the query search.
    Then the records are inserted into the songplays table with the modified artist id and song id.
    '''
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    # insert time data records
    time_df = df[['ts']]

    time_df['timestamp'] = pd.to_datetime(time_df['ts'])
    time_df['hour'] = time_df['timestamp'].dt.hour
    time_df['day'] = time_df['timestamp'].dt.day
    time_df['week_of_year'] = time_df['timestamp'].dt.week
    time_df['month'] = time_df['timestamp'].dt.month
    time_df['year'] = time_df['timestamp'].dt.year
    time_df['weekday'] = time_df['timestamp'].dt.weekday

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
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function crawls through every file in the given filepath parameter and appends it to a list.
    
    The function will output the number of files found by measuring the list lenght.
    Then process each file in the file list with a given function (func parameter)
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
    
    """
    Connects to the database and process_data with the func as process_song_file and log_file
    This basically runs the whole script
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()