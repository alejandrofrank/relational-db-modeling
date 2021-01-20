# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, \
                                        start_time BIGINT, \
                                        user_id INTEGER NOT NULL, \
                                        level TEXT, \
                                        song_id TEXT, \
                                        artist_id TEXT, \
                                        session_id INTEGER NOT NULL, \
                                        location TEXT, \
                                        user_agent TEXT); """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id TEXT PRIMARY KEY, \
                                        first_name TEXT, \
                                        last_name TEXT, \
                                        gender TEXT, \
                                        level TEXT);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id TEXT PRIMARY KEY, \
                                        title TEXT, \
                                        artist_id TEXT, \
                                        year INTEGER, \
                                        duration FLOAT8);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id TEXT PRIMARY KEY, \
                                        name TEXT, \
                                        location TEXT, \
                                        latitude FLOAT8, \
                                        longitude FLOAT8);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (ts BIGINT PRIMARY KEY, \
                                        start_time TIMESTAMP, \
                                        hour INTEGER, \
                                        day INTEGER, \
                                        week INTEGER, \
                                        month INTEGER, \
                                        year INTEGER, \
                                        weekday INTEGER);""")

# DROP TABLES

songplay_table_drop = ("""DROP TABLE IF EXISTS songplays;""")

user_table_drop = ("""DROP TABLE IF EXISTS users;""")

song_table_drop = ("""DROP TABLE IF EXISTS songs;""")

artist_table_drop = ("""DROP TABLE IF EXISTS artists;""")

time_table_drop = ("""DROP TABLE IF EXISTS time;""")

# INSERT TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO
    UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO
    UPDATE SET title=EXCLUDED.title;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO
    UPDATE SET location=EXCLUDED.location;
""")

time_table_insert = ("""
INSERT INTO time (ts, start_time, hour, day, week, month, year, weekday)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts)
DO NOTHING;
""")

# DROP RECORDS

songplay_table_trunc = "TRUNCATE songplays;"
user_table_trunc = "TRUNCATE table_name;"
song_table_trunc = "TRUNCATE table_name;"
artist_table_trunc = "TRUNCATE table_name;"
time_table_trunc = "TRUNCATE table_name;"

# FIND SONGS

song_select = ("""
SELECT
    A.song_id,
    B.artist_id
FROM songs A
INNER JOIN artists B on A.artist_id = B.artist_id
WHERE 
    A.title = %s
AND
    B.name = %s
AND 
    A.duration = %s;
""")

# TESTS
not_null_songid = ("""
SELECT 
    song_id, 
    artist_id 
FROM songplays 
WHERE 
    song_id IS NOT NULL;
""")

not_null_artistid = ("""
SELECT 
    song_id, 
    artist_id 
FROM songplays 
WHERE 
    song_id IS NOT NULL;
""")

not_null_ids = ("""
SELECT 
    song_id, 
    artist_id 
FROM songplays 
WHERE 
    song_id IS NOT NULL 
    AND
    artist_id IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
drop_records_table_queries = [songplay_table_trunc, user_table_trunc, song_table_trunc, artist_table_trunc, time_table_trunc]
test_queries = [not_null_songid, not_null_artistid, not_null_ids]

