# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS  songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id serial PRIMARY KEY,
    start_time timestamp REFERENCES time(start_time),
    user_id integer REFERENCES users(user_id),
    level char(4),
    song_id text REFERENCES songs(song_id),
    artist_id text REFERENCES artists(artist_id),
    session_id integer,
    location text,
    user_agent text
)"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id integer PRIMARY KEY,
    first_name text,
    last_name text,
    level char(4),
    gender char(1)
)"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY,
    title text,
    artist_id text REFERENCES artists(artist_id),
    year integer,
    duration float
)"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY,
    name text,
    location text,
    latitude float,
    longitude float
)"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer
)"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO NOTHING
"""

song_table_insert = """
INSERT INTO SONGS (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING
"""


time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING
"""

# FIND SONGS

song_select = """
SELECT sp.song_id, sp.artist_id FROM songplays as sp 
INNER JOIN songs as s  ON sp.song_id = s.song_id 
INNER JOIN artists as a ON sp.artist_id = a.artist_id 
WHERE s.title = %s 
AND a.name = %s 
AND s.duration = %s
"""

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]