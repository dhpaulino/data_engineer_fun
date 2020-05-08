import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS  songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
         artist TEXT,
           auth TEXT,
      firstName TEXT,
         gender TEXT,
  iteminsession INTEGER,
       lastname TEXT,
         length FLOAT,
          level TEXT,
       location TEXT,
         method TEXT,
           page TEXT,
   registration FLOAT,
      sessionid INTEGER,
           song TEXT,
         status INTEGER,
             ts TIMESTAMP,
      useragent TEXT,
         userid FLOAT
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
           song_id TEXT,
             title TEXT,
          duration FLOAT,
              year INTEGER,
         num_songs INTEGER,
         artist_id TEXT,
       artist_name TEXT,
   artist_latitude FLOAT,
  artist_longitude FLOAT,
   artist_location TEXT
);
"""
songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
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
    gender char(1) ENCODE BYTEDICT,
    level TEXT ENCODE BYTEDICT
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

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} CREDENTIALS 'aws_iam_role={}'
region 'us-west-2' json {} timeformat as 'epochmillisecs'""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {} CREDENTIALS 'aws_iam_role={}'
region 'us-west-2' json 'auto' """).format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id ,level, song_id, artist_id, session_id, location, user_agent)
(
    SELECT ts, userid, level, song_id, artist_id, sessionid, location, useragent
    FROM staging_events as se
    INNER JOIN staging_songs as ss ON se.song=ss.title
) 
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level) 
(
    SELECT DISTINCT userid, firstName, lastName, gender, level
    FROM staging_events
    WHERE userid IS NOT NULL
)
"""

song_table_insert = """
INSERT INTO SONGS (song_id, title, artist_id, year, duration)
(
   SELECT song_id, title, artist_id, year, duration
   FROM staging_songs
)
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
(
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
)
"""


time_table_insert = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
 (
   SELECT DISTINCT ts, EXTRACT(hour from ts), EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(month from ts), EXTRACT(year from ts), EXTRACT(weekday from ts)
   FROM staging_events
)
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]