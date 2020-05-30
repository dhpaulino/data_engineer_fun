-- STAGING TABLES
CREATE TABLE IF NOT EXISTS public.staging_events(
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
    ts int8,
    useragent TEXT,
    userid FLOAT
);
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

TRUNCATE public.staging_events;
TRUNCATE public.staging_songs;

-- FACTS AND DIMENSIONS
CREATE TABLE IF NOT EXISTS artists (
    artist_id text PRIMARY KEY,
    name text,
    location text,
    latitude float,
    longitude float
);
CREATE TABLE IF NOT EXISTS songs (
    song_id text PRIMARY KEY,
    title text,
    artist_id text REFERENCES artists(artist_id),
    year integer,
    duration float
);

CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour integer,
    week integer,
    month integer,
    year integer,
    weekday integer
);
CREATE TABLE IF NOT EXISTS users (
    user_id integer PRIMARY KEY,
    first_name text,
    last_name text,
    gender char(1) ENCODE BYTEDICT,
    level TEXT ENCODE BYTEDICT
);
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
);