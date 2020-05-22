# Sparkify Data Lake

## Plot
*A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.*

## Objective

Ingest data relative to music info and users usage of the streaming service from files in S3, process it using Apache Spark, create tables in a snowflake architecture, save in a columnar format (.parquet) to future analysis about the users and the musics played by them.


## Datasets exemples

### Song dataset
```json
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}
```

### Log dataset

```json
{
    "artist": null,
    "auth": "Logged In",
    "firstName": "Walter",
    "gender": "M",
    "itemInSession": 0,
    "lastName": "Frye",
    "length": null,
    "level": "free",
    "location": "San Francisco-Oakland-Hayward, CA",
    "method": "GET",
    "page": "Home",
    "registration": 1540919166796.0,
    "sessionId": 38,
    "song": null,
    "status": 200,
    "ts": 1541105830796,
    "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
    "userId": "39"
}
```
## Tables schema

**songplays** - Fact table - records in log that correspond a music being played
- songplay_id (INT): ID of each user song played event - generated with `monotonically_increasing_id()` 
- start_time (TIMESTAMP): time when the event started
- user_id (INT) : ID of user
- level (STRING): User level {free | paid}
- song_id (STRING): ID of Song played
- artist_id (STRING): ID of Artist of the song played
- session_id (INT): ID of the user Session 
- location (STRING): User location 
- user_agent (STRING): Agent used by user to access Sparkify platform

**users** - users in the platform
- user_id (INT): ID of user
- first_name (STRING): Name of user
- last_name (STRING): Last Name of user
- gender (STRING): Gender of user {M | F}
- level (STRING): User (last) level {free | paid}

**songs** - songs in music database
- song_id (STRING): ID of Song
- title (STRING): Title of Song
- artist_id (STRING): ID of song Artist
- year (INT): Year of song release
- duration (FLOAT): Song duration in milliseconds

**artists** - artists in music database
- artist_id (STRING): ID of Artist
- name (STRING): Name of Artist
- location (STRING): Name of Artist city
- lattitude (FLOAT): Lattitude location of artist
- longitude (FLOAT): Longitude location of artist

**time** - timestamps of records in songplays broken down into specific units
- start_time (TIMESTAMP) PRIMARY KEY: Timestamp of row
- hour (INT): Hour associated to start_time
- day (INT): Day associated to start_time
- week (INT): Week of year associated to start_time
- month (INT): Month associated to start_time 
- year (INT): Year associated to start_time
- weekday (STRING): Name of week day associated to start_time

