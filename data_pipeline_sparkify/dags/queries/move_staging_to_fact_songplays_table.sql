INSERT INTO songplays
    (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    (
         SELECT
            TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
            events.userid,
            events.level,
            events.song,
            events.artist,
            events.sessionid,
            events.location,
            events.useragent
        FROM
            staging_events AS events
            LEFT JOIN staging_songs songs
                ON events.song = songs.title
                    AND events.artist = songs.artist_name
                    AND events.length = songs.duration
        WHERE page='NextSong'
    )