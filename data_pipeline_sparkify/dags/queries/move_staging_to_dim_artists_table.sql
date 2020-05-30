INSERT INTO artists (artist_id, name, location, latitude, longitude)
(
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
)