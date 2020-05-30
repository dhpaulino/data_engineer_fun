INSERT INTO SONGS (song_id, title, artist_id, year, duration)
(
   SELECT song_id, title, artist_id, year, duration
   FROM staging_songs
)