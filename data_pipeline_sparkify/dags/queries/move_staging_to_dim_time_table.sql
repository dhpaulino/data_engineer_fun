INSERT INTO time (start_time, hour, day, week, month, year, weekday)
 (
   SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, EXTRACT(hour from ts), EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(month from ts), EXTRACT(year from ts), EXTRACT(weekday from ts)
   FROM staging_events
)
-- got the raw time from staging instead of songplays, because eventually there will be a lot of data

