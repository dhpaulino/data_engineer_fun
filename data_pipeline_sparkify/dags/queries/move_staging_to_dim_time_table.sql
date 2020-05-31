INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    (
        SELECT
            DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
            EXTRACT(hour from start_time),
            EXTRACT(day from start_time),
            EXTRACT(week from start_time),
            EXTRACT(month from start_time),
            EXTRACT(year from start_time),
            EXTRACT(weekday from start_time)
        FROM staging_events
    )

