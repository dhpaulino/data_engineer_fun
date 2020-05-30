INSERT INTO users (user_id, first_name, last_name, gender, level)
(
    SELECT DISTINCT userid, firstName, lastName, gender, level
    FROM staging_events
    WHERE userid IS NOT NULL
)