-- Insert only the users that are not in the users table yet
INSERT INTO users
    (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    (
        SELECT DISTINCT
            se.userid,
            se.firstName,
            se.lastName,
            se.gender,
            se.level
        FROM staging_events AS se
            LEFT JOIN users AS u ON se.userid = u.user_id
        WHERE u.user_id IS NULL AND se.page='NextSong'
    )