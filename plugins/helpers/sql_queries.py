class SqlQueries:

    songplay_table_insert = ("""
            SELECT
                    md5events.sessionid || events.start_time songplay_id,
                    events.start_time, 
                    events.userid, 
                    events.level, 
                    songs.song_id, 
                    songs.artist_id, 
                    events.sessionid, 
                    events.location, 
                    events.useragent
                    FROM SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                FROM staging_events
                WHERE page='NextSong' events
                LEFT JOIN staging_songs songs
                ON events.song = songs.title
                    AND events.artist = songs.artist_name
                    AND events.length = songs.duration;
        """
    )
    user_table_insert = """
            SELECT distinct userid, firstname, lastname, gender, level
            FROM staging_events
            WHERE page='NextSong';
        """

    song_table_insert = """
            SELECT distinct song_id, title, artist_id, year, duration
            FROM staging_songs;
        """

    artist_table_insert = """
            SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
            FROM staging_songs;
        """

    time_table_insert = """
            SELECT start_time, extracthour from start_time, extractday from start_time, extractweek from start_time, 
                extractmonth from start_time, extractyear from start_time, extractdayofweek from start_time
            FROM songplays;
        """
    ## Create tables
    staging_events_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_events(
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender VARCHAR,
            itemInSession INT,
            lastName VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration FLOAT,
            sessionId INT,
            song VARCHAR,
            status INT,
            ts BIGINT,
            userAgent VARCHAR,
            userId INT)
        ;
        """)
    staging_songs_table_create = """
        CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        duration FLOAT,
        year INT)
    ;
    """

    songplay_table_create = """
        CREATE TABLE IF NOT EXISTS songplays( 
        songplay_id INT IDENTITY (0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR)
    ;
    """

    users_table_create = """
        CREATE TABLE IF NOT EXISTS users( 
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR)
    ;
    """

    song_table_create = """
        CREATE TABLE IF NOT EXISTS song( 
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT)
    ;
    """

    artist_table_create = """
        CREATE TABLE IF NOT EXISTS artist( 
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT)
    ;
    """

    time_table_create = """
        CREATE TABLE IF NOT EXISTS time( 
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT)
    ;
    """
    ## Insert 
    songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + log.ts/1000 * INTERVAL '1 second' AS start_time,
        log.userId AS user_id,
        log.level AS level,
        song.song_id AS song_id,
        song.artist_id AS artist_id,
        log.sessionId AS session_id,
        log.location AS location,
        log.userAgent AS user_agent
    FROM staging_events log
    JOIN staging_songs song ON log.song = song.title AND log.artist = song.artist_name
    WHERE log.page = 'NextSong' AND log.length = song.duration;
    """)

    user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender AS gender,
            level AS level
        FROM staging_events
        WHERE userId IS NOT NULL;
    """)

    song_table_insert = ("""
        INSERT INTO song (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            song_id AS song_id,
            title AS title,
            artist_id AS artist_id,
            year AS year,
            duration AS duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """)

    artist_table_insert = ("""
        INSERT INTO artist (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            artist_id AS artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL;
    """)

    time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
            TIMESTAMP 'epoch' + log.ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(weekday FROM start_time) AS weekday
        FROM staging_events log
        WHERE log.page = 'NextSong';
    """)
    ## Quality Checks
    data_quality_checks = [
    {
        'sql': 'SELECT COUNT(*) FROM users WHERE user_id IS NULL',
        'expected_result': 0
    },
    {
        'sql': 'SELECT COUNT(*) FROM song WHERE song_id IS NULL',
        'expected_result': 0
    }
    ]