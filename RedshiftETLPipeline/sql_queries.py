import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS dim_users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS dim_time CASCADE;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS
    staging_events
    (
        artist VARCHAR,
        auth VARCHAR(20),
        first_name VARCHAR,
        gender VARCHAR(2),
        item_in_session INTEGER,
        last_name VARCHAR,
        length REAL,
        level VARCHAR(10),
        location VARCHAR(100),
        method VARCHAR(5),
        page VARCHAR(30),
        registration BIGINT,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    staging_songs
    (
        artist_id VARCHAR(50),
        artist_latitude VARCHAR(30),
        artist_location VARCHAR,
        artist_longitude VARCHAR(30),
        artist_name VARCHAR,
        duration REAL,
        num_songs INTEGER,
        song_id VARCHAR(50),
        title VARCHAR,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    fact_songplays
    (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER NOT NULL DISTKEY,
        location VARCHAR,
        user_agent VARCHAR
    )
    DISTSTYLE KEY
    SORTKEY (user_id, start_time);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    dim_users
    (
        user_id INTEGER PRIMARY KEY DISTKEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        level VARCHAR NOT NULL
    )
    DISTSTYLE KEY;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    dim_songs
    (
        song_id VARCHAR PRIMARY KEY DISTKEY,
        title VARCHAR NOT NULL,
        artist_id_fk VARCHAR NOT NULL,
        year INTEGER SORTKEY,
        duration REAL
    )
    DISTSTYLE KEY;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    dim_artists
    (
        artist_id VARCHAR PRIMARY KEY DISTKEY,
        name VARCHAR NOT NULL SORTKEY,
        location VARCHAR,
        latitude VARCHAR(30),
        longitude VARCHAR(30)
    )
    DISTSTYLE KEY;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS
    dim_time
    (
        id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL DISTKEY SORTKEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    )
    DISTSTYLE KEY;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM '{source}'
    IAM_ROLE '{role_arn}'
    FORMAT AS JSON '{json_path}'
    REGION '{region}';
""").format(
        source=config.get('S3', 'LOG_DATA'), 
        role_arn=config.get('IAM_ROLE', 'ARN'), 
        json_path=config.get('S3', 'LOG_JSONPATH'), 
        region=config.get('AWS', 'REGION')
    )

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{source}'
    CREDENTIALS 'aws_iam_role={role_arn}'
    REGION '{region}'
    JSON 'auto';
""").format(
        source=config.get('S3', 'SONG_DATA'), 
        role_arn=config.get('IAM_ROLE', 'ARN'),
        region=config.get('AWS', 'REGION')
    )

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO fact_songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + se.ts / 1000 * INTERVAL '1 second' as start_time,
        se.user_id,
        se.level,
        so.song_id,
        so.artist_id,
        se.session_id,
        se.location,
        se.user_agent
    FROM staging_events se
    LEFT JOIN staging_songs so 
        ON se.song = so.title AND se.artist = so.artist_name
    WHERE se.page = 'NextSong';
    """
)

user_table_insert = (
    """
    INSERT INTO dim_users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT 
        se.user_id, 
        se.first_name, 
        se.last_name, 
        se.gender, 
        se.level
    FROM staging_events se
    JOIN 
        (
            SELECT 
                user_id,
                max(ts) as ts
            FROM staging_events
            WHERE page = 'NextSong'
            GROUP BY user_id
        ) sea on sea.user_id = se.user_id and sea.ts = se.ts;
    """
)

song_table_insert = (
    """
    INSERT INTO dim_songs (
        song_id,
        title,
        artist_id_fk,
        year,
        duration
    )
    SELECT
        song_id,
        title,
        artist_id,
        CASE WHEN year = 0 THEN NULL ELSE year END AS year,
        duration
    FROM staging_songs;
    """
)

artist_table_insert = (
    """
    INSERT INTO dim_artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    GROUP BY 1,2,3,4,5;
    """
)

time_table_insert = (
    """
    INSERT INTO dim_time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT
        t.start_time,
        extract(hour from t.start_time) as hour,
        extract(day from t.start_time) as day,
        extract(week from t.start_time) as week,
        extract(month from t.start_time) as month,
        extract(year from t.start_time) as year,
        extract(weekday from t.start_time) as weekday
    FROM 
        (
            SELECT
                TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' as start_time
            FROM staging_events
            WHERE page = 'NextSong'
            GROUP BY 1
        ) t;
    """
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
