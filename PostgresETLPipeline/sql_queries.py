# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
    songplay_id SERIAL PRIMARY KEY,
    start_time time NOT NULL,
    user_id int NOT NULL,
    level varchar(10),
    song_id varchar,
    artist_id varchar,
    session_id int,
    location varchar,
    user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar(5),
    level varchar(10)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id_fk varchar NOT NULL,
    year int,
    duration float
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude varchar,
    longitude varchar
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    id SERIAL PRIMARY KEY,
    start_time time NOT NULL,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO fact_songplays 
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES
(%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO dim_users 
(user_id, first_name, last_name, gender, level)
VALUES
(%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO NOTHING;
""")

song_table_insert = ("""
INSERT INTO dim_songs 
(song_id, title, artist_id_fk, year, duration)
VALUES
(%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO dim_artists
(artist_id, name, location, latitude, longitude)
VALUES
(%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING;
    
""")


time_table_insert = ("""
INSERT INTO dim_time
(start_time, hour, day, week, month, year, weekday)
VALUES
(%s, %s, %s, %s, %s, %s, %s);
""")

# FIND SONGS

song_select = ("""
SELECT 
    artist_id,
    song_id
FROM dim_artists da
JOIN dim_songs ds
ON ds.artist_id_fk = da.artist_id
WHERE lower(replace(ds.title,' ','')) = lower(replace(%s,' ',''))
AND lower(replace(da.name,' ','')) = lower(replace(%s,' ',''))
and ds.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]