import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE staging_events"
staging_songs_table_drop = "DROP TABLE staging_songs"
songplay_table_drop = "DROP TABLE songplays"
user_table_drop = "DROP TABLE users"
song_table_drop = "DROP TABLE songs"
artist_table_drop = "DROP TABLE artists"
time_table_drop = "DROP TABLE time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist varchar
        ,auth varchar
        ,firstName varchar not null
        ,gender varchar
        ,itemInSession int
        ,lastName varchar
        ,length float not null
        ,level varchar not null
        ,location varchar
        ,method varchar
        ,page varchar
        ,registration float
        ,sessionId int
        ,song varchar not null
        ,status int
        ,ts bigint not null
        ,userAgent varchar
        ,userId int not null
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs int
        ,artist_id varchar not null
        ,artist_latitude float
        ,artist_longitude float
        ,artist_location varchar
        ,artist_name varchar
        ,song_id varchar not null
        ,title varchar not null
        ,duration float not null
        ,year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id IDENTITY(0, 1) PRIMARY KEY,
        start_time timestamp not null,
        user_id int not null,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id int PRIMARY KEY,
        first_name varchar not null,
        last_name varchar,
        gender varchar,
        level varchar not null
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar PRIMARY KEY,
        title varchar not null,
        artist_id varchar,
        year int,
        duration float not null
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar PRIMARY KEY,
        name varchar not null,
        location varchar,
        latitude float,
        longitude float
    )
""")

time_table_create = ("""
        CREATE TABLE time (
        start_time timestamp PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
