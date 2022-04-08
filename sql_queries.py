import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist varchar
        ,auth varchar
        ,firstName varchar
        ,gender varchar
        ,itemInSession int
        ,lastName varchar
        ,length float
        ,level varchar
        ,location varchar
        ,method varchar
        ,page varchar
        ,registration float
        ,sessionId int
        ,song varchar
        ,status int
        ,ts bigint
        ,userAgent varchar
        ,userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs int
        ,artist_id varchar
        ,artist_latitude float
        ,artist_longitude float
        ,artist_location varchar
        ,artist_name varchar
        ,song_id varchar
        ,title varchar
        ,duration float
        ,year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE fct_songplays (
        songplay_id int identity(1, 1) primary key,
        start_time timestamp not null sortkey,
        user_id int not null distkey,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar
    ) diststyle key
""")

user_table_create = ("""
    CREATE TABLE dim_users (
        user_id int primary key sortkey,
        first_name varchar not null,
        last_name varchar,
        gender varchar,
        level varchar not null
    ) diststyle all
""")

song_table_create = ("""
    CREATE TABLE dim_songs (
        song_id varchar primary key sortkey,
        title varchar not null,
        artist_id varchar distkey,
        year int,
        duration float not null
    ) diststyle key
""")

artist_table_create = ("""
    CREATE TABLE dim_artists (
        artist_id varchar primary key sortkey,
        name varchar not null,
        location varchar,
        latitude float,
        longitude float
    ) diststyle all
""")

time_table_create = ("""
        CREATE TABLE dim_time (
        start_time timestamp primary key sortkey ,
        hour smallint,
        day smallint,
        week smallint,
        month smallint,
        year int distkey,
        weekday smallint
    ) diststyle key
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {bucket}
    credentials 'aws_iam_role={arn}'
    region 'us-west-2' format as json {logpath}
    timeformat as 'epochmillisecs'
""").format(bucket = config.get('S3', 'LOG_DATA')
            ,arn = config.get('IAM_ROLE', 'ARN')
            ,logpath = config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs
    from {bucket}
    credentials 'aws_iam_role={arn}'
    region 'us-west-2' format as json 'auto'
""").format(bucket = config.get('S3', 'SONG_DATA')
            , arn = config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fct_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (b.ts / 1000) * INTERVAL '1 second' as start_time
        ,b.userId as user_id
        ,b.level
        ,a.song_id
        ,a.artist_id
        ,b.sessionId as session_id
        ,b.location
        ,b.userAgent as user_agent
    FROM staging_songs a
    JOIN staging_events b
        ON a.title  =   b.song
        AND a.artist_name    =   b.artist
    WHERE
        b.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId as user_id
        ,firstName as first_name
        ,lastName as last_name
        ,gender
        ,level
    FROM staging_events
    WHERE
        userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id
        ,title
        ,artist_id
        ,year
        ,duration
    FROM staging_songs
    WHERE
        song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id
        ,artist_name
        ,artist_location
        ,artist_latitude
        ,artist_longitude
    FROM staging_songs a
""")

time_table_insert = ("""
    INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
    WITH time_d as (
        SELECT
            TIMESTAMP 'epoch' + (b.ts / 1000) * INTERVAL '1 second' as timestamp_col
        FROM stagin_events)
    SELECT
        timestamp_col as start_time
        ,EXTRACT(hour FROM timestamp_col) as hour
        ,EXTRACT(day FROM timestamp_col) as day
        ,EXTRACT(week FROM timestamp_col) as week
        ,EXTRACT(month FROM timestamp_col) as month
        ,EXTRACT(year FROM timestamp_col) as year
        ,EXTRACT(weekday FROM timestamp_col) as weekday
    from time_d
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
