import os
import configparser


# CONFIG
config = configparser.ConfigParser()
config_dir = os.path.dirname(os.getcwd()) + "\config\dwh.cfg"
config.read(config_dir)

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender CHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration FLOAT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_location VARCHAR,
        artist_longitude FLOAT,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INTEGER,
        song_id VARCHAR,
        title VARCHAR,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id       BIGINT IDENTITY(1,1)        NOT NULL,
        start_time        TIMESTAMP                   NOT NULL,
        user_id           INTEGER                     NOT NULL,
        level             VARCHAR                     NOT NULL,
        song_id           VARCHAR                     NOT NULL,
        artist_id         VARCHAR                     NOT NULL,
        session_id        INTEGER                     NOT NULL,
        location          VARCHAR                     NOT NULL,
        user_agent        VARCHAR                     NOT NULL,
        PRIMARY KEY(songplay_id)
    )
    DISTSTYLE KEY
    DISTKEY ( start_time )
    SORTKEY (start_time);
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id        INTEGER        NOT NULL,
        first_name     VARCHAR        NOT NULL,
        last_name      VARCHAR        NOT NULL,
        gender         CHAR           NOT NULL,
        level          VARCHAR        NOT NULL,
        PRIMARY KEY(user_id)
    )
    SORTKEY(user_id);
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id        VARCHAR        NOT NULL,
        title          VARCHAR        NOT NULL,
        artist_id      VARCHAR        NOT NULL,
        year           INTEGER        NOT NULL,
        duration       FLOAT          NOT NULL,
        PRIMARY KEY(song_id)
    )
    SORTKEY(song_id);
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id        VARCHAR        NOT NULL,
        name             VARCHAR        NOT NULL,
        location         VARCHAR        NOT NULL,
        latitude         FLOAT          NOT NULL,
        longitude        FLOAT          NOT NULL,
        PRIMARY KEY(artist_id)
    )
    SORTKEY(artist_id);
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time        TIMESTAMP        NOT NULL,
        hour              INTEGER          NOT NULL,
        day               INTEGER          NOT NULL,
        week              INTEGER          NOT NULL,
        month             INTEGER          NOT NULL,
        year              INTEGER          NOT NULL,
        weekday           VARCHAR          NOT NULL,
        PRIMARY KEY(start_time)
    )
    SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE '{}'
    JSON 'auto';
""").format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    IAM_ROLE '{}'
    JSON 'auto';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        timestamp with time zone 'epoch' + se.ts/1000 * interval '1 second', se.userId, se.level, 
        ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events AS se INNER JOIN staging_songs AS ss
    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE se.page = 'NextSong' AND userId IS NOT NULL;      
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        se.userId,
        se.firstName,
        se.lastName,
        se.gender,
        se.level
    FROM staging_events se
    WHERE se.userId IS NOT NULL
        AND se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs ss
    JOIN staging_events se
    ON ss.title = se.song
        AND se.page = 'NextSong'
    WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_songs ss
    JOIN staging_events se
    ON ss.artist_name = se.artist
        AND se.page = 'NextSong'
    WHERE ss.artist_id IS NOT NULL AND ss.artist_location IS NOT NULL AND ss.artist_latitude IS NOT NULL AND ss.artist_longitude IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
        EXTRACT(hour FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') as hour,
        EXTRACT(day FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') as day,
        EXTRACT(week FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') as week,
        EXTRACT(month FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') as month,
        EXTRACT(year FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') AS year,
        EXTRACT(dow FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') as weekday
    FROM staging_events se
    WHERE se.page = 'NextSong';
        
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [songplay_table_insert]