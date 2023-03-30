
class SqlQueries:

    table_drop=("DROP TABLE IF EXISTS {}")

    staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                    artist VARCHAR,
                    auth VARCHAR,
                    firstname VARCHAR,
                    gender CHAR(1),
                    itemInSession INTEGER,
                    lastname VARCHAR,
                    length FLOAT,
                    level VARCHAR,
                    location VARCHAR,
                    method VARCHAR,
                    page VARCHAR,
                    registration FLOAT,
                    sessionid INTEGER,
                    song VARCHAR,
                    status INTEGER,
                    ts BIGINT,
                    useragent VARCHAR,
                    userid INTEGER)
    """)

    staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                    num_songs INTEGER,
                    artist_id VARCHAR,
                    artist_latitude FLOAT8,
                    artist_longtitude FLOAT8,
                    artist_location VARCHAR,
                    artist_name VARCHAR,
                    song_id VARCHAR,
                    title VARCHAR,
                    duration FLOAT,
                    year INTEGER)
    """)

    
    
    songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
                song VARCHAR NOT NULL PRIMARY KEY,
                userid INTEGER,
                level VARCHAR,
                song_id VARCHAR,
                sessionid INTEGER,
                location VARCHAR,
                useragent VARCHAR)
    """)

    user_table_create = ("""CREATE TABLE IF NOT EXISTS users(  
                    userid INTEGER,
                    firstname VARCHAR,
                    lastname VARCHAR,
                    gender CHAR(1),
                    level VARCHAR
                    )
    """)

    song_table_create = ("""CREATE TABLE IF NOT EXISTS song(
                    song_id VARCHAR NOT NULL PRIMARY KEY ,
                    title  VARCHAR,
                    artist_id  VARCHAR,
                    year  INTEGER,
                    duration  FLOAT8)
    """)

    artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
                      artist_id VARCHAR NOT NULL PRIMARY KEY,
                      artist_name VARCHAR,
                      artist_location VARCHAR,
                      artist_latitude FLOAT8,
                      artist_longtitude FLOAT8)
    """)

    time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                    ts TIMESTAMP,
                    hour INTEGER,
                    day INTEGER,
                    week INTEGER,
                    month INTEGER,
                    year INTEGER,
                    weekday INTEGER)
    """)


    songplay_table_insert=("""INSERT INTO
                songplay(song,userid,level,song_id,sessionid,location,useragent)
                SELECT ss.title,se.userid,se.level,ss.song_id,se.sessionid,
                se.location,se.useragent
                FROM staging_events se
                INNER JOIN staging_songs ss on ss.title=se.song
                WHERE se.page='NextSong'
""")

    user_table_insert = ("""INSERT INTO users(
            userid,firstname,lastname,gender,level)
            SELECT DISTINCT userid,firstname,lastname,gender,level
            FROM staging_events
            WHERE page='NextSong'
""")

    song_table_insert = ("""INSERT INTO song(
            song_id,title,artist_id,year,duration)
            SELECT song_id,title,artist_id,year,duration
            FROM staging_songs 
""")

    artist_table_insert = ("""INSERT INTO artist(
            artist_id,artist_name,artist_location,artist_latitude,artist_longtitude)
            SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,
            artist_longtitude
            FROM staging_songs
""")

    time_table_insert =("""
INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

    truncate_table=("""
        TRUNCATE TABLE {}
    
    
    """)

    COPY_SQL = ("""
     COPY {}
     FROM '{}'
     JSON '{}'
     ACCESS_KEY_ID '{{}}'
     SECRET_ACCESS_KEY '{{}}'
     REGION 'us-west-2'
    """)

    COPY_STAGING_EVENTS_SQL = COPY_SQL.format(
     "staging_events",
     's3://udacity-dend/log_data',
     's3://udacity-dend/log_json_path.json'
    )


     
     
    COPY_STAGING_SONGS_SQL = COPY_SQL.format(
     "staging_songs",
     "s3://udacity-dend/song_data/A",
     'auto'
    )




    tables_dictionary={'songplaycreate':songplay_table_create,'songplayinsert':songplay_table_insert,
     'userscreate':user_table_create,'usersinsert':user_table_insert,
     'songcreate':song_table_create,'songinsert':song_table_insert,
     'artistcreate':artist_table_create,'artistinsert':artist_table_insert,
     'timecreate':time_table_create,'timeinsert':time_table_insert}