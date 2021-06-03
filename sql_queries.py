import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config['IAM_ROLE']['ARN']
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

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
     CREATE TABLE IF NOT EXISTS staging_events (
         event_id INT IDENTITY(0,1) PRIMARY KEY,
         artist TEXT,
         auth VARCHAR(16),
         firstName TEXT,
         gender VARCHAR(1),
         itemInSession INT,
         lastName TEXT,
         lenght FLOAT,
         level VARCHAR(4),
         location TEXT,
         method VARCHAR(6),
         page TEXT,
         registration NUMERIC,
         sessionId INT,
         song TEXT,
         status INT,
         ts NUMERIC,
         userAgent TEXT,
         userId INT
     );
""")

staging_songs_table_create = ("""
     CREATE TABLE IF NOT EXISTS staging_songs (
         id INT IDENTITY(0,1) PRIMARY KEY,
         num_songs INT, 
         artist_id VARCHAR (18), 
         artist_latitude VARCHAR, 
         artist_longitude VARCHAR, 
         artist_location VARCHAR, 
         artist_name TEXT, 
         song_id VARCHAR(18), 
         title TEXT, 
         duration FLOAT, 
         year INT
     );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY SORTKEY DISTKEY, 
        start_time TIMESTAMP NOT NULL, 
        user_id INT NOT NULL, 
        level VARCHAR(6) NOT NULL, 
        song_id VARCHAR(18) NOT NULL, 
        artist_id VARCHAR (18) NOT NULL, 
        session_id INT NOT NULL, 
        location TEXT, 
        user_agent TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY SORTKEY DISTKEY,
        first_name TEXT,
        last_name TEXT,
        gender VARCHAR(1),
        level VARCHAR(6)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR(18) PRIMARY KEY,
        title TEXT,
        artist_id VARCHAR(18) NOT NULL,
        year INT,
        duration INT
    )DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR (18) PRIMARY KEY, 
        name TEXT, 
        location TEXT, 
        latitude VARCHAR, 
        longitude VARCHAR
    )DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE {}
    FORMAT as json {}
    REGION 'us-west-2';
""").format(
    LOG_DATA, ARN, LOG_JSONPATH
)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    IAM_ROLE {}
    FORMAT as json 'auto'
    REGION 'us-west-2';
""").format(
    SONG_DATA, ARN
)
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
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
        timestamp 'epoch' + se.ts/1000 * interval '1 second',
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se
    JOIN staging_songs ss 
    ON se.artist = ss.artist_name 
    AND se.song = ss.title
    WHERE se.page LIKE 'nextSong'
    
        
    
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
        )
    SELECT DISTINCT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE page LIKE 'nextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
        )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs

""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
        )
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
        )
    SELECT DISTINCT
        start_time,
        EXTRACT(h FROM start_time),
        EXTRACT(d FROM start_time),
        EXTRACT(w FROM start_time),
        EXTRACT(m FROM start_time),
        EXTRACT(y FROM start_time),
        EXTRACT(dow FROM start_time)
    FROM songplays
""")


#DEBUG QUERY
check_errors = ("""
    SELECT * FROM stl_load_errors
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
errors_query = [check_errors]
