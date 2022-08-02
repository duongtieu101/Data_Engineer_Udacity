import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

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
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id      bigint identity(0,1) NOT NULL,
        artist        varchar              NULL,
        auth          varchar              NULL,
        firstName     varchar              NULL,
        gender        varchar              NULL,
        itemInSession varchar              NULL,
        lastName      varchar              NULL,
        length        varchar              NULL,
        level         varchar              NULL,
        location      varchar              NULL,
        method        varchar              NULL,
        page          varchar              NULL,
        registration  varchar              NULL,
        sessionId     int                  NOT NULL DISTKEY SORTKEY,
        song          varchar              NULL,
        status        int                  NULL,
        ts            bigint               NULL,
        userAgent     varchar              NULL,
        userId        int                  NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id        varchar    NOT NULL DISTKEY SORTKEY,
        artist_latitude  varchar    NULL,
        artist_location  varchar    NULL,
        artist_longitude varchar    NULL,
        artist_name      varchar    NULL,
        duration         decimal    NULL,
        num_songs        int        NULL,
        song_id          varchar    NOT NULL,
        title            varchar    NULL,
        year             int        NULL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int identity(0,1) NOT NULL SORTKEY,
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level varchar NULL,
        song_id varchar NOT NULL,
        artist_id varchar NOT NULL,
        session_id int NOT NULL,
        location varchar NULL,
        user_agent varchar NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id    int     NOT NULL SORTKEY,
        first_name varchar NULL,
        last_name  varchar NULL,
        gender     varchar NULL,
        level      varchar NULL
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id   varchar NOT NULL SORTKEY,
        title     varchar NULL,
        artist_id varchar NULL,
        year      int     NULL,
        duration  decimal NULL
    ) diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar NOT NULL SORTKEY,
        name      varchar NULL,
        location  varchar NULL,
        latitude  decimal NULL,
        longitude decimal NULL
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL SORTKEY,
        hour       int NULL,
        day        int NULL,
        week       int NULL,
        month      int NULL,
        year       int NULL,
        weekday    int NULL
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    format as json {}
    region 'us-west-2'; 
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    format as json 'auto'
    region 'us-west-2'; 
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
                                    se.userId as user_id,
                                    se.level as level,
                                    ss.song_id as song_id,
                                    ss.artist_id as artist_id,
                                    se.sessionId as session_id,
                                    se.location as location,
                                    se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss
        ON se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id,
                       first_name,
                       last_name,
                       gender,
                       level)
    SELECT DISTINCT se.userId as user_id,
                    se.firstName as first_name,
                    se.lastName as last_name,
                    se.gender as gender,
                    se.level as level
    
    FROM staging_events se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,
                        title,
                        artist_id,
                        year,
                        duration)
    SELECT DISTINCT ss.song_id as song_id,
                    ss.title as title,
                    ss.artist_id as artist_id,
                    ss.year as year,
                    ss.duration as duration
    FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id,
                        name,
                        location,
                        latitude,
                        longitude)
    SELECT DISTINCT ss.artist_id as artist_id,
                                    ss.artist_name as name,
                                    ss.artist_location as location,
                                    ss.artist_latitude as latitude,
                                    ss.artist_longitude as longitude
    FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time,
                      hour,
                      day,
                      week,
                      month,
                      year,
                      weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
        DATE_PART(hour, start_time) as hour,
            DATE_PART(day, start_time) as day,
            DATE_PART(week, start_time) as week,
            DATE_PART(month, start_time) as month,
            DATE_PART(year, start_time) as year,
            DATE_PART(week, start_time) as weekday
        
    FROM staging_events se
    WHERE se.page = 'NextSong'; 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
