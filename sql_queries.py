import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_log;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS event_log (
                                   artist VARCHAR ENCODE ZSTD,
                                   auth VARCHAR ENCODE ZSTD NOT NULL,
                                   firstName VARCHAR ENCODE ZSTD,
                                   gender VARCHAR ENCODE ZSTD,
                                   iteminSession smallint ENCODE ZSTD NOT NULL,
                                   lastName VARCHAR ENCODE ZSTD,
                                   length DOUBLE PRECISION ENCODE ZSTD,
                                   level VARCHAR ENCODE ZSTD,
                                   location VARCHAR ENCODE ZSTD,
                                   method VARCHAR ENCODE ZSTD,
                                   page VARCHAR ENCODE ZSTD,
                                   registration DOUBLE PRECISION ENCODE ZSTD,
                                   sessionId INTEGER ENCODE ZSTD NOT NULL,
                                   song VARCHAR ENCODE ZSTD,
                                   status INTEGER ENCODE ZSTD,
                                   ts BIGINT ENCODE ZSTD,
                                   userAgent VARCHAR ENCODE ZSTD,
                                   userId SMALLINT ENCODE ZSTD);
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS songs_staging(
                                  num_songs SMALLINT ENCODE ZSTD,
                                  artist_id VARCHAR ENCODE ZSTD,
                                  artist_latitude DOUBLE PRECISION ENCODE ZSTD,
                                  artist_longitude DOUBLE PRECISION ENCODE ZSTD,
                                  artist_location VARCHAR ENCODE ZSTD,
                                  artist_name VARCHAR ENCODE ZSTD,
                                  song_id VARCHAR ENCODE ZSTD,
                                  title VARCHAR ENCODE ZSTD,
                                  duration DOUBLE PRECISION ENCODE ZSTD,
                                  year INTEGER ENCODE ZSTD);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
                            (songplay_id INTEGER IDENTITY(0,1) distkey, 
                            start_time BIGINT sortkey NOT NULL, 
                            user_id INT NOT NULL, 
                            level VARCHAR, 
                            song_id VARCHAR, 
                            artist_id VARCHAR, 
                            session_id INT, 
                            location VARCHAR, 
                            user_agent VARCHAR,
                            PRIMARY KEY (songplay_id),
                            CONSTRAINT start_time_fk FOREIGN KEY (start_time) REFERENCES time(start_time),
                            CONSTRAINT user_id_fk FOREIGN KEY (user_id) REFERENCES users(user_id),
                            CONSTRAINT song_id_fk FOREIGN KEY (song_id) REFERENCES songs(song_id),
                            CONSTRAINT artist_id_fk FOREIGN KEY (artist_id) REFERENCES artists(artist_id));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
                        (user_id INT sortkey, 
                        first_name VARCHAR, 
                        last_name VARCHAR, 
                        gender VARCHAR, 
                        level VARCHAR, 
                        PRIMARY KEY(user_id))diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
                        (song_id VARCHAR sortkey, 
                        title VARCHAR, 
                        artist_id VARCHAR NOT NULL, 
                        year INT, 
                        duration FLOAT, 
                        PRIMARY KEY(song_id))diststyle all;
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
                          (artist_id VARCHAR sortkey, 
                          name VARCHAR, 
                          location VARCHAR, 
                          lattitude FLOAT, 
                          longitude FLOAT, 
                          PRIMARY KEY(artist_id))diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                        (start_time TIMESTAMP, 
                        hour INT, 
                        day VARCHAR, 
                        week INT, 
                        month INT, 
                        year INT, 
                        weekday VARCHAR, 
                        PRIMARY KEY(start_time))diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy event_log from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 's3://udacity-dend/log_json_path.json';
""").format(LOG_DATA,ARN)

staging_songs_copy = ("""
copy songs_staging from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
                            FROM event_log e LEFT JOIN songs_staging s
                            ON e.song = s.title
                            AND e.artist = s.artist_name
                            WHERE page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        WITH _events AS (
                        SELECT userId, firstName, lastName, gender, level, ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS row
                        FROM event_log
                        )

                        SELECT userId, firstName, lastName, gender, level
                        FROM _events
                        WHERE row = 1 AND userId IS NOT NULL;""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT song_id, title, artist_id, year, duration
                        FROM songs_staging;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                          SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM songs_staging;""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        with _time AS (
                        SELECT (TIMESTAMP 'epoch' + ts/1000 * interval '1 second') AS ts_date
                        FROM event_log
                        )
                        
                        SELECT ts_date,
                        EXTRACT(HOUR FROM ts_date) AS hour,
                        EXTRACT(DAY FROM ts_date) AS day,
                        EXTRACT(WEEK FROM ts_date) AS week,
                        EXTRACT(MONTH FROM ts_date) AS month,
                        EXTRACT(YEAR FROM ts_date) AS year,
                        EXTRACT(DOW FROM ts_date) AS weekday
                        FROM _time;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
