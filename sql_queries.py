# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = "create table if not exists songplays (songplay_id int, start_time bigint not null, user_id int not null, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar, primary key (songplay_id, start_time, session_id));"

user_table_create = "create table if not exists users (user_id int primary key, first_name varchar, last_name varchar, gender varchar, level varchar);"

song_table_create = "create table if not exists songs (song_id varchar primary key, title varchar, artist_id varchar, year int, duration float);"

artist_table_create = "create table if not exists artists (artist_id varchar primary key, artist_name varchar, artist_location varchar, artist_latitude float, artist_longitude float);"

time_table_create = "create table if not exists time (start_time timestamp, hour int, day int, week int, month int, year int, weekday int);"

# INSERT RECORDS

songplay_table_insert = ("insert into songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) "
                         "values (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                         "")

user_table_insert = ("insert into users (user_id, first_name, last_name, gender, level) "
                     "values (%s, %s, %s, %s, %s) "
                     "on conflict(user_id) do update set level = excluded.level ")

song_table_insert = ("insert into songs (song_id, title, artist_id, year, duration) "
                     "values (%s, %s, %s, %s, %s) "
                     "on conflict(song_id) do nothing")

artist_table_insert = ("insert into artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude) " 
                       "values (%s, %s, %s, %s, %s) "
                       "on conflict(artist_id) do nothing")

time_table_insert = time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)

SELECT a.start_time,

EXTRACT (HOUR FROM a.start_time), EXTRACT (DAY FROM a.start_time),

EXTRACT (WEEK FROM a.start_time), EXTRACT (MONTH FROM a.start_time),

EXTRACT (YEAR FROM a.start_time), EXTRACT (WEEKDAY FROM a.start_time) FROM

(SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplays) a;

""")

# FIND SONGS

song_select = ("select song_id, artists.artist_id "
               "from songs join artists on songs.artist_id = artists.artist_id "
               "where title = %s and artist_name = %s and duration = %s")

# QUERY LISTS

create_table_queries = [artist_table_create, song_table_create, songplay_table_create, user_table_create, time_table_create]
drop_table_queries = [artist_table_drop, song_table_drop, songplay_table_drop, user_table_drop, time_table_drop]