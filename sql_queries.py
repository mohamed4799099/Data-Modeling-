


songplay_table_drop="drop table if exists songplays"
users_table_drop="drop table if exists users"
songs_table_drop="drop table if exists songs"
artists_table_drop="drop table if exists artists"
time_table_drop="drop table if exists time"




songplays_table_create="create table if not exists songplays(songplay_id int primary key,start_time date,user_id int \
,level text, song_id text,artist_id text,\
session_id int,location text,user_agent text)"


users_table_create="create table if not exists users(user_id int primary key,first_name text,last_name text,gender text,level text)"
songs_table_create="create table if not exists songs(song_id text primary key,title text,artist_id text,year int,duration float)"
artists_table_create="create table if not exists artists(artist_id text primary key,name text,location text,lattitude float,longitude float)"
time_table_create="create table if not exists time(start_time date primary key,hour int,day int,week int,month int,year int,weekday text )"

songplays_table_insert="insert into songplays(songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)\
                       values(%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (songplay_id) do nothing"

users_table_insert="insert into users(user_id,first_name,last_name,gender,level)\
                   values(%s,%s,%s,%s,%s) on conflict (user_id) do nothing"

song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING;
""") ####### on conflict de m3naha lw 7d by8yer f row b index kan mwgod abl kda ana kda h5leh y3ml updata f nfs l row msh h3ml row gded

artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, lattitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("\
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)\
    VALUES (%s, %s, %s, %s, %s, %s, %s)\
    ON CONFLICT (start_time) DO NOTHING")

song_select = ("""
    SELECT song_id, artists.artist_id
    FROM songs JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s
    AND artists.name = %s
    AND songs.duration = %s
""")



create_table=[songplays_table_create,users_table_create,songs_table_create,artists_table_create,time_table_create]
drop_table=[songplay_table_drop,users_table_drop,songs_table_drop,artists_table_drop,time_table_drop]
