import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *



def process_song_file(cur, filepath):
    """Reads songs log file row by row, selects needed fields and inserts them into song and artist tables.
        Parameters:
            cur (psycopg2.cursor()): Cursor of the sparkifydb database
            filepath (str): Filepath of the file to be analyzed
    """




    df=pd.read_json(filepath,lines=True)
    for j,row in df.iterrows():
            n, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year =row
            cur.execute(song_table_insert,[song_id,title,artist_id,year,duration])

            cur.execute(artist_table_insert, [artist_id, artist_name, artist_location,artist_latitude,artist_longitude])

def process_log_file(cur, filepath):
    """Reads user activity log file row by row, filters by NexSong, selects needed fields, transforms them and inserts
    them into time, user and songplay tables.
            Parameters:
                cur (psycopg2.cursor()): Cursor of the sparkifydb database
                filepath (str): Filepath of the file to be analyzed
    """
    df=pd.read_json(filepath,lines=True)
    df2=df
    df=df[df['page']=='NextSong']
    ser=pd.to_datetime(df['ts'],unit='ms')
    times=[]
    for i in ser:
        times.append([i,i.hour,i.day,i.week,i.month,i.year,i.day_name()])
    for i in times:
        cur.execute(time_table_insert,i)
    df=df[['userId','firstName','lastName','gender','level']]
    for i,row in df.iterrows():
        cur.execute(users_table_insert,list(row))
    for i, row in df2.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        res = cur.fetchone()
        if res:
            song_id, artist_id = res
        else:
            song_id, artist_id = None, None

        songplay_data = (
        i, pd.to_datetime(row.ts, unit='ms'),int(row.userId), row.level, song_id, artist_id, row.sessionId,
        row.location, row.userAgent)
        cur.execute(songplays_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Walks through all files nested under filepath, and processes all logs found.
    Parameters:
        cur (psycopg2.cursor()): Cursor of the sparkifydb database
        conn (psycopg2.connect()): Connectio to the sparkifycdb database
        filepath (str): Filepath parent of the logs to be analyzed
        func (python function): Function to be used to process each log
    Returns:
        Name of files processed
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))



    # iterate over files and process
    for datafile in all_files:
        func(cur, datafile) ######### de function zy procces song file bta5od l filepath w currsor
        conn.commit()

    return all_files


def main():
    """Function used to extract, transform all data from song and user activity logs and load it into a PostgreSQL DB
        Usage: python etl.py
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb2 user=postgres password=261998")
    cur = conn.cursor()

    process_data(cur, conn, filepath='C:/Users/AG/Downloads/Data-engineering-nanodegree-master/Data-engineering-nanodegree-master/1_dend_data_modeling/P1_Postgres_Data_Modeling_and_ETL/data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='C:/Users/AG/Downloads/Data-engineering-nanodegree-master/Data-engineering-nanodegree-master/1_dend_data_modeling/P1_Postgres_Data_Modeling_and_ETL/data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
