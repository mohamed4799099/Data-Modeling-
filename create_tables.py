import psycopg2
from sql_queries import create_table, drop_table

def create_database():
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=postgres password=261998")
    cur = conn.cursor()
    conn.set_session(autocommit=True)
    #### 3mlt l connection dh m3 eny hrg3 a3ml db tanya asln ?? 3shan nta m7taga l cursor f enk t-execute new db
    #cur.execute("drop database if exists sparkifydb2 ")
    #cur.execute("create database if not exists sparkifydb2 WITH ENCODING 'utf8' TEMPLATE template0")
    conn.close()
    conn=psycopg2.connect("host=127.0.0.1 dbname=sparkifydb2 user=postgres password=261998")
    cur=conn.cursor()
    #conn.set_session(autocommit=True)
    return conn,cur


def drop_tables(cur,conn):
    for i in drop_table:
        cur.execute(i)
        conn.commit()


def create_tables(cur,conn):
    for i in create_table:
        cur.execute(i)
        conn.commit()

conn,cur=create_database()
drop_tables(cur,conn)
create_tables(cur,conn)
conn.close()