import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Get data (log_data and song_data) from S3 and insert
        into staging_events and staging_songs tables.
    Input:
        conn:connection to db
        cur: cursor that run the query to connect the DB.
    Output:
        log_data in staging_events table.
        song_data in staging_songs table.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Process and insert data from staging tables to songplays, users, artists, songs and time tables.
    Input:
        conn:connection to db
        cur: cursor that run the query to connect the DB.
    Output:
        songplays, users, artists, songs and time tables.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Get the information config from dwh.cfg and connect to AWS Redshift.
        
    - Get data (log_data and song_data) from S3 and insert
        into staging_events and staging_songs tables.
    
    - Process and insert data from staging tables to songplays, users, artists, songs and time tables. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()