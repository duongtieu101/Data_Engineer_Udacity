import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    Input:
        conn:connection to db
        cur: cursor that run the query
    Output: Drop all tables that in drop_table_queries list
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    Input:
        conn:connection to db
        cur: cursor that run the query
    Output: Create all tables that in create_table_queries list
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Get the information config from dwh.cfg and connect to AWS Redshift.
        
    - Drops all the tables if exist.  
    
    - Creates all needed tables. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()