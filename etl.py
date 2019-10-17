import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time


def load_staging_tables(cur, conn):
    
    """ 
    This function will load data from log_data and song_data JSON files and 
insert into the staging tables staging_events and staging_songs 
    
    Parameters:
        cur: curser to the database, used to execute queries.
        conn: connection to the database.
        
    Return:
        None    
    
    """
    
    print('Loading data from S3 to staging tables in AWS Redshift')
    t = time()
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print('Tables loading: DONE')
    print('Processing time: {} second'.format(time()-t))


def insert_tables(cur, conn):
    
    """ 
    This function will insert data from staging tables into analysis tables: songplays, users, songs, artists, time. 
    
    Parameters:
        cur: curser to the database, used to execute queries.
        conn: connection to the database.
        
    Return:
        None    
    
    """
    
    print('Inserting data from staging tables to analysis tables')
    t = time()
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print('Tables Inserted: DONE')
    print('Processing time: {} second'.format(time()-t))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()