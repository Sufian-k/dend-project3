import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
    This function will drop existing tables (if any) in the database. 
    
    Parameters:
        cur: curser to the database, used to execute queries.
        conn: connection to the database.
        
    Return:
        None
    
    """
    
    print('Drop existing tables')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    print('DONE')


def create_tables(cur, conn):
    
    """
    This function will create staging and analysis tables 
    
    Parameters:
        cur: curser to the database, used to execute queries.
        conn: connection to the database.
        
    Return:
        None
        
    """
    
    print('Creating staging and analysis tables')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print('DONE')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()