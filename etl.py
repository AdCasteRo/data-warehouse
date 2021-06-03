import configparser
import psycopg2
from time import time
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load the information from S3 to staging_events and staging_songs. 
    Keyword arguments:
    cur -- Server side cursor
    conn -- Psycopg2 database session
    """
    for query in copy_table_queries:
        loadTimes = []
        t0 = time()
        print("======= LOADING STAGING TABLE =======")
        print(query)
        
        cur.execute(query)
        conn.commit()

        loadTime = time()-t0
        loadTimes.append(loadTime)
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))

def insert_tables(cur, conn):
    """
    Copy and transform the data from the staging tables to the star tables.
    Keyword arguments:
    cur -- Server side cursor
    conn -- Psycopg2 database session
    """
    for query in insert_table_queries:
        loadTimes = []
        t0 = time()
        print("======= LOADING DATABASE TABLE =======")
        print(query)
        
        cur.execute(query)
        conn.commit()
        
        loadTime = time()-t0
        loadTimes.append(loadTime)
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))

def main():
    """Load the information from S3, and copy it to the database"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Loading staging tables")
    load_staging_tables(cur, conn)
    print("Staging tables loaded")
    print("Loading database")
    insert_tables(cur, conn)
    print("Database loaded")
    conn.close()


if __name__ == "__main__":
    main()
