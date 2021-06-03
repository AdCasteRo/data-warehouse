import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all the tables created if they exists
    Keyword arguments:
    cur -- Server side cursor
    conn -- Psycopg2 database session
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create all the tables needed
    Keyword arguments:
    cur -- Server side cursor
    conn -- Psycopg2 database session
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Drop old tables and create new ones"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Dropping old tables")
    drop_tables(cur, conn)
    print("Old tables dropped")
    print("Creating new tables")
    create_tables(cur, conn)
    print("New tables created")

    conn.close()


if __name__ == "__main__":
    main()