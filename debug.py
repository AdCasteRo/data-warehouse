import configparser
import psycopg2
from sql_queries import errors_query


def debug_queries(cur, conn):
    for query in errors_query:
        cur.execute(query)
        conn.commit()
        print("The number of parts: ", cur.rowcount)
        row = cur.fetchone()
        while row is not None:
            print(row)
            row = cur.fetchone()
        cur.close()
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    debug_queries(cur, conn)    
    
    
    conn.close()


if __name__ == "__main__":
    main()