import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Running query:")
        print(query)
        cur.execute(query)
        print("executed")
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Running query:")
        print(query)
        cur.execute(query)
        print("executed")
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print("Loading staging tables...")
    load_staging_tables(cur, conn)
    print("Load analytics tables...")
    insert_tables(cur, conn)
    print("Finishing...")

    conn.close()


if __name__ == "__main__":
    main()
