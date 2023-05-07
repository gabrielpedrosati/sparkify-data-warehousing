import os
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(("="*10) + " LOADING TO STAGING AREA " + ("="*10))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(("="*10) + " INSERTING DATA TO DWH " + ("="*10))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config_dir = os.path.dirname(os.getcwd()) + "\config\dwh.cfg"
    config.read(config_dir)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()