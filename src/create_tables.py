import os
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(("="*10) + " DROPING TABLES " + ("="*10))
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print(("="*10) + " CREATING TABLES " + ("="*10))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config_dir = os.path.dirname(os.getcwd()) + "\config\dwh.cfg"
    config.read(config_dir)

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()