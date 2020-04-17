import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print('Preparing DB by dropping tables if they exist...')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    print('Creating tables to spec...')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}".format(**dict(i for i in config['CLUSTER'].items())))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print('Done. DB ready.')


if __name__ == "__main__":
    main()