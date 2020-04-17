import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print('Loading staging tables from S3 bucket files...')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    print('Populating final tables from staging tables...')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}".format(**dict(i for i in config['CLUSTER'].items())))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    
    print('Data load process done.')


if __name__ == "__main__":
    main()