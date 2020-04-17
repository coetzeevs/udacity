import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Function to drop DB tables
    Args:
        cur: DB connection cursor (object)
        conn: DB connection (object)

    Returns: None

    """
    print('Preparing DB by dropping tables if they exist...')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Function to create DB tables
    Args:
        cur: DB connection cursor (object)
        conn: DB connection (object)

    Returns: None

    """
    print('Creating tables to spec...')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main script function to initiate a DB connection, drop the existing tables, and create new tables
    Returns: None

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={host} dbname={db_name} user={db_user} password={db_password} port={db_port}".format(
        **dict(i for i in config['CLUSTER'].items())))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print('Done. DB ready.')


if __name__ == "__main__":
    main()
