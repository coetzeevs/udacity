import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator


def log_oldest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")

def log_youngest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")
        
def log_lifetime_rides():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT bikeid FROM lifetime_rides ORDER BY cnt DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Most lifetime rides was done on BikeID {records[0][0]}")

def log_city_stations():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT city FROM city_station_counts ORDER BY cnt DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"City with the most stations is {records[0][0]}")
        
dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)

create_oldest_task = PostgresOperator(
    task_id="create_oldest",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest
)

create_youngest_task = PostgresOperator(
    task_id="create_youngest",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_youngest_task = PythonOperator(
    task_id="log_youngest",
    dag=dag,
    python_callable=log_youngest
)

create_lifetime_rides_task = PostgresOperator(
    task_id="create_lifetime_rides",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
            SELECT bikeid, COUNT(bikeid) as cnt
            FROM trips
            GROUP BY bikeid
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_lifetime_rides_task = PythonOperator(
    task_id="log_lifetime_rides",
    dag=dag,
    python_callable=log_lifetime_rides
)

create_city_stations_task = PostgresOperator(
    task_id="create_city_stations",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS(
            SELECT city, COUNT(city) as cnt
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_city_stations_task = PythonOperator(
    task_id="log_city_stations",
    dag=dag,
    python_callable=log_city_stations
)

create_oldest_task >> log_oldest_task
create_youngest_task >> log_youngest_task
create_lifetime_rides_task >> log_lifetime_rides_task
create_city_stations_task >> log_city_stations_task
