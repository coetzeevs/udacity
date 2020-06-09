from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    PostgresOperator,
    StageToRedshiftOperator
)
from helpers import CreateTablesSqlQueries, InsertSqlQueries

default_args = {
    'catchup': False,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'owner': 'coetzee',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 12),
}

dag = DAG(
    'sparkify_etl_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=1,
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create destination SQL tables
# Dimension tables
create_artists_task = PostgresOperator(
    task_id="create_artists",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_artists
)

create_songplays_task = PostgresOperator(
    task_id="create_songplays",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_songplays
)

create_songs_task = PostgresOperator(
    task_id="create_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_songs
)

create_time_task = PostgresOperator(
    task_id="create_time",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_time
)

create_users_task = PostgresOperator(
    task_id="create_users",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_users
)

# Staging tables
create_staging_events_task = PostgresOperator(
    task_id="create_staging_events",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_staging_events
)

create_staging_songs_task = PostgresOperator(
    task_id="create_staging_songs",
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_staging_songs
)

# Move data from S3 to staging tables
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
)

# Load data from staging tables to final destination tables
# Fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

# Dimension tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

# Check data quality
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# create tables
start_operator >> create_artists_task
start_operator >> create_songplays_task
start_operator >> create_songs_task
start_operator >> create_time_task
start_operator >> create_users_task
start_operator >> create_staging_events_task
start_operator >> create_staging_songs_task

# load staging tables
create_staging_events_task >> stage_events_to_redshift
create_staging_songs_task >> stage_songs_to_redshift

# load fact table
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

# load dimension tables
create_users_task >> load_user_dimension_table
load_songplays_table >> load_user_dimension_table

create_time_task >> load_time_dimension_table
load_songplays_table >> load_time_dimension_table

create_songs_task >> load_song_dimension_table
load_songplays_table >> load_song_dimension_table

create_artists_task >> load_artist_dimension_table
load_songplays_table >> load_artist_dimension_table

# run quality checks
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks

# finish
run_quality_checks >> end_operator