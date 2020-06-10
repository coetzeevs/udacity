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
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_artists,
    task_id="create_artists"
)

create_songplays_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_songplays,
    task_id="create_songplays"
)

create_songs_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_songs,
    task_id="create_songs"
)

create_time_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_time,
    task_id="create_time"
)

create_users_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_users,
    task_id="create_users"
)

# Staging tables
create_staging_events_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_staging_events,
    task_id="create_staging_events"
)

create_staging_songs_task = PostgresOperator(
    dag=dag,
    postgres_conn_id="redshift",
    sql=CreateTablesSqlQueries.create_staging_songs,
    task_id="create_staging_songs"
)

# Move data from S3 to staging tables
stage_events_to_redshift = StageToRedshiftOperator(
    aws_credentials_id='aws_credentials',
    dag=dag,
    json_option="s3://udacity-dend/log_json_path.json",
    redshift_conn_id='redshift',
    region='us-west-2',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    table='staging_events',
    task_id='stage_events'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    aws_credentials_id='aws_credentials',
    dag=dag,
    json_option="auto",
    redshift_conn_id='redshift',
    region='us-west-2',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    table='staging_songs',
    task_id='stage_songs'
)

# Load data from staging tables to final destination tables
# Fact table
load_songplays_table = LoadFactOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql=InsertSqlQueries.songplay_table_insert,
    table="songplays",
    task_id='load_songplays_fact_table'
)

# Dimension tables
load_user_dimension_table = LoadDimensionOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql=InsertSqlQueries.user_table_insert,
    table="users",
    task_id='load_user_dim_table'
)

load_song_dimension_table = LoadDimensionOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql=InsertSqlQueries.song_table_insert,
    table="songs",
    task_id='load_song_dim_table'
)

load_artist_dimension_table = LoadDimensionOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql=InsertSqlQueries.artist_table_insert,
    table="artists",
    task_id='load_artist_dim_table',
    upsert=True,
    p_key="artistid"
)

load_time_dimension_table = LoadDimensionOperator(
    dag=dag,
    redshift_conn_id="redshift",
    sql=InsertSqlQueries.time_table_insert,
    table='"time"',
    task_id='load_time_dim_table'
)

# Check data quality
quality_check_users_task = DataQualityOperator(
    column="userid",
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    task_id='quality_check_users'
)

quality_check_artists_task = DataQualityOperator(
    column="artistid",
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    task_id='quality_check_artists'
)

quality_check_time_task = DataQualityOperator(
    column="start_time",
    dag=dag,
    redshift_conn_id="redshift",
    table='"time"',
    task_id='quality_check_time'
)

quality_check_songs_task = DataQualityOperator(
    assert_value=0,
    column="songid",
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    task_id='quality_check_songs',
    test_query="SELECT COUNT(*) FROM songs where duration = 0;"
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

# create tables
start_operator >> create_artists_task
start_operator >> create_songplays_task
start_operator >> create_songs_task
start_operator >> create_staging_events_task
start_operator >> create_staging_songs_task
start_operator >> create_time_task
start_operator >> create_users_task

# load staging tables
create_staging_events_task >> stage_events_to_redshift
create_staging_songs_task >> stage_songs_to_redshift

# load fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# load dimension tables
create_artists_task >> load_artist_dimension_table
create_songplays_task >> load_songplays_table
create_songs_task >> load_song_dimension_table
create_time_task >> load_time_dimension_table
create_users_task >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table

# run quality checks
load_artist_dimension_table >> quality_check_artists_task
load_song_dimension_table >> quality_check_songs_task
load_time_dimension_table >> quality_check_time_task
load_user_dimension_table >> quality_check_users_task

# finish
quality_check_artists_task >> end_operator
quality_check_songs_task >> end_operator
quality_check_time_task >> end_operator
quality_check_users_task >> end_operator
