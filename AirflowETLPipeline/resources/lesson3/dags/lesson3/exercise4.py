import datetime

import airflow
from airflow import DAG

from airflow.operators.subdag_operator import SubDagOperator

import sql_statements
from airflow.operators import FactsCalculatorOperator
from lesson3.exercise3.subdag import get_s3_to_redshift_dag


start_date=datetime.datetime.utcnow()

dag = DAG(
    "lesson3.exercise4", 
    start_date=start_date,

)

copy_trips_task_id = "copy_trips_subdag"
copy_trips_subdag_task = SubDagOperator(
    subdag=get_s3_to_redshift_dag(
        parent_dag_name="lesson3.exercise4",
        task_id=copy_trips_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="trips",
        create_sql_stmt=sql_statements.CREATE_TRIPS_TABLE_SQL,
        s3_bucket="udacity-dend",
        s3_prefix="data-pipelines",
        s3_key="divvy/unpartitioned/divvy_trips_2018.csv",
        start_date=start_date,
    ),
    task_id=copy_trips_task_id,
    dag=dag,
)

calculate_facts_task = FactsCalculatorOperator(
    dag=dag,
    task_id="calculate_facts",
    redshift_conn_id="redshift",
    origin_table="trips",
    destination_table="fact_tripduration",
    fact_column="tripduration",
    groupby_column="bikeid"
)

copy_trips_subdag_task >> calculate_facts_task
