# Project: Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation 
and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best 
tool to achieve this is Apache Airflow.

## Context
The task at hand is to create high grade data  pipelines that are dynamic and built from 
reusable tasks, can be monitored, and allow easy backfills. 
They have also noted that the data quality plays a big part when analyses are 
executed on top the data warehouse and want to run tests against their datasets after the 
ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in 
Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in 
the application and JSON metadata about the songs the users listen to.

## Project Overview
This project introduces the user to the core concepts of Apache Airflow. 
The project is made up of writing custom operators to perform tasks such as staging the data, 
filling the data warehouse, and running checks on the data as the final step.

All the imports are taken care of and four empty operators were provided that needed to be 
implemented into functional pieces of a data pipeline. 
The template also contained a set of tasks that needed to be linked to achieve a coherent 
and sensible data flow within the pipeline.

All the SQL transformations are provided. Thus, there is no need to write the ETL itself, 
but it need to be executed custom operators.

## Datasets
For this project, there are two datasets. Here are the s3 links for each:

- Log data: s3://udacity-dend/log_data
- Song data: s3://udacity-dend/song_data

## DAG
The DAG for this pipeline looks as follows:
![Sparkify ETL DAG](https://github.com/coetzeevs/udacity/blob/master/AirflowETLPipeline/media/SparkifyDAG.png?raw=true)

### Udacity reviewer's note
Ignore the content in the `resources` folder. All project-related files are contained in the following folders:
- [dags](https://github.com/coetzeevs/udacity/tree/master/AirflowETLPipeline/dags)
- [plugins](https://github.com/coetzeevs/udacity/tree/master/AirflowETLPipeline/plugins)

