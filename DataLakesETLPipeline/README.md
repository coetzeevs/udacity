# Data Lakes: The new Sparkify way

## Background

<p>A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Querying the data is a cumbersome process at the moment and is the data on user activity resides in JSON logs that are streamed to an S3 storage bucket in Amazon Web Services, together with JSON metadata on the songs in their app. A typical analytics team or analyst will have a difficult time manipulating JSON formatted data and extracting valuable insights, easily and efficiently. The Sparkify team wants a Data Lake designed to optimize analysing song play data, which uses the data files stored in the S3 bucket as source.</p>

## Project overview

The code base establishes an ETL pipeline using Spark to build a data lake hosted on AWS S3 for analytics purposes. The process loads songs and logs data in JSON format from AWS S3 and subsequently processes the data, turning it into analytics tables in a star schema. Spark is used to do this transformation. Finally the process writes these tables into partitioned parquet files. A star schema is used to facilitate running queries against the data to analyze user activity on the Sparkify app. 

## The data

### Datasets

- **Song dataset**: All song data is in JSON format under subdirectories stored in an S3 bucket: *s3://udacity-dend/song_data*

- **Log dataset**: All log data are in JSON format under a subdirectories stored in an S3 bucket: *s3://udacity-dend/log_data* 

### Data lake schema
The data lake consists of songplay data (acting as the fact table) and user, artist, song, and time datasets (acting as the dimension tables), which is all structured in a start schema.

### Tables
**songplays**
- songplay_id
- start_time
- user_id
- level
- song_id 
- artist_id
- session_id
- location
- user_agent

#### Dimension Table(s)
**users**
- user_id
- first_name
- last_name
- gender
- level
_The collection of users listening on the platform_

**songs**
- song_id
- title
- artist_id_fk
- year
- duration
_A collection of songs being listened to by users on the platform_

**artists**
- artist_id
- name
- location
- latitude
- longitude
_A collection of artists being listened to by users on the platform_

**time**
- start_time
- hour
- day
- week
- month
- year
- weekday
_A record of the time instances when users are listening to songs on the platform_

## File structure (project)

The project contains the following components:

* `etl.py` - main ETL file to orchestrate the process
* `functions.py` - collection of functions for processing the data
* `dl.cfg` [not tracked in this repo] contains AWS credentials
* `DevNotebook.ipynb` - Jupyter notebook containing the project functions ready to execute cell by cell.
* `data` - folder with zipped data for use locally, must be unzipped for use. 

## Project instructions

Clone this repo to your local file system. Edit the configuration file called `dl.cfg` with the following structure:

```
[AWS]
ACCESS_KEY_ID=<your_aws_access_key_id>
REGION=<your_aws_region>
SECRET_ACCESS_KEY=<your_aws_secret_access_key>
BUCKET=<your-bucket-name>
```

To run the pipeline from the command line, enter the following:

```
python etl.py
```
