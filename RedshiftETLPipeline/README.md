# Sparkify Project

## Background

<p>A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Querying the data is a cumbersome process at the moment and is the data on user activity resides in JSON logs that are streamed to an S3 storage bucket in Amazon Web Services, together with JSON metadata on the songs in their app. A typical analytics team or analyst will have a difficult time manipulating JSON formatted data and extracting valuable insights, easily and efficiently. The Sparkify team wants a Redshift database with tables designed to optimize queries on song play analysis, which uses the data files stored in the S3 bucket as source.</p>

<p>Components involved in the desired solution: </p>

- 1. Create the logical data models, optimising for queries on song play analysis.
- 2. Write the DDL scripts and other functions required to create the data models and insert the data from the source files.
- 3. Automate an ETL pipeline that transfers data from staging tables, populated by the data stored in S3.

### Sparkify Datasets

- **Song dataset**: All song data is in JSON format under subdirectories stored in an S3 bucket: *s3://udacity-dend/song_data*

- **Log dataset**: All log data are in JSON format under a subdirectories stored in an S3 bucket: *s3://udacity-dend/log_data* 


### Database Schema
The database consists of a fact table and four dimension tables, structured in a start schema.
- staging_events
- staging_songs
- fact_songplays
- dim_songs
- dim_artists
- dim_users
- dim_time

See below for a breakdown of the individual table structures.

#### Staging Table(s)
**staging_events**
- artist VARCHAR,
- auth VARCHAR(20),
- first_name VARCHAR,
- gender VARCHAR(2),
- item_in_session INTEGER,
- last_name VARCHAR,
- length REAL,
- level VARCHAR(10),
- location VARCHAR(100),
- method VARCHAR(5),
- page VARCHAR(30),
- registration BIGINT,
- session_id INTEGER,
- song VARCHAR,
- status INTEGER,
- ts BIGINT,
- user_agent VARCHAR,
- user_id INTEGER

**staging_songs**
- artist_id VARCHAR(50),
- artist_latitude VARCHAR(30),
- artist_location VARCHAR,
- artist_longitude VARCHAR(30),
- artist_name VARCHAR,
- duration REAL,
- num_songs INTEGER,
- song_id VARCHAR(50),
- title VARCHAR,
- year INTEGER

#### Fact Table(s)
**fact_songplays**
- songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
- start_time TIMESTAMP NOT NULL,
- user_id INTEGER NOT NULL,
- level VARCHAR,
- song_id VARCHAR,
- artist_id VARCHAR,
- session_id INTEGER NOT NULL DISTKEY,
- location VARCHAR,
- user_agent VARCHAR

#### Dimension Table(s)
**dim_users**
- user_id INTEGER PRIMARY KEY DISTKEY,
- first_name VARCHAR NOT NULL,
- last_name VARCHAR NOT NULL,
- gender VARCHAR NOT NULL,
- level VARCHAR NOT NULL
_The collection of users listening on the platform_

**dim_songs**
- song_id VARCHAR PRIMARY KEY DISTKEY,
- title VARCHAR NOT NULL,
- artist_id_fk VARCHAR NOT NULL,
- year INTEGER SORTKEY,
- duration REAL
_A collection of songs being listened to by users on the platform_

**dim_artists**
- artist_id VARCHAR PRIMARY KEY DISTKEY,
- name VARCHAR NOT NULL SORTKEY,
- location VARCHAR,
- latitude VARCHAR(30),
- longitude VARCHAR(30)
_A collection of artists being listened to by users on the platform_

**dim_time**
- id INTEGER IDENTITY(0,1) PRIMARY KEY,
- start_time TIMESTAMP NOT NULL DISTKEY SORTKEY,
- hour INTEGER NOT NULL,
- day INTEGER NOT NULL,
- week INTEGER NOT NULL,
- month INTEGER NOT NULL,
- year INTEGER NOT NULL,
- weekday INTEGER NOT NULL
_A record of the time instances when users are listening to songs on the platform_

### Solution discussion
<p>
An RDB is chosen for this particular solution for the following reasons:
<ul>
<li>ACID transactions</li>
<li>There is a need to use JOINS when querying the data</li>
<li>SQL lends itself well to analysis, including but not limited to aggregations and other calculations</li>
<li>Referential integrity</li>
</ul>
</p>
<p>Further, a star schema is chosen to reduce redundancy in storing data and to faciliate interpretability of the individual data tables. </p>

<p>A note on Redshift: the solution infrastructure allows for parallelisation of queries run against the DB tables, which speeds up basic CRUD operations, making it
and ideal choice when the need for an RDB is also present. This way, a "best of both worlds" scenario is available to analysts working with large datasets.</p>

### Project structure

Files in the repository for the project:
1. **helpers** - folder containing **functions.py** which is used during startup and deletion of the Redshift cluster.
2. **notebooks** - folder containing Jupyter notebooks used for developing and testing the solution.
3. **__init__.py** - Initialisation file used to initialise the helpers functions module.
4. **clean_up.py** - Script used to safely delete the Redshift cluster once usage is done.
5. **create_tables.py** - Script used to create the database table structure once the Redshift cluster is available.
6. **dwh.cfg** - Configuration file containing variables used during execution of the different scripts. _Note: See the section below on the config file for important information on its usage_. 
7. **etl.py** - Script to load data into the staging tables and finally insert the data into each of the fact and dim tables.
8. **init_redshift_cluster.py** - Script used to create the Redshift cluster if it doesn't already exist. The script outputs the host address which needs to be used in the configuration file.
9. **README.md** - Markdown file containing project information.
10. **sql_queries.sql** - Script containing all SQL queries required to create the analytics DB.

### Configuration file (dwh.cfg)
Two things are important to note:
1. The AWS SECRET and KEY values are omitted for security reasons. The user attempting to run this repo's content should create a new user account in their AWS console and add those security credentials into this file.
2. Once the Redshift cluster has been initialised the script outputs the host where the cluster is running. The user attempting to run this repo's content should copy this string and add it to the configuration file.

### How to run the project

1.  Initialise the Redshift cluster:
```bash
python init_redshift_cluster.py
```

2. Create the database tables:
```bash
python create_tables.py
```

3. Populate the database with data:
```bash
python etl.py
```

_The database is now up and running and ready for analytical work. Once done, remember to clean up after yourself to reduce costs of running a Redshift cluster._

4. [Optional] - clean up the Redshift cluster:
```bash
python clean_up.py
```

### Example queries and results
<p>Use these queries as examples to explore the database tables and get started analysing the data:</p>

**Explore users**
_Count the number of users listening on the platform_

Input:
```sql
SELECT
    count(user_id) 
FROM dim_users;
```

Output:
```bash
 count 
-------
    96
(1 row)
```

**Explore artists**
_Count the number of artists being listened to on the platform_

Input:
```sql
SELECT
    count(artist_id) 
FROM dim_artists;
```

Output:
```bash
-------
  10025
(1 row)
```


**Explore frequency of artist play**
_Determine which artists are being listened to the most_

Input:
```sql
SELECT
    da.name AS artist_name, 
    count(*) AS listen_count 
FROM fact_songplays sp
JOIN dim_artists da
    ON da.artist_id = sp.artist_id
GROUP BY 1 order by 2 desc
LIMIT 10;
```

Output:
```bash
artist_name                                     | listen_count
------------------------------------------------+--------------
Dwight Yoakam                                   | 37
Kid Cudi / Kanye West / Common                  | 10
Kid Cudi                                        | 10
Ron Carter                                      | 9
Lonnie Gordon                                   | 9
B.o.B                                           | 8
Usher                                           | 6
Muse                                            | 6
Usher featuring Jermaine Dupri                  | 6
Richard Hawley And Death Ramps_ Arctic Monkeys  | 5
```
