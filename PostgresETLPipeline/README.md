# Sparkify Project

## Background

<p>A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Querying the data is a cumbersome process at the moment and is the data resides in JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. A typical analytics team or analyst will have a difficult time manipulating JSON formatted data and extracting valuable insights, easily and efficiently. The Sparkify team wants a Postgres database with tables designed to optimize queries on song play analysis.</p>

<p>Components involved in the desired solution: </p>

- 1. Create the logical data models, optimising for queries on song play analysis.
- 2. Write the DDL scripts and other functions required to create the data models and insert the data from the JSON files.
- 3. Automate an ETL pipeline that transfers data from two local directories to the designed DB.

### Sparkify Datasets

- **Song dataset**: All song data is in JSON format under a subdirectory from root in */data/song_data*.

- **Log dataset**: All log data are in JSON format under a subdirectory from root in */data/log_data*. 


### Database Schema
The database consists of a fact table and four dimension tables, structured in a start schema. 
- fact_songplays
- dim_songs
- dim_artists
- dim_users
- dim_time

See below for a breakdown of the individual table structures.

#### Fact Table
**fact_songplays**
- songplay_id (INT) PRIMARY KEY
- start_time (TIME) NOT NULL
- user_id (INT) NOT NULL
- level (VARCHAR)
- song_id (VARCHAR)
- artist_id (VARCHAR)
- session_id (INT)
- location (VARCHAR)
- user_agent (VARCHAR)

#### Dimension Tables
**dim_users**
- user_id (INT) PRIMARY KEY
- first_name (VARCHAR) 
- last_name (VARCHAR) 
- gender (VARCHAR)
- level (VARCHAR)
_The collection of users listening on the platform_

**dim_songs**
- song_id (VARCHAR) PRIMARY KEY
- title (VARCHAR)
- artist_id_fk (VARCHAR) NOT NULL
- year (INT)
- duration (FLOAT) 
_A collection of songs being listened to by users on the platform_

**dim_artists**
- artist_id (VARCHAR) PRIMARY KEY
- name (VARCHAR)
- location (VARCHAR)
- latitude (NUMERIC)
- longitude (NUMERIC)
_A collection of artists being listened to by users on the platform_

**dim_time**
- id SERIAL PRIMARY KEY
- start_time (TIME) NOT NULL
- hour (INT)
- day (INT)
- week (INT)
- month (INT)
- year (INT)
- weekday (VARCHAR)
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

### Project structure

Files in the repository for the project:
1. **data** - JSON files containing data for the ELT project.
2. **create_tables.py** - Functions to connect to the DB and DROP and CREATE tables. This is the first step in the ETL process. In a real setting you'd run this only once, unless something went wrong and you need to rebuild your DB.
3. **etl.ipynb** - Development Jupyter notebook to process a single song data file and log file and load the ETL data into the respective DB tables.
4. **README.md** - Markdown file containint project information.
5. **sql_queries.py** - Python script that contains all the SQL queries for the ETL project.
6. **etl.py** - Functions to extract and transform data from song_data and log_data files and load into the DB tables. 
7. **test.ipynb** - Jupyter notebook to test whether the ETL process has worked.


### How to run project

1. Set up a database connection and create the DB:
```bash
python create_tables.py
```

2. Populate the data tables with data from the songs and logs files:

```bash
python etl.py
```

3. [Optional] Verify the data loaded correctly:
- Open **test.ipynb** and run the cells from top to bottom. 
- Restart the kernel to shut down the connection to the database.

### Example queries and results
<p>Use these queries as examples to explore the database tables and get started analysing the data:</p>

**Explore users**
_Count the number of users listening on the platform_

Input:
```sql
select count(user_id) from dim_users;
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
select count(artist_id) from dim_artists;
```

Output:
```bash
-------
    69
(1 row)
```


**Explore frequency of artist play**
_Determine which artists are being listened to the most_

Input:
```sql
select artist_id, count(*) as listen_count from fact_songplays group by 1 order by 2 desc;
```

Output:
```bash
     artist_id      | listen_count 
--------------------+--------------
                    |         6819
 SOZCTXZ12AB0182364 |            1
```
