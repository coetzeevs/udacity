import configparser
from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    udf, 
    col, 
    year, 
    month, 
    dayofmonth, 
    hour,
    weekofyear, 
    date_format, 
    dayofweek, 
    monotonically_increasing_id,
    max
)
from pyspark.sql.types import (
    StructType, 
    StructField as Fld, 
    StringType as Str, 
    DoubleType as Dbl, 
    IntegerType as Int, 
    TimestampType as Ts
)

logger = logging.getLogger()


def create_spark_session():
    """
    Function to initiate a Spark session and return the resulting object.
    
    Returns: SparkSession object

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
    spark.conf.set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    spark.conf.set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")

    return spark


def create_s3_bucket(acl="private"):
    """
    Create S3 bucket in AWS region
    
    Args:
        acl: access control level for the bucket being created - set it to public to be accessible from anywhere
    
    Returns: created bucket/None
    
    """
    import boto3
    from botocore.exceptions import ClientError
    
    try:
        if os.environ.get('REGION', None) is None:
            client = boto3.client('s3')
            client.create_bucket(Bucket=os.environ.get('BUCKET'))
        else:
            client = boto3.client('s3', region_name=os.environ.get('REGION'))
            loc = {'LocationConstraint': os.environ.get('REGION')}
            client.create_bucket(
                Bucket=os.environ.get('BUCKET'),
                CreateBucketConfiguration=loc,
                ACL=acl
            )
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except ClientError as e:
        logging.error(e)
        raise e
    
    return


def process_song_data(spark, input_data, output_data):
    """
    TL;DR: Read raw song data in S3 - transform - store as analytics data in S3
    
    Function to read in JSON data from an S3 bucket containing song data for the Sparkify 
    streaming service; define an appropriate analytics schema for songs and artists;
    extract the raw data into the defined schemas; store the resulting data as
    parquet files on S3. 
    
    Args:
        spark: Spark session object
        input_data: input S3 bucket string
        output_data: output S3 bucket string

    Returns: Parquet data stored in output_data directory / None
    
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # create song_data schema
    schema__song_data = StructType([
        # StructField(field_name, field_type, nullable_boolean)
        Fld("artist_id", Str(), False),
        Fld("artist_latitude", Str(), True),
        Fld("artist_longitude", Str(), True),
        Fld("artist_location", Str(), True),
        Fld("artist_name", Str(), False),
        Fld("song_id", Str(), False),
        Fld("title", Str(), False),
        Fld("duration", Dbl(), False),
        Fld("year", Int(), False)
    ])
    
    # read song data file
    song_df = spark.read.json(song_data, schema=schema__song_data)

    # extract columns to create songs table
    process_songs(song_df, spark, input_data, output_data)
        
    # extract columns to create artists table
    process_artists(song_df, spark, input_data, output_data)
    
    return


def process_log_data(spark, input_data, output_data):
    """
    TL;DR: Read raw log data in S3 - transform - store as analytics data in S3
    
    Function to read in JSON data from an S3 bucket containing logs data for the Sparkify 
    streaming service; define an appropriate analytics schema for time, users, and songplays;
    extract the raw data into the defined schemas; store the resulting data as
    parquet files on S3. 
    
    Args:
        spark: Spark session object
        input_data: input S3 bucket string
        output_data: output S3 bucket string

    Returns: Parquet data stored in output_data directory / None
    
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # create log_data schema
    schema__log_data = StructType([
        # StructField(field_name, field_type, nullable_boolean)
        Fld("artist", Str(), True),
        Fld("auth", Str(), False),
        Fld("firstName", Str(), True),
        Fld("gender", Str(), True),
        Fld("itemInSession", Int(), False),
        Fld("lastName", Str(), True),
        Fld("length", Dbl(), True),
        Fld("level", Str(), False),
        Fld("location", Str(), True),
        Fld("method", Str(), False),
        Fld("page", Str(), False),
        Fld("registration", Dbl(), True),
        Fld("sessionId", Int(), False),
        Fld("song", Str(), True),
        Fld("status", Int(), False),
        Fld("ts", Dbl(), False),
        Fld("userAgent", Str(), True),
        Fld("userId", Str(), True)
    ])

    # read log data file
    log_df = spark.read.json(log_data, schema=schema__log_data)
    
    log_df.printSchema()
    
    # filter by actions for song plays
    log_df = log_df.filter(col("page") == "NextSong" )

    # extract columns for users table
    process_users(log_df, spark, input_data, output_data)

    # create timestamp column from original timestamp column
    process_time(log_df, spark, input_data, output_data)

    # read in song data to use for songplays table
    process_songplays(log_df, spark, input_data, output_data)
    
    return
    

def process_songs(df, spark, input_data, output_data):
    """
    Function to extract song data from the given dataframe and store it into parquet files on S3.
    
    Args:
        df: Spark dataframe object
        spark: Spark session object
        input_data: input S3 bucket string
        output_data: output S3 bucket string

    Returns: Parquet data stored in S3

    """
    
    songs_table = df.select(
        "song_id", 
        "title", 
        "artist_id", 
        "year", 
        "duration"
    )
        
    # write songs table to parquet files partitioned by year and artist
    try:
        songs_table.write.mode(
            'overwrite'
        ).partitionBy(
            'year', 'artist_id'
        ).parquet(
            output_data + "songs/songs_table.parquet"
        )
    except Exception as e:
        logger.error(e)
        raise e

    return


def process_artists(df, spark, input_data, output_data):
    """
    Function to extract artist data from the given dataframe and store it into parquet files on S3.
    
    Args:
        df: Spark dataframe object
        spark: Spark session object
        input_data: input S3 bucket string
        output_data: output S3 bucket string

    Returns: Parquet data stored in S3

    """

    artists_table = df.select(
            "artist_id",
            col("artist_name").alias("name"),
            col("artist_location").alias("location"),
            col("artist_latitude").alias("latitude"),
            col("artist_longitude").alias("longitude")
    ).distinct()
        
    # write artists table to parquet files
    try:
        artists_table.write.mode(
            'overwrite'
        ).parquet(
            output_data + "artists/artists_table.parquet"
        )
    except Exception as e:
        logger.error(e)
        raise e
        
    return


def process_users(df, spark, input_data, output_data):
    """
    Function to extract user data from the given dataframe and store it into parquet files on S3.
    
    Args:
        df: Spark dataframe object
        spark: Spark session object
        input_data: input S3 bucket string
        output_data: output S3 bucket string

    Returns: Parquet data stored in S3

    """

    # to get only applicable rows: 
    #     - aggregate ts by user to find last activity date
    #     - filter where ts = max_ts & user_id != "" & user_id is not null
    users_table = df.withColumn(
        "max_ts", fn.max("ts").over(Window.partitionBy("userId"))
    ).filter(
        (
            (col("ts") == col("max_ts")) & (col("userId") != "") & (col("userId").isNotNull())
        )
    ).select(
        col("userID").alias("id"),
        col("firstName").alias("first_name"),
        col("lastName").alias("last_name"),
        "gender",
        "level"
    )
    
    # write users table to parquet files
    try:
        users_table.write.mode(
            'overwrite'
        ).parquet(
            output_data + "users/users_table.parquet"
        )
    except Exception as e:
        logger.error(e)
        raise e
        
    return


def process_time(df, spark, input_data, output_data):
    """
    Function to extract time data from the given dataframe and store it into parquet files on S3.

    Args:
        df: Spark dataframe object
        spark: Spark session object
        input_data: input S3 bucket string
        output_data: output S3 bucket string

    Returns: Parquet data stored in S3

    """
    
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x / 1000).replace(microsecond=0),
        Ts()
    )
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn(
        "hour", hour("start_time")
    ).withColumn(
        "day", dayofmonth("start_time")
    ).withColumn(
        "week", weekofyear("start_time")
    ).withColumn(
        "month", month("start_time")
    ).withColumn(
        "year", year("start_time")
    ).withColumn(
        "weekday", dayofweek("start_time")
    ).select(
        "start_time", 
        "hour", 
        "day", 
        "week", 
        "month", 
        "year", 
        "weekday"
    ).distinct()
    
    # write time table to parquet files partitioned by year and month
    try:
        time_table.write.mode(
            'overwrite'
        ).partitionBy(
            'year', 'month'
        ).parquet(
            output_data + "time/time_table.parquet"
        )
    except Exception as e:
        logger.error(e)
        raise e
    
    return


def process_songplays(df, spark, input_data, output_data):
    """
    Function to extract songplay data from the given dataframe and store it into parquet files on S3.

    Args:
        df: Spark dataframe object
        spark: Spark session object
        input_data: input S3 bucket string
        output_data: output S3 bucket string

    Returns: Parquet data stored in S3

    """
    
    songs = spark.read.parquet(output_data + 'songs/songs_table.parquet')
    
    # read in artist data to use for songplays table
    artists = spark.read.parquet(output_data + 'artists/artists_table.parquet')
    
    # read in time data to use for songplays table
    time_table = spark.read.parquet(output_data + 'time/time_table.parquet')

    # create a table joining songs and artists
    artists_songs = songs.join(
        artists, "artist_id", "full"
    ).select(
        "song_id", "title", "artist_id", "name", "duration"
    )
    
    # extract columns from joined song and log datasets to create songplays table
    # inflate logs dataframe with song/artist data by left joining artists_songs
    songplays_table = df.join(
        artists_songs,
        [
            df.song == songs.title,
            df.artist == songs.name,
            df.length == songs.duration
        ],
        "left"
    )
    
    # inflate the new songplays dataframe with the time table data
    # select the final table schema columns from the joined dataframes
    # create a self-incrementing songplay ID column
    songplays_table = songplays_table.join(
            time_table, "start_time", "left"
        ).select(
            "start_time",
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent"),
            "year",
            "month"
        ).withColumn(
            "songplay_id", monotonically_increasing_id()
        )

    # write songplays table to parquet files partitioned by year and month
    try:
        songplays_table.write.mode(
            'overwrite'
        ).partitionBy(
            'year', 'month'
        ).parquet(
            output_data + "songplays/songplays_table.parquet"
        )
    except Exception as e:
        logger.error(e)
        raise e
        
    return
