import os
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger()


def create_spark_session():
    """
    Function to initiate a Spark session and return the resulting object.
    
    Returns: SparkSession object

    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
    spark.conf.set("spark.executor.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")
    spark.conf.set("spark.driver.extraJavaOptions","-Dcom.amazonaws.services.s3.enableV4=true")

    return spark


def create_s3_bucket(config, acl="private"):
    """
    Create S3 bucket in AWS region
    
    Args:
        acl: access control level for the bucket being created - set it to public to be accessible from anywhere
        config: configuration settings from file
    
    Returns: created bucket/None
    
    """
    import boto3
    from botocore.exceptions import ClientError

    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'SECRET_ACCESS_KEY')

    try:
        if config.get('AWS', 'REGION') is None:
            client = boto3.client('s3')
            client.create_bucket(Bucket=config.get('S3', 'BUCKET'))
        else:
            client = boto3.client('s3', region_name=config.get('AWS', 'REGION'))
            loc = {'LocationConstraint': config.get('AWS', 'REGION')}
            client.create_bucket(
                Bucket=config.get('S3', 'BUCKET'),
                CreateBucketConfiguration=loc,
                ACL=acl
            )
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except ClientError as e:
        logging.error(e)
        raise e
    
    return
