import configparser

from .functions import (
    create_spark_session,
    create_s3_bucket,
    process_song_data,
    process_log_data
)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['REGION']=config['AWS']['REGION']
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET_ACCESS_KEY']
os.environ['BUCKET']=config['AWS']['BUCKET']


def main():
    """
    Function to kick off ETL process.

    Args:
        

    Returns:

    """
    spark = create_spark_session()
    
    create_s3_bucket(acl="public-read")
    
    input_data = "s3a://udacity-dend/"
    output_data = f"s3a://{os.environ.get('BUCKET')}/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
    
    
if __name__ == '__main__':
    main()