import configparser
import logging
import os

from configs.sources import set_source_dict_cfg

from ops.storage.functions import Initializer
from ops.data.data_cleaning import DataCleaningOps
from ops.data.data_transformation import DataTransformationOps
from ops.etl.source_ops import SourceOps
from ops.etl.warehouse_ops import WarehouseOps

config = configparser.ConfigParser()
config.read_file(open(os.path.realpath('./configs/dwh.cfg')))
logger = logging.getLogger()
output_data_path = f"s3a://{config.get('S3', 'BUCKET')}/{config.get('S3', 'OUTPUT_PATH')}/"


def _init():
    """
    Initialisation step for ETL pipeline.
    Create S3 bucket to be used as final storage for solution data; return spark session

    Returns:
            Spark Session object
    """
    init_client = Initializer(config=config)
    init_client.create_s3_bucket(acl="public-read")
    return init_client.create_spark_session()


def _main():
    """
    Primary execution steps for ETL pipeline

    Steps:
            0) initialise pipeline √
            1) extract raw data from sources
            2) clean extracted data
            3) transform cleaned data
            4) create logical models and store in S3
            5) validate final results
    Args:
        **kwargs:

    Returns:
            Boolean: True if all validations passed
                        End of pipeline
                     False if one of validation checks failed
                        Raise error
    """
    # 0) initialise pipeline
    logging.info('Initialising Spark environment and creating S3 bucket...')
    spark = _init()

    # 1) extract raw data from source
    # init objects and client
    logging.info('Initialising source config and client...')
    sources_dict = dict()
    source_dict_cfg = set_source_dict_cfg()
    src_client = SourceOps(spark=spark, source_dict=source_dict_cfg)

    # iterate through source config object and load source data in dictionary object
    logging.info('Processing source data into dictionary object...')
    for k, v in source_dict_cfg.items():
        src = src_client.load(v)
        sources_dict.update(src)

    print('########## sources ##############')
    print(sources_dict)
    print('########## sources ##############')
    # 2) clean extracted data
    logging.info('Initialising data cleaner client...')
    cleaner_client = DataCleaningOps(data_dict=sources_dict)

    logging.info('Cleaning source data...')
    cleaned_data_dict = cleaner_client.clean_dataset_dict()

    print('########## cleaned ##############')
    print(cleaned_data_dict)
    print('########## cleaned ##############')
    ########################
    # SUCCESS up to here
    ########################

    # 3) transform cleaned data
    logging.info('Initialising data transformation client...')
    transformation_client = DataTransformationOps(data_dict=cleaned_data_dict)

    logging.info('Transformation cleaned data...')
    transformed_data_dict = transformation_client.transform_data()

    print('########## transformed ##############')
    print(transformed_data_dict)
    print('########## transformed ##############')

    # 4) create logical models and store in S3
    logging.info('Initialising warehousing ops client...')
    warehousing_client = WarehouseOps(
        spark=spark,
        data_dict=transformed_data_dict,
        destination_storage=output_data_path
    )
    success = warehousing_client.to_storage_parquet()

    print('########## warehoused ##############')
    print(success)
    print('########## warehoused ##############')

    # 5) validate final results
    # run validation checks against final data model, checking for data existing in each source,
    # and confirming join results between fact and dim tables yields expected results


if __name__ == '__main__':
    _main()
