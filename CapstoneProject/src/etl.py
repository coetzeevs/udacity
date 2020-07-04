import os
import configparser

from ops.storage import (
    create_spark_session,
    create_s3_bucket,
)

from configs.sources import set_source_dict_cfg

from ops.etl.source_ops import SourceOps

config = configparser.ConfigParser()
config.read_file(open(os.path.realpath('./configs/dwh.cfg')))
output_data = f"s3a://{config.get('S3', 'BUCKET')}/{config.get('S3', 'OUTPUT_PATH')}/"


def _init():
    """
    Initialisation step for ETL pipeline.
    Create S3 bucket to be used as final storage for solution data; return spark session

    Returns:
            Spark Session object
    """
    create_s3_bucket(config=config, acl="public-read")
    return create_spark_session()


def _main(**kwargs):
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
    spark = _init()

    # 1) extract raw data from source
    # init objects and client
    sources_dict = dict()
    source_dict_cfg = set_source_dict_cfg()
    src_client = SourceOps(spark=spark, source_dict=source_dict_cfg)

    # iterate through source config object and load source data in dictionary object
    for k, v in source_dict_cfg.items():
        src_name, data = src_client.load(k, v)
        sources_dict[src_name] = data

    print(sources_dict)

    # 2) clean extracted data
    # clean SparkDFs, filling null values, renaming columns,
    # filtering on US only data, casting to appropriate field types

    # 3) transform cleaned data
    # Do necessary aggregations and date formatting, to prep for facts table model

    # 4) create logical models and store in S3
    # create logical model in parquet format, stored in S3 bucket

    # 5) validate final results
    # run validation checks against final data model, checking for data existing in each source,
    # and confirming join results between fact and dim tables yields expected results


if __name__ == '__main__':
    _main()
