import logging

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

logger = logging.getLogger()


class SourceOps(object):
    """
    Class object to load raw source data into Spark DataFrames
    """

    def __init__(self, spark, source_dict):
        self.spark = spark
        self.paths = source_dict
        self.airlines_schema = StructType(
            [
                StructField(
                    "airline_id",
                    IntegerType(),
                    True
                ),
                StructField(
                    "name",
                    StringType(),
                    True
                ),
                StructField(
                    "alias",
                    StringType(),
                    True
                ),
                StructField(
                    "iata",
                    StringType(),
                    True
                ),
                StructField(
                    "icao",
                    StringType(),
                    True
                ),
                StructField(
                    "call_sign",
                    StringType(),
                    True
                ),
                StructField(
                    "country",
                    StringType(),
                    True
                ),
                StructField(
                    "active",
                    StringType(),
                    True
                )
            ]
        )

    def _load_raw_file(self, path, file_format="csv", delimiter=",", header="true", schema=None):
        """
        Load raw CSV data into SparkDF
        Args:
            path: [str] - file path to CSV source file
            delimiter: [str] - column delimiter separating data points

        Returns:
                Spark DataFrame
        """
        return self.spark.read.format(file_format) \
            .option("header", header) \
            .option("delimiter", delimiter) \
            .schema(schema) \
            .load(path)

    def _load_raw_sas(self, path):
        """
        Load raw SAS data into SparkDF
        Args:
            path: [str] - file path to SAS part files

        Returns:
                Spark DataFrame
        """
        return self.spark.read.parquet(path)

    def load(self, source_dict):
        """
        Class method to iterate over source dictionary configuration and load data into dict object
        Args:
            source_dict:

        Returns:
                [dict] - dictionary of Spark DataFrames loaded from source files
        """
        folder_path = source_dict.get('folder_path')
        file_format = source_dict.get('file_format')
        single_entity = source_dict.get('single_entity')
        config_spec = source_dict.get('config_spec')
        ret_dict = dict()

        if isinstance(config_spec, list):
            for i in config_spec:
                source_name = i.get("source_name")
                file_name = i.get("file_name")
                header = i.get("header", "true")
                delimiter = i.get("delimiter", ",")

                try:
                    if single_entity:
                        ret_dict[source_name] = self._load_raw_sas(path=folder_path)
                    else:
                        ret_dict[source_name] = self._load_raw_file(
                            path=folder_path + f'/{file_name}',
                            file_format=file_format,
                            delimiter=delimiter,
                            header=header,
                            schema=self.airlines_schema if source_name == 'airlines' else None
                        )
                except Exception as e:
                    logging.error(e)
                    raise e
            return ret_dict
        else:
            logging.error(TypeError('Incorrect arg type provided. config_spec must be of type list.'))
            raise TypeError('Incorrect arg type provided. config_spec must be of type list.')
