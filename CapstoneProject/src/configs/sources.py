import os

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def set_source_dict_cfg():
    """
    Set source_dict configuration to isolate configs to a single file

    Dict struct:
            name: [dict]
                folder_path: [str] - path to data source file(s)
                file_format: [str] - file format in above source path
                compression: [str] - specification of compression applied to files in path
                single_entity: [boolean] - True if all files in path related to the same source (e.g. part files),
                                           False otherwise
                config_spec: [list]
                    for each entry: [dict]
                        source_name: [str] - name of the data source - used in identifying ops down the pipeline
                                             to be applied
                        file_name: [str] - name of file to apply configurations to
                        options: [str] - variable arg name with specific required configuration,
                                         e.g. schema or delimiter
    Returns:
            dictionary of source file configurations
    """
    return dict(
        provided_sas=dict(
            folder_path=os.path.realpath('../data/sources/immigration_sas_data/'),
            file_format='parquet',
            single_entity=True,
            config_spec=[
                dict(
                    source_name='immigration_data',
                )
            ]
        ),
        provided_sup=dict(
            os.path.realpath('../data/sources/supplementary/'),
            file_format='csv',
            single_entity=False,
            config_spec=[
                dict(
                    source_name='airport_codes',
                    file_name='airport_codes.csv',
                    delimiter=','
                ),
                dict(
                    source_name='airports',
                    file_name='airports.csv',
                    delimiter=','
                ),
                dict(
                    source_name='cities',
                    file_name='cities_us.csv',
                    delimiter=','
                ),
                dict(
                    source_name='countries',
                    file_name='countries_global.csv',
                    delimiter=','
                ),
                dict(
                    source_name='entry_modes',
                    file_name='immigration_entry_modes.csv',
                    delimiter=','
                ),
                dict(
                    source_name='states',
                    file_name='states_us.csv',
                    delimiter=','
                ),
                dict(
                    source_name='demographics',
                    file_name='us_cities_demographics.csv',
                    delimiter=';'
                ),
                dict(
                    source_name='visa_types',
                    file_name='visa_types.csv',
                    delimiter=','
                ),
                dict(
                    source_name='airlines',
                    file_name='airlines.dat',
                    header='false',
                    schema=StructType(
                        [
                            StructField(
                                "Airline_ID",
                                IntegerType(),
                                True
                            ),
                            StructField(
                                "Name",
                                StringType(),
                                True
                            ),
                            StructField(
                                "Alias",
                                StringType(),
                                True
                            ),
                            StructField(
                                "IATA",
                                StringType(),
                                True
                            ),
                            StructField(
                                "ICAO",
                                StringType(),
                                True
                            ),
                            StructField(
                                "Callsign",
                                StringType(),
                                True
                            ),
                            StructField(
                                "Country",
                                StringType(),
                                True
                            ),
                            StructField(
                                "Active",
                                StringType(),
                                True
                            )
                        ]
                    )
                )
            ]
        ),
    )
