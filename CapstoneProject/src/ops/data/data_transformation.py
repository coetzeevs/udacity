import logging

from pyspark.sql.functions import (
    col,
    split,
    sum as _sum,
    round
)

logger = logging.getLogger()


class DataTransformationOps(object):
    """
    Class object to transform selected data from the cleaned sources
    """

    def __init__(self, data_dict):
        self.data_dict = data_dict

    def _demographics_transform(self):
        """
        Class method to transform and aggregate demographics data, grouping by state and calculating gender and race
        ratios for each state.
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('demographics', None)
        if df is not None:
            data = df \
                .groupBy(
                    col("state_code").alias("State_code"),
                    col("state")
                ).agg(
                    _sum("total_population"),
                    _sum("male_population"),
                    _sum("female_population"),
                    _sum("American Indian and Alaska Native").alias("american_indian_and_alaska_native"),
                    _sum("Asian").alias("asian"),
                    _sum("Black or African-American").alias("black_or_african_american"),
                    _sum("Hispanic or Latino").alias("hispanic_or_latino"),
                    _sum("White").alias("white")
                ) \
                .withColumn(
                    "male_population_ratio",
                    round(
                        (col("male_population") / col("total_population")), 2
                    )
                ) \
                .withColumn(
                    "female_population_ratio",
                    round(
                        (col("female_population") / col("total_population")), 2
                    )
                ) \
                .withColumn(
                    "american_indian_and_alaska_native_ratio",
                    round(
                        (col("american_indian_and_alaska_native") / col("total_population")), 2
                    )
                ) \
                .withColumn(
                    "asian_ratio",
                    round(
                        (col("asian") / col("total_population")), 2
                    )
                ) \
                .withColumn(
                    "black_or_african_american_ratio",
                    round(
                        (col("black_or_african_american") / col("total_population")), 2
                    )
                ) \
                .withColumn(
                    "hispanic_or_latino_ratio",
                    round(
                        (col("hispanic_or_latino") / col("total_population")), 2
                    )
                ) \
                .withColumn(
                    "white_ratio",
                    round(
                        (col("white") / col("total_population")), 2
                    )
                )

            return dict(demographics=data)
        else:
            logger.error(ValueError('No dataset named "demographics" found in cleaned data dict.'))
            raise ValueError('No dataset named "demographics" found in cleaned data dict.')

    def _immigration_transform(self):
        """
        Class method to split arrival date into date parts (day, month, year)
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('immigration_data', None)
        if df is not None:
            data = df \
                .withColumn(
                    "arrival_date_split", split(col("arrival_date"), "-")
                ) \
                .withColumn(
                    "arrival_year", col("arrival_date_split")[0]
                ) \
                .withColumn(
                    "arrival_month", col("arrival_date_split")[1]
                ) \
                .withColumn(
                    "arrival_day", col("arrival_date_split")[2]
                ) \
                .drop("arrival_date_split")

            return dict(immigration_data=data)

        else:
            logger.error(ValueError('No dataset named "immigration_data" found in cleaned data dict.'))
            raise ValueError('No dataset named "immigration_data" found in cleaned data dict.')

    def transform_data(self):
        """
        Class method to transform the cleaned demographics dataset and immigration dataset
        Returns:
                [dict] - transformed Spark DataFrames
        """
        self.data_dict.update(self._demographics_transform())
        self.data_dict.update(self._immigration_transform())

        return self.data_dict
