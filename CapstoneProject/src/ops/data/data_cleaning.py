import logging

from pyspark.sql.functions import (
    col,
    expr,
    lit,
    substring,
    to_date
)

logger = logging.getLogger()


class DataClean(object):
    """
    Class object to clean the initialised raw data sets
    """

    def __init__(self, data_dict):
        self.data_dict = data_dict

    def _clean_demographics(self):
        """
        Class method to clean demographics data

        Operations:
                    - pivot on "race" column and aggregate on count of entries
                    - fill null value count with 0
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('demographics', None)
        if df is not None:
            data = df.groupBy(
                col("City"),
                col("State"),
                col("Median Age"),
                col("Male Population"),
                col("Female Population"),
                col("Total Population"),
                col("Number of Veterans"),
                col("Foreign-born"),
                col("Average Household Size"),
                col("State Code"))\
                .pivot("Race")\
                .agg(sum("count").cast("integer"))\
                .fillna(
                {
                    "American Indian and Alaska Native": 0,
                    "Asian": 0,
                    "Black or African-American": 0,
                    "Hispanic or Latino": 0,
                    "White": 0,
                }
            )

            return dict(demographics=data)
        else:
            logger.error(ValueError('No dataset named "demographics" found in sources dict.'))
            raise ValueError('No dataset named "demographics" found in sources dict.')

    def _clean_airports(self):
        """
        Class method to clean airports data

        Operations:
                    - select only US airports
                    - select only airports where type is in ("large_airport", "medium_airport", "small_airport")
                    - isolate region substring from iso_region field
                    - cast elevation in feet to float
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('airports', None)
        if df is not None:
            data = df\
                .where(
                    (col("iso_country") == "US") &
                    (col("type").isin("large_airport", "medium_airport", "small_airport"))
                )\
                .withColumn("iso_region", substring(col("iso_region"), 4, 2))\
                .withColumn("elevation_ft", col("elevation_ft").cast("float"))

            return dict(airports=data)
        else:
            logger.error(ValueError('No dataset named "airports" found in sources dict.'))
            raise ValueError('No dataset named "airports" found in sources dict.')

    def _clean_countries(self):
        """
        Class method to clean countries data

        Operations:
                    - cast code field to integer
                    - rename code field to country code
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('countries', None)
        if df is not None:
            data = df\
                .withColumn("country_code", col("code").cast("integer"))
            return dict(countries=data)
        else:
            logger.error(ValueError('No dataset named "countries" found in sources dict.'))
            raise ValueError('No dataset named "countries" found in sources dict.')

    def _clean_visa_types(self):
        """
        Class method to clean visa types data

        Operations:
                    - cast visa_code field to integer
                    - rename visa field to visa_type
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('visa_types', None)
        if df is not None:
            data = df\
                .withColumn("visa_code", col("visa_code").cast("integer"))\
                .withColumnRenamed("visa", "visa_type")
            return dict(visa_types=data)
        else:
            logger.error(ValueError('No dataset named "visa_types" found in sources dict.'))
            raise ValueError('No dataset named "visa_types" found in sources dict.')

    def _clean_entry_modes(self):
        """
        Class method to clean entry mode data

        Operations:
                    - cast mode field to integer
                    - rename mode field to visa_type
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('entry_modes', None)
        if df is not None:
            data = df\
                .withColumn("mode_code", col("mode").cast("integer"))
            return dict(entry_modes=data)
        else:
            logger.error(ValueError('No dataset named "entry_modes" found in sources dict.'))
            raise ValueError('No dataset named "entry_modes" found in sources dict.')

    def _clean_cities(self):
        """
        Class method to clean cities data

        Operations:
                    - rename abr_city field to city_code
                    - rename City field to city_name
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('cities', None)
        if df is not None:
            data = df\
                .withColumnRenamed("abr_city", "city_code")\
                .withColumnRenamed("City", "city_name")
            return dict(cities=data)
        else:
            logger.error(ValueError('No dataset named "cities" found in sources dict.'))
            raise ValueError('No dataset named "cities" found in sources dict.')

    def _clean_states(self):
        """
        Class method to clean states data

        Operations:
                    - rename Abbreviation field to state_code
                    - rename State to state_name
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('states', None)
        if df is not None:
            data = df\
                .withColumnRenamed("Abbreviation", "state_code")\
                .withColumnRenamed("State", "state_name")
            return dict(states=data)
        else:
            logger.error(ValueError('No dataset named "states" found in sources dict.'))
            raise ValueError('No dataset named "states" found in sources dict.')

    def _clean_airlines(self):
        """
        Class method to clean airlines data

        Operations:
                    - drop private and unknown airlines: (col("airline_id") > 1)
                    - select only airlines with IATA codes: (col("iata").isNotNull())
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('airlines', None)
        if df is not None:
            data = df\
                .where((col("iata").isNotNull()) & (col("airline_id") > 1))
            return dict(airlines=data)
        else:
            logger.error(ValueError('No dataset named "airlines" found in sources dict.'))
            raise ValueError('No dataset named "airlines" found in sources dict.')

    def _clean_immigration_data(self):
        """
        Class method to clean immigration data

        Operations:
                    - set column types and rename to appropriate names
                    - remove columns renamed and/or no longer of use
                    - additional fault check: only select columns that will be used/useful
        Returns:
                [dict] - object with source-name: SparkDF key-value pairs
        """
        df = self.data_dict.get('immigration_data', None)
        if df is not None:
            # set column types and rename
            tmp = df\
                .withColumn("cic_id", col("cicid").cast("integer"))\
                .withColumn("visa_code", col("i94visa").cast("integer"))\
                .withColumn("mode_code", col("i94mode").cast("integer"))\
                .withColumn("orig_country_code", col("i94res").cast("integer"))\
                .withColumn("cit_country_code", col("i94cit").cast("integer"))\
                .withColumn("year", col("i94yr").cast("integer"))\
                .withColumn("month", col("i94mon").cast("integer"))\
                .withColumn("birth_year", col("biryear").cast("integer"))\
                .withColumn("age", col("i94bir").cast("integer"))\
                .withColumn("counter", col("count").cast("integer"))\
                .withColumn("sas_date", to_date(lit("01/01/1960"), "MM/dd/yyyy"))\
                .withColumn("arrival_date", expr("date_add(data_base_sas, arrdate)"))\
                .withColumn("departure_date", expr("date_add(data_base_sas, depdate)")) \
                .withColumnRenamed("i94addr", "state_code") \
                .withColumnRenamed("i94port", "port_code") \
                .withColumnRenamed("visapost", "visa_post") \
                .withColumnRenamed("visatype", "visa_type")

            # drop original/renamed columns
            tmp = tmp\
                .drop("cicid")\
                .drop("i94visa")\
                .drop("i94mode")\
                .drop("i94res")\
                .drop("i94cit")\
                .drop("i94yr")\
                .drop("i94mon")\
                .drop("biryear")\
                .drop("i94bir")\
                .drop("count")\
                .drop("data_base_sas", "arrdate", "depdate")

            # fault check: select only what will be useful
            data = tmp.select(
                col("cic_id"),
                col("port_code"),
                col("state_code"),
                col("visa_post"),
                col("matflag"),
                col("dtaddto"),
                col("gender"),
                col("airline"),
                col("admnum"),
                col("fltno"),
                col("visa_type"),
                col("mode_code"),
                col("orig_country_code"),
                col("cit_country_code"),
                col("year"),
                col("month"),
                col("birth_year"),
                col("age"),
                col("counter"),
                col("arrival_date"),
                col("departure_date"))
        
            return dict(immigration_data=data)
    
        else:
            logger.error(ValueError('No dataset named "immigration_data" found in sources dict.'))
            raise ValueError('No dataset named "immigration_data" found in sources dict.')

    def clean_dataset_dict(self):
        """
        Class method to clean and update each dataset in the sources data dictionary
        Returns:
                [dict] - cleaned source Spark DataFrames
        """
        self.data_dict.update(self._clean_airlines())
        self.data_dict.update(self._clean_airports())
        self.data_dict.update(self._clean_cities())
        self.data_dict.update(self._clean_countries())
        self.data_dict.update(self._clean_demographics())
        self.data_dict.update(self._clean_entry_modes())
        self.data_dict.update(self._clean_immigration_data())
        self.data_dict.update(self._clean_states())
        self.data_dict.update(self._clean_visa_types())

        return self.data_dict
