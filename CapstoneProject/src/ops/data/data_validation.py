from pyspark.sql.functions import (
    col,
)


class DataValidationOps(object):
    """
    Class object to validate the final data models and check data integrity
    """

    def __init__(self, spark, data_dict, source_path_root):
        self.spark = spark
        self.data_dict = data_dict
        self.source_path_root = source_path_root

    def _storage_to_local(self):
        """
        Class method that retrieves newly stored model data from storage and stores into dict object for validation
        Returns:
                [dict] - Updated data dictionary object
        """
        for source_name, data in self.data_dict.items():
            if source_name == 'immigration_facts':
                self.data_dict[source_name] = self.spark.read.parquet(
                    self.source_path_root + f"/facts/{source_name}"
                )
            elif source_name == 'immigration_data':
                self.data_dict[source_name] = self.spark.read.parquet(
                    self.source_path_root + f"/immigration_backup/{source_name}"
                )
            else:
                self.data_dict[source_name] = self.spark.read.parquet(
                    self.source_path_root + f"/dimensions/dim_{source_name}"
                )

        return

    def _validate_has_rows(self):
        """
        Class method to check if each SparkDF in resultant data model has rows of data.
        Returns:
                [list] - For each dataset elements are either False if no data is present, True otherwise
        """
        check = list()
        for source_name, data in self.data_dict.items():
            check.append(data.count() > 0)

        return check

    @staticmethod
    def _join_integrity(fact, dim, on_fact, on_dim, how="left_anti", assert_value=0):
        """
        Class method to check data model integrity.
        Join immigration_facts data columns with corresponding dimension table columns, and assert if correct values
        are returned.
        Returns:
                [boolean] - Either False if integrity check fails, True otherwise
        """
        return fact.select(col(on_fact)).distinct().join(dim, fact[on_fact] == dim[on_dim], how).count() == assert_value

    def _facts_data_type_integrity(self):
        """
        Class method to validate whether the facts data model adheres to the following constraint:
            - Data types: check that the resultant fact model adheres to the intended model data types
        Returns:
                [list] - Either False if integrity check fails, True otherwise - for each data field checked
        """
        target_data_types_model = dict(
            cic_id="int",
            port_code="string",
            state_code="string",
            visa_post="string",
            matflag="string",
            dtaddto="string",
            gender="string",
            airline="string",
            admnum="double",
            fltno="string",
            visa_type="string",
            mode_code="int",
            orig_country_code="int",
            cit_country_code="int",
            year="int",
            month="int",
            birth_year="int",
            age="int",
            counter="int",
            arrival_date="date",
            departure_date="date",
            arrival_year="string",
            arrival_month="string",
            arrival_day="string"
        )
        immigration_fact_data_types = self.data_dict['immigration_facts'].dtypes
        check = list()

        # iterate over each data type tuple in the immigration_fact_data_types list and return true if the field type
        # matches with the expected data type from the dictionary config above
        for tup in immigration_fact_data_types:
            field_name = tup[0]
            field_type = tup[1]

            check.append(target_data_types_model[field_name] == field_type)

        return check

    def _fact_uniqueness_constraint(self):
        """
        Class method to validate whether the facts data model adheres to the following constraint:
            - Uniqueness: rows are unique and are identifiable by a unique key
        Returns:
                [list] - Either False if integrity check fails, True otherwise
        """
        check = list()

        # check if the entire count of the facts table's rows matches a distinct count of the rows
        check.append(
            self.data_dict["immigration_facts"].count() == self.data_dict["immigration_facts"].distinct().count()
        )

        # check if the count of rows matches the distinct count of a unique identifier field - in this case "cic_id"
        # this checks the unique identifier constraint
        check.append(
            self.data_dict["immigration_facts"].count() == self.data_dict["immigration_facts"].select(
                ["cic_id"]
            ).distinct().count()
        )

        return check

    def validate(self):
        """
        Class method to run through validation checks and return result.
        Returns:
                [boolean] - True if all checks passed, False otherwise
        """
        checks = list()
        checks.append(self._validate_has_rows())
        checks.append(self._facts_data_type_integrity())
        checks.append(self._fact_uniqueness_constraint())

        # airports
        checks.append(
            self._join_integrity(
                fact=self.data_dict['immigration_facts'],
                dim=self.data_dict['airports'],
                on_fact='port_code',
                on_dim='port_code'
            )
        )

        # airlines
        checks.append(
            self._join_integrity(
                fact=self.data_dict['immigration_facts'],
                dim=self.data_dict['airlines'],
                on_fact='airline',
                on_dim='airline'
            )
        )

        # countries
        checks.append(
            self._join_integrity(
                fact=self.data_dict['immigration_facts'],
                dim=self.data_dict['countries'],
                on_fact='orig_country_code',
                on_dim='country_code'
            )
        )

        # demographics
        checks.append(
            self._join_integrity(
                fact=self.data_dict['immigration_facts'],
                dim=self.data_dict['demographics'],
                on_fact='state_code',
                on_dim='state_code'
            )
        )

        # entry_modes
        checks.append(
            self._join_integrity(
                fact=self.data_dict['immigration_facts'],
                dim=self.data_dict['entry_modes'],
                on_fact='mode_code',
                on_dim='mode_code'
            )
        )

        # states
        checks.append(
            self._join_integrity(
                fact=self.data_dict['immigration_facts'],
                dim=self.data_dict['states'],
                on_fact='state_code',
                on_dim='state_code'
            )
        )

        # visa_types
        checks.append(
            self._join_integrity(
                fact=self.data_dict['immigration_facts'],
                dim=self.data_dict['visa_types'],
                on_fact='visa_type',
                on_dim='visa_code'
            )
        )

        return all(checks)
