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

    def validate(self):
        """
        Class method to run through validation checks and return result.
        Returns:
                [boolean] - True if all checks passed, False otherwise
        """
        checks = list()
        checks.append(self._validate_has_rows())

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
