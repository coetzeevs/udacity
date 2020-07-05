class WarehouseOps(object):
    """
    Class object to handle creating the final logical data models and store to parquet in S3
    """

    def __init__(self, spark, data_dict, destination_storage):
        self.spark = spark
        self.data_dict = data_dict
        self.destination_storage = destination_storage

    def _dim_to_storage_parquet(self):
        """
        Class method to write dimension SparkDFs to parquet in destination_storage location in S3 bucket
        Returns:
                Parquet formatted dataset in S3 at `destination_storage/dimensions/dataset`
        """
        for source_name, data in self.data_dict.items():
            if source_name == 'immigration_data':
                # as-is state backup
                # store immigration data in current state, before being joined and partitioned to create facts
                data.write.mode('overwrite')\
                    .parquet(self.destination_storage + f"/immigration_backup/{source_name}.parquet")
            else:
                # store dimension tables
                data.write.mode('overwrite')\
                    .parquet(self.destination_storage + f"/dimensions/dim_{source_name}.parquet")

        return

    def _fact_to_storage_parquet(self):
        """
        Class method to write facts SparkDF to parquet in destination_storage location in S3 bucket

        Returns:
                Parquet formatted dataset, partitioned by arrival date columns,
                in S3 at `destination_storage/facts/dataset`
        """
        self.data_dict['immigration_facts']\
            .write.partitionBy("arrival_year", "arrival_month", "arrival_day")\
            .mode('overwrite')\
            .parquet(self.destination_storage + "/facts/immigration_facts")

        return

    def _create_fact_data_model(self):
        """
        Class method to create final facts data model for destination storage
        Returns:
                Immigration facts SparkDF added to data_dict
        """
        immigration_facts = self.data_dict['immigration_data']

        immigration_facts = immigration_facts \
            .join(self.data_dict['demographics'],
                  immigration_facts["state_code"] == self.data_dict['demographics']["state_code"], "left_semi") \
            .join(self.data_dict['airports'],
                  immigration_facts["port_code"] == self.data_dict['airports']["port_code"], "left_semi") \
            .join(self.data_dict['airlines'],
                  immigration_facts["airline"] == self.data_dict['airlines']["airline"], "left_semi") \
            .join(self.data_dict['countries'],
                  immigration_facts["orig_country_code"] == self.data_dict['countries']["country_code"], "left_semi") \
            .join(self.data_dict['visa_types'],
                  immigration_facts["visa_type"] == self.data_dict['visa_types']["visa_code"], "left_semi") \
            .join(self.data_dict['entry_modes'],
                  immigration_facts["mode_code"] == self.data_dict['entry_modes']["mode_code"], "left_semi") \
            .join(self.data_dict['states'],
                  immigration_facts["state_code"] == self.data_dict['states']["state_code"], "left_semi")

        self.data_dict['immigration_facts'] = immigration_facts

        return

    def to_storage_parquet(self):
        """
        Class method to write collection of finalised datasets, in target data model format, to S3 bucket
        Returns:
                Parquet formatted dataset in S3
        """
        self._dim_to_storage_parquet()
        self._create_fact_data_model()
        self._fact_to_storage_parquet()

        return True, self.data_dict
