from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HasRowsDataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 column="",
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(HasRowsDataQualityOperator, self).__init__(*args, **kwargs)
        self.column = column
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def check_records(self, hook):
        """
        Pull records count from designated table and check for value. If no data is retrieved, return an error.
        Args:
            hooks: Postgres connection hook

        Returns: records and no error if records are present; no records and an error if no records are present

        """
        records = hook.get_records(f"SELECT COUNT(*) FROM {self.table}")

        if len(records) < 1 or len(records[0]) < 1:
            return None, ValueError(f"Data quality check failed. {self.table} returned no results")
        return records, None

    def check_nulls(self, hook):
        """
        Pull null record counts from designated table and column. If count is greater than zero, return error.
        Args:
            hook: Postgres connection hook

        Returns: True boolean and no error if no NULLS are found; False boolean and an error if NULLS are found

        """
        records = hook.get_records(f"SELECT COUNT(*) FROM {self.table} where {self.column} IS NULL;")

        if records[0][0] > 0:
            return False, ValueError(f"Data quality check failed. {self.table} returned NULL values for column {self.column}")
        return True, None

    def check_rows(self, hook):
        """
        Pull records and check if a successful response is returned AND if there are rows in the response
        Args:
            hook: Postgres connection hook

        Returns: False boolean and error if data retrieval failed; True boolean and no error if data retrieval was
                 successful and returned data

        """
        records, err = self.check_records(hook=hook)

        if err is None:
            if records[0][0] < 1:
                return False, ValueError(f"Data quality check failed. {self.table} contained 0 rows")
            return True, None
        else:
            return False, err

    def execute(self, context):
        """
        Primary execute function - called during Operator invocation during DAG run
        Args:
            context: DAG run context passed during execution/call

        Returns: Info logs based on outcome of data quality checks performed

        """
        redshift_conn_id = PostgresHook(self.redshift_conn_id)

        # check if table has rows
        _rows_bool, _rows_err = self.check_rows(hook=redshift_conn_id)
        if _rows_err is not None:
            raise _rows_err

        # check if nulls were found
        if self.column != "":
            _nulls_bool, _nulls_err = self.check_nulls(hook=redshift_conn_id)
            if _nulls_err is not None:
                raise _nulls_err

        self.log.info(f"Data quality checks on table {self.table} passed.")
