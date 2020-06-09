from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Run specific quality checks against database tables; raise error if not passed

    Args:
        assert_value: value used to evaluate outcome of test_query (must be passed if the other is passed)
        column: column name to use when checking for null values; will only check for null values if column is passed
        redshift_conn_id: string identifying Redshift connection details in Airflow environment
        table: table to check for data quality
        test_query: custom query to run for quality check; must be passed with assert_value

    Returns: ValueError if a test didn't pass or success logs if all checks passed

    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 assert_value="",
                 column="",
                 redshift_conn_id="",
                 table="",
                 test_query="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.assert_value = assert_value
        self.column = column
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.test_query = test_query

    def check_records(self, hook):
        """
        Pull records count from designated table and check for value. If no data is retrieved, return an error.
        Args:
            hooks: Postgres connection hook

        Returns: records and no error if records are present; no records and an error if no records are present

        """
        records = hook.get_records(f"SELECT COUNT(*) FROM public.{self.table}")

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
        records = hook.get_records(f"SELECT COUNT(*) FROM public.{self.table} where {self.column} IS NULL;")

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

        if self.test_query != "":
            if self.assert_value == "":
                raise ValueError(f"Please provide an assertion value for the custom "
                                 f"test query value to be evaluated against.")
            records = redshift_conn_id.get_records(self.test_query)
            if records[0][0] != self.assert_value:
                raise ValueError(f"Custom data quality check failed. "
                                 f"{records[0][0]} does not equal {self.expected_result}")

        self.log.info(f"Data quality checks on table {self.table} passed.")
