from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Operator to load fact table data into target table by running given query
    Args:
        redshift_conn_id: Postgres connection ID string
        sql: SQL query string to run
        table: Destination table name

    Returns: Data in target table or raised exception on fail

    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table

    def execute(self, context):
        """
        Run given SQL query against DB and populate fact table data
        Args:
            context: Airflow environment context passed on call

        Returns: None if successful, logs if failed

        """
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        try:
            hook.run(f"""
                INSERT INTO {self.table}
                {self.sql};
                """
            )
        except Exception as e:
            self.log.error(f"Error encountered populating the fact table. Error: {e}")
            raise e
