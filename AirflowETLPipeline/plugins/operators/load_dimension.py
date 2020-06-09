from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Load dimension table into destination Redshift RDB
    Args:
        p_key: primary key on table - to be used with table argument
        redshift_conn_id: Postgres connection ID string
        sql: SQL select string to retrieve data from staging tables
        table: destination table for SQL output
        upsert: Boolean (default False) | truncate and replace data if False, upsert otherwise

    Returns:

    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 p_key="",
                 redshift_conn_id="",
                 sql="",
                 table="",
                 upsert=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.p_key = p_key
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.upsert = upsert

    def execute(self, context):
        """
        Prep SQL query for loading data to dimension table and run
        Args:
            context: Airflow context passed from environment on call

        Returns: return None if successful, log error if not

        """
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.upsert:
            # set SQL query
            build_sql = f"""
                CREATE TEMP TABLE sg_{self.table} (like {self.table}); 
                
                INSERT INTO sg_{self.table}
                {self.sql};
                
                DELETE FROM {self.table}
                USING sg_{self.table}
                WHERE {self.table}.{self.p_key} = sg_{self.table}.{self.p_key};
                
                INSERT INTO {self.table}
                SELECT * FORM sg_{self.table};
            """
        else:
            # Truncate destination table in preparation for clean data
            hook.run(f"TRUNCATE TABLE ONLY {self.table};")

            # set SQL query
            build_sql = f"""
                INSERT INTO {self.table}
                {self.sql};
            """

        try:
            hook.run(build_sql)
        except Exception as e:
            self.log.error(f"Error encountered running SQL query. Error: {e}")
            raise e
