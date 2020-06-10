from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Coyp JSON formatted data from source S3 bucket to destination Redshift table
    Args:
        aws_credentials_id: AWS connection credentials ID string
        json_option: path to json object mapping file or auto
        redshift_conn_id: Postgres connection ID string
        region: region where the s# bucket is located (e.g. us-west-2
        s3_bucket: AWS S3 bucket where the data lives
        s3_key: rest of the path to the data in question
        table: destination table

    Returns:

    """
    copy_sql = """
            COPY {table}
            FROM '{path}'
            ACCESS_KEY_ID '{key_id}'
            SECRET_ACCESS_KEY '{key}'
            REGION AS '{region}'
            FORMAT as json '{json_option}'
        """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 json_option="auto",
                 redshift_conn_id="",
                 region="",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.json_option = json_option
        self.redshift_conn_id = redshift_conn_id
        self.region = region
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table

    def execute(self, context):
        """
        Execution method to copy data from S3 to Redshift
        Args:
            context: Airflow environment context passed on call

        Returns: None if successful, logs and raised error if failed

        """
        # Init hooks, credentials, and S3 bucket path
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)

        # prepare destination table
        redshift_hook.run(f"TRUNCATE TABLE {self.table};")

        # Prep formatted SQL
        sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            path=s3_path,
            key_id=credentials.access_key,
            key=credentials.secret_key,
            region=self.region,
            json_option=self.json_option
        )

        # execute SQL to staging table
        try:
            redshift_hook.run(sql)
        except Exception as e:
            self.log.error(f"Error encountered extracting data from s# to Redshift staging table {self.table}: "
                           f"Error: {e}")
            raise e
