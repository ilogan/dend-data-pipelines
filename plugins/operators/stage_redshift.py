from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    redshift_copy_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 s3_bucket="udacity-dend",
                 s3_key="",
                 s3_jsonpaths_file="",
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_jsonpaths_file = s3_jsonpaths_file
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Getting AWS Credentials...")
        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()

        self.log.info("Connecting to Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing rows from {self.table}...")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Copying data from S3 to Redshift...")
        s3_key_rendered = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{s3_key_rendered}"

        json_format = "auto"
        if self.s3_jsonpaths_file:
            self.log.info(
                f" - jsonpaths file detected: {self.s3_jsonpaths_file}")
            json_format = f"s3://{self.s3_bucket}/{self.s3_jsonpaths_file}"

        redshift_copy = StageToRedshiftOperator.redshift_copy_template.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_format
        )
        redshift.run(redshift_copy)
        self.log.info("StageToRedshiftOperator complete!")
