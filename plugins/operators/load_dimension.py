from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    sql_insert_template = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 sql_select="",
                 truncate=True,
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_select = sql_select
        self.truncate = truncate
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Connecting to Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Truncate set to {self.truncate}")
        if self.truncate:
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Loading data into {self.table}...")
        sql_insert = LoadDimensionOperator.sql_insert_template.format(
            self.table,
            self.sql_select
        )

        redshift.run(sql_insert)
        self.log.info("LoadDimensionOperator complete!")
