from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    sql_insert_template = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 sql_select="",
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_select = sql_select
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Connecting to Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading data into {self.table}...")
        sql_insert = LoadFactOperator.sql_insert_template.format(
            self.table,
            self.sql_select
        )

        redshift.run(sql_insert)
        self.log.info("LoadFactOperator complete!")
