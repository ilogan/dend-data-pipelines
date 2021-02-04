from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables=[],
                 custom_query_asserts=[],
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.custom_query_asserts = custom_query_asserts
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # general assertions - all tables must pass row check before custom assertions
        for table in self.tables:
            self.log.info(f"Asserting table {table} table is not empty...")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} table returned no results")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check failed. {table} table contained 0 rows")
            self.log.info(
                f"{num_records} records found in {table} table. Passed!")

        # custom assertions
        for custom_query_asserts in self.custom_query_asserts:
            query, assertion = custom_query_asserts

            self.log.info(
                f"Performing query: {query} | Expecting {assertion}...")
            records = redshift.get_records(query)
            result = records[0][0]
            if result != assertion:
                raise ValueError(
                    f"Data quality check failed. Received {result}")
            self.log.info("Assertion passed!")

        self.log.info('DataQualityOperator complete!')
