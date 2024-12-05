from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tests,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests  # List of dictionaries containing SQL tests and expected results

    def execute(self, context):
        self.log.info("Starting data quality checks")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            sql = test.get('sql')
            expected_result = test.get('expected_result')

            self.log.info(f"Running test: {sql}")
            result = redshift.get_records(sql)

            if not result:
                raise ValueError(f"Data quality check failed. Query returned no results for: {sql}")

            actual_result = result[0][0]  # Assuming the result is a single value

            if actual_result != expected_result:
                raise ValueError(f"Data quality check failed. Expected {expected_result}, but got {actual_result} for query: {sql}")

            self.log.info(f"Data quality check passed for query: {sql}")
