from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 sql,
                 append_mode=True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info(f"Loading data into dimension table {self.table}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_mode:
            self.log.info("Appending data to the dimension table")
            redshift.run(f"{self.sql}")
        else:
            self.log.info("Truncating and loading data into the dimension table")
            redshift.run(f"DELETE FROM {self.table}")
            redshift.run(f"{self.sql}")

        self.log.info(f"Data loaded into dimension table {self.table} successfully")
