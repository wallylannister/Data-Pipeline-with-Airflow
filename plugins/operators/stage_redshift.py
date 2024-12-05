from airflow.hooks.postgres_hook import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_conn_id,
                 table,
                 s3_bucket,
                 s3_key,
                 json_path,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info('Starting data load from S3 to Redshift')

        # Create hooks
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3 = S3Hook(self.aws_conn_id)

        # Generate the SQL COPY command
        copy_sql = f"""
            COPY {self.table}
            FROM 's3://{self.s3_bucket}/{self.s3_key}'
            IAM_ROLE 'arn:aws:iam::466770078143:role/my-redshift-service-role'
            JSON '{self.json_path}'
            """
        
        # Execute the COPY command
        redshift.run(copy_sql)
        self.log.info('Staging data load completed successfully')



