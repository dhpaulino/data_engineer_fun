from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """ Copies the data from a given s3 location to a table in redshift"""
    ui_color = '#358140'
    template_fields = ['s3_bucket','s3_key']

    @apply_defaults
    def __init__(self, redshift_conn_id, arn_iam_role, output_table, s3_bucket, s3_key, copy_parameters,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.arn_iam_role = arn_iam_role
        self.output_table = output_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_parameters = copy_parameters

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        copy_sql = f"COPY {self.output_table} FROM 's3://{self.s3_bucket}/{self.s3_key}'\
                    CREDENTIALS 'aws_iam_role={self.arn_iam_role}'\
                    {self.copy_parameters}"
        self.log.info('Executing COPY command...')
        self.logger.info(copy_sql)
        hook.run(copy_sql)
