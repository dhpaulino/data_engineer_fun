from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadTableToTableOperator(BaseOperator):
    """Receive a query to load the from one table to another"""

    ui_color = '#80BD9E'
    template_fields = ('sql',)
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(self, postgres_conn_id, sql, table_to_truncate=None, *args, **kwargs):
        super(LoadTableToTableOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.table_to_truncate = table_to_truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        if self.table_to_truncate:
            self.logger.info(f"Truncating table {self.table_to_truncate}")
            redshift.run(f"TRUNCATE TABLE {self.table_to_truncate}")
        self.logger.info("Loading table")
        redshift.run(self.sql)
