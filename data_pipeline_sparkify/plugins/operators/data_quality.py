from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """" Checks if a given list of sql queries matches the expected results"""
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, conn_id, queries, expected_results, *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        if len(queries) != len(expected_results):
            raise ValueError("expected_results size should match sql_list size")
        self.sql_list = queries
        self.expected_results = expected_results

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)

        for query, expected in zip(self.sql_list, self.expected_results):
            self.logger.info(f"Executing check (expected result {expected}):\n\t{query}")
            result = hook.get_records(query)
            if len(result) != 1:
                raise Exception(f"The number of rows is different than 1 ({len(result)} got)")
            if result[0][0] != expected:
                raise Exception(f"Expected value {expected}, but got {result[0][0]}")
            self.logger.info("Check passed!")
