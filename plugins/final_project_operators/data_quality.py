from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id:str,
                 sql_queries:list,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.sql_queries=sql_queries

    def execute(self, context):
        self.log.info('Connected')
        hook = PostgresHook(self.conn_id)
        for i in range(0,len(self.sql_queries)):
            records=hook.get_records(self.sql_queries[i]['check_sql'])
            if records[0][0] > self.sql_queries[i]['expected_result']:
                self.log.info('Data quality check failed,the table contains NULL values')
            else:
                self.log.info('Data quality check passed with 0 NULL values')



