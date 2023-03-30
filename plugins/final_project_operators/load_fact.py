from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from final_project_sql_statements import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                conn_id:str,
                table_name:str,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.table_name=table_name
    def execute(self, context):
        self.log.info('Connected')
        hook = PostgresHook(self.conn_id)
        hook.run(SqlQueries.table_drop.format(self.table_name))
        hook.run(SqlQueries.songplay_table_create)
        hook.run(SqlQueries.songplay_table_insert)
        