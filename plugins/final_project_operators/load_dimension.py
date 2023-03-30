from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from final_project_sql_statements import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id:str,
                 table_name:str,
                 truncate,*args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.table_name=table_name
        self.truncate=truncate

    def execute(self, context):
        hook=PostgresHook(self.conn_id)
        self.log.info('Connected')
        if self.truncate==False:
            hook.run(SqlQueries.table_drop.format(self.table_name))
            hook.run(SqlQueries.tables_dictionary[self.table_name+'create'])
            hook.run(SqlQueries.tables_dictionary[self.table_name+'insert'])
        else:
            hook.run(SqlQueries.truncate_table.format(self.table_name))
            hook.run(SqlQueries.tables_dictionary[self.table_name+'insert'])
