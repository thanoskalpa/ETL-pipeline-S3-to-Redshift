from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from final_project_sql_statements import SqlQueries
from airflow.secrets.metastore import MetastoreBackend



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 table_name: str,
                 json,
                 data_source_s3,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.table_name=table_name
        self.json=json
        self.data_source_s3=data_source_s3

    
    def execute(self, context):
        self.log.info('Connected')
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection("aws_credentials")
        hook = PostgresHook(self.conn_id)
        hook.run(SqlQueries.table_drop.format(self.table_name))
        if self.table_name=='staging_songs':
            hook.run(SqlQueries.staging_songs_table_create)
            hook.run(SqlQueries.COPY_SQL.format(self.table_name,self.data_source_s3,self.json,aws_connection.login,aws_connection.password))
        elif self.table_name=='staging_events':
            hook.run(SqlQueries.staging_events_table_create)
            hook.run(SqlQueries.COPY_SQL.format(self.table_name,self.data_source_s3,self.json,aws_connection.login,aws_connection.password))
        else:
            self.log.info('The table name that you provided is not in our Database')
        


