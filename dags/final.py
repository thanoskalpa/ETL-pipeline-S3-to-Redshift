from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_sql_statements import SqlQueries
from airflow.models.baseoperator import chain
import datetime



default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.yesterday(),
    'email_on_retry': False,
    'retry_delay':datetime.timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    schedule_interval='@hourly',
    description='Load and transform data in Redshift with Airflow',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        conn_id='redshift',
        table_name='staging_events',
        data_source_s3='s3://udacity-dend/log_data',
        json='s3://udacity-dend/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
          task_id='Stage_songs',
          conn_id='redshift',
          table_name='staging_songs',
          data_source_s3='s3://udacity-dend/song_data/A/A/A',
          json='auto'
    )

    
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id='redshift',
        table_name='songplay'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id='redshift',
        table_name='users',
        truncate=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id='redshift',
        table_name='song',
        truncate=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id='redshift',
        table_name='artist',
        truncate=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id='redshift',
        table_name='time',
        truncate=False
    )
    
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id='redshift',
        sql_queries=SqlQueries.data_quality_checks
    )

    

    chain(
        start_operator,
        [stage_events_to_redshift,stage_songs_to_redshift],
        load_songplays_table,
        [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table],
        run_quality_checks
    )




final_project_dag = final_project()