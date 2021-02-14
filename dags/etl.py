from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (    
    PostgresOperator,
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator, 
    DataQualityOperator
)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'marcus',
    'depends_on_past': False, 
    'retries': 0, #3
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    #'catchup': True,
    'start_date': datetime(2018, 11, 1, 0, 0, 0, 0),
    'end_date': datetime(2018, 11, 30, 0, 0, 0, 0) #must be removed
}

dag = DAG('sparkify-airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly',
          #schedule_interval='@daily',
          catchup=True,
          max_active_runs=1 #must be removed
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

## create all tables in redshift
create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    clear_table=False,
    s3_bucket='udacity-dend',
    #s3_key='log-data/{execution_date.year}/{execution_date.month}/{ds}-events.json',
    s3_key='log-data/{execution_date.year}/{execution_date.month}',
    json_path='log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    clear_table=True,
    s3_bucket='udacity-dend',
    #s3_key='song-data/A/A/A/TRAAAAK128F9318786.json'
    s3_key='song-data/A/A/',
    json_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    clear_table=False,
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    clear_table=True,
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songs',
    clear_table=True,
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists',
    clear_table=True,
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='time',
    clear_table=True,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator