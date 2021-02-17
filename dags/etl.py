from datetime import datetime, timedelta
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


default_args = {
    'owner': 'marcus',
    'depends_on_past': False, 
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
    'start_date': datetime(2018, 11, 1)
    #'end_date': datetime(2018, 11, 30)
}

dag = DAG('sparkify-airflow',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          #,catchup=True
          #,max_active_runs=1
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
    s3_key='log-data/{execution_date.year}/{execution_date.month}/{ds}-events.json',
    #s3_key='log-data/{execution_date.year}/{execution_date.month}',
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
    s3_key='song-data'
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
    dag=dag,
    redshift_conn_id="redshift",
    dq_params = {
        'has_rows':['time','users','songs','artists','songplays'],
        'has_nulls':
            {'time':['start_time'],
             'users':['userid'],
             'songs':['songid'],
             'artists':['artistid'],
             'songplays':['playid', 'start_time']}     
    }
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> [stage_events_to_redshift, 
                  stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_song_dimension_table, 
                         load_user_dimension_table, 
                         load_artist_dimension_table, 
                         load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator