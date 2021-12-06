from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator
 

# Default args 
default_args = {
    'owner': 'vivian',
    'depends_on_past': False, 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}
     
# monthly ='0 0 1 * *',
# One DAG run at one time  max_active_runs = 1
dag = DAG('udac_example_dag',
          default_args = default_args, 
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '0 0 1 * *', # monthly
          start_date = datetime(2018, 11, 1),
          end_date = datetime(2018, 12, 1),
          max_active_runs = 1   
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

table_creation = PostgresOperator(
    task_id='tables_creation',  
    dag=dag,
    postgres_conn_id='redshift',
    sql = 'sql/create_tables.sql'
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket='udacity-dend',
    s3_key = "log-data/{execution_date.year}/{execution_date.month:02d}",    
    table="staging_events",
    file_format="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_songs",
    dag = dag,
    table = "staging_songs",
    conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = 'udacity-dend',
    s3_key = 'song_data',
    file_format = " JSON  'auto' "
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = "songplays",
    conn_id = 'redshift',
    sql = SqlQueries.songplay_table_insert    
)

load_user_table = LoadDimensionOperator(
     task_id = 'Load_user_dim_table',     
     dag = dag , 
     conn_id = "redshift",
     table = "users",
     sql = SqlQueries.user_table_insert,  
     append_only = False
 )
 

load_song_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    conn_id = "redshift", 
    table = "songs",
    sql = SqlQueries.song_table_insert,  
    append_only  =False
)

load_artist_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    conn_id = "redshift",
    table = "artists",
    sql = SqlQueries.artist_table_insert,  
    append_only = False
    
)

load_time_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    conn_id = "redshift",
    table = "time",
    sql = SqlQueries.time_table_insert,  
    append_only = False
)

 
# check 4 tables have recond 
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=[ "songplays", "songs", "artists",  "time", "users"]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

 


start_operator  \
    >> table_creation  \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [ load_song_table, load_artist_table, load_time_table, load_user_table] \
    >> run_quality_checks \
    >> end_operator


