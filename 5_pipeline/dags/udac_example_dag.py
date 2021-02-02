from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    file_type="json",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json_path="auto",
    file_type="json",
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table='songplays',
    sql_stmt=SqlQueries.songplay_table_insert,    
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table='songs',
    sql_stmt=SqlQueries.song_table_insert,    
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table='users',
    sql_stmt=SqlQueries.user_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table='artists',
    sql_stmt=SqlQueries.artist_table_insert,    
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table='time',
    sql_stmt=SqlQueries.time_table_insert,    
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=['songplays', 'users', 'songs', 'artists', 'time'],    
    dag=dag
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)

# DAG ARCHITECTURE:
## Stage 1
#Begin_execution >> Stage_events
start_operator >> stage_events_to_redshift
#Begin_execution >> Stage_songs
start_operator >> stage_songs_to_redshift
## Stage 2
#Stage_events >> Load_songplays_fact_table
stage_events_to_redshift >>  load_songplays_table
#Stage_songs >> Load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_table
## Stage 3
#Load_songplays_fact_table >> Load_song_dim_table
load_songplays_table >> load_song_dimension_table
#Load_songplays_fact_table >> Load_user_dim_table
load_songplays_table >> load_user_dimension_table
#Load_songplays_fact_table >> Load_artist_dim_table
load_songplays_table >> load_artist_dimension_table
#Load_songplays_fact_table >> Load_time_dim_table
load_songplays_table >> load_time_dimension_table
## Stage 4
#Load_song_dim_table >> Run_data_quality_checks
load_song_dimension_table >> run_quality_checks
#Load_user_dim_table >> Run_data_quality_checks
load_user_dimension_table >> run_quality_checks
#Load_artist_dim_table >> Run_data_quality_checks
load_artist_dimension_table >> run_quality_checks
#Load_time_dim_table >> Run_data_quality_checks
load_time_dimension_table >> run_quality_checks
## Stage 5
#Run_data_quality_checks >> End_execution
run_quality_checks >> end_operator