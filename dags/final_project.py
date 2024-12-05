from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'wallace',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    start_date=pendulum.now()
)
def final_project():

    start_operator = PostgresOperator(task_id='create_tables',
        postgres_conn_id='redshift', 
        sql=[
        SqlQueries.staging_events_table_create,
        SqlQueries.staging_songs_table_create,
        SqlQueries.songplay_table_create,
        SqlQueries.users_table_create,
        SqlQueries.song_table_create,
        SqlQueries.artist_table_create,
        SqlQueries.time_table_create
        ]     
    )
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_conn_id='aws_admin',
        table='staging_events',
        s3_bucket='sparkify-source-data',
        s3_key='log-data/2018/11/',
        json_path='s3://sparkify-source-data/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_conn_id='aws_admin',
        table='staging_songs',
        s3_bucket='sparkify-source-data',
        s3_key='song-data/A/B/C/',
        json_path='auto'
        
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='song',
        sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artist',
        sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tests=SqlQueries.data_quality_checks
    )

    stop_operator = DummyOperator(task_id='Stop_execution')


    start_operator >> (stage_events_to_redshift, stage_songs_to_redshift)
    (stage_events_to_redshift, stage_songs_to_redshift) >> load_songplays_table
    load_songplays_table >> (load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table)
    (load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table) >> run_quality_checks
    run_quality_checks >> stop_operator

final_project_dag = final_project()
