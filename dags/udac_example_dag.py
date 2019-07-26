from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.models import Variable


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
song_address = "'s3://udacity-dend/song_data'"
event_address = "'s3://udacity-dend/log_data'"


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_cred_id="aws_credentials",
    redshift_conn_id="redshift",
    table_name='staging_events',
    s3_address= event_address    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_cred_id="aws_credentials",
    redshift_conn_id="redshift",
    table_name='staging_songs',
    s3_address=song_address

)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    dag=dag
)

insert_user_sql = """INSERT INTO users (
            userid,
            first_name ,
            last_name,
            gender ,
            "level" 
        )
        """

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    statement_sql=(insert_user_sql + SqlQueries.user_table_insert)
)
insert_song_sql = """INSERT INTO songs (
            songid ,
            title ,
            artistid ,
            "year", 
            duration 
        )"""
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    statement_sql=(insert_song_sql + SqlQueries.song_table_insert)
)

insert_artist_sql = """INSERT INTO artists (
                artistid ,
                name ,
                location ,
                lattitude,
                longitude 
            )"""
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    statement_sql=(insert_artist_sql + SqlQueries.artist_table_insert)
)

insert_time_sql = """INSERT INTO time (
             t_start_time  ,
                        t_hour , 
                        t_day , 
                        t_week , 
                        t_month , 
                        t_year , 
                        t_weekday 
            )"""
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    statement_sql=(insert_time_sql + SqlQueries.time_table_insert)
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_list=['users','songs','time','artists','songplays']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift  >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table  >> run_quality_checks
load_songplays_table >> load_artist_dimension_table  >> run_quality_checks
load_songplays_table >> load_time_dimension_table  >> run_quality_checks
run_quality_checks >> end_operator

