import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import  PostgresOperator
from airflow.operators.python_operator   import  PythonOperator


CREATE_ARTISTS_TABLE_SQL = """CREATE TABLE IF NOT EXISTS public.artists (
                    artistid varchar(256) NOT NULL,
                    name varchar(256),
                    location varchar(256),
                    lattitude numeric(18,0),
                    longitude numeric(18,0)
                    );
                    """
CREATE_SONGPLAYS_TABLE_SQL = """CREATE TABLE IF NOT EXISTS public.songplays (
                    playid varchar(32) NOT NULL,
                    start_time timestamp NOT NULL,
                    userid int4 NOT NULL,
                    "level" varchar(256),
                    songid varchar(256),
                    artistid varchar(256),
                    sessionid int4,
                    location varchar(256),
                    user_agent varchar(256),
                    CONSTRAINT songplays_pkey PRIMARY KEY (playid)
                    );
                    """

CREATE_SONGS_TABLE_SQL = """ CREATE TABLE IF NOT EXISTS public.songs (
                    songid varchar(256) NOT NULL,
                    title varchar(256),
                    artistid varchar(256),
                    "year" int4,
                    duration numeric(18,0),
                    CONSTRAINT songs_pkey PRIMARY KEY (songid)
                );
                """

CREATE_ST_EVENTS_TABLE_SQL = """  CREATE TABLE IF NOT EXISTS public.staging_events (
                        artist varchar(256),
                        auth varchar(256),
                        firstname varchar(256),
                        gender varchar(256),
                        iteminsession int4,
                        lastname varchar(256),
                        length numeric(18,0),
                        "level" varchar(256),
                        location varchar(256),
                        "method" varchar(256),
                        page varchar(256),
                        registration numeric(18,0),
                        sessionid int4,
                        song varchar(256),
                        status int4,
                        ts int8,
                        useragent varchar(256),
                        userid int4
                    );
                    """

CREATE_ST_SONGS_TABLE_SQL = """CREATE TABLE IF NOT EXISTS public.staging_songs (
                        num_songs int4,
                        artist_id varchar(256),
                        artist_name varchar(256),
                        artist_latitude numeric(18,0),
                        artist_longitude numeric(18,0),
                        artist_location varchar(256),
                        song_id varchar(256),
                        title varchar(256),
                        duration numeric(18,0),
                        "year" int4
                    );
                    """

CREATE_USERS_TABLE_SQL = """CREATE TABLE IF NOT EXISTS public.users (
                    userid int4 NOT NULL,
                    first_name varchar(256),
                    last_name varchar(256),
                    gender varchar(256),
                    "level" varchar(256),
                    CONSTRAINT users_pkey PRIMARY KEY (userid)
                );
                """
CREATE_TIME_TABLE_SQL ="""CREATE TABLE IF NOT EXISTS public.time (
                        t_start_time timestamp PRIMARY KEY ,
                        t_hour int, 
                        t_day int, 
                        t_week int, 
                        t_month int, 
                        t_year int, 
                        t_weekday int)
                    """

dag = DAG(
    "create_tables_dag",
    start_date=datetime.datetime(2019,1,12),
    max_active_runs=1,
    schedule_interval="@once"
)
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
create_st_events_table = PostgresOperator(
    task_id="create_st_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    database="dwh",
    sql=CREATE_ST_EVENTS_TABLE_SQL
)
create_st_songs_table = PostgresOperator(
    task_id="create_st_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    database="dwh",
    sql=CREATE_ST_SONGS_TABLE_SQL
)

create_artist_table = PostgresOperator(
    task_id="create_artist_table",
    dag=dag,
    postgres_conn_id="redshift",
    database="dwh",
    sql=CREATE_ARTISTS_TABLE_SQL
)
create_users_table = PostgresOperator(
    task_id="create_users_table",
    dag=dag,
    postgres_conn_id="redshift",
    database="dwh",
    sql=CREATE_USERS_TABLE_SQL
)

create_songply_table = PostgresOperator(
    task_id="create_songply_table",
    dag=dag,
    postgres_conn_id="redshift",
    database="dwh",
    sql=CREATE_SONGPLAYS_TABLE_SQL
)

create_songs_table = PostgresOperator(
    task_id="create_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    database="dwh",
    sql=CREATE_SONGS_TABLE_SQL
)

create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id="redshift",
    database="dwh",
    sql=CREATE_TIME_TABLE_SQL
)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_st_events_table
start_operator >> create_songply_table >> create_time_table
start_operator >> create_st_songs_table >> create_artist_table 
start_operator >> create_songs_table >> create_users_table
create_artist_table >> end_operator
create_users_table >> end_operator
create_time_table >> end_operator


