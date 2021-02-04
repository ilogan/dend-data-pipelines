from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    "owner": "Ian Logan",
    "start_date": datetime(2018, 11, 1),
    "end_date": datetime(2018, 11, 30),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "catchup_by_default": False
}

dag = DAG("s3_to_redshift_etl_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval="@daily",
          )

# start
start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

# staging in database
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    s3_jsonpaths_file="log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    s3_key="song_data"
)

# load fact table
load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    table="songplays",
    sql_select=SqlQueries.songplay_table_insert
)

# load dimension tables
load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    table="users",
    sql_select=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    table="songs",
    sql_select=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    table="artists",
    sql_select=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    table="time",
    sql_select=SqlQueries.time_table_insert
)

# data quality checking
run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag
)

# end
end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)

# dag dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
