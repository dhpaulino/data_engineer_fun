from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator

queries_has_row = [f"SELECT EXISTS(SELECT * FROM {table})"
                   for table in ["songplays", "songs", "artists", "users", "time"]]

# TODO: PUT  WHERE page='NextSong' ON QUERIES
# TODO: JOIN ON NULL FOR ONLY ADD NO REPEATED VALUES
# FIXME: data is being added (not truncate first) on dimensions based on staging (not songplays), but how to ensure
# no duplicates
dag = DAG('songplays_S3_to_DWH',
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          start_date=datetime(2020, 5, 29),
          default_args={
              "owner": "dhpaulino"
          }
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
drop_all_tables_for_test_dag = PostgresOperator(
    task_id="drop_all_tables_for_test_dag",
    dag=dag,
    postgres_conn_id="redshift_conn",
    sql=[f"DROP TABLE IF EXISTS {table}"
         for table in ["staging_events", "staging_songs", "songplays", "songs", "artists", "users", "time"]]
)
drop_all_tables_for_test_dag >> start_operator

prepare_tables = PostgresOperator(
    task_id="prepare_tables",
    dag=dag,
    postgres_conn_id="redshift_conn",
    sql="queries/prepare_tables.sql"
)
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift_conn",
    arn_iam_role=Variable.get("redshift_s3_role"),
    output_table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    copy_parameters="JSON 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift_conn",
    arn_iam_role=Variable.get("redshift_s3_role"),
    output_table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    copy_parameters="JSON 'auto'",
    dag=dag
)

load_songplays_fact_table = PostgresOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    postgres_conn_id="redshift_conn",
    sql="queries/move_staging_to_fact_songplays_table.sql"
)
load_users_dimension_table = PostgresOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    postgres_conn_id="redshift_conn",
    sql="queries/move_staging_to_dim_users_table.sql"
)

load_songs_dimension_table = PostgresOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    postgres_conn_id="redshift_conn",
    sql="queries/move_staging_to_dim_songs_table.sql"
)

load_artists_dimension_table = PostgresOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    postgres_conn_id="redshift_conn",
    sql="queries/move_staging_to_dim_artists_table.sql"
)

# FIXME: no data is load in the table
load_time_dimension_table = PostgresOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id="redshift_conn",
    sql="queries/move_staging_to_dim_time_table.sql"
)
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id="redshift_conn",
    queries=queries_has_row,
    expected_results=[True, True, True, True, True]
)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
##### ORDERING DAGS #####
start_operator >> prepare_tables

prepare_tables >> stage_events_to_redshift
prepare_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

load_songplays_fact_table >> load_users_dimension_table
load_songplays_fact_table >> load_songs_dimension_table
load_songplays_fact_table >> load_artists_dimension_table
load_songplays_fact_table >> load_time_dimension_table

load_users_dimension_table >> run_quality_checks
load_songs_dimension_table >> run_quality_checks
load_artists_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator