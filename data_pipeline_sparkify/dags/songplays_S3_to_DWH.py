from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from airflow.operators.postgres_operator import PostgresOperator

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
##### ORDERING DAGS #####

prepare_tables >> stage_events_to_redshift
prepare_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

load_songplays_fact_table >> load_users_dimension_table
load_songplays_fact_table >> load_songs_dimension_table
load_songplays_fact_table >> load_artists_dimension_table
load_songplays_fact_table >> load_time_dimension_table
