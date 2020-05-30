from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

dag = DAG('songplays_S3_to_DWH',
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None,
          start_date=datetime(2020, 5, 29),
          default_args={
              "owner": "dhpaulino"
          }
        )


# TODO: create/check/empty tables before the process
'''
CREATE TABLE IF NOT EXISTS public.staging_events(
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

TRUNCATE  public.staging_events;
'''
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift_conn",
    arn_iam_role=Variable.get("redshift_s3_role"),
    output_table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    copy_parameters="JSON 's3://udacity-dend/log_json_path.json' timeformat as 'epochmillisecs'",
    dag=dag
)
