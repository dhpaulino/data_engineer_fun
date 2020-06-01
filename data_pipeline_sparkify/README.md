# Sparkify's Data Lake

  

## Plot

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.
  

## How to run
1. Install airflow
```console
$ pip install airflow 
$ airflow initdb
```
2. Create a IAM Role on AWS with permission to read from S3
3. Create a Redshift cluster, giving the IAM role created to it
2. Run airflow web UI:
```console
$ airflow webserver
```
4. Create the variable `redshift_s3_role` with the IAM role ARN as the value on the UI.
5. Create a connection called `redshift_conn` on the UI with the redshift database credentials
6. Copy the project files to `AIRFLOW_HOME`(eg. `~/airflow`)
7. Start the scheduler:
```console
$ airflow scheduler
```
8. Use the UI to enable the `songplays_S3_to_DWH`dag


