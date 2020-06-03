# Sparkify's Data Warehouse

## Plot
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## How to run

1. Install project dependencies
```console
$ pip install -r requirements.txt
```
2. Create a IAM role with permission to read from S3 on AWS
3. Create a redshift cluster, giving the IAM role to it
4. Fill the `dwh.cfg` with the database connection info and the ARN of the role
5. Create/reset the database:
```console
$ python create_table.py
```
6. Run the ETL:
```console
$ python etl.py
```
## Project structure

* `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
* `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
* `sql_queries.py` contains all your sql queries, and is imported into the last three files above
* `dwh.cfg` is the project configuration file