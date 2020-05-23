"""
Extract data from the song_data and log_data dataset, transformit it and create
tables related with a music played by the user event
"""

import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import boto3


def create_spark_session(aws_key, aws_secret):
    """Configure the SparkSession to work with AWS S3"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", aws_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret) \
        .getOrCreate()
    return spark


'''
# functions to work with local filesystem
def get_local_song_data():
    import glob
    files = glob.glob("data/song_data/**/*.json", recursive=True)
    return files
def get_local_log_data():
    import glob
    files = glob.glob("data/log-data/*.json", recursive=True)
    return files
'''


def get_files_paths_s3(bucket, directory):
    """Gets all files from a S3 bucket inside a specified directory and it's
    subdirectories"""

    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=directory)
    files = ["s3a://{}/{}"
                .format(bucket, item["Key"]) for item in response["Contents"]]
    del files[0]  # remove the directory itself
    return files


def process_song_data(spark, input_bucket, output_data):
    """ Reads the songs dataset, transforms it, creating artists and
    songs table in parquet files"""

    song_data = get_files_paths_s3(input_bucket, "song_data")
    # song_data = get_local_song_data()

    # specify schema to improve read speed
    song_schema = T.StructType()\
        .add("num_songs", T.IntegerType())\
        .add("artist_id", T.StringType())\
        .add("artist_latitude", T.DoubleType())\
        .add("artist_longitude", T.DoubleType())\
        .add("artist_location", T.StringType())\
        .add("artist_name", T.StringType())\
        .add("song_id", T.StringType())\
        .add("title", T.StringType())\
        .add("duration", T.DoubleType())\
        .add("year", T.IntegerType())

    df = spark.read.schema(song_schema).json(song_data)
    songs_table = df.select(["song_id", "title", "artist_id", "year",
                            "duration"])
    songs_table.write.partitionBy("year", "artist_id")\
        .parquet("{}/songs_table.parquet".format(output_data),
                mode="overwrite")
    # Since the data is song based there can be duplicated artists
    artists_table = df.selectExpr(["artist_id", "artist_name as name",
                                "artist_location as location",
                                "artist_latitude as latitude",
                                "artist_longitude as longitude"])\
                        .dropDuplicates(["artist_id"])

    artists_table.write.parquet("{}artists_table.parquet".format(output_data),
                            mode="overwrite")


def process_log_data(spark, input_bucket, output_data):
    """ Reads the log_data dataset, transforms it, creating the songplays and
    time table, saving it in parquet files"""

    log_data = get_files_paths_s3(input_bucket, "log_data")
    # log_data = get_local_log_data()

    df = spark.read.json(log_data)

    # Gets only the music play event
    df = df.filter("page == 'NextSong'")

    # Getting the user's data from his last activity so the info is up to date
    user_activity_window = Window.partitionBy("userId").orderBy(F.desc("ts"))
    users_table = df.withColumn("user_row", F.row_number()
                                .over(user_activity_window))\
                    .where(F.col("user_row") == 1)
    users_table = users_table.selectExpr(["userId as user_id",
                                        "firstName as first_name",
                                        "lastName as last_name",
                                        "gender", "level"])

    users_table.write.parquet("{}users_table.parquet".format(output_data),
                            mode="overwrite")

    df = df.withColumn("ts_timestamp", (df.ts/1000).cast(T.TimestampType()))

    time_table = df.selectExpr(["ts_timestamp as start_time",
                                "hour(ts_timestamp) as hour",
                                "weekofyear(ts_timestamp) as week",
                                "month(ts_timestamp) as month",
                                "year(ts_timestamp) as year",
                                "weekday(ts_timestamp) as weekday"])\
        .dropDuplicates(["start_time"])

    time_table.write.partitionBy(["year", "month"])\
        .parquet("{}time_table.parquet".format(output_data), mode="overwrite")

    # Obtain the songs table, since the song_id and artist_id is needed and we
    # only have song_name
    song_df = spark.read.parquet("{}songs_table.parquet".format(output_data))

    df_joined = df.join(F.broadcast(song_df), df.song == song_df.title)
    songplays_table = df_joined\
        .selectExpr(["monotonically_increasing_id() as songplay_id",
                    "ts_timestamp as start_time", "userId as user_id", "level",
                    "song_id", "artist_id", "sessionId as session_id",
                    "location", "userAgent as user_agent"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn("month",
                                                F.month("start_time"))
    songplays_table = songplays_table.withColumn("year", F.year("start_time"))
    songplays_table.write.partitionBy("year", "month")\
        .parquet("{}songplays_table.parquet".format(output_data),
                mode="overwrite")


def main():

    config = configparser.ConfigParser()
    config.read('dl.cfg')
    # Put AWS credentials on enviroment for boto3
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS',
                                                    'AWS_SECRET_ACCESS_KEY')
    spark = create_spark_session(os.environ['AWS_ACCESS_KEY_ID'],
                                os.environ['AWS_SECRET_ACCESS_KEY'])
    input_bucket = "udacity-dend"
    output_data = "s3a://davisson-udacity/"
    # output_data = "data/output/"

    process_song_data(spark, input_bucket, output_data)
    process_log_data(spark, input_bucket, output_data)
    spark.stop()


if __name__ == "__main__":
    main()
