import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def get_song_data():
    # Get from local filesystem
    import glob
    files = glob.glob("data/song_data/**/*.json", recursive=True)
    #del files[0] #remove the directory itself
    return files


def get_log_data():
    import glob
    files = glob.glob("data/log-data/*.json", recursive=True)
    #del files[0] #remove the directory itself
    return files


def get_files_paths_s3(bucket, prefix):
    import boto3
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = ["s3a://{}/{}".format(bucket, item["Key"]) for item in response["Contents"]]
    del files[0] #remove the directory itself
    return files


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data = input_data + 'song_data/*/*/*/*.json' 
    #song_data = "s3a://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json"
    song_data = get_files_paths_s3('udacity-dend', "song_data")
    print(song_data)

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

    # read song data file
    df = spark.read.schema(song_schema).json(song_data) 

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year",\
        "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id")\
    .parquet("{}/songs_table.parquet".format(output_data), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", "artist_name as name",\
        "artist_location as location",\
            "artist_latitude as latitude", "artist_longitude as longitude"])
    
    # write artists table to parquet files
    artists_table.write.parquet("{}artists_table.parquet".format(output_data),\
        mode="overwrite")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    #log_data = input_data + "log-data/*.json"
    log_data = get_files_paths_s3("udacity-dend", "log_data")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.selectExpr(["userId as user_id",\
         "firstName as first_name", "lastName as last_name","gender", "level"]) 
    
    # write users table to parquet files
    users_table.write.parquet("{}users_table.parquet".format(output_data), mode="overwrite")

    # create timestamp column from original timestamp column
    df = df.withColumn("ts_timestamp", (df.ts/1000).cast(T.TimestampType()))
    
    # extract columns to create time table
    time_table = df.selectExpr("ts_timestamp as start_time",\
        "hour(ts_timestamp) as hour","weekofyear(ts_timestamp) as week",\
        "month(ts_timestamp) as month", "year(ts_timestamp) as year",\
        "weekday(ts_timestamp) as weekday") 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(["year", "month"])\
        .parquet("{}time_table.parquet".format(output_data), mode="overwrite")
    # read in song data to use for songplays table
    song_df = spark.read.parquet("{}songs_table.parquet".format(output_data))

    df_joined = df.join(F.broadcast(song_df), df.song == song_df.title)
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_joined\
        .selectExpr(["monotonically_increasing_id() as id",\
            "ts_timestamp as start_time", "userId as user_id", "level",\
            "song_id", "artist_id", "sessionId as session_id", "location",\
            "userAgent as user_agent"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn("month", F.month("start_time"))
    songplays_table = songplays_table.withColumn("year", F.year("start_time"))
    songplays_table.write.partitionBy("year", "month")\
        .parquet("{}songplays_table.parquet".format(output_data), mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://davisson-udacity/"
    #input_data = "data/"
    #output_data = "data/"
    process_song_data(spark, input_data, output_data)
    print("=====END PART 1======") 
    process_log_data(spark, input_data, output_data)
    print("=====END PART 2======") 
    spark.stop()

if __name__ == "__main__":
    main()
