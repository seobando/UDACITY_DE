import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Process the songs data files and create extract songs table and artist table data from it.

    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # Song Schema
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])
    
    # read song data file
    df = spark.read.json(song_data,schema=song_schema)

    # extract columns to create songs table
    songs_table = (df
                   .select(col("song_id")
                           ,col("title")
                           ,col("artist_id")
                           ,col("year")
                           ,col("duration"))
                   .dropDuplicates()
                  )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(f'{output_data}songs_table' , mode="overwrite")
    
    # extract columns to create artists table
    artists_table = (df
                     .select(col("artist_id")
                             ,col("artist_name")
                             ,col("artist_location")
                             ,col("artist_latitude")
                             ,col("artist_longitude"))
                     .dropDuplicates()
                    )  
    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}artists_table.parquet' , mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Description: Process the event log file and extract data for table time, users and songplays from it.
    
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """   
    
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # Log Schema
    log_schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", DoubleType()),
        StructField("first_name", StringType()),
        StructField("gender", StringType()),
        StructField("item_in_session", LongType()),
        StructField("last_name", StringType()),
        StructField("length", DoubleType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", DoubleType()),
        StructField("session_id", LongType()),
        StructField("song", StringType()),
        StructField("status", LongType()),
        StructField("ts", LongType()),
        StructField("user_agent", StringType()),
        StructField("user_id", StringType())
    ])    
    
    
    # read log data file
    df = spark.read.json(log_data,schema= log_schema)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = (df.select(col("user_id")
                               ,col("first_name")
                               ,col("last_name")
                               ,col("gender")
                               ,col("level")
                              )
                     .dropDuplicates()
                    ) 
    
    # write users table to parquet files
    users_table.write.parquet(f'{output_data}users_table' , mode="overwrite") 

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn('datetime', get_datetime('ts'))
    df = (df
          .withColumnRenamed("ts","start_time")
          .withColumn('hour',hour("timestamp"))
          .withColumn('day',dayofmonth("datetime"))
          .withColumn('week',weekofyear("datetime"))
          .withColumn('month',month("datetime"))
          .withColumn('year',year("datetime"))
          .withColumn('weekday',dayofweek("datetime"))
         )
    
    # extract columns to create time table
    time_table = (df.select(col("start_time")
                          ,col("hour")
                          ,col("day")
                          ,col("week")
                          ,col("month")
                          ,col("year")
                          ,col("weekday")
                          )
                  .dropDuplicates()
                 )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(f'{output_data}time_table',mode="overwrite")

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # Song Schema
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", StringType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data,schema=song_schema).drop("year")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (df.join(song_df, df["song"] == song_df["title"], how='inner')
                   .select(monotonically_increasing_id().alias("songplay_id")
                           ,col("start_time")
                           ,col("user_id")
                           ,col("level")
                           ,col("song_id")
                           ,col("artist_id")
                           ,col("session_id")
                           ,col("location")
                           ,col("user_agent")
                           ,col("year")
                           ,col("month")
                          )
                  )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(f'{output_data}songplays_table',mode="overwrite")


def main():
    spark = create_spark_session()
    
    input_data ='s3://udacity-dend-2021/input/'
    output_data ='s3://udacity-dend-2021/output/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
