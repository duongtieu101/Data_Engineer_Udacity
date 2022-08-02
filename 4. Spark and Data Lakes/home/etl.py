import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create spark session.
    Input:
        None
    Output:
        spark: is the spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song data file and add data to songs_table and artists_tables table.
    Input:
        spark: spark session.
        input_data: path of input data folder (song data).
        output_data: place to write the output (this workspace).
    Output:
        songs_table and artists_table tables.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # create song temp view for joining to create songplays table
    df.createOrReplaceTempView("song_view")
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id") \
                .parquet(output_data + 'songs_data')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', \
                              'artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_data')


def process_log_data(spark, input_data, output_data):
    """
    Process the log data file and add data to time_table, users_table and songplays_table tables.
    Input:
        spark: spark session.
        input_data: path of input data folder (log data).
        output_data: place to write the output (this workspace).
    Output:
        users_table, time_table and songplays_table tables.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_data')

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn('timestamp', df['ts']/1000)
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = df.withColumn('start_time', from_unixtime(df['timestamp']))
    
    # extract columns to create time table
    time_table = df.select('start_time') \
                .withColumn('hour', hour(df['start_time'])) \
                .withColumn('day', dayofmonth(df['start_time'])) \
                .withColumn('week', weekofyear(df['start_time'])) \
                .withColumn('month', month(df['start_time'])) \
                .withColumn('year', year(df['start_time'])) \
                .withColumn('weekday', (dayofweek(df['start_time'])+5)%7+1) \
                .distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + 'time_data')

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT * FROM song_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) \
                                       & (df.artist == song_df.artist_name)) \
                        .distinct() \
                        .select('start_time', 'userId', 'level', 'song_id',\
                                'artist_id', 'sessionId','location','userAgent') \
                        .withColumn("year", year(df['start_time'])) \
                        .withColumn("month", month(df['start_time'])) \
                        .withColumn("songplay_id", monotonically_increasing_id())
            

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month') \
                   .parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
