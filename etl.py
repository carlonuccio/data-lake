import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Get or Create a Spark Session
    :return: Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Get Input Data
    - Process Song Data with Spark SQL
    - Write data in parquet format to S3 Bucket
    :param spark: spark session
    :param input_data: Path Song Data
    :param output_data: path output data
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    staging_songs = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = staging_songs.select(['song_id', 'title', 'artist_id', 'year', 'duration'])\
                               .where(staging_songs.song_id.isNotNull())\
                               .dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
        .parquet(os.path.join(output_data, 'dend_4/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = staging_songs.selectExpr("artist_id", "artist_name as name",
                                             "artist_location as location", "artist_latitude as latitude",
                                             "artist_longitude as longitude")\
                                 .where(staging_songs.artist_id.isNotNull()) \
                                 .dropDuplicates(['artist_id'])

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'dend_4/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    """
    - Get Input Data
    - Process Log Data with Spark SQL
    - Write data in parquet format to S3 Bucket
    :param spark: spark session
    :param input_data: Path Log Data
    :param output_data: path output data
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    staging_events = spark.read.json(log_data)

    # filter by actions for song plays
    staging_events = staging_events.filter(staging_events.page == 'NextSong')

    # extract columns for users table
    users_table = staging_events.selectExpr("userId as user_id", "firstName as first_name",
                                             "lastName as last_name", "gender",
                                             "level")\
                                 .where(staging_events.userId.isNotNull() &
                                        staging_events.level.isNotNull()) \
                                 .dropDuplicates(['user_id', 'level'])

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'dend_4/users.parquet'), 'overwrite')


    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    staging_events = staging_events.withColumn("start_time", get_datetime(staging_events.ts))

    # extract columns for time table
    time_table = staging_events.selectExpr("start_time", "EXTRACT(hour FROM start_time) as hour",
                                             "EXTRACT(day FROM start_time) as day",
                                             "EXTRACT(week FROM start_time)       AS week",
                                             "EXTRACT(month FROM start_time)      AS month",
                                             "EXTRACT(year FROM start_time)       AS year",
                                             "EXTRACT(dayofweek FROM start_time)  as weekday")\
                                 .where(staging_events.start_time.isNotNull()) \
                                 .dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'dend_4/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    staging_songs = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = staging_events.join(staging_songs, (staging_events.song == staging_songs.title) &
                                          (staging_events.artist == staging_songs.artist_name), "inner")\
                                    .selectExpr("start_time", "userId AS user_id", "level", "song_id",
                                                "artist_id", "sessionId AS session_id", "location",
                                                "userAgent     AS user_agent")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'dend_4/songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://<output-data>/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()