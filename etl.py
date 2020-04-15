import configparser
import os
from pyspark.sql import SparkSession, SQLContext


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
    df = spark.read.json(song_data)
    df.registerTempTable("staging_songs")

    sqlContext = SQLContext(spark.sparkContext)

    # extract columns to create songs table
    songs_table = sqlContext.sql("""
                     SELECT  DISTINCT song_id AS song_id,
                                      title,
                                      artist_id,
                                      year,
                                      duration
                     FROM staging_songs
                     WHERE song_id IS NOT NULL;
                  """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, 'dend_4/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = sqlContext.sql("""
                         SELECT  DISTINCT artist_id  AS artist_id,
                                          artist_name         AS name,
                                          artist_location     AS location,
                                          artist_latitude     AS latitude,
                                          artist_longitude    AS longitude
                                 FROM staging_songs
                                 WHERE artist_id IS NOT NULL;
                         """)
    
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
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.registerTempTable("staging_events")

    sqlContext = SQLContext(spark.sparkContext)

    # extract columns for users table    
    users_table = sqlContext.sql("""
                                SELECT  DISTINCT userId     AS user_id,
                                        firstName           AS first_name,
                                        lastName            AS last_name,
                                        gender,
                                        level
                                FROM staging_events
                                WHERE user_id IS NOT NULL and level IS NOT NULL;
                         """)
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'dend_4/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    time_table = sqlContext.sql("""
                                SELECT  DISTINCT start_time                 AS start_time,
                                        EXTRACT(hour FROM start_time)       AS hour,
                                        EXTRACT(day FROM start_time)        AS day,
                                        EXTRACT(week FROM start_time)       AS week,
                                        EXTRACT(month FROM start_time)      AS month,
                                        EXTRACT(year FROM start_time)       AS year,
                                        EXTRACT(dayofweek FROM start_time)  as weekday
                                FROM staging_events
                                """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
        .parquet(os.path.join(output_data, 'dend_4/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    song_df.registerTempTable("staging_songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = sqlContext.sql("""
                                   SELECT  e.ts            AS start_time, 
                                            e.userId        AS user_id, 
                                            e.level         AS level, 
                                            so.idSong       AS idSong,
                                            a.idArtist      AS idArtist
                                            e.sessionId     AS session_id, 
                                            e.location      AS location, 
                                            e.userAgent     AS user_agent
                                    FROM staging_events e
                                    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
                                    JOIN songs so on s.song_id = so.song_id
                                    JOIN artists a on s.artist_id = a.artist_id
                                """)

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
