import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
       Description: This function prepare a spark instance
    """
    return SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()


def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and extracting the songs and artist tables
        to send it back to S3

        Parameters:
            spark       : Spark Session
            input_data  : S3 song_data's location
            output_data : S3 output's location
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # created song view to write SQL Queries
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT 
                                song_id, 
                                title,
                                artist_id,
                                year,
                                duration
                            FROM song_data_table
                            WHERE song_id IS NOT NULL
                        """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT 
                                    artist_id, 
                                    artist_name,
                                    artist_location,
                                    artist_latitude,
                                    artist_longitude
                                FROM song_data_table
                                WHERE artist_id IS NOT NULL
                            """)

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
       Description: This function loads song_data from S3 and extracting the songs and artist tables
        to write it back to S3.

        Parameters:
            spark       : Spark Session
            input_data  : S3 song_data's location
            output_data : S3 output's location

   """
    # get filepath to log data file
    # Use a subset to build quickly
    log_path = input_data + 'log_data/A/B/C.json'

    # read log data file
    df = spark.read.json(log_path)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # created log view to write SQL Queries
    df.createOrReplaceTempView("log_data_table")

    # extract columns for users table
    users_table = spark.sql("""
                                SELECT 
                                    DISTINCT(ldt.userId) AS user_id, 
                                    ldt.firstName AS first_name,
                                    ldt.lastName AS last_name,
                                    ldt.gender AS gender,
                                    ldt.level AS level
                                FROM log_data_table ldt
                                WHERE ldt.userId IS NOT NULL
                            """)

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table/')

    ## create timestamp column from original timestamp column
    # get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    # df = df.withColumn("timestamp", get_timestamp(df.ts))
    #
    # # create datetime column from original timestamp column
    # get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    # df = df.withColumn("datetime", get_datetime(df.ts))
    #
    # # extract columns to create time table
    # time_table = df.select(
    #     'timestamp',
    #     hour('datetime').alias('hour'),
    #     dayofmonth('datetime').alias('day'),
    #     weekofyear('datetime').alias('week'),
    #     month('datetime').alias('month'),
    #     year('datetime').alias('year'),
    #     date_format('datetime', 'F').alias('weekday')
    # )

    # extract columns to create time table
    time_table = spark.sql("""
                                SELECT 
                                    TT.time as start_time,
                                    hour(TT.time) as hour,
                                    dayofmonth(TT.time) as day,
                                    weekofyear(TT.time) as week,
                                    month(TT.time) as month,
                                    year(TT.time) as year,
                                    dayofweek(TT.time) as weekday
                                FROM (
                                    SELECT 
                                        to_timestamp(ldt.ts/1000) as time
                                    FROM log_data_table ldt
                                    WHERE ldt.ts IS NOT NULL
                                ) TT
                            """)

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')

    # read in song data to use for songplays table
    # song_df = spark.read.parquet(output_data + 'songs_table/')

    # read song data file
    # song_df_upd = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    # created song view to write SQL Queries
    # song_df_upd.createOrReplaceTempView("song_data_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
                                    SELECT 
                                        monotonically_increasing_id() AS songplay_id,
                                        to_timestamp(logT.ts/1000) AS start_time,
                                        month(to_timestamp(logT.ts/1000)) AS month,
                                        year(to_timestamp(logT.ts/1000)) AS year,
                                        logT.userId AS user_id,
                                        logT.level AS level,
                                        songT.song_id AS song_id,
                                        songT.artist_id AS artist_id,
                                        logT.sessionId AS session_id,
                                        logT.location AS location,
                                        logT.userAgent AS user_agent
                                    FROM log_data_table logT
                                    JOIN song_data_table songT 
                                        ON logT.artist = songT.artist_name 
                                        AND logT.song = songT.title
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays_table/')


def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/dloutput/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
