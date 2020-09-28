#!/usr/bin/env python
# coding: utf-8

# In[30]:


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, StructField, StructType, LongType


# In[31]:


def create_spark_session():
    spark = SparkSession         .builder         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")         .getOrCreate()
    return spark


# In[32]:


def process_song_data(spark, input_data, output_data):
    '''
    This function to read song data from JSON files, then will extract columns for songs & artists tables, then write their data into parquet files.
    
    Following steps will be performed:
    
    1. Specify the path of song data JSON files is "s3a://udacity-dend/song_data/A/A/*/*.json". 
        It means all JSON files located inside subdirectories of "s3a://udacity-dend/song_data/A/A" will be read.
        Data will be read and stored in a dataframe.
        
    2. Call function process_songs_table() to process songs table
    
    3. Call function process_artists_table() to process artists table
    '''
    
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/*/*.json"
    
    # read song data file
    df = read_song_data(spark, song_data)
    
    # process songs table
    process_songs_table(df, output_data)

    # process artist table
    process_artists_table(df, output_data)


# In[33]:


def process_log_data(spark, input_data, output_data):
    '''
    This function to read log data from JSON files, then will extract columns for users, time & songplays tables, then write their data into parquet files.
    
    Following steps will be performed:
    
    1. Specify the path of song data JSON files is "s3a://udacity-dend/log_data/2018/11/*.json". 
        It means all JSON files located inside subdirectories of "s3a://udacity-dend/log_data/2018/11" will be read.
        Data will be read into a dataframe.
        
    2. Filtering the dataframe at step 1 to select only rows with 'page' of 'NextSong', 
        and also remove records which has NA value in one of following columns: 'ts', 'song', 'userId', 'lastName' or 'firstName'.
        
    3. Call function process_users_table() to process users table.
    
    4. Adding new columns into log dataframe, then call function process_time_table() to process time table.
    
    5. Call function process_songplays_table() to process songplays table.
    '''
    
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = read_log_data(spark, log_data)
    
    # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong').dropna(subset=['ts', 'song', 'userId', 'firstName', 'lastName'])
    
    # create timestamp column from original timestamp column
    df = df.withColumn("timesttamp", F.from_unixtime(df.ts/1000))
    
    # create datetime column from original timestamp column
    df = df.withColumn("datetime", F.from_utc_timestamp(df["timesttamp"], 'PST'))
    
    # process users table
    process_users_table(df, output_data)
    
    # process time table
    process_time_table(df, output_data) 
    
    # process songplays table
    process_songplays_table(df, output_data)
    


def process_songs_table(df, output_data):
    '''
    This function to select columns from song dataframe to build songs table.
    
    Input:
        - song dataframe
        - S3 path where parquet files will be stored.
    
    Following steps will be perform:
        - From song dataframe, create a temporary view, then we will use SQL command to query data from it.
        Retrieved data will be kept in 'songs_table' dataframe.
        
        - Write 'songs_table' dataframe into parquet files.
    
    '''
    
    # create a temp view for song data
    df.createOrReplaceTempView('song_data')
    
    # extract columns to create songs table
    songs_table = spark.sql('''
                            select DISTINCT song_id, title, artist_id, year, duration
                            from song_data
                            ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs.parquet")
    

    
    
def process_artists_table(df, output_data):
    '''
    This function to select columns from song dataframe to build artists table.
    
    Input:
        - song dataframe.
        - a S3 path where parquet files will be stored.
    
    Following steps will be performed:
        - From song dataframe, create a temporary view, then we will use SQL command to query data from it.
        Retrieved data will be kept in 'songs_table' dataframe.
        
        - Write 'songs_table' dataframe into parquet files.
    
    '''
    
    # create a temp view for song data
    df.createOrReplaceTempView('song_data')
    
    # extract columns to create artists table
    artists_table = spark.sql('''
                                select  DISTINCT artist_id, artist_name as name, artist_location as location, 
                                        artist_latitude as latitude, artist_longitude as longitude
                                from song_data
                                ''')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artist.parquet")
    

    
    
def process_users_table(df, output_data):
    '''
    This function to select columns needed to build users table, then write data into parquet files.
    
    Input:
        - a dataframe of log data.
        - a S3 path where parquet files will be stored.
        
    Following steps will be performed:
        - Select columns from log dataframe, rename columns and remove duplicated records.
            Retrieved data will be kept in 'users_table' dataframe.
            
        - write data of 'users_table' into parquet files.
    '''
    
    # extract columns for users table    
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet")


    
    
def process_time_table(df, output_data):
    '''
    This function to select columns needed to build time table, then write data into parquet files.
    
    Input:
        - a dataframe of log data.
        - a S3 path where parquet files will be stored.
        
    Following steps will be performed:
        - From log dataframe, select columns and rename them. Result will be kept in a 'time_table' dataframe.
        - Write 'time_table' data into parquet files.

    '''
    
    # extract columns to create time table
    time_table = df.select( df.datetime.alias('start_time'),
                    F.hour(df.datetime).alias('hour'),
                    F.dayofmonth(df.datetime).alias('day'),
                      F.weekofyear(df.datetime).alias('week'),
                      F.month(df.datetime).alias('month'),
                      F.year(df.datetime).alias('year'),
                      F.dayofweek(df.datetime).alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + "time.parquet")


    
    
    
def process_songplays_table(df, output_data):
    '''
    This function to select columns needed to build songplays table, then write data into parquet files.
    
    Input:
        - a dataframe of log data.
        - a S3 path where parquet files will be stored.
        
    Following steps will be performed:
        - Select some columns of song data from the temporary view 'song_data' defined inside process_songs_table() function.
        - Joining song data with log data.
        - Create temporary view from joined dataset.
        - Select columns from dataset, we also define some new columns like: songplay_id, year, month which are needed to make songplays table as well as partition.
        - Write data to parquet files.
    '''
    
    # read in song data to use for songplays table
    song_df = spark.sql('''
                        select song_id, title, artist_id, artist_name, duration
                        from song_data
                        ''')

    # extract columns from joined song and log datasets to create songplays table
    s = song_df.alias('s')
    l = df.alias('l')
    songplays_table = l.join(s, (l.song == s.title) & (l.length == s.duration) & (l.artist == s.artist_name))
    
    # create a temporary view of songplays_table
    songplays_table.createOrReplaceTempView('songplays')
    
    
    # selecting columns from temporary view, and define some new columns required to make songplays table as well as partition.
    songplays_table = spark.sql('''
                            select row_number()over(order by datetime) as songplay_id, datetime as start_time, year(datetime) as year, month(datetime) as month,
                                userId as user_id, level, song_id, artist_id, sessionId as session_id, location,  userAgent as user_agent
                            from songplays
                            ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + "songplays.parquet")


# In[34]:


def read_song_data(spark, song_data_files):
    '''
    This function to read JSON song files into dataframe with predefined schema.
    
    Input: 
        - spark application.
        - path where JSON song files located.
    
    Output:
        - return a PySpark dataframe of song data with a schema.
    '''
    
    song_template = [
                        StructField('artist_id', StringType()),
                        StructField('artist_latitude', DoubleType()),
                        StructField('artist_location', StringType()),
                        StructField('artist_longitude', DoubleType()),
                        StructField('artist_name', StringType()),
                        StructField('duration', DoubleType()),
                        StructField('song_id', StringType()),
                        StructField('title', StringType()),
                        StructField('year', IntegerType())
    ]

    song_data_struct = StructType(fields = song_template)
    
    return spark.read.json(song_data_files, schema = song_data_struct)


# In[35]:


def read_log_data(spark, log_data_files):
    '''
    This function to read JSON log files into dataframe with predefined schema.
    
    Input: 
        - spark application.
        - path where JSON log files located.
    
    Output:
        - return a PySpark dataframe of log data with a schema.
    '''
    
    log_template = [
                    StructField('artist', StringType()),
                    StructField('auth', StringType()),
                    StructField('firstName', StringType()),
                    StructField('gender', StringType()),
                    StructField('itemInSession', LongType()),
                    StructField('lastName', StringType()),
                    StructField('length', DoubleType()),
                    StructField('level', StringType()),
                    StructField('location', StringType()),
                    StructField('method', StringType()),
                    StructField('page', StringType()),
                    StructField('registration', DoubleType()),
                    StructField('sessionId', LongType()),
                    StructField('song', StringType()),
                    StructField('status', IntegerType()),
                    StructField('ts', LongType()),
                    StructField('userAgent', StringType()),
                    StructField('userId', StringType())   
        ]

    log_data_struct = StructType(fields = log_template)
    
    return spark.read.json(log_data_files, schema = log_data_struct)


# In[36]:


def main():
    '''
    1. First, initialize a Spark application
    2. Define paths of input as well as output data.
    3. Call function process_song_data() to process song data, then write parquet files of songs & artists tables.
    4. Call function process_log_data() to process log data, then write parquet files of users, time & songplays tables.
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-udacity-bucket/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


# In[37]:


if __name__ == "__main__":
    main()


# In[ ]:




