# Data Lake


## 1. dwh.cfg

This is configuration file which contains AWS credential information.


## 2. etl.py

This is where all activities will be happened. The process should be:

1. Initializing Spark application as well as path of input & output data.

2. Processing **SONG** data:
    * Reading data from JSON files into a dataframe.
    * Extracting data for ***songs*** table from the dataframe and writting files.
    * Extracting data for ***artists*** table from the dataframe and writting files.

3. Processing **LOG** data:
    * Reading data from JSON files into a dataframe.
    * Cleaning data by removing 'NA' values, data conversion to datetime values.
    * Extracting data for ***users*** table from the dataframe and writting files.
    * Extracting data for ***time*** table from the dataframe and writting files.
    * Joining **SONG** and **LOG** data together to extracting data for ***songplays*** table, then writting files.

```
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-udacity-bucket/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
```

### 2.1. Initializtion

When this file executed, the `main()` function will be invoked first and this is where Spark application initialized by calling function `create_spark_session()`.

```
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
```

We also specify the path of input & output using AWS S3. While input data comes from S3 of Udacity, output data is from user's S3.

```
input_data = "s3a://udacity-dend/"
output_data = "s3a://datalake-udacity-bucket/output/"
```


### 2.2. Processing **SONG** data

```
def process_song_data(spark, input_data, output_data):
   
    song_data = input_data + "song_data/A/A/*/*.json"
    
    df = read_song_data(spark, song_data)
    
    process_songs_table(df, output_data)

    process_artists_table(df, output_data)
```

This process is split into 3 following small processes:

#### 2.2.1.  Reading **SONG** data from JSON files to Dataframe

Helper function named `read_song_data()` will read data from JSONs file into a dataframe which will be returned as output.

We define the schema of output dataframe with type for each column before applying it when data read.

```
def read_song_data(spark, song_data_files):
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
```


#### 2.2.2. Processing *songs" table

```
def process_songs_table(df, output_data):
   
    df.createOrReplaceTempView('song_data')
    
    songs_table = spark.sql('''
                            select DISTINCT song_id, title, artist_id, year, duration
                            from song_data
                            ''')
    
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs.parquet")
```

From the output dataframe of 4.2.1, we create a temporary view `song_data`, then we could use SQL command to querying data. After that, we write the retrieved data to parquet files. Data will be partitioned by `year` and `artist_id`.


#### 2.2.3. Processing *artists* table

```
def process_artists_table(df, output_data):
   
    df.createOrReplaceTempView('song_data')
    
    artists_table = spark.sql('''
                                select  DISTINCT artist_id, artist_name as name, artist_location as location, 
                                        artist_latitude as latitude, artist_longitude as longitude
                                from song_data
                                ''')
    
    artists_table.write.parquet(output_data + "artist.parquet")
```

Similarly, we create a temporary view `song_data` (actually we don't need to define it since temporary view is already existing at 4.2.2), after we just select columns we need for *artists*, then write the out into parquet files.


### 2.3. Processing **LOG** data

```
def process_log_data(spark, input_data, output_data):
    
    log_data = input_data + "log_data/2018/11/*.json"

    df = read_log_data(spark, log_data)
    
    df = df.filter(df['page'] == 'NextSong').dropna(subset=['ts', 'song', 'userId', 'firstName', 'lastName'])
    
    df = df.withColumn("timesttamp", F.from_unixtime(df.ts/1000))
    
    df = df.withColumn("datetime", F.from_utc_timestamp(df["timesttamp"], 'PST'))
    
    process_users_table(df, output_data)

    process_time_table(df, output_data) 

    process_songplays_table(df, output_data)
```

This process could be split into 4 following small processes:

#### 2.3.1. Reading **LOG** data from JSON files to dataframe, cleaning and adding new columns

```
def read_log_data(spark, log_data_files):
    
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
```

Helper function named `read_log_data()` will read data from JSONs file into a dataframe which will be returned as output.

We define the schema of output dataframe with type for each column before applying it when data read.

Then we just select rows which has `page` value is `NextSong`, also drop all rows which has one of `ts`, `song`, `userId`, `firstName`, `lastName` column with `NA` value.

```
df = df.filter(df['page'] == 'NextSong').dropna(subset=['ts', 'song', 'userId', 'firstName', 'lastName'])
```

We also need to have some additional columns which are conversion of `ts` column to `datetime` value. I order to do that we will use functions from from PySpark SQL: 
    * `from_unixtime()` to convert from milliseconds value to `timestamp` string value
    * `from_utc_timestamp()` to convert `timestamp` string to `datetime` value.

After this we have a dataframe of **LOG** data ready for next steps.

```
df = df.withColumn("timesttamp", F.from_unixtime(df.ts/1000))
    
df = df.withColumn("datetime", F.from_utc_timestamp(df["timesttamp"], 'PST'))

```


#### 2.3.2. Processing *users* table

```
def process_users_table(df, output_data):
       
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates()
    
    users_table.write.parquet(output_data + "users.parquet")
```

For *users*, we just simply select columns from **LOG** dataframe, rename them as we want, and drop any duplicated records. Then we write the result to parquet files.



#### 2.3.3. Processing *time* table
```
def process_time_table(df, output_data):
   
    time_table = df.select( df.datetime.alias('start_time'),
                    F.hour(df.datetime).alias('hour'),
                    F.dayofmonth(df.datetime).alias('day'),
                      F.weekofyear(df.datetime).alias('week'),
                      F.month(df.datetime).alias('month'),
                      F.year(df.datetime).alias('year'),
                      F.dayofweek(df.datetime).alias('weekday'))
    
    time_table.write.partitionBy('year', 'month').parquet(output_data + "time.parquet")

```

For *time* table, beside of a `datetime` column, we also need to extract datetime part from it, like: hour, day of month, week, month, year and day of week. This is where PySpark SQL functions `hour(), dayofmonth(), weekofyear(), month(), year(), dayofweek()` will do their jobs.
Retrieved data will be kept in a dataframe before being written to parquet files.


#### 2.3.4. Processing *songplays* table
```
def process_songplays_table(df, output_data):

    song_df = spark.sql('''
                        select song_id, title, artist_id, artist_name, duration
                        from song_data
                        ''')


    s = song_df.alias('s')
    l = df.alias('l')
    songplays_table = l.join(s, (l.song == s.title) & (l.length == s.duration) & (l.artist == s.artist_name))
    

    songplays_table.createOrReplaceTempView('songplays')    
    

    songplays_table = spark.sql('''
                            select row_number()over(order by datetime) as songplay_id, datetime as start_time, 
                                year(datetime) as year, month(datetime) as month,
                                userId as user_id, level, song_id, artist_id, sessionId as session_id, location,  userAgent as user_agent
                            from songplays
                            ''')

    songplays_table.write.partitionBy('year', 'month').parquet(output_data + "songplays.parquet")
```

Data needed for *songplays* come from both **LOG** and **SONG** so we need to join them together.

First, we query **SONG** data into a dataframe named `song_df` by queying a temporary view `song_data` which we have already defined at 4.2.2.

```
    song_df = spark.sql('''
                       select song_id, title, artist_id, artist_name, duration
                       from song_data
                        ''')
```

Then we join the result with **LOG** dataframe, the output called `songplays_table` will contain all columns come from both of input dataframes.

```
    s = song_df.alias('s')
    l = df.alias('l')
    songplays_table = l.join(s, (l.song == s.title) & (l.length == s.duration) & (l.artist == s.artist_name))
```

After that, we create view for the `songplays_table` since we want to use SQL command to generate an auto increase values for `songplay_id` column. Beside of that, we will use SQL functions `year(), month()` to extract `year` and `month` columns which will be used for data partition.

```
    songplays_table.createOrReplaceTempView('songplays')    
    

    songplays_table = spark.sql('''
                            select row_number()over(order by datetime) as songplay_id, datetime as start_time, 
                                year(datetime) as year, month(datetime) as month,
                                userId as user_id, level, song_id, artist_id, sessionId as session_id, location,  userAgent as user_agent
                            from songplays
                            ''')
```

Finally, we write data we got into parquet files.

```
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + "songplays.parquet")
```