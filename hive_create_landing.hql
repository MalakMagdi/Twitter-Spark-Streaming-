CREATE EXTERNAL TABLE IF NOT EXISTS spark_tweets_landing(
    text STRING,
    tweet_id STRING,
    tweet_created_at TIMESTAMP,
    user_id STRING,
    user_name STRING,
    user_verified BOOLEAN,
    user_created_at TIMESTAMP,
    batch_time TIMESTAMP)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/projects/spark-twitter-project/landing/';

-- This code is used in order to help hive see the partitions that were made
MSCK REPAIR TABLE spark_tweets_landing add partitions;


select 'current record count is: ' ||count(*) from spark_tweets_landing;