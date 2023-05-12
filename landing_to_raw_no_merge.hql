set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO user_dim_raw partition(year, month, day, hour)
SELECT distinct user_id, user_name, user_verified, user_created_at,
year(user_created_at) as year, month(user_created_at) as month,
day(user_created_at) as day, hour(user_created_at) as hour
FROM spark_tweets_landing
where batch_time between  from_unixtime(unix_timestamp(current_timestamp) - 30000) and current_timestamp;

INSERT INTO tweet_dim_raw partition(year, month, day, hour)
SELECT distinct user_id, tweet_id, text, tweet_created_at ,
year(tweet_created_at) as year, month(tweet_created_at) as month,
day(tweet_created_at) as day, hour(tweet_created_at) as hour
FROM spark_tweets_landing
where batch_time between  from_unixtime(unix_timestamp(current_timestamp) - 10000) and current_timestamp;

INSERT INTO date_dim_raw partition(year, month, day, hour)
SELECT distinct DATE_FORMAT(tweet_created_at, "yyyy-MM-dd HH:00:00") date_key ,
year(tweet_created_at) as year, month(tweet_created_at) as month,
day(tweet_created_at) as day, hour(tweet_created_at) as hour
FROM spark_tweets_landing
where batch_time between  from_unixtime(unix_timestamp(current_timestamp) - 10000) and current_timestamp;

