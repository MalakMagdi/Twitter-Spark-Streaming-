CREATE TABLE user_hourly_tweets(user_name string, date_key string, tweets_count bigint)
STORED AS PARQUET;

CREATE TABLE hourly_tweets (date_key STRING, tweet_counts BIGINT) STORED AS PARQUET;