from pyspark.sql import SparkSession

user_fact_query ="""
select ur.user_name, tir.date_key, count(tr.tweet_id) as tweets_count 
from user_dim_raw ur
left join tweet_dim_raw tr
on ur.user_id = tr.user_id
left join date_dim_raw tir
on DATE_FORMAT(tr.tweet_created_at, "yyyy-MM-dd HH:00:00") = tir.date_key
where tr.tweet_created_at between from_unixtime(unix_timestamp(current_timestamp) - 3600) and current_timestamp
group by ur.user_name, tir.date_key;
"""

tweets_fact_query ="""
select tir.date_key, count(tr.tweet_id) as tweet_counts 
from tweet_dim_raw tr
left join date_dim_raw tir
on DATE_FORMAT(tr.tweet_created_at, "yyyy-MM-dd HH:00:00") = tir.date_key
group by tir.date_key;
"""


spark = SparkSession.\
    builder.\
    appName("twitter-fact-loading") \
    .enableHiveSupport() \
    .getOrCreate()





user_tweets_fact = spark.sql(user_fact_query)

tweets_hourly_fact = spark.sql(tweets_fact_query)


user_tweets_fact.repartition(1).write.format("hive").mode('append').saveAsTable("user_hourly_tweets")
tweets_hourly_fact.repartition(1).write.format("hive").mode('append').saveAsTable("hourly_tweets")





