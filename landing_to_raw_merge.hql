set hive.support.concurrency=true;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=2;
set hive.auto.convert.join=false;

-- Create Temporary tables to get data from the landing table where the BATCH ARRIVAL difference is equal 5 minutes

CREATE TEMPORARY TABLE user_tmp
AS SELECT distinct user_id, user_name, user_verified, user_created_at,
year(user_created_at) as year, month(user_created_at) as month,
day(user_created_at) as day, hour(user_created_at) as hour
FROM spark_tweets_landing
where batch_time between  from_unixtime(unix_timestamp(current_timestamp) - 30000) and current_timestamp;

-- As we want to get the data into the dimension tables we are going to use UPSERT mode , so we will check if the last arrived batch contains data that not found in the dimension it will insert it as a new record
-- else it will update the record already exists in the dimension with the new information this will be done using MERGE between DIMENSIONS and TEMPORARY tables

MERGE INTO user_dim_raw
using user_tmp t
ON user_dim_raw.user_id = t.user_id
WHEN matched THEN
UPDATE SET user_name = t.user_name, user_verified = t.user_verified
WHEN NOT matched THEN
INSERT VALUES (t.user_id , t.user_name , t.user_verified , t.user_created_at, t.year, t.month, t.day, t.hour);


CREATE TEMPORARY TABLE tweet_tmp
AS SELECT distinct user_id, tweet_id, text, tweet_created_at ,
year(tweet_created_at) as year, month(tweet_created_at) as month,
day(tweet_created_at) as day, hour(tweet_created_at) as hour
FROM spark_tweets_landing
where batch_time between  from_unixtime(unix_timestamp(current_timestamp) - 10000) and current_timestamp;

MERGE INTO tweet_dim_raw
using tweet_tmp t
ON tweet_dim_raw.tweet_id = t.tweet_id
WHEN matched THEN
UPDATE SET text = t.text
WHEN NOT matched THEN
INSERT VALUES (t.user_id,t.tweet_id, t.text, t.tweet_created_at, t.year, t.month, t.day, t.hour);



CREATE TEMPORARY TABLE date_tmp
AS SELECT distinct DATE_FORMAT(tweet_created_at, "yyyy-MM-dd HH:00:00") date_key ,
year(tweet_created_at) as year, month(tweet_created_at) as month,
day(tweet_created_at) as day, hour(tweet_created_at) as hour
FROM spark_tweets_landing
where batch_time between  from_unixtime(unix_timestamp(current_timestamp) - 10000) and current_timestamp;

MERGE INTO date_dim_raw
using date_tmp t
on date_dim_raw.date_key = t.date_key
when not matched then insert
values (t.date_key, t.year, t.month, t.day, t.hour);
