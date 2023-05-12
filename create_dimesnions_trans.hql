set hive.support.concurrency=true;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=2;
set hive.auto.convert.join=false;

CREATE TABLE IF NOT EXISTS user_dim_raw (user_id STRING, user_name STRING, user_verified BOOLEAN, user_created_at TIMESTAMP)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS ORC
LOCATION 'hdfs://localhost:9000/projects/spark-twitter-project/twitter-raw-data/user-raw'
TBLPROPERTIES ("transactional"="true");

CREATE  TABLE IF NOT EXISTS tweet_dim_raw(user_id STRING , tweet_id STRING ,text STRING, tweet_created_at TIMESTAMP)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS ORC
LOCATION 'hdfs://localhost:9000/projects/spark-twitter-project/twitter-raw-data/tweet-raw'
TBLPROPERTIES ("transactional"="true");

CREATE  TABLE IF NOT EXISTS date_dim_raw(date_key STRING)
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS ORC
LOCATION 'hdfs://localhost:9000/projects/spark-twitter-project/twitter-raw-data/date-raw'
TBLPROPERTIES ("transactional"="true");

