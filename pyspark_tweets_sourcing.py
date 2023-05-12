import pyspark # run after findspark.init() if you need it
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


schema = 'created_at TIMESTAMP, text STRING, id STRING, user STRUCT<id STRING, created_at TIMESTAMP, verified BOOLEAN, ' \
         'username STRING> '

spark = SparkSession.builder.appName("Project").getOrCreate()

tweet_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "127.0.0.1") \
    .option("port", 7775) \
    .load()

tweet_df.printSchema()

tweet_df_string = tweet_df.selectExpr("CAST(value AS STRING)")

tweet_df_columns = tweet_df_string. \
            select(from_json(col("value"),schema).alias("json_value")). \
            select("json_value.*"). \
            withColumn("batch_time", current_timestamp()). \
            selectExpr("text",
                       "id as tweet_id",
                       "created_at as tweet_created_at",
                       "user.id as user_id",
                       "user.username as user_name",
                       "user.verified as user_verified",
                       "user.created_at as user_created_at",
                       "batch_time",
                       "year(batch_time) as year",
                       "month(batch_time) as month",
                       "day(batch_time) as day",
                       "hour(batch_time) as hour"
                       )

writeTweet = tweet_df_columns. \
    writeStream. \
    partitionBy("year","month","day","hour"). \
    outputMode("append"). \
    format("parquet"). \
    option("path", "hdfs://localhost:9000/projects/spark-twitter-project/landing"). \
    option("checkpointLocation", "/public/checkpoint_stream_script"). \
    start()

writeTweet.awaitTermination()

