# Twitter-Spark-Streaming-

This repository contains the implementation of a data pipeline that collects tweets from Twitter on the topic of Tesla. The pipeline consists of five stages:

1. Data Source System
2. Data Collection System
3. Landing Data Persistence
4. Landing to Raw ETL
5. Raw to Processed ETL

The goal of this project is to move the data through the pipeline and reach the final stage for analysis.

## Technologies Used

The following technologies are used in this project:

- Hadoop (HDFS)
- Spark
- Hive
- Flask

## Architecture Details

### 1. Data Source System

To set up the Data Source System, follow the steps outlined in the `steps.md` file. This includes running the `step1-run-prerequisites.sh` script, which performs the following tasks:

- Installs curl, Flask, and PySpark
- Installs cron
- Sets up aliases for running the project
- Creates the necessary directories in HDFS with appropriate permissions

After running the script, execute the `run_listener` alias in the terminal. This will start a Flask application that opens a socket and waits for Spark to connect. The Flask application acts as an interface for fetching data from the Twitter API.

To trigger the `twitter_listener.py` script, a web application is created using Flask. This approach allows for easier scheduling changes and separates the script execution from the architecture.

The Flask application runs on port 8887 with the endpoint `/run-5-min-batch` to fetch data from Twitter.

In a new terminal, run the `run_project` alias.

To schedule the data collection process, two scripts (`cron_file_5_min` and `cron_file_1_hour`) need to be added to the crontab. This can be done by opening a new terminal and running the following commands:

```
crontab -e
```

In the crontab file, add the following scheduling lines:

```
*/5 * * * * . ~/itversity-material/cron_file_5_min
0 */1 * * * . ~/itversity-material/cron_file_1_hour
```

### 2. Data Collection System

After running the `pyspark_tweets_sourcing.py` script, JSON files containing the required tweet information will be generated. The data is partitioned by year, month, day, and hour based on the `created_at` date field. Additionally, a `batch_interval` column is added to indicate the timing of data receipt for analysis purposes.

### 3. Landing Data Persistence

In this step, a Hive external table named `spark_tweets_landing` is created using the `hive_create_landing.hql` script. The table contains all the required data and is partitioned by year, month, day, and hour. The data is stored as Parquet format in the `landing` directory on HDFS.

### 4. Landing to Raw ETL

The Landing to Raw ETL step involves creating three dimensions:

1. `User_dim_raw`: Contains information about the user.
2. `Tweet_dim_raw`: Contains information about the tweet.
3. `Date_dim_raw`: Contains date information.

### 5. Raw to Processed ETL

This stage is responsible for transforming the raw data into a processed format suitable for analysis. Further processing steps and analysis can be performed on the processed data.

For more detailed information and steps, please refer to the individual script files in the repository.

**Note**: Make sure to configure the necessary Twitter API credentials before running the pipeline.
