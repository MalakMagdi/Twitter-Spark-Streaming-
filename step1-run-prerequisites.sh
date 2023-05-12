sudo apt update
sudo apt upgrade
sudo apt install curl
sudo apt install lsof
sudo apt install cron

pip install flask
pip install pyspark

echo 'alias run_listener="kill -9 `lsof -t -i:8887`; cd ; python3 itversity-material/twitter_listener.py"' >> ~/.bashrc
echo 'alias run_project="cd; sudo rm -rf /public/;sudo python3 itversity-material/pyspark_tweets_sourcing.py"' >> ~/.bashrc


hdfs dfs -mkdir -p /projects/spark-twitter-project/
hdfs dfs -mkdir -p /projects/spark-twitter-project/landing
hdfs dfs -mkdir /projects/spark-twitter-project/twitter-raw-data/
hdfs dfs -mkdir /projects/spark-twitter-project/twitter-processed-data/
hdfs dfs -chmod 777 /projects/*/*


