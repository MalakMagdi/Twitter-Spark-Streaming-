# For sending GET requests from the API
import requests
from http import HTTPStatus

# For saving access tokens and for file management when creating and adding to the dataset
import os

# For dealing with json responses we receive from the API
import json

# For parsing the dates received from twitter in readable formats
import datetime

# To add wait time between requests
import time

# To open up a port to forward tweets
import socket

#Flask is a python web framework that is used in order to make web applications, so i will use it here in order to make an app that will trigger the calling of twitter listener script from outside
from flask import Flask, Response

from datetime import datetime, timedelta

# Making of Flask Application
app = Flask(__name__)

#Environment Variable with the token value is made
os.environ['TOKEN'] =  "AAAAAAAAAAAAAAAAAAAAAIdCcgEAAAAADxqlmxSiZLO05fKmfbrX7G3ckqQ%3DCCPSGoWTDF6uu4qdFsncsuOat5GFTTFv5blXPdA4ueK4YLu3gg"


def auth():
    return os.getenv('TOKEN')


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, start_date, end_date, max_results=10):
    search_url = "https://api.twitter.com/2/tweets/search/recent"  # Change to the endpoint you want to collect data from

    # change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,geo.place_id',
                    'tweet.fields': 'id,text,created_at,author_id',
                    'user.fields': 'id,name,username,created_at,verified',
                    'place.fields': 'id,country,country_code,name,place_type',
                    'next_token': {}}
    return search_url, query_params


def connect_to_endpoint(url, headers, params, next_token=None):
    params['next_token'] = next_token  # params object received from create_url function
    response = requests.request("GET", url, headers=headers, params=params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

s = socket.socket()
# This will be used to solve a problem that appears " This address is already used", so it when close and open the socket again it will reuse it
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
host = "127.0.0.1"
port = 7775
s.bind((host, port))
print("Listening on port: %s" % str(port))
s.listen(1)
clientsocket, address = s.accept()
print("Received request from: " + str(address), " connection created.")



@app.route("/run-5-min-batch", methods=['POST'])
def run_5_min_batch():
    x = 0
    bearer_token = auth()
    headers = create_headers(bearer_token)
    keyword = "tesla lang:en"
    end_time = (datetime.now()- timedelta(minutes=1)).isoformat("T") + "Z"
    start_time = (datetime.now() - timedelta(minutes=6)).isoformat("T") + "Z"
    max_results = 10
    url = create_url(keyword, start_time, end_time, max_results)
    json_response = connect_to_endpoint(url[0], headers, url[1])
    print(f"start publishing {len(json_response['data'])} tweets")

    for data in json_response['data']:
        #timing = (datetime.now() - timedelta(days=random.randint(-5,5))).isoformat("T") + "Z"
        #data = {"text":"Test tweet as api is down","created_at":timing}

        # format the tweet json
        del data['edit_history_tweet_ids']
        user_data = [x for x in json_response['includes']['users'] if data['author_id'] == x['id']][0]
        data['user'] = user_data
        del data['author_id']
        oneline_data = json.dumps(data)
        print("Sending:", oneline_data.encode('utf-8'))

        # send to the client socket
        clientsocket.send((oneline_data+"\n").encode('utf-8'))

        # We will sleep for 2 second here to demostrate how data can come faster than we are collecting it
        time.sleep(1)

        # validate count of sent queries
        x+=1
        if x > 10:
            break

    return Response(status=200)


app.run(host='localhost', port=8887)



