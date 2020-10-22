import requests
import os
import json
from kafka import KafkaProducer
from cassandra.cluster import Cluster
from time import time
from threading import Timer

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def auth():
    return os.environ.get("BEARER_TOKEN")


def create_url(ids):
    tweet_fields = "tweet.fields=id,public_metrics,created_at"
    url = "https://api.twitter.com/2/tweets?ids={}&{}".format(ids, tweet_fields)
    return url


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

def get_batches(session, t):
    batches_list = [session.execute(f"SELECT * FROM batches WHERE timestamp >= {t - 60 * i - 10} AND timestamp < {t - 60 * i + 10} ALLOW FILTERING") for i in range(1, 6)]
    final_batch = session.execute(f"SELECT * FROM batches WHERE timestamp >= {t - 1790} AND timestamp < {t - 1810} ALLOW FILTERING")
    return batches_list, final_batch

def send_results(session, headers, producer):
    t = int(time())
    batches_list, final_batch = get_batches(session, t)
    for batch in batches_list:
        for row in batch:
            url = create_url(row.ids)
            json_response = connect_to_endpoint(url, headers)
            data = json.dumps(json_response, indent=4, sort_keys=True)
            producer.send("lookup", data.encode("utf-8"))
            print(data)
    
    for row in final_batch:
        url = create_url(row.ids)
        json_response = connect_to_endpoint(url, headers)
        data = json.dumps(json_response, indent=4, sort_keys=True)
        producer.send("final_lookup", data.encode("utf-8"))
        print("FINAL LOOKUP"+data)
    
    Timer(20, lambda: send_results(session, headers, producer)).start()


def main():
    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("tweets_space")
    bearer_token = auth()
    headers = create_headers(bearer_token)
    producer = KafkaProducer()
    send_results(session, headers, producer)


if __name__ == "__main__":
    main()