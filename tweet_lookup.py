import requests
import os
import json
from kafka import KafkaConsumer, KafkaProducer

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def auth():
    return os.environ.get("BEARER_TOKEN")


def create_url(ids):
    tweet_fields = "tweet.fields=id,public_metrics,created_at"
    url = "https://api.twitter.com/2/tweets?{}&{}".format(ids, tweet_fields)
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


def main():
    #bearer_token = auth()
    consumer = KafkaConsumer("lookup")
    for message in consumer:
        print(message)
    #url = create_url("ids=1318518600064446471")
    #headers = create_headers(bearer_token)
    #json_response = connect_to_endpoint(url, headers)
    #print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()