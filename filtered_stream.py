import requests
import os
import json
from tweepy import API, OAuthHandler
from access_tokens import access_token, access_token_secret, consumer_key, consumer_secret # Add your own
from kafka import KafkaProducer
from time import time

# Modified version of sample code https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/master/Filtered-Stream/filtered_stream.py

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def get_rules(headers, bearer_token):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, bearer_token, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))

def get_trending(regioncode):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)
    trends = api.trends_place(regioncode)
    s = str(trends).replace("'", '"').replace("None", "null").replace("L\"a", "L'a").replace("l\"a", "l'a")
    print(s[2500:2550])
    jsn = json.loads(s)
    return [x["name"] for x in jsn[0]["trends"]]

def set_rules(headers, trends, bearer_token):
    # You can adjust the rules if needed
    sample_rules = [{"value": x + " -is:retweet -is:quote -is:reply", "tag": x} for x in trends[:3]]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(headers, bearer_token, producer):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            json_response["timestamp"] = int(time())
            data = json.dumps(json_response, indent=4, sort_keys=True)
            producer.send("tweets", data.encode("utf-8"))
            print(data)


def main():
    bearer_token = os.environ.get("BEARER_TOKEN")
    headers = create_headers(bearer_token)
    rules = get_rules(headers, bearer_token)
    delete_all_rules(headers, bearer_token, rules)
    trends = get_trending(23424819)
    set_rules(headers, trends, bearer_token)

    producer = KafkaProducer()    
    get_stream(headers, bearer_token, producer)


if __name__ == "__main__":
    main()