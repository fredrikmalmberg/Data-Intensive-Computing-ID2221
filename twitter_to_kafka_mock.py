from tweet_parser.tweet import Tweet
import json
from tweepy import StreamListener, OAuthHandler, Stream
from kafka import KafkaClient, KafkaProducer
from access_tokens import access_token, access_token_secret, consumer_key, consumer_secret # Add your own

producer = KafkaProducer()
f = open("json_tweets.txt", "r")
Lines = f.readlines()
for l in Lines:
    producer.send("tweets", l.encode('utf-8'))
    tweet_dict = json.loads(l)
    tweet = Tweet(tweet_dict)
    print(tweet.created_at_string, tweet.all_text)
f.close()




