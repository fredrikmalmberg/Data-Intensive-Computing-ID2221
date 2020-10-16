from tweet_parser.tweet import Tweet
import json
from tweepy import StreamListener, OAuthHandler, Stream
from kafka import KafkaClient, KafkaProducer
from access_tokens import access_token, access_token_secret, consumer_key, consumer_secret # Add your own

class Listener(StreamListener):
    def on_data(self, data):
        producer.send("tweets", data.encode('utf-8'))
        tweet_dict = json.loads(data)
        tweet = Tweet(tweet_dict)
        print(tweet.created_at_string, tweet.all_text)
        #print(tweet)

        f.write(data)

        return True
    def on_error(self, status):
        print("Error code:", status)
        if status == 420:
            return False

producer = KafkaProducer()
f = open("json_tweets.txt", "a")
listener = Listener()
oAuth = OAuthHandler(consumer_key, consumer_secret)
oAuth.set_access_token(access_token, access_token_secret)
tweet_stream = Stream(oAuth, listener)
tweet_stream.filter(track=["google"]) # Tracks word
f.close()
# tweet_stream.filter(follow=['@malmbrg']) # follow
# tweet_stream.filter(locations=['Whatever'], is_async=False) # location


