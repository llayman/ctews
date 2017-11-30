# From http://pythondata.com/collecting-storing-tweets-python-mysql/

import tweepy
import json


class StreamListener(tweepy.StreamListener):    
    # This is a class provided by tweepy to access the Twitter Streaming API.

    isCanceled = False

    def __init__(self, api, mongodb, mysql):
        super().__init__(api)
        self.mongodb = mongodb
        self.mysql = mysql
        self.count = mongodb.count_tweets()

    def on_connect(self):
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        if self.isCanceled:
            return False

        try:
            datajson = json.loads(data)
            self.mysql.insert_tweet(datajson)
            self.mongodb.insert_tweet(datajson)

            self.count += 1
            if self.count % 500 == 0:
                print(self.count, "tweets collected.")

        except Exception as e:
            print(e)

    def cancel(self):
        self.isCanceled = True


class TwitterStreamer(tweepy.StreamListener):

    def __init__(self, mysql, mongodb):
        self.listener = None
        self.mysql = mysql
        self.mongodb = mongodb

    def stream(self, consumer_key, consumer_secret, access_token, access_token_secret, words):

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        # Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
        self.listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True), mongodb=self.mongodb, mysql=self.mysql)
        streamer = tweepy.Stream(auth=auth, listener=self.listener)

        print("Tracking: " + str(words))
        streamer.filter(track=words)

    def disconnect(self):
        if self.listener:
            self.listener.cancel()