# From http://pythondata.com/collecting-storing-tweets-python-mysql/

import tweepy
import json
import logging

log = logging.getLogger(__name__)


class StreamListener(tweepy.StreamListener):    
    # This is a class provided by tweepy to access the Twitter Streaming API.

    isCanceled = False

    def __init__(self, api, mongodb, mysql):
        super().__init__(api)
        self.mongodb = mongodb
        self.mysql = mysql
        self.count = mongodb.count_tweets()

    def on_connect(self):
        log.info("You are now connected to the streaming API.")

    def on_error(self, status_code):
        log.error('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        if self.isCanceled:
            return False

        try:
            datajson = json.loads(data)
            if 'text' not in datajson:
                log.warning(datajson)
            else:
                self.mysql.insert_tweet(datajson)
                self.mongodb.insert_tweet(datajson)

                self.count += 1
                if self.count % 1000 == 0:
                    log.info("Tweets collected: %d", self.count)

        except Exception as e:
            log.error(e)

    def cancel(self):
        self.isCanceled = True


class TwitterStreamer(tweepy.StreamListener):

    def __init__(self, mysql, mongodb):
        self.listener = None
        self.mysql = mysql
        self.mongodb = mongodb
        self.streamer = None
        self.words = None

    def create_listener(self, consumer_key, consumer_secret, access_token, access_token_secret):
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        # Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
        self.listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True), mongodb=self.mongodb, mysql=self.mysql)
        self.streamer = tweepy.Stream(auth=auth, listener=self.listener)
    
    def setWords(self, words):
        self.words = words

    def getStatus(self):
        return self.streamer.running

    def stream(self, words = None):
        if words is not None:
            self.words = words
        log.info("Tracking: " + str(self.words))
        self.listener.isCanceled = False
        self.streamer.filter(track=self.words, async=True)


    def disconnect(self):
        if self.listener:
            self.listener.cancel()
            self.streamer.disconnect()