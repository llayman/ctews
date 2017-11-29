# From http://pythondata.com/collecting-storing-tweets-python-mysql/

from __future__ import print_function
import tweepy
import traceback
import json
import MySQLdb 
from pymongo import MongoClient
import time
from dateutil import parser
from ConfigParser import SafeConfigParser
import os


conf = SafeConfigParser()
conf.read('config.conf')

#conf file contains a comma-delimited list of words, which will be parsed into a list
WORDS = conf.get('filters','white_list').split(',')

CONSUMER_KEY = conf.get('twitter', 'consumer_key')
CONSUMER_SECRET = conf.get('twitter', 'consumer_secret')
ACCESS_TOKEN = conf.get('twitter', 'access_token')
ACCESS_TOKEN_SECRET = conf.get('twitter', 'access_token_secret')

HOST = conf.get('mysql', 'host')
USER = conf.get('mysql', 'user')
PASSWD = conf.get('mysql', 'password')
DATABASE = conf.get('mysql', 'database')

MONGO_HOST = conf.get('mongo', 'host')
MONGO_USER = conf.get('mongo', 'user')
MONGO_PASSWD = conf.get('mongo', 'password')
MONGO_DATABASE = conf.get('mongo', 'database')
MONGO_COLLECTION = conf.get('mongo', 'collection')

# This function takes the 'created_at', 'text', 'screen_name' and 'tweet_id' and stores it
# into a MySQL database

class StreamListener(tweepy.StreamListener):    
    #This is a class provided by tweepy to access the Twitter Streaming API. 

    insert_query = "INSERT INTO twitter (tweet_id, screen_name, created_at, text) VALUES (%s, %s, %s, %s)"


    def __init__(self, api):
        mongo_client = MongoClient(MONGO_HOST)
        mongo_db = mongo_client[MONGO_DATABASE]
        mongo_db.authenticate(MONGO_USER, MONGO_PASSWD)
        self.coll = mongo_db[MONGO_COLLECTION]
        self.count = self.coll.count()
        print(self.count, "tweets currently in database.")


    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        #This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
           # Decode the JSON from Twitter
            datajson = json.loads(data)

            #grab the wanted data from the Tweet
            text = datajson['text']
            screen_name = datajson['user']['screen_name']
            tweet_id = datajson['id']
            created_at = parser.parse(datajson['created_at'])

            #insert the data into the MySQL database
            conn=MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DATABASE, charset="utf8mb4")
            with conn as cursor:
                cursor.execute(self.insert_query, (tweet_id, screen_name, created_at.strftime('%Y-%m-%d %H:%M:%S'), text))
            conn.close()

            #insert into MongoDB
            self.coll.insert(datajson)

            self.count = self.count + 1
            if self.count % 1000 == 0:
                print(self.count, "tweets collected.")

        except Exception as e:
            print(e)

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
#Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True)) 
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
streamer.filter(track=WORDS)
