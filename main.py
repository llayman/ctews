# From http://pythondata.com/collecting-storing-tweets-python-mysql/

from __future__ import print_function
import tweepy
import json
import MySQLdb 
import time
import logging
from dateutil import parser
from ConfigParser import SafeConfigParser
import os

LOG_DIR = 'log'

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)


COUNT = 0

WORDS = ['#bigdata', '#AI', '#datascience', '#machinelearning', '#ml', '#iot']

conf = SafeConfigParser()
conf.read('config.conf')

CONSUMER_KEY = conf.get('twitter', 'consumer_key')
CONSUMER_SECRET = conf.get('twitter', 'consumer_secret')
ACCESS_TOKEN = conf.get('twitter', 'access_token')
ACCESS_TOKEN_SECRET = conf.get('twitter', 'access_token_secret')

HOST = conf.get('mysql', 'host')
USER = conf.get('mysql', 'user')
PASSWD = conf.get('mysql', 'password')
DATABASE = conf.get('mysql', 'database')

# This function takes the 'created_at', 'text', 'screen_name' and 'tweet_id' and stores it
# into a MySQL database
def store_data(created_at, text, screen_name, tweet_id):
    db=MySQLdb.connect(host=HOST, user=USER, passwd=PASSWD, db=DATABASE, charset="utf8mb4")
    cursor = db.cursor()
    insert_query = "INSERT INTO twitter (tweet_id, screen_name, created_at, text) VALUES (%s, %s, %s, %s)"
    cursor.execute(insert_query, (tweet_id, screen_name, created_at.strftime('%Y-%m-%d %H:%M:%S'), text))
    db.commit()
    cursor.close()
    db.close()

    global COUNT
    COUNT = COUNT + 1
    if not COUNT % 100:
        print(COUNT, "tweets collected.")
    return

class StreamListener(tweepy.StreamListener):    
    #This is a class provided by tweepy to access the Twitter Streaming API. 

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

            #print out a message to the screen that we have collected a tweet
            #print("Tweet collected at " + str(created_at))
            
            #insert the data into the MySQL database
            store_data(created_at, text, screen_name, tweet_id)
        
        except Exception as e:
           print(e)

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
#Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True)) 
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
streamer.filter(track=WORDS)
