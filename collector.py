from configparser import ConfigParser
import yaml
import logging.config
import sys
from ctews import db, twitter

#This class creates a version of TwitterStreamer that gets configuration from a config file
class TweetCollector(twitter.TwitterStreamer):
    def __init__(self):
        with open('logging.yaml') as f:
            logging.config.dictConfig(yaml.load(f))

        self.log = logging.getLogger(__name__)

        conf = ConfigParser()
        conf.read('config.conf')

        # conf file contains a comma-delimited list of words, which will be parsed into a list
        self.WORDS = conf.get('filters', 'white_list').split(',')

        self.CONSUMER_KEY = conf.get('twitter', 'consumer_key')
        self.CONSUMER_SECRET = conf.get('twitter', 'consumer_secret')
        self.ACCESS_TOKEN = conf.get('twitter', 'access_token')
        self.ACCESS_TOKEN_SECRET = conf.get('twitter', 'access_token_secret')

        HOST = conf.get('mysql', 'host')
        USER = conf.get('mysql', 'user')
        PASSWD = conf.get('mysql', 'password')
        DATABASE = conf.get('mysql', 'database')

        MONGO_HOST = conf.get('mongo', 'host')
        MONGO_USER = conf.get('mongo', 'user')
        MONGO_PASSWD = conf.get('mongo', 'password')
        MONGO_DATABASE = conf.get('mongo', 'database')
        MONGO_COLLECTION = conf.get('mongo', 'collection')
        #End of Configuration

        #Database objects, 
        mongo = db.MongoWrapper(MONGO_HOST, MONGO_USER, MONGO_PASSWD, MONGO_DATABASE, MONGO_COLLECTION)
        mysql = db.MySqlWrapper(HOST, USER, PASSWD, DATABASE)
        #Get DB status
        self.log.info("Tweets in MongoDB: %d", mongo.count_tweets())
        self.log.info("Tweets in MySQL: %d", mysql.count_tweets())

        

        
        #mongo.truncate()
        #mysql.truncate()

        #Initialize the Streamer with DBs
        super().__init__(mongo, mysql)
        #Add the listener
        self.create_listener(self.CONSUMER_KEY, self.CONSUMER_SECRET, self.ACCESS_TOKEN, self.ACCESS_TOKEN_SECRET)
        #Start the Streamer
        self.stream(self.WORDS)



    def restart(self):
        self.create_listener(self.CONSUMER_KEY, self.CONSUMER_SECRET, self.ACCESS_TOKEN, self.ACCESS_TOKEN_SECRET)
        self.stream(self.WORDS)
    

        
    
        
        
