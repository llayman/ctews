from configparser import ConfigParser
from ctews import db, twitter

conf = ConfigParser()
conf.read('config.conf')

# conf file contains a comma-delimited list of words, which will be parsed into a list
WORDS = conf.get('filters', 'white_list').split(',')

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

mongo = db.MongoWrapper(MONGO_HOST, MONGO_USER, MONGO_PASSWD, MONGO_DATABASE, MONGO_COLLECTION)
mysql = db.MySqlWrapper(HOST, USER, PASSWD, DATABASE)

# mongo.truncate()
# mysql.truncate()

print("Tweets in MongoDB:", mongo.count_tweets())
print("Tweets in MySQL:", mysql.count_tweets())

twitter = twitter.TwitterStreamer(mysql=mysql, mongodb=mongo)
twitter.stream(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, WORDS)
