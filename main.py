from configparser import ConfigParser
import yaml
import logging.config
from ctews import db, twitter


with open('logging.yaml') as f:
    logging.config.dictConfig(yaml.load(f))

log = logging.getLogger(__name__)


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

#mongo.truncate()
#mysql.truncate()

log.info("Tweets in MongoDB: %d", mongo.count_tweets())
log.info("Tweets in MySQL: %d", mysql.count_tweets())

twitter = twitter.TwitterStreamer(mysql=mysql, mongodb=mongo)
twitter.stream(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, WORDS)
