from pymongo import MongoClient
from configparser import ConfigParser
import csv
import json

with open('tweetData.csv', 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['_id','id_str','screen_name','created_at','text'])


conf = ConfigParser()
conf.read('config.conf')

client = MongoClient(conf.get('mongo', 'host'));
db = client[conf.get('mongo', 'database')];
db.authenticate(conf.get('mongo', 'user'), conf.get('mongo', 'password'))
tweets = db[conf.get('mongo', 'collection')]

for tweet in tweets.find({text: /.*jackpotting.*/}):
    parsedTweet = json.loads(tweet)
    writer.writerow([parsedTweet['_id'], parsedTweet['str_id'], parsedTweet["user"]["screen_name"], parsedTweet['created_at'], parsedTweet['text']])
    


