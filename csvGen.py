from pymongo import MongoClient
from configparser import ConfigParser
import csv
import pprint

conf = ConfigParser()
conf.read('config.conf')

client = MongoClient(conf.get('mongo', 'host'));
db = client[conf.get('mongo', 'database')];
db.authenticate(conf.get('mongo', 'user'), conf.get('mongo', 'password'))
tweets = db.tweets

with open('tweetData.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['_id','id_str','screen_name','created_at','retweeted','text'])
    for tweet in tweets.find({"$or":[ {'text':{'$regex':'.*jackpotting.*'}}, {'full_text':{'$regex':'.*jackpotting.*'}}], 'lang' : 'en'}):
        print("Tweet Found")
        retweeted = 'False'
        text = ""
        #pprint.pprint(tweet)
        #print(tweet['text'].strip('\n'))
        if "retweeted_status" in tweet:
            retweeted = tweet['retweeted_status']['user']['screen_name']
        if "full_text" in tweet:
            text = tweet['full_text'].replace("\n","").replace("\r","").replace(",","")
        writer.writerow([tweet['_id'], tweet['id_str'], tweet["user"]["screen_name"], tweet['created_at'], retweeted, tweet['text'].replace("\n", "").replace("\r", "").replace(",",""), text])

print("CSV Finished")
