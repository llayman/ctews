from pymongo import MongoClient
from configparser import ConfigParser
from datetime import datetime
from pytz import timezone
import csv
import time
import pprint
import random

conf = ConfigParser()
conf.read('config.conf')

client = MongoClient(conf.get('mongo', 'host'));
db = client[conf.get('mongo', 'database')];
db.authenticate(conf.get('mongo', 'user'), conf.get('mongo', 'password'))
tweets = db.tweets


start = time.time()
tweetObj = []
retweets = {}

with open('tweetData.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['_id','id_str','screen_name','created_at','retweets','text'])
    count = 0
    retweetCount = 0
    print("Progress:----Starting Mongo Queries----")
    for tweet in tweets.find({ 'created_at' : { '$gte': datetime(2018, 1, 22), '$lt': datetime(2018, 1, 27)}, 'lang': 'en'}):
        if count%1000 == 0:
            print("Tweets found ", count)
        

        if "retweeted_status" in tweet:
            retweetCount += 1
            if tweet['retweeted_status']['id_str'] in retweets:
                retweets[tweet['retweeted_status']['id_str']] += 1
            else:
                if datetime(2018,1,22, tzinfo=timezone('UTC')) < datetime.strptime( tweet['retweeted_status']['created_at'], "%a %b %d %H:%M:%S %z %Y" ) < datetime(2018, 1, 27,tzinfo=timezone('UTC')):
                    retweets[tweet['retweeted_status']['id_str']] = 1
                else:
                    pass
                    retweetCount -= 1
        else:
            count += 1
            text = ""
            if "extended_tweet" in tweet:
                text = tweet['extended_tweet']['full_text'].replace("\n","").replace("\r","").replace(",","")
            else:
                text = tweet['text'].replace("\n","").replace("\r","").replace(",","")
            
            tweetObj.append([tweet['_id'], tweet['id_str'], tweet['user']['screen_name'], tweet['created_at'], 0, text])
    
    print("Progress:----Counting tweets----")
    for t in tweetObj:
        try:
            t[4] = retweets[t[1]]
        except KeyError:
            pass
    
    sample = random.sample(tweetObj,500)

    print("Progress:----Writing CSV File----")
    for t in sample:
        writer.writerow(t)
    

    print("Progress:----Count Retweets----")
    rtCount = 0
    for rtID, val in retweets.items():
        rtCount += val


end = time.time()
elapsed = end - start 

print("CSV Finished ", (count + retweetCount), " total tweets found.\t Original Tweets: ", count," Retweets: ", retweetCount ," Time taken : ", (int)(elapsed/60), " Minutes ", (int)(elapsed%60), " Seconds")
print(rtCount)