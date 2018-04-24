from pymongo import MongoClient
from configparser import ConfigParser
from datetime import datetime
from pytz import timezone
import csv
import time
import pprint
import random

#Gets the same information for db login
conf = ConfigParser()
conf.read('config.conf')

#Login to db and get collection tweets
client = MongoClient(conf.get('mongo', 'host'));
db = client[conf.get('mongo', 'database')];
db.authenticate(conf.get('mongo', 'user'), conf.get('mongo', 'password'))
tweets = db.tweets


start = time.time()
tweetObj = []
retweets = {}

#Creates the csv file for writing
with open('tweetData.csv', 'w', newline='') as csvfile:
    #Seperates cols by comma
    writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['_id','id_str','screen_name','created_at','retweets','text'])
    count = 0
    retweetCount = 0
    print("Progress:----Starting Mongo Queries----")

    #Query for date range and language
    #Check between 1/22/2018 and 1/27/2018
    #Regex commands are permmited for example:
    #'$regex':'.*word.*' will find tweets containing word
    for tweet in tweets.find({ 'created_at' : { '$gte': datetime(2018, 1, 22), '$lt': datetime(2018, 1, 27)}, 'lang': 'en'}):
        if count%1000 == 0:
            print("Tweets found ", count)
        
        #The field retweeted_status is only in tweets that are retweets.
        #The dictionary retweets holds an id_str of the original tweet and how many retweets associated with that particular tweet during the date range.
        if "retweeted_status" in tweet:
            retweetCount += 1
            if tweet['retweeted_status']['id_str'] in retweets:
                retweets[tweet['retweeted_status']['id_str']] += 1
            else:
                #ensures that adding a new entry into retweets is not a ghost retweet which is associated with tweets before the date range
                if datetime(2018,1,22, tzinfo=timezone('UTC')) < datetime.strptime( tweet['retweeted_status']['created_at'], "%a %b %d %H:%M:%S %z %Y" ) < datetime(2018, 1, 27,tzinfo=timezone('UTC')):
                    retweets[tweet['retweeted_status']['id_str']] = 1
                else:
                    #If the tweet is a retweet it is not considered
                    pass
                    retweetCount -= 1
        else:
            count += 1
            text = ""
            #uses the extended tweet if it exists
            if "extended_tweet" in tweet:
                text = tweet['extended_tweet']['full_text'].replace("\n","").replace("\r","").replace(",","")
            else:
                text = tweet['text'].replace("\n","").replace("\r","").replace(",","")
            
            tweetObj.append([tweet['_id'], tweet['id_str'], tweet['user']['screen_name'], tweet['created_at'], 0, text])
    
    #assigns retweet values for each original tweet
    print("Progress:----Counting tweets----")
    for t in tweetObj:
        try:
            t[4] = retweets[t[1]]
        except KeyError:
            pass
    
    #Gets a sample of 500
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