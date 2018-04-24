from pymongo import MongoClient
import MySQLdb
from dateutil import parser


class MongoWrapper:

    def __init__(self, mongo_host, mongo_user, mongo_passwd, mongo_database, mongo_collection):
        mongo_client = MongoClient(mongo_host)
        mongo_db = mongo_client[mongo_database]
        mongo_db.authenticate(mongo_user, mongo_passwd)
        self.coll = mongo_db[mongo_collection]

    def insert_tweet(self, datajson):
        self.coll.insert(datajson)

    def count_tweets(self):
        return self.coll.count()

    def truncate(self):
        self.coll.drop()


class MySqlWrapper:

    insert_query = "INSERT INTO twitter (tweet_id, screen_name, created_at, text) VALUES (%s, %s, %s, %s)"
    count_query = "SELECT COUNT(*) FROM twitter";
    truncate_query = "TRUNCATE TABLE twitter";

    def __init__(self, mysql_host, mysql_user, mysql_passwd, mysql_database):
        self.host = mysql_host
        self.user = mysql_user
        self.passwd = mysql_passwd
        self.database = mysql_database

    def insert_tweet(self, datajson):
        # grab the wanted data from the Tweet
        text = datajson['text']
        screen_name = datajson['user']['screen_name']
        tweet_id = datajson['id']
        created_at = parser.parse(datajson['created_at'])

        conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.database, charset="utf8mb4")
        with conn as cursor:
            cursor.execute(self.insert_query, (tweet_id, screen_name, created_at.strftime('%Y-%m-%d %H:%M:%S'), text))
        conn.close()

    def count_tweets(self):
        conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.database, charset="utf8mb4")
        with conn as cursor:
            cursor.execute(self.count_query)
            result = cursor.fetchone()
        conn.close()
        return result[0]

    def truncate(self):
        conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.database, charset="utf8mb4")
        with conn as cursor:
            cursor.execute(self.truncate_query)
        conn.close()
