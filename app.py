import collector
from configparser import ConfigParser
from flask import Flask, render_template, redirect, url_for, request, flash, jsonify
from flask_restful import Resource, Api
import json

import os
import time
from datetime import datetime

#Flask app initializes the collector
Collect = collector.TweetCollector();
conf = ConfigParser()
conf.read('config.conf')

app = Flask(__name__)
api = Api(app)
app.config['SECRET_KEY'] = conf.get('flask', 'SECRET_KEY')


#Home route
@app.route('/')
def main():
    return render_template('main.html')


#Restful url for status
@app.route('/api/ctews/status', methods=['GET'])
def getStatus():
    data = {'status': Collect.getStatus(),'words': Collect.words, 'tweets': Collect.mongodb.count_tweets()}
    return jsonify(data), 200    


#Restful url for updating
@app.route('/api/ctews/status', methods=['POST'])
def updateStatus():
    data = request.get_json()
    status = data["status"]
    if status == "stream":
        Collect.restart()
    elif status == "disconnect":
        Collect.disconnect()
    else:
         return jsonify("Incorrect Data"), 400
    return getStatus()
    

#About route
@app.route('/about')
def about():
    return render_template('about.html')

#Uses flask_restful to update the log
class LogUpdate(Resource):
    
    def _is_updated(self, request_time):
        """
        Returns if resource is updated or it's the first
        time it has been requested.
        args:
            request_time: last request timestamp
        """
        return os.stat('log/ctews.log').st_mtime > request_time

    def get(self):
        """
        Returns 'data.txt' content when the resource has
        changed after the request time
        """
        request_time = time.time()
        while not self._is_updated(request_time):
            time.sleep(0.5)
        content = ''
        with open('log/ctews.log') as data:
            content = data.read()
        return {'content': content,
                'date': datetime.now().strftime('%Y/%m/%d %H:%M:%S')}

#returns the initial log
class Log(Resource):

    def get(self):
        """
        Returns the current data content
        """
        content = ''
        with open('log/ctews.log') as data:
            content = data.read()
        return {'content': content}

#assigns urls for restful retrieval of the ctews.log
api.add_resource(LogUpdate, '/log-update')
api.add_resource(Log, '/log')


#starts the app
if __name__ == '__main__':
    app.run(debug = True)

