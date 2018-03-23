# Author: Raphael Fonseca
# Social Computing and Social Network Analysis Laboratory, Federal University of Rio de Janeiro, Brazil
# Create Date: 2015-07
# Last Update: 2017-07-27

# -*- coding: utf-8 -*-

import oauth2 as oauth
import json
import time
import pymongo

# API Authentication - Fill in credentials
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_KEY = ""
ACCESS_SECRET = ""

# Prepare Authentication
consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
access_token = oauth.Token(key=ACCESS_KEY, secret=ACCESS_SECRET)
client = oauth.Client(consumer, access_token)

# Connect to Database
clientMongo = pymongo.MongoClient("localhost", 27017)
db = clientMongo.chosenDatabase

# Choose if you will use since_id. If you do choose to use it, include it in the URL.
since_id = '' 

# Geolocation of Central Point
# Format: Latitude, Longitude, Radius from position
# Example: -22.903426,-43.191478,70km
geo=''

# Starting Date. 
# Format: YYYY-MM-DD
since=''

# Until the day prior to... 
# Format: YYYY-MM-DD.
until=''

# List of Terms, separate by commas
query = ['']

# Language
# Format: &lang=languageCode
# Example for Portuguese: &lang=pt
language=""

for q in query:

        max_id = '0'
        tweetsCounter = 0
        continueFlag = 1

        while(continueFlag == 1):
                try:

                        if(max_id == '0'):

                                URL = "https://api.twitter.com/1.1/search/tweets.json?geocode="+geo+"&since="+since+"&until="+until+"&count=100"+language
                        else:

                                URL = "https://api.twitter.com/1.1/search/tweets.json?geocode="+geo+"&since="+since+"&until="+until+"&count=100"+language+"&max_id="+str(max_id)
                        max_id_prior = max_id
                        response, data = client.request(URL, "GET")
                        localCollection = json.loads(data)
                        for tweet in localCollection['statuses']:
                                                        
                                db.localCollection.update({'id': tweet['id']},tweet, upsert=True)
                                tweetsCounter = tweetsCounter + 1
                                tweet['text']==dict
                                tx = tweet['text']
                                print("Search Term: "+ q)
                                print("\n")
                                print(str("Tweet: " + tx.encode('utf-8')))
                                print("\n")
                                print("Number of tweets with current term: ")
                                print(tweetsCounter)
                                print("\n")
                                print("Tweet ID: ")
                                print(max_id)
                                print('======================================================')
                                max_id = tweet['id'] - 1
                                
                        time.sleep(2)
                        if(max_id == max_id_prior):                                
                                continueFlag = 0
                        
                except Exception, e:

                        print(e)
                        print('slept')
                        time.sleep(15*60)
                        pass
