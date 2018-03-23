# -*- coding: utf-8 -*-

import tweepy

auth = tweepy.OAuthHandler("Ur5J5gHT6uYg7zms7GaLUNYWf","SXSyveGpYiTzjifwC84IZ7IJBP7Lx138nODPhGW7CkykjIKTyu")
auth.set_access_token("2471507719-kvni00S7JySj8Iq3PUD80d01jrh8HpSodLnoABo","EjbRRnAf9E8CgedjX9qBZ2XZABnRGI8vHeHft50CU6f8S")

api = tweepy.API(auth)
places = api.geo_search(query="BR", granularity="country")
place_id = places[0].id

tweets = api.search(q="place:%s" % place_id)
for tweet in tweets:
    print tweet.text + " | " + tweet.place.name + " | " + tweet.place.id if tweet.place else "Undefined place"