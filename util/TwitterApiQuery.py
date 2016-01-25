# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 11:09:43 2016

@author: jprawiharjo
"""

import tweepy
from tweepy.streaming import StreamListener
from tweepy import Stream
import json
import os
import kafka #v0.9.5 at writing time
import time
from collections import deque
import argparse
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

class MyKafkaProducer(object):
    def __init__(self, kafkahost, kafkaport, kafkatopics):
        kafkaaddress = '{0}:{1}'.format(kafkahost,kafkaport)
        self.__kafkaclient = kafka.KafkaClient(kafkaaddress)
        self.__kafkaproducer = kafka.SimpleProducer(self.__kafkaclient, async=False)
        self.__kafkatopics = kafkatopics
        self.__counter = 0

    # this is the event handler for new data
    def send(self, data):
        if self.__kafkatopics is None: 
            raise ValueError
        #print data.encode('utf-8')
        self.__kafkaproducer.send_messages(self.__kafkatopics, data.encode('utf-8'))
        self.__counter += 1
        print "Processsed {0} messages".format(self.__counter)


def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            print "Waiting: RateLimitError"
            time.sleep(15 * 60)
            
def get_follower_ids(tapi, userid, limit = 200):
    userinfo = tapi.get_user(userid)
    print "Processing user id = ", userid, "Name = ", userinfo.name
    ids = []
    for page in limit_handled(tweepy.Cursor(tapi.followers, id=userid).pages()):
        print "handling followers for", userid
        for l in page:
            ob = l._json
            MyProducer.send(json.dumps(ob))
        for l in page:
            if l.followers_count >= limit:
                ids.append(l.id)
                get_follower_ids(tapi, l.id, 100)
                time.sleep(60)
        time.sleep(60)
    return ids
    
def get_follower_idslist(tapi, userid, limit = 200):
    #userinfo = tapi.get_user(userid)
    print "Processing user id = ", userid
    uid = deque([userid])
    
    while len(uid) > 0:
        uid.extend(processV(uid.popleft()))
        print "Queue length=",len(uid)
       
def processV(userid):
    ids = []
    ob = {}
    ob['parent'] = userid
    ob['follower'] = []
    counter = 0
    for page in limit_handled(tweepy.Cursor(tapi.followers_ids, id=userid).pages()):
        ids.extend(page)
        ob['follower'] = page
        MyProducer.send(json.dumps(ob))
        counter += 1
        print "page", counter, ": processing ", userid, "Followers= ", len(page)
        # try to go below the rate limit11
        time.sleep(40)
    return ids


if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument('key', type=int, help='pick Twitter key')
    args = parser.parse_args()

    with open('twitterkey.txt') as twitter_file:  
        key = json.load(twitter_file)
        
    # authentications
    s = 'key{}'.format(args.key)
    access_token = key[s]["access_token"]
    access_token_secret = key[s]["access_token_secret"]
    consumer_key = key[s]["consumer_key"]
    consumer_secret = key[s]["consumer_secret"]
    
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    tapi = tweepy.API(auth)
    
    MyProducer = MyKafkaProducer("ec2-52-34-178-13.us-west-2.compute.amazonaws.com", 9092, "twitterusers")

    #HillaryClinton is 1339835893
    #BernieSanders is 216776631
    #danielysebasti is 1567723934
    #tedcruz is 23022687
    if args.key == 1:
        ids = get_follower_idslist(tapi, 1339835893)
    elif args.key == 2:
        ids = get_follower_idslist(tapi, 216776631)
    elif args.key == 3:
        ids = get_follower_idslist(tapi, 23022687)
    elif args.key == 4:
        ids = get_follower_idslist(tapi, 25073877) #Donald Trump
    elif args.key == 5:
        ids = get_follower_idslist(tapi, 65493023) #Sarah Palin
    #ids = get_follower_ids(tapi, 216776631)