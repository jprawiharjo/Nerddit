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
import argparse
import sys
import requests.packages.urllib3
import random
import time
requests.packages.urllib3.disable_warnings()

# authentications
#access_token = os.environ['TWITTER_ACCESS_TOKEN']
#access_token_secret = os.environ['TWITTER_ACCESS_TOKEN_SECRET']
#consumer_key = os.environ['TWITTER_CONSUMER_KEY']
#consumer_secret = os.environ['TWITTER_CONSUMER_SECRET']

leaders = ["ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
           "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
           "ec2-52-10-19-240.us-west-2.compute.amazonaws.com",
           "ec2-52-27-157-187.us-west-2.compute.amazonaws.com"]

class TwitterStreamer(StreamListener):
    """ A listener handles tweets that are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    __kafkatopics = None

    def __init__(self, writetofile=False, sendtokafka=True):
        self.__writetofile = writetofile
        self.__sendtokafka = sendtokafka
        self.__counter = 0
        self.__msg = []
        print self.__sendtokafka

    #method to setup kafka parameters        
    def setupKafka(self, kafkahost, kafkaport, kafkatopics):
        kafkaaddress = '{0}:{1}'.format(kafkahost,kafkaport)
        self.__kafkaclient = kafka.KafkaClient(kafkaaddress)
        self.__kafkaproducer = kafka.SimpleProducer(self.__kafkaclient, async=False)
        self.__kafkatopics = kafkatopics

    # this is the event handler for new data
    def on_data(self, data):
        #print data
        if self.__writetofile:
            if not os.path.isfile(self.filename):    # check if file doesn't exist
                f = file(self.filename, 'w')
                f.close()
            with open(self.filename, 'ab') as f:
                f.write(data)
        elif self.__sendtokafka:
            #print data
            if self.__kafkatopics is None: raise ValueError
            if 'text' in data:
                self.__counter += 1
                self.__msg.append(data.encode('utf-8'))
                if self.__counter % 10 == 0:
                    self.__kafkaproducer.send_messages(self.__kafkatopics, *self.__msg)
                    self.__msg = []
                    #print 'msg sent', self.__counter
            if self.__counter % 1000 == 0: 
                print "Processsed {0} messages".format(self.__counter)
        else:
            #print data
            q = json.loads(data.encode('utf-8'))
            if 'text' in q.keys():
                self.__counter += 1
                if self.__counter == 2: return

    # this is the event handler for errors
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

#Function to laod list of words
def LoadTwitterWords(filename):
    fopen = open(filename)
    a = []
    for line in fopen:
        a.append(line.strip().decode("utf8"))
    return a

if __name__ == "__main__" :
    parser = argparse.ArgumentParser()
    parser.add_argument('key', type=int, help='pick Twitter key')
    parser.add_argument('words', nargs="+", help='words to track')
    args = parser.parse_args()
    print "Args =", args.key, args.words
    
    with open('twitterkey.txt') as twitter_file:  
        key = json.load(twitter_file)
        
    # authentications
    s = 'key{}'.format(args.key)
    access_token = str(key[s]["access_token"])
    access_token_secret = str(key[s]["access_token_secret"])
    consumer_key = str(key[s]["consumer_key"])
    consumer_secret = str(key[s]["consumer_secret"])
    #print access_token
    #print consumer_key
    #print access_token_secret

    if '.txt' in args.words:
        if os.path.exists(args.words):
            #words = LoadTwitterWords('twitterwordsw.txt') #load most frequent 500 words in Twitter
            words = LoadTwitterWords(args.words) #load most frequent 500 words in Twitter
            words = words[:400]
        else:
            print "File not exist"
            sys.exit(1)
    else:
        words = args.words
        
    #print words

    listener = TwitterStreamer(sendtokafka = True)
    listener.setupKafka(leaders[random.randint(0,len(leaders)*4)%len(leaders)], 9092, "twitterstream2")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print "Use CTRL + C to exit at any time.\n"
    #stream.filter(locations=[-180,-90,180,90]) # this is the entire world, any tweet with geo-location enabled
    while True:    
        stream = Stream(auth, listener)
        if not stream.filter(languages=["en"],track=words):
            time.sleep(5*60)
        stream.disconnect()