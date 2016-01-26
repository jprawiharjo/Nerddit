# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 23:50:17 2016

@author: jprawiharjo
"""
from pyspark import SparkContext, SparkConf
import json
#from elasticsearch import Elasticsearch

esnodes = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
        "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
        "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
        'ec2-52-10-19-240.us-west-2.compute.amazonaws.com']

esport = 9200

esrsc = "twittertest/text"  

fn = "hdfs://ec2-52-88-193-39.us-west-2.compute.amazonaws.com:9000/camus/topics/twitterstream/hourly/2016/01/20/10/twitterstream.2.0.1343645.1343645.1453312800000.gz"      
#fn = "hdfs://ec2-52-88-193-39.us-west-2.compute.amazonaws.com:9000/user/jerry/retweet.txt"

def writeToES(hostlist, index_name, doc_type, rdditer):
    # this needs to be here for distribution to workers    
    from elasticsearch import Elasticsearch 
    es = Elasticsearch(hosts = hostlist)
    
    for data_dict in rdditer:
        _id = data_dict['id']
        es.index(index=index_name, doc_type=doc_type, id=_id, body=data_dict)

def reMapTweetData(item):
    retweeted = "retweeted_status" in item.keys()
    data_dict = {"id": item["id_str"], 
                "text": item["text"],
                "user_id": item['user'],
                "retweet_id" : item["retweeted_status"]['id_str'] if retweeted else item["id_str"],
                "retweted": retweeted}
    return data_dict

if __name__ == "__main__":
    conf = SparkConf().setAppName("pushToES")
    sc = SparkContext(conf=conf)

    #print 'begin'    
    words_rdd = sc.textFile(fn, use_unicode=True)
    cleanedup_words = words_rdd.filter(lambda x: len(x) >0).map(lambda x: json.loads(x.encode('utf-8')))
    
    datamap = cleanedup_words.map(lambda x: reMapTweetData(x))
    
    datamap.foreachPartition(lambda x: writeToES(esnodes, "twittertest", "text", x))