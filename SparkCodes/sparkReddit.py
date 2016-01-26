# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 12:55:53 2016

@author: jprawiharjo
"""

from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from operator import add
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
    conf = SparkConf().setAppName("reddit")
    sc = SparkContext(conf=conf, pyFiles=['word_parser.py'])
    from word_parser import SentenceTokenizer
    sqlContext = SQLContext(sc)

    fields = [StructField("archived", BooleanType(), True),  
          StructField("author", StringType(), True),
          StructField("author_flair_css_class", StringType(), True),
          StructField("body", StringType(), True),
          StructField("controversiality", LongType(), True),
          StructField("created_utc", StringType(), True),
          StructField("distinguished", StringType(), True),
          StructField("downs", LongType(), True),
          StructField("edited", StringType(), True),
          StructField("gilded", LongType(), True),
          StructField("id", StringType(), True),
          StructField("link_id", StringType(), True),
          StructField("name", StringType(), True),
          StructField("parent_id", StringType(), True),
          StructField("retrieved_on", LongType(), True),
          StructField("score", LongType(), True),
          StructField("score_hidden", BooleanType(), True),
          StructField("subreddit", StringType(), True),
          StructField("subreddit_id", StringType(), True),
          StructField("ups", LongType(), True)]

    #rawDF = sqlContext.read.json("s3n://reddit-comments/2007/RC_2007-10", StructType(fields))\
    #              .persist(StorageLevel.MEMORY_AND_DISK_SER)\
    #              .registerTempTable("comments")
                  
#    distinct_gilded_authors_by_subreddit = sqlContext.sql("""  
#        SELECT subreddit, COUNT(DISTINCT author) as authors
#        FROM comments
#        WHERE gilded > 0
#        GROUP BY subreddit
#        ORDER BY authors DESC
#        """)    

    #distinct_gilded_authors_by_subreddit.take(5)

    myTokenizer = SentenceTokenizer()

    data_rdd = sc.textFile("s3n://reddit-comments/2007/RC_2007-10")
    jsonformat = data_rdd.filter(lambda x: len(x)>0).map(lambda x: json.loads(x.encode('utf-8')))
    jsonformat = jsonformat.map(lambda x: "{0}::x['body'])\
                    .flatMap(lambda x: myTokenizer.Ngrams(x.encode('utf-8')))\
                    .map(lambda x: (x, 1)) \
                    .reduceByKey(add)

    output = jsonformat.take(100)
    for (word, count) in output:
        print("%s: %i" % (word, count))                    

    sc.stop()