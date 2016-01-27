# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 12:55:53 2016

@author: jprawiharjo
"""

from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from operator import add
from word_parser import SentenceTokenizer
import json
import datetime
import time

nodes = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
        "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
        "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
        'ec2-52-10-19-240.us-west-2.compute.amazonaws.com']


keyspace = "reddit"
tablename = "NgramsTable1"

#fn = "hdfs://ec2-52-88-193-39.us-west-2.compute.amazonaws.com:9000/camus/topics/twitterstream/hourly/2016/01/20/10/twitterstream.2.0.1343645.1343645.1453312800000.gz"      
#fn = "hdfs://ec2-52-88-193-39.us-west-2.compute.amazonaws.com:9000/user/jerry/retweet.txt"

def ConvertToYearDate(x):
    return time.strftime('%Y-%m-%d', time.gmtime(int(x)))
    
def ConvertToDatetime(x):
    return datetime.datetime.fromtimestamp(time.mktime(time.strptime(x,'%Y-%m-%d')))

def pushToCassandra(tbname, ngramcount, rdditer):
    # this needs to be here for distribution to workers    
    from cassandra.cluster import Cluster
    
    CassandraCluster = Cluster(nodes)
    session = CassandraCluster.connect(keyspace)
    #session.default_consistency_level = cassandra.ConsistencyLevel.ALL
    #self.session.encoder.mapping[tuple] = self.session.encoder.cql_encode_set_collection

    query = "INSERT INTO %s (date, subreddit, Ngram, N, count, percentage) VALUES (?, ?, ?, ?, ? ,?) IF NOT EXISTS" %(tbname,)
    prepared = session.prepare(query)

    for datatuple in rdditer:
        line = datatuple[0].split("::")
        
        ngram = line[0]
        createdtime = ConvertToDatetime(line[1])
        subreddit = line[2]
        
        count = datatuple[1]
        
        bound = prepared.bind((createdtime, subreddit, ngram, ngramcount, count, 0.0))
        session.execute_async(bound)
    session.shutdown()


if __name__ == "__main__":
    conf = SparkConf().setAppName("reddit")
    sc = SparkContext(conf=conf, pyFiles=['word_parser.py'])
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
    
    myTokenizer = SentenceTokenizer()
    
    data_rdd = sc.textFile("s3n://reddit-comments/2007/RC_2007-10")
    jsonformat = data_rdd.filter(lambda x: len(x)>0).map(lambda x: json.loads(x.encode('utf8')))
    jsonformat.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    ngram = 1
    etlData = jsonformat.map(lambda x: (x['body'], ConvertToYearDate(x['created_utc']), x['subreddit']))\
                    .flatMap(lambda x: ["{0}::{1}::{2}".format(y,x[1],x[2]) for y in myTokenizer.Ngrams(x[0].encode('utf-8'), ngram)])\
                    .map(lambda x: (x, 1))\
                    .reduceByKey(add)
    
    etlData.foreachPartition(lambda x: pushToCassandra(tablename, ngram, x))
    sc.stop()