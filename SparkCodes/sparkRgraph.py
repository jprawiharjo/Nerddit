# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 12:55:53 2016

@author: jprawiharjo
"""

from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Row
from cassandra.cluster import Cluster
from operator import add
import json
import datetime
import time
import argparse
import os
import sys
from itertools import combinations
from pprint import pprint

from boto.s3.connection import S3Connection

#dateformat for year and week
dateformat = '%Y-%m'

#pruning the data
CassandraWait = 70
queryWait = 0.001
Npart = 100

nodes = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
        "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
        "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
        'ec2-52-10-19-240.us-west-2.compute.amazonaws.com',
        "ec2-54-69-144-162.us-west-2.compute.amazonaws.com"]

#nodes = ["ec2-52-88-193-39.us-west-2.compute.amazonaws.com",
#         "ec2-52-89-85-78.us-west-2.compute.amazonaws.com",
#         "ec2-54-69-118-49.us-west-2.compute.amazonaws.com",
#         "ec2-52-89-50-44.us-west-2.compute.amazonaws.com",
#         "ec2-52-89-54-203.us-west-2.compute.amazonaws.com"]

keyspace = "reddit"

def Splitnodes(x):
    splitstr = x[0].split("::")
    
    return (splitstr[0], (splitstr[1], x[1]))
    
def CombineNodes(x):
    if x[0].lower()[0] < x[1].lower()[0]:
        a = x[0]
        b = x[1]
    else:
        a = x[1]
        b = x[0]
    y = "{0}::{1}".format(a,b)
    return y

def pushToCassandraGraph(year, rdditer):
    tbname = "redditgraphmap2"
    # this needs to be here for distribution to workers    
    import cassandra
    from cassandra.cluster import Cluster

    #try to reconnect if connection is down
    CassandraCluster = Cluster(nodes, connect_timeout = 120)
    #CassandraCluster.load_balancing_policy = cassandra.policies.RoundRobinPolicy
    session = CassandraCluster.connect(keyspace)
    session.default_timeout = 600
        
    query = "INSERT INTO %s (year, node1, node2) VALUES (?, ?, ?)" %(tbname)
    prepared = session.prepare(query)

    for datarow in rdditer:
        bound = prepared.bind((year, datarow[0], datarow[1]))
        #session.execute(bound)
        session.execute_async(bound)
        time.sleep(queryWait) #slow it down, as it's done over a lot of partitions

    session.shutdown()

   
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('key', type=int, help='select key')
    args = parser.parse_args()

    conn = S3Connection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
    bucket = conn.get_bucket('reddit-comments')
    keys = list(bucket.list())

    #for namenode
    keys = keys[::-1]
    #keys = keys[1:45] 

    #for kafka4
    #keys = keys[42:64] #41 = 2010-11
    
    #keys = keys[0:2]
    #print "================================================"
    #print keys[0], keys[-1]
    #print "================================================"
    
    
    if args.key < 2000:    
        keys = [keys[args.key]]
        sname = "reddit-" + str(keys[0].key)
        fn = "s3n://reddit-comments/{0}".format(keys[0].key)
        if keys[0].key[-1] == "/":
            sys.exit()
        
        splitstr = keys[0].key.split("/")
        year = int(splitstr[0])
    else:
        sname = "reddit-" + str(args.key)
        fn = "s3n://reddit-comments/{0}/*".format(args.key)
        year = args.key


    print 'year = ', year
    
    print "Processing:", fn

    
    conf = SparkConf().setAppName(sname)
    sc = SparkContext(conf=conf)
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

    rawDF = sqlContext.read.json(fn, StructType(fields))\
                  .registerTempTable("comments")
                  #.persist(StorageLevel.MEMORY_AND_DISK_SER)\
    
    author_subreddit = sqlContext.sql("""
                                        SELECT DISTINCT(author) as authors, subreddit, COUNT(*) as com
                                        FROM comments
                                        WHERE author <> '[deleted]' 
                                            AND author <> 'AutoModerator' 
                                            AND subreddit <> 'AskReddit'
                                            AND subreddit <> 'reddit.com'
                                        GROUP BY author, subreddit
                                        ORDER BY authors DESC
                                        """).registerTempTable("author_subreddit")
    
    print "Filtered RDD"
    
    filtered_subreddit = sqlContext.sql("""
        SELECT * 
        FROM author_subreddit
        WHERE  com > 50
        ORDER BY authors DESC
        """)

    subreddit_connection = filtered_subreddit.map(lambda x: (x.authors,x.subreddit)).groupByKey().filter(lambda x: len(x[1]) > 1)
    graph_nodes = subreddit_connection.flatMap(lambda x: combinations(x[1], 2))
    
    reduced_graph_nodes = graph_nodes.map(lambda x: (CombineNodes(x), 1)).reduceByKey(add)
    
    #pprint(reduced_graph_nodes.take(5))

    filtered_nodes = reduced_graph_nodes.filter(lambda x: x[1] > 5)\
                                        .map(lambda x: Splitnodes(x))\
                                        .groupByKey()\
                                        .map(lambda x: (x[0], dict(x[1])))
    print "Writing to Cassandra"    

    filtered_nodes.foreachPartition(lambda x: pushToCassandraGraph(year, x) )
    
