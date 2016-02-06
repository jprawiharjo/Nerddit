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

def pushToCassandraStat(rdditer):
    tbname = "redditstat"
    # this needs to be here for distribution to workers    
    import cassandra
    from cassandra.cluster import Cluster

    CassandraCluster = Cluster(nodes, connect_timeout = 120)
    #CassandraCluster.load_balancing_policy = cassandra.policies.RoundRobinPolicy
    session = CassandraCluster.connect(keyspace)
    session.default_timeout = 600
        
    query = """INSERT INTO %s (year, date, authorcount, commentcount, subredditcount)
                VALUES (?, ?, ?, ?, ?)""" %(tbname)
    prepared = session.prepare(query)

    for datarow in rdditer:
        timestamp = datetime.datetime.strptime(datarow.date_created, "%Y-%m-%d")
        year = int(timestamp.year)
        bound = prepared.bind((year, timestamp, datarow.distinct_authors, datarow.comments_count, datarow.distinct_subr))
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
    keys = [keys[args.key]]
    #print "================================================"
    #print keys[0], keys[-1]
    #print "================================================"
    
    sname = "reddit-" + str(keys[0].key)
        
    
    fn = "s3n://reddit-comments/{0}".format(keys[0].key)
    print "Processing:", fn

    if keys[0].key[-1] == "/":
        sys.exit()
    
    splitstr = keys[0].key.split("/")
    year = int(splitstr[0])
    
    
    print 'year = ', year
    
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
                  .persist(StorageLevel.MEMORY_AND_DISK_SER)\
                  .registerTempTable("comments")
    
    unique_perday = sqlContext.sql("""
                                    SELECT FROM_UNIXTIME(created_utc,'YYYY-MM-dd') as date_created, 
                                        COUNT(DISTINCT(subreddit)) as distinct_subr,
                                        COUNT(*) AS comments_count,
                                        COUNT(DISTINCT(author)) as distinct_authors
                                    FROM comments GROUP BY FROM_UNIXTIME(created_utc,'YYYY-MM-dd')
                                    """)
   
    unique_perday.foreachPartition(lambda x: pushToCassandraStat(x))