# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 12:55:53 2016

@author: jprawiharjo
"""

from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Row
from cassandra.cluster import Cluster
from pyspark.sql import SQLContext
from operator import add
from word_parser import SentenceTokenizer
import json
import datetime
import time
import argparse
import os

from boto.s3.connection import S3Connection

#dateformat for year and week
dateformat = '%Y-%m'

#pruning the data
CassandraWait = 5
queryWait = 0.001
Npart = 100
table1 = 'ngramstable1'
table2 = 'ngramstable2'

nodes = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
        "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
        "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
        'ec2-52-10-19-240.us-west-2.compute.amazonaws.com']

keyspace = "reddit"

#fn = "hdfs://ec2-52-88-193-39.us-west-2.compute.amazonaws.com:9000/camus/topics/twitterstream/hourly/2016/01/20/10/twitterstream.2.0.1343645.1343645.1453312800000.gz"      
#fn = "hdfs://ec2-52-88-193-39.us-west-2.compute.amazonaws.com:9000/user/jerry/retweet.txt"

def ConvertToYearDate(x):
    return time.strftime(dateformat, time.gmtime(int(x)))
    
def ConvertToDatetime(x):
    if dateformat == '%Y-%W':
        return datetime.datetime.strptime(x + '-0', "%Y-%W-%w")
    else:
        return datetime.datetime.strptime(x, dateformat)

def splitByDate(tupdata):
    #x = ( "Ngram::time::subreddit", count)
    splitline = tupdata[0].split("::")
    #returned value = ( "time", (Ngram, subreddit, count))
    return (splitline[1], (splitline[0], splitline[2], tupdata[1]))

#splitting tuple key by subreddit
def splitBySubreddit(tupdata):
    #x = ( "Ngram::time::subreddit", count)
    splitline = tupdata[0].split("::")
    #returned value = ( "subreddit::Ngram", count)
    return ("{0}::{1}".format(splitline[2],splitline[0]), tupdata[1])
    
def combineData(tokenizer, tupdata, ngram):
    body = tupdata[0]
    utctime = tupdata[1]
    subreddit = tupdata[2]
    tokens = tokenizer.Ngrams(body.encode('utf-8'), ngram)
    rtnval =  ["{0}::{1}::{2}".format(y, utctime, subreddit) for y in tokens]
    return rtnval
    
def pushToCassandraTable1(ngramcount, rdditer, async = True):
    tbname = table1
    # this needs to be here for distribution to workers    
    from cassandra.cluster import Cluster
    CassandraCluster = Cluster(nodes)

    success = False
    #try to reconnect if connection is down
    while not success:
        try:
            session = CassandraCluster.connect(keyspace)
            session.default_timeout = 60
            success = True
        except:
            success = False
            time.sleep(CassandraWait)
        
    #session.default_consistency_level = cassandra.ConsistencyLevel.ALL
    #self.session.encoder.mapping[tuple] = self.session.encoder.cql_encode_set_collection

    query = "INSERT INTO %s (date, subreddit, Ngram, N, wcount, percentage) VALUES (?, ?, ?, ?, ? ,?)" %(tbname,)
    prepared = session.prepare(query)

    counter = 0
    for datatuple in rdditer:
        #returned value = ( "time", ((Ngram, subreddit, count), total))
       
        createdtime = ConvertToDatetime(datatuple[0])

        ngram = str(datatuple[1][0][0])
        subreddit = str(datatuple[1][0][1])
        count = int(datatuple[1][0][2])
        total = float(datatuple[1][1])
        percentage = float(count) / total
        
        #print createdtime, subreddit, ngram, ngramcount, count, percentage        
        
        bound = prepared.bind((createdtime, subreddit, ngram, ngramcount, count, percentage))
        if async:        
            session.execute_async(bound)
            time.sleep(queryWait) #slow it down, as it's done over a lot of partitions
            counter += 1
        else:
            session.execute(bound)

    session.shutdown()

def pushToCassandraTable2(ngramcount, rdditer):
    tbname = table2
    # this needs to be here for distribution to workers    
    from cassandra.cluster import Cluster
    CassandraCluster = Cluster(nodes)

    success = False
    #try to reconnect if connection is down
    while not success:
        try:
            session = CassandraCluster.connect(keyspace)
            session.default_timeout = 60
            success = True
        except:
            success = False
            time.sleep(CassandraWait)

    #session.default_consistency_level = cassandra.ConsistencyLevel.ALL
    #self.session.encoder.mapping[tuple] = self.session.encoder.cql_encode_set_collection

    queryupdate = """UPDATE %s SET wcount = wcount + ?, total = total + ? where subreddit = ? AND Ngram = ? AND N = ?""" %(tbname,)
    preparedupdate = session.prepare(queryupdate)

    for datatuple in rdditer:
        #etldata2 ( "subreddit::Ngram", (count, total))
 
        splitline = datatuple[0].split("::")

        subreddit = splitline[0]
        ngram = splitline[1]
        
        count = datatuple[1][0]
        total = datatuple[1][1]
        #percentage = float(count) / total
        
        bound = preparedupdate.bind((count, total, subreddit, ngram, ngramcount))
        session.execute_async(bound)
        time.sleep(queryWait) #slow it down, as it's done over a lot of partitions
        
    session.shutdown()

def map_func(key):
    try:
    # Use the key to read in the file contents, split on line endings
        for line in key.get_contents_as_string().splitlines():
            # parse one line of json
            if len(line) > 0:        
                j = json.loads(line.encode('utf-8'))
                yield j
    except:
        raise StopIteration

if __name__ == "__main__":
    #parser = argparse.ArgumentParser()
    #parser.add_argument('year', type=int, help='select year', action = 'store_true')
    #args = parser.parse_args()
    
    conf = SparkConf().setAppName("reddit")
    sc = SparkContext(conf=conf, pyFiles=['word_parser.py'])
    sqlContext = SQLContext(sc)
    
    myTokenizer = SentenceTokenizer()

    fw = open("foreignsubredditlist.txt",'r')
    blacklist = []
    for line in fw:
        blacklist.append(line.rstrip().lower())
    fw.close()
    print blacklist[0]
    
    fw = open("redditwhitelist.txt",'r')
    whitelist = []
    for line in fw:
        whitelist.append(line.rstrip().lower())
    fw.close()
    whitelist = whitelist[:1001]
    print whitelist[1]
    #data_rdd = sc.textFile("s3n://reddit-comments/2007/RC_2007-10")
    #data_rdd = sc.textFile("s3n://reddit-comments/2015/*")
    
    #fns = ['20' + str(x).zfill(2) for x in range(9,16)]
    #fns = ['2007', '2008']

    conn = S3Connection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
    bucket = conn.get_bucket('reddit-comments')
    keys = list(bucket.list())
    keys = keys[::-1]
    keys = keys[1:45]
    #print keys
    # Call the map step to handle reading in the file contents
    #jsonformat = pkeys.flatMap(lambda x: map_func(x)).filter(lambda x: not(x['subreddit'] in frlist))

    for fn in keys:
        print fn.key
        if fn.key[-1] == "/":
            continue
        if len(fn.key.split("/")) !=2:
            continue
        
        ts = fn.key.split("/")
        dt = mn(ts[0].split("_")[0])
        mn = int(ts[1])
        
        data_rdd = sc.textFile("s3n://reddit-comments/{0}".format(fn.key))
        
        jsonformat = data_rdd.filter(lambda x: len(x) > 0)\
                                .map(lambda x: json.loads(x.encode('utf8')))\
                                .filter(lambda x: not(x['subreddit'].lower() in blacklist))\
                                .filter(lambda x: x['subreddit'].lower() in whitelist)
        
        #print jsonformat.count()
        #make key from token, date and subreddit, and persist
        etlDataMain = jsonformat.map(lambda x: [x['body'], ConvertToYearDate(x['created_utc']), x['subreddit']])
        etlDataMain.persist(StorageLevel.MEMORY_AND_DISK_SER)
        #print etlDataMain.count()

        #make key from token, date and subreddit, and persist
        etlDataMain = jsonformat.map(lambda x: [x['body'], ConvertToYearDate(x['created_utc']), x['subreddit']])
        etlDataMain.repartition(Npart)
        etlDataMain.persist(StorageLevel.MEMORY_AND_DISK_SER)
        print "etlDataMain Number of partitions= ", etlDataMain.getNumPartitions()
    
        for ngram in range(1,4):
            print "Ngram = ", ngram
            #prepare for word count
            #word count
            #persist for future use
            print "first batch query"
            etlData = etlDataMain.flatMap(lambda x: combineData(myTokenizer, x,ngram))\
                            .map(lambda x: (x, 1))\
                            .reduceByKey(add, Npart)#\
                            #.persist(StorageLevel.MEMORY_AND_DISK_SER)
            #we should end up with ( "Ngram::time::subreddit", count)
        
            #now we're going to transform ( "time", (Ngram, subreddit, count))
            etlData1 = etlData.map(lambda x: splitByDate(x)) #split key into dates for summation
            
            #For summation, we need ( "time", count) for time based summation
            etlDataSum = etlData1.map(lambda x: (x[0], x[1][2])).reduceByKey(add, Npart)
            print "etlDataSum Number of partitions= ", etlDataSum.getNumPartitions()
            
            if ngram > 1:
                threshold = 3
            else:
                threshold = 20
            
            #pruning (filter)
            #we now join to get  ( "time", ((Ngram, subreddit, count), total))
            combinedEtl = etlData1.filter(lambda x: x[1][2] > threshold)\
                                .leftOuterJoin(etlDataSum)
    
            print "combinedEtl Number of partitions= ", combinedEtl.getNumPartitions()
            #print "Data example = ", combinedEtl.take(1)
            
            combinedEtl.foreachPartition(lambda x: pushToCassandraTable1(ngram, x))
    
            print "second batch query"
            ##############################################################
            # Second batch query
            ##############################################################
            
            #we also want to split by subreddit :  ("subreddit::Ngram", count)
            #and sum it
            #etldata2 ( "subreddit::Ngram", count)
            etlData2 = etlData.map(lambda x: splitBySubreddit(x))\
                            .reduceByKey(add, Npart)
                            
            #For summation, we will sum everything
            totalSum = etlData2.map(lambda x: x[1]).sum()
            
            #remember that totalSum = ["key", total]!!!
            subredditEtl = etlData2.filter(lambda x: x[1] > threshold)\
                                .map(lambda x: (x[0], (x[1], totalSum)))
                                
            subredditEtl.foreachPartition(lambda x: pushToCassandraTable2(ngram, x))
            etlData.unpersist() ##DONT forget to unpersist!!
            etlData2.unpersist()
            
        print "end of program"
        etlDataMain.unpersist() ##DONT forget to unpersist!!