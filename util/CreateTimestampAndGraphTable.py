# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 14:50:15 2016

@author: jprawiharjo
"""
from cassandra.cluster import Cluster

import cassandra
import datetime


# Cassandra 
cluster = Cluster(["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                    "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                    "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                    'ec2-52-10-19-240.us-west-2.compute.amazonaws.com'
                    ])


keyspace = 'reddit'
#tablename1 = "redditstat"
tablename2 = "redditgraphmap2"


def log_results(results):
    for row in results:
        print "Results: %s" %(row,)

def log_error(exc):
    print "Operation failed: %s" %(exc,)


if __name__=="__main__":
    session = cluster.connect(keyspace)
    session.default_timeout = 600
    session.default_consistency_level = cassandra.ConsistencyLevel.QUORUM
    #session.encoder.mapping[tuple] = session.encoder.cql_encode_set_collection

#    res = session.execute("DROP TABLE IF EXISTS %s" %(tablename1,))
#    print res.current_rows
#
    res = session.execute("DROP TABLE IF EXISTS %s" %(tablename2,))
    print res.current_rows
#
#  
#    print "Creating Cassandra timestamp Table"
#    res = session.execute("""CREATE TABLE %s (
#                    year int,
#                    date timestamp,
#                    subredditCount bigint,
#                    authorCount bigint,
#                    commentCount bigint,
#                    PRIMARY KEY (year,date)
#                    )""" %(tablename1,))
#    print res.current_rows

    print "Creating second table"
    res = session.execute("""CREATE TABLE %s (
                    year int,
                    node1 text,
                    node2 map<text,int>,
                    PRIMARY KEY (year, node1)
                    )""" %(tablename2,))
    print res.current_rows

#    node1 = "test"
#    node2 = {"a": 1, "b" : 2}
#    
#    query = "INSERT INTO %s (year, node1, node2) VALUES (?, ?, ?)" %(tablename2)
#    prepared = session.prepare(query)
#    bound = prepared.bind((2015, node1, node2))
#    session.execute(bound)