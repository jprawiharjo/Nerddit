# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 14:50:15 2016

@author: jprawiharjo
"""
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

import cassandra
import datetime


# Cassandra 
cluster = Cluster(["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                   "ec2-52-34-178-13.us-west-2.compute.amazonaws.com"])

es = Elasticsearch(hosts = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                            "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                            "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                            'ec2-52-10-19-240.us-west-2.compute.amazonaws.com'])

keyspace = 'reddit'
tablename = "NgramsTable1"

# cluster arrangement
request_body = {
    "settings" : {
        "number_of_shards": 2,
        "number_of_replicas": 3
    }
}

INDEX_NAME = 'reddit'
TYPE_NAME = 'text'
ID_FIELD = 'text'

def log_results(results):
    for row in results:
        print "Results: %s" %(row,)

def log_error(exc):
    print "Operation failed: %s" %(exc,)


if __name__=="__main__":
    session = cluster.connect(keyspace)
    session.default_consistency_level = cassandra.ConsistencyLevel.QUORUM
    #session.encoder.mapping[tuple] = session.encoder.cql_encode_set_collection

    print "Dropping Cassandra Table"
    res = session.execute("DROP TABLE IF EXISTS %s" %(tablename,))
    print "Recreating Cassandra Table"
    res = session.execute("""CREATE TABLE %s (
                    date timestamp,
                    subreddit text,
                    Ngram text,
                    N int,
                    count bigint,
                    percentage double,
                    PRIMARY KEY (date, subreddit, Ngram)
                    )""" %(tablename,))

    #Testing insert, update and query
    #query1 = "INSERT INTO %s (date, subreddit, Ngram, N, count, percentage) VALUES (?, ?, ?, ?, ? ,?) IF NOT EXISTS" %(tablename,)
    #prepared1 = session.prepare(query1)
    #bound = prepared1.bind((datetime.datetime.now(), "test", "test32", 1, 2, 0.3))
    #session.execute_async(bound)
    
#    query2 = "UPDATE %s SET referredby = referredby + ? WHERE title = ? IF EXISTS" %(tablename,)
#    prepared2 = session.prepare(query2)
#
#    query3= "INSERT INTO %s (title, referredby) VALUES (?, ?) IF NOT EXISTS" %(tablename,)
#    prepared3 = session.prepare(query3)
#
#    query4 = "UPDATE %s SET id = ?, linksto = ?, referredby = referredby + ? WHERE title = ? IF EXISTS" %(tablename,)
#    prepared4 = session.prepare(query4)
#
#
#    print "Attemp to insert into Cassandra table"
#    bound = prepared1.bind(("b7c68bb17aeb4c68a4f69ccdb3a78231", "Circular Symmetry", ['a']))
#    res = session.execute(bound)
#    log_results(res)
#
#    session.execute("INSERT INTO {} (id, title, linksto) VALUES (%s, %s, %s) IF NOT EXISTS".format(tablename), ("asd", "asd", {"asdf"}))
#
#    bound = prepared1.bind(("1123", "title", ['asdfas']))
#    res = session.execute(bound)
#    log_results(res)
#
#    bound = prepared2.bind((['asdfas', 'swerew'], 'test'))
#    res = session.execute(bound)
#    res = res.current_rows[0].applied
#    print "Update result:", res
#    
#    if not res:
#        bound = prepared3.bind(( 'test', ['asdfas', 'swerew']))
#        res = session.execute(bound)
#        print res.current_rows[0].applied
#        print "Update result:"
#        log_results(res)
#    
#    rows = session.execute_async('SELECT * FROM titlelinks')
#    print "Query result:"
#    #rows.add_callbacks(log_results, log_error)
#    rows = session.execute('SELECT * FROM titlelinks')
#    log_results(rows)
#
#    bound = prepared4.bind(( '2343252', ['asdfas', 'swerew'], ['jtjhfg'], 'test'))
#    res = session.execute(bound)
#    print res.current_rows[0].applied
#
#    rows = session.execute_async('SELECT * FROM titlelinks')
#    print "Query result:"
#    #rows.add_callbacks(log_results, log_error)
#    rows = session.execute('SELECT * FROM titlelinks')
#    log_results(rows)
#    
#    #Reset the table
#    print "Dropping table again"
#    session.execute("DROP TABLE IF EXISTS %s" %(tablename,))
#    print "Recreating table"
#    session.execute("""CREATE TABLE %s (
#                    id text,
#                    title text PRIMARY KEY,
#                    linksto set<text>,
#                    referredby set<text>
#                    )""" %(tablename,))
#    print "Shutting down connection to Cassandra"
#    cluster.shutdown()
#    
#    print "==========================================="    
#    print "Recreating ES index"
#    if es.indices.exists(INDEX_NAME):
#        print("deleting '%s' index..." % (INDEX_NAME))
#        res = es.indices.delete(index = INDEX_NAME)
#        print(" response: '%s'" % (res))
#        
#    print("creating '%s' index..." % (INDEX_NAME))
#    res = es.indices.create(index = INDEX_NAME, body = request_body)
#    print(" response: '%s'" % (res))
