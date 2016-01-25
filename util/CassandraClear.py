# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 14:50:15 2016

@author: jprawiharjo
"""
from cassandra.cluster import Cluster
import cassandra
import datetime

cluster = Cluster(["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                   "ec2-52-34-178-13.us-west-2.compute.amazonaws.com"])

keyspace = 'wikidata'
tablename = "titlelinks"

def log_results(results):
    for row in results:
        print "Results: %s" %(row,)

def log_error(exc):
    print "Operation failed: %s" %(exc,)

if __name__=="__main__":
    session = cluster.connect(keyspace)
    session.default_consistency_level = cassandra.ConsistencyLevel.QUORUM
    session.encoder.mapping[tuple] = session.encoder.cql_encode_set_collection

    res = session.execute("DROP TABLE IF EXISTS %s" %(tablename,))
    res = session.execute("""CREATE TABLE %s (
                    id text,
                    title text PRIMARY KEY,
                    linksto set<text>,
                    referredby set<text>
                    )""" %(tablename,))

    #Testing insert, update and query
    query1 = "INSERT INTO %s (id, title, linksto, referredby) VALUES (?, ?, ?, ?) IF NOT EXISTS" %(tablename,)
    prepared1 = session.prepare(query1)
    query2 = "UPDATE %s SET referredby = referredby + ? WHERE title = ? IF EXISTS" %(tablename,)
    prepared2 = session.prepare(query2)

    query3= "INSERT INTO %s (title, referredby) VALUES (?, ?) IF NOT EXISTS" %(tablename,)
    prepared3 = session.prepare(query3)

    query4 = "UPDATE %s SET id = ?, linksto = ?, referredby = referredby + ? WHERE title = ? IF EXISTS" %(tablename,)
    prepared4 = session.prepare(query4)


    bound = prepared1.bind(("1123", "title", ['asdfas', 'dafsad'], ['adadsf', 'asdadf']))
    res = session.execute(bound)
    log_results(res)

    bound = prepared1.bind(("1123", "title", ['asdfas', 'dafsad'], ['adadsf', 'asdadf', 'ghdgf']))
    res = session.execute(bound)
    log_results(res)

    bound = prepared2.bind((['asdfas', 'swerew'], 'test'))
    res = session.execute(bound)
    res = res.current_rows[0].applied
    print "Update result:", res
    
    if not res:
        bound = prepared3.bind(( 'test', ['asdfas', 'swerew']))
        res = session.execute(bound)
        print res.current_rows[0].applied
        print "Update result:"
        log_results(res)
    
    

    rows = session.execute_async('SELECT * FROM titlelinks')
    print "Query result:"
    #rows.add_callbacks(log_results, log_error)
    rows = session.execute('SELECT * FROM titlelinks')
    log_results(rows)

    bound = prepared4.bind(( '2343252', ['asdfas', 'swerew'], ['jtjhfg'], 'test'))
    res = session.execute(bound)
    print res.current_rows[0].applied

    rows = session.execute_async('SELECT * FROM titlelinks')
    print "Query result:"
    #rows.add_callbacks(log_results, log_error)
    rows = session.execute('SELECT * FROM titlelinks')
    log_results(rows)
    
    
    
    #Reset the table
    session.execute("DROP TABLE IF EXISTS %s" %(tablename,))
    session.execute("""CREATE TABLE %s (
                    id text,
                    title text PRIMARY KEY,
                    linksto set<text>,
                    referredby set<text>
                    )""" %(tablename,))
    cluster.shutdown()