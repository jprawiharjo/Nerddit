# -*- coding: utf-8 -*-
"""
Created on Thu Feb  4 09:33:27 2016

@author: jprawiharjo
"""
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

from datetime import date, datetime, timedelta
import time
from pprint import pprint


keyspace = 'reddit'
tablename1 = "redditstat"

cluster = Cluster(["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
        "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
        "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
        'ec2-52-10-19-240.us-west-2.compute.amazonaws.com',
        "ec2-54-69-144-162.us-west-2.compute.amazonaws.com"])

session = cluster.connect(keyspace)
session.default_timeout = 600
session.default_consistency_level = ConsistencyLevel.QUORUM

comments = []
authors = []
subreddits = []

for year in range(2007,2016):
    res = session.execute("SELECT * FROM redditstat WHERE year = %s" %(year,))
    
    for row in res:
        timestamp = time.mktime(row.date.timetuple()) * 1000
        comments.append([timestamp, row.commentcount])
        authors.append([timestamp, row.authorcount])
        subreddits.append([timestamp, row.subredditcount])

session.shutdown()

comments = sorted(comments, key = lambda x: x[0])
authors = sorted(authors, key = lambda x: x[0])
subreddits = sorted(subreddits, key = lambda x: x[0])

with open('comments.json','w') as fcomments:
    pprint(comments,stream = fcomments)
    
with open('authors.json','w') as fauthors:
    pprint(authors,stream = fauthors)

with open('subreddits.json','w') as fsr:
    pprint(subreddits,stream = fsr)