# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 22:31:35 2016

@author: jprawiharjo
"""

#Clearing and rebuilding Elasticsearch index

from elasticsearch import Elasticsearch

es = Elasticsearch(hosts = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                            "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                            "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                            'ec2-52-10-19-240.us-west-2.compute.amazonaws.com'])

# cluster arrangement
request_body = {
    "settings" : {
        "number_of_shards": 2,
        "number_of_replicas": 3
    }
}

INDEX_NAME = 'twittertest'
TYPE_NAME = 'text'

ID_FIELD = 'text'
   
if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))
    
print("creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print(" response: '%s'" % (res))
