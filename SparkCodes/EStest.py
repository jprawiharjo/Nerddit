# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 22:31:35 2016

@author: jprawiharjo
"""

from elasticsearch import Elasticsearch

# create ES client, create index
es = Elasticsearch(hosts = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                            "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                            "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                            'ec2-52-10-19-240.us-west-2.compute.amazonaws.com'])

INDEX_NAME = 'wikitest'
TYPE_NAME = 'text'

#sanitycheck
#res = es.search(index = INDEX_NAME, size = 80, body={"query": {"match_all": {}}})
res = es.search(index = INDEX_NAME, size = 80, body={"query": {"match": {"Text": "symmetry"}}})
print len(res['hits']['hits'])
#print(" response: '%s'" % (res))

#print("results:")
for hit in res['hits']['hits']:
    print(hit["_source"]['Title'])
    
