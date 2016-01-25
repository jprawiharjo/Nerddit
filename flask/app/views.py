# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 15:39:45 2016

@author: jprawiharjo
"""

from app import app
from flask import jsonify
from elasticsearch import Elasticsearch

# create ES client
es = Elasticsearch(hosts = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                            "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                            "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                            'ec2-52-10-19-240.us-west-2.compute.amazonaws.com'])

INDEX_NAME = 'twittertest'
TYPE_NAME = 'text'
ID_FIELD = 'text'

@app.route('/')
@app.route('/index')
def index():
    return "Hello, Jerry!"
    
@app.route('/api/<query>')
def get_data(query):
    print "Querying ", query
    res = es.search(index = INDEX_NAME, size = 10, body={"query": {"match": {"text": query}}})
    #print(" response: '%s'" % (res))

    data = []
    for hit in res['hits']['hits']:
        data.append({"text": unicode(hit["_source"]['text']), "uid" :hit["_source"]['id_str']})
    print "Found ", len(data)
    return jsonify(text = data)