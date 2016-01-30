# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 15:39:45 2016

@author: jprawiharjo
"""

from app import app
from flask import jsonify, render_template, url_for, request, flash, redirect
from app.stock_scraper import *
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

from bokeh.embed import components
from bokeh.plotting import figure
from bokeh.resources import INLINE
from bokeh.util.string import encode_utf8
import random
from datetime import date, datetime, timedelta
from monthdelta import monthdelta



cluster = Cluster(["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                    "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                    "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                    'ec2-52-10-19-240.us-west-2.compute.amazonaws.com'])

keyspace = 'reddit'
tablename1 = "NgramsTable1"
tablename2 = "NgramsTable2"

session = cluster.connect(keyspace)
session.default_timeout = 120
session.default_consistency_level = ConsistencyLevel.QUORUM

query1 = "SELECT * FROM %s WHERE ngram = ? AND SUBREDDIT = ?" %(tablename1,)
prepared1 = session.prepare(query1)

query2 = "SELECT * FROM %s WHERE ngram = ? and n = ?" %(tablename2,)
prepared2 = session.prepare(query2)

#Creating a list of time with start, end and step
def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta
        
#Creating a list of time for our known time list
Xtime = [x for x in perdelta(date(2007, 10, 01), date(2016, 01, 01), monthdelta(1))]

@app.route('/')
@app.route("/index")
def index():
    return render_template("index.html" )

def get_timeseries_data(ngram, subreddit):
    bound = prepared1.bind((ngram, subreddit))
    res = session.execute(bound)
    
    response_list = []
    for val in res:
        response_list.append(val)

    results = []    
    
    if len(response_list) == 0:
        return results
    
    #If the data is not there, put zero in it
    j = 0
    for k in Xtime:
        if j < len(response_list):
            if k == datetime.date(response_list[j].date):
                results.append({'ngram': ngram, 
                                "subreddit" : subreddit, 
                                "percentage" : response_list[j].percentage, 
                                "date" : response_list[j].date, 
                                "count": response_list[j].wcount})
                j += 1
            else:
                results.append({'ngram': ngram, 
                                "subreddit" : subreddit, 
                                "percentage" : 0, 
                                "date" : k, 
                                "count": 0})
        else:
            results.append({'ngram': ngram, 
                            "subreddit" : subreddit, 
                            "percentage" : 0, 
                            "date" : k, 
                            "count": 0})
            
    return results

def get_subreddit_data(ngram):
    n = len(ngram.split(" "))
    bound = prepared2.bind((ngram, n))
    res = session.execute(bound)
    
    response_list = []
    for val in res:
        response_list.append(val)
    
    results = [{'ngram': x.ngram, "subreddit" : x.subreddit, "count": x.wcount} for x in response_list]
    sorted_results = sorted(results, key = lambda x: x["count"], reverse = True)
    return sorted_results[:5]
    
@app.route("/subredditdata")
def subredditdata():
    return jsonify(children=get_subreddit_data("obama"))

@app.route("/timedata")
def timedata():
    return jsonify(children=get_timeseries_data("obama","politics"))

def getitem(obj, item, default):
    if item not in obj:
        return default
    else:
        return obj[item]

#Randomize the color
r = lambda: random.randint(0,255)
colors = ['#%02X%02X%02X' % (r(),r(),r()) for x in range(100)]


@app.route("/ngram", methods = ['GET'])
def ngramgraph():
    # Grab the inputs arguments from the URL
    # This is automated by the button
    args = request.args

    # Get all the form arguments in the url with defaults
    #default to obama
    query = getitem(args, 'input', 'obama')

    #splittign the query
    splitstr = query.split(",")

    #Fetching data from the query
    XX = []
    YY = []
    lgnd = []
    cnt = 0
    for strs in splitstr:
        ngram = strs.rstrip().lstrip().lower()
        print "ngram = ", ngram
        subreddits = get_subreddit_data(ngram)
        print "subreddits", subreddits
        
        if len(subreddits) > 0:
            mainsr = subreddits[0]['subreddit']        

            lgnd.append("{0}::{1}".format(ngram, mainsr))

            data = get_timeseries_data(ngram, mainsr)
            x = [ k['date'] for k in data]
            y = [k['count'] for k in data]
        else:
            x = Xtime
            y = [0] * len(Xtime)
            lgnd.append(ngram)
        XX.append(x)
        YY.append(y)
        cnt +=1

    #Creating figure box
    fig = figure(title="Trends", width=1000, height=600, x_axis_type="datetime")
    
    #Plot the lines
    for k in range(cnt):
        fig.line(XX[k], YY[k], color=random.choice(colors), legend = lgnd[k], line_width=2)
    
    fig.legend.orientation = "top_left"
        
    # Configure resources to include BokehJS inline in the document.
    # For more details see:
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    script, div = components(fig, INLINE)
    html = render_template(
        'Ngram.html',
        plot_script=script,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources
        )

    return encode_utf8(html)