# -*- coding: utf-8 -*-
"""
Created on Thu Feb  4 15:04:32 2016

@author: jprawiharjo
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Feb  4 09:33:27 2016

@author: jprawiharjo
"""
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
import math

import networkx as nx

keyspace = 'reddit'
tablename1 = "redditgraphmap"

cluster = Cluster(["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
        "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
        "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
        'ec2-52-10-19-240.us-west-2.compute.amazonaws.com',
        "ec2-54-69-144-162.us-west-2.compute.amazonaws.com"])

session = cluster.connect(keyspace)
session.default_timeout = 600
session.default_consistency_level = ConsistencyLevel.QUORUM

years = range(2008,2016)

cutoff = 100

for year in years:

    res = session.execute("SELECT * FROM redditgraphmap2 WHERE year = %s limit 100000" %(year,))
    
    G=nx.Graph()
    
    ww = []
    response_list = []
    for row in res:
        if row.node1 =="reddit.com":
            continue
        response_list.append(row)
        weights = row.node2.values()
        thres = 0.2 * max(weights)
        print max(weights), thres
        for k,v in row.node2.iteritems():
            if k == "reddit.com":
                continue
            if v > thres:
                ww.append(v)
                G.add_edge(row.node1, k, weight=v)
    
    nx.draw(G)
    print max(ww), min(ww)
    nx.write_gexf(G, 'Rgraph{0}.gexf'.format(year))
    #SG=nx.Graph( [ (u,v,d) for u,v,d in G.edges(data=True) if d['weight']>cutoff] )
    outdeg = G.degree()
    to_keep = [n for n in outdeg if outdeg[n] < cutoff]
    SG = G.subgraph(to_keep)

    outdeg = SG.degree()
    to_keep = [n for n in outdeg if outdeg[n] > 0]
    SG = SG.subgraph(to_keep)

    
    nx.write_gexf(SG, 'Rsubgraph{0}.gexf'.format(year))

session.shutdown()

