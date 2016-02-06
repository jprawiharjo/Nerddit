# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 15:39:01 2016

@author: jprawiharjo
"""

from flask import Flask
from cassandra.cluster import Cluster

app = Flask(__name__)
cluster = Cluster(["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                    "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                    "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                    'ec2-52-10-19-240.us-west-2.compute.amazonaws.com',
                    "ec2-54-69-144-162.us-west-2.compute.amazonaws.com"])


from app import views
