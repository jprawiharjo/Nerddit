# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 15:39:01 2016

@author: jprawiharjo
"""

from flask import Flask
from flask_bootstrap import Bootstrap
#from .frontend import frontend

app = Flask(__name__)
Bootstrap(app)
from app import views
