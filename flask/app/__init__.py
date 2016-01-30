# -*- coding: utf-8 -*-
"""
Created on Thu Jan 21 15:39:01 2016

@author: jprawiharjo
"""

from flask import Flask
from flask_bootstrap import Bootstrap

app = Flask(__name__)
from app import views

#from .frontend import frontend
#from .nav import nav
#Bootstrap(app)
#app.register_blueprint(frontend)
#
## Because we're security-conscious developers, we also hard-code disabling
## the CDN support (this might become a default in later versions):
#app.config['BOOTSTRAP_SERVE_LOCAL'] = True
#
## We initialize the navigation as well
#nav.init_app(app)