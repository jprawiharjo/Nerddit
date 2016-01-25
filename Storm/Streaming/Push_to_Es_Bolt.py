# -*- coding: utf-8 -*-
"""
Created on Sat Jan 23 13:37:20 2016

@author: jprawiharjo
"""

from elasticsearch import Elasticsearch
from collections import namedtuple
from pyleus.storm import SimpleBolt
from Streaming.Doc_Processor import DataFrame
import logging
log = logging.getLogger('es_bolt')

# create ES client, create index
es = Elasticsearch(hosts = ["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                            "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                            "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                            'ec2-52-10-19-240.us-west-2.compute.amazonaws.com'])

INDEX_NAME = 'wikitest'
TYPE_NAME = 'text'

class Push_to_Es(SimpleBolt):
    def initialize(self):
        self.bulk_data = []
        log.debug("Initialized")
        
    def process_tick(self):
        log.debug("Process Tick")
        if len(self.bulk_data) > 0:
            es.bulk(index = INDEX_NAME, body = self.bulk_data, refresh = True)
            #log.debug(res)
        log.debug(len(self.bulk_data))
        self.bulk_data = []
        
    def process_tuple(self, tup):
        result = DataFrame(*tup.values)
        log.debug(result.Title)
        data_dict = {'Id': result.Id, 'Title': result.Title, 'Text': result.Text}

        op_dict = {
            "index": {
            	"_index": INDEX_NAME, 
            	"_type": TYPE_NAME, 
            	"_id": data_dict['Id']
            }
        }

        self.bulk_data.append(op_dict)
        self.bulk_data.append(data_dict)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/es_bolt.log',
        filemode='a',
    )

    Push_to_Es().run()