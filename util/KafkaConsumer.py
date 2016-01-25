from __future__ import absolute_import
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 23 16:59:31 2016

@author: jprawiharjo
"""

# -*- coding: utf-8 -*-
"""
Spout for Kafka Consumer
"""

import json
import os
import kafka #v0.9.5 at writing time
import time
import logging
from lxml import etree
from collections import namedtuple
#import requests.packages.urllib3
#requests.packages.urllib3.disable_warnings()

leaders = ["ec2-52-34-178-13.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-35-186-215.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-10-19-240.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-27-157-187.us-west-2.compute.amazonaws.com:9092"]


log = logging.getLogger('test_kafka_spout')


class KafkaConsumer(object):
    OPTIONS = ["base_log"]
    OUTPUT_FIELDS = ["words"]

    def initialize(self):
        cluster = kafka.KafkaClient(leaders[0])
        self.consumer = kafka.SimpleConsumer(cluster,"default_group","WikiTest", buffer_size=16384, max_buffer_size=(10*1024*1024))
        self.consumer.seek(0)
        self.counter = 0

    def getData(self):
        cnt = 0
        for raw in self.consumer:
            if len(raw) > 0:
                msg = raw.message.value
                q = etree.fromstring(msg)
                context = etree.iterwalk( q, events=events )
                for action, elem in context:
                    if action =="end" and "title" in elem.tag:
                        title = elem.text
                    if action =="end" and "text" in elem.tag:
                        text = elem.text
                cnt +=1
                print cnt
                #log.critical(msg['text'])
          

if __name__ == '__main__':
    C = KafkaConsumer()
    C.initialize()
    C.getData()