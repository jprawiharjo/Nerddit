# -*- coding: utf-8 -*-
"""
Spout for Kafka Consumer
"""

from __future__ import absolute_import
import json
import os
import kafka #v0.9.5 at writing time
import time
import logging
from pyleus.storm import Spout
from collections import namedtuple
#import requests.packages.urllib3
#requests.packages.urllib3.disable_warnings()

leaders = ["ec2-52-34-178-13.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-35-186-215.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-10-19-240.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-27-157-187.us-west-2.compute.amazonaws.com:9092"]


log = logging.getLogger('test_kafka_spout')


class KafkaConsumer(Spout):
    OPTIONS = ["base_log"]
    OUTPUT_FIELDS = ["words"]

    def initialize(self):
        cluster = kafka.KafkaClient(leaders[0])
        self.consumer = kafka.SimpleConsumer(cluster,"default_group","WikiTest",buffer_size=8192, max_buffer_size=(10*1024*1024))
        self.consumer.seek(0)
        self.counter = 0
        log.debug("Starting Kafka Consumer")

    def next_tuple(self):
        log.debug("Fetching messages from consumer")
        for raw in self.consumer:
            try:
                msg = str(raw.message.value)
                #log.debug
                if len(msg) > 0:
                    self.emit((msg,))
                    self.counter += 1
                    log.debug(self.counter)
            except:
                log.debug(error)
          

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/docspout.log',
        filemode='a',
    )

    KafkaConsumer().run()