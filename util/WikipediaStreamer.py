# -*- coding: utf-8 -*-
"""
Created on Sat Jan 23 14:48:52 2016

@author: jprawiharjo
"""

from lxml import etree
import kafka

filename = "/media/jprawiharjo/Data1/Downloads/wikisample.xml"

leaders = ["ec2-52-34-178-13.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-35-186-215.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-10-19-240.us-west-2.compute.amazonaws.com:9092",
           "ec2-52-27-157-187.us-west-2.compute.amazonaws.com:9092"]

if __name__ == "__main__" :

    events = ("start", "end")
    context = etree.iterparse( filename, events=events )
    kafkaclient = kafka.KafkaClient(leaders[0])
    kafkaproducer = kafka.SimpleProducer(kafkaclient, async=False)
    
    for action, elem in context:
        #print elem.tag
        if action =="end" and "page" in elem.tag:
            msg = etree.tostring(elem)
            #kafkaproducer.send_messages("WikiTest", msg)
            q = etree.fromstring(msg)
            context2 = etree.iterwalk( q, events=events )
            for action, elem in context:
                if action =="end" and "title" in elem.tag:
                    print elem.text
                #if action =="end" and "text" in elem.tag:
                #    print elem.text
            
            #print "send"
