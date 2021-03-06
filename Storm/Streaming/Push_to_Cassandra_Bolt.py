# -*- coding: utf-8 -*-
"""
Created on Sat Jan 23 13:37:20 2016

@author: jprawiharjo
"""

from cassandra.cluster import Cluster
import cassandra
from collections import namedtuple
from pyleus.storm import SimpleBolt
from Streaming.Doc_Processor import DataFrame
import logging
log = logging.getLogger('cassandra_bolt')

# create CassandraCluster
CassandraCluster = Cluster(["ec2-52-27-157-187.us-west-2.compute.amazonaws.com",
                   "ec2-52-34-178-13.us-west-2.compute.amazonaws.com",
                   "ec2-52-35-186-215.us-west-2.compute.amazonaws.com",
                   'ec2-52-10-19-240.us-west-2.compute.amazonaws.com'])

keyspace = 'wikidata'
tablename = "titlelinks"

class Push_to_Cassandra(SimpleBolt):
    def initialize(self):
        self.session = CassandraCluster.connect(keyspace)
        self.session.default_consistency_level = cassandra.ConsistencyLevel.ALL
        #self.session.encoder.mapping[tuple] = self.session.encoder.cql_encode_set_collection

        queryAddNew1 = "INSERT INTO {} (id, title, linksto) VALUES (?, ?, ?) IF NOT EXISTS".format(tablename)
        self.preparedAddNew1 = self.session.prepare(queryAddNew1)

        queryAddNew2 = "INSERT INTO {} (id, title, linksto, referredby) VALUES (?, ?, ?, ?) IF NOT EXISTS".format(tablename)
        self.preparedAddNew2 = self.session.prepare(queryAddNew2)

        queryUpdateReferredbyTitle = "UPDATE {} SET id = ?, linksto = ? WHERE title = ? IF EXISTS".format(tablename)
        self.preparedReferredbyTitle = self.session.prepare(queryUpdateReferredbyTitle)

        queryUpdateReferredbyOnly = "UPDATE {} SET referredby = referredby + ? WHERE title = ? IF EXISTS".format(tablename)
        self.preparedReferredbyOnly = self.session.prepare(queryUpdateReferredbyOnly)

        queryAddNewReferredBy = "INSERT INTO {} (title, referredby) VALUES (?, ?) IF NOT EXISTS".format(tablename)
        self.preparedAddNewReferredBy = self.session.prepare(queryAddNewReferredBy)

        self.bulk_data = []

        log.debug("Initialized")
        
    def process_tick(self):
        log.debug("Process Tick")
        log.debug(len(self.bulk_data))

        linkage = {}
        for row in self.bulk_data:
            if len(row.Links) > 0:
                log.debug('Processing Links')
                for link in row.Links:
                    if link in linkage.keys():
                        linkage[link].add(row.Title)
                    else:
                        linkage[link] = set([row.Title])

        for row in self.bulk_data:
            log.debug(row.Title)
            
            if row.Title in linkage.keys():
                bound1 = self.preparedAddNew2.bind((str(row.Id), str(row.Title), row.Links, linkage[row.Title]))
            else:
                bound1 = self.preparedAddNew1.bind((str(row.Id), str(row.Title), row.Links))
            
            res = self.session.execute(bound1)
            res = res.current_rows[0].applied
            #log.debug("Insertion Result = " + str(res))

            if not(res):
                bound2 = self.preparedReferredbyTitle.bind((str(row.Id), row.Links, str(row.Title)))
                self.session.execute_async(bound2)
                
        #Inserting into database
        for k,v in linkage.iteritems():
            log.debug(k)
            log.debug(v)
            bound3 = self.preparedReferredbyOnly.bind((v, k))
            res = self.session.execute(bound3)
            res = res.current_rows[0].applied
            if not(res):
                bound4 = self.preparedAddNewReferredBy.bind((k, v))
                res = self.session.execute_async(bound4)

        self.bulk_data = []
        
    def process_tuple(self, tup):
        result = DataFrame(*tup.values)
        self.bulk_data.append(result)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/cassandra_bolt.log',
        filemode='a',
    )

    Push_to_Cassandra().run()