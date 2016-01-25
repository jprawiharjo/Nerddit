from __future__ import absolute_import, division
"""
Processor for documents coming out of Spout
"""

import logging
from collections import namedtuple
from lxml import etree
import uuid
from pyleus.storm import SimpleBolt
import re

log = logging.getLogger('doc_processor')

WordsFilter = ["template:"]

DataFrame = namedtuple("WikiPage", "Id, Title, Text, Links")

def ExtractLinks(text):
    return ["a"]

class DocProcessor(SimpleBolt):
    OUTPUT_FIELDS = DataFrame
    events = ("start", "end")

    def process_tuple(self, tup):
        log.debug("new tuple")
        msg = str(tup.values[0])
        xmltree = etree.fromstring(msg)
        context = etree.iterwalk(xmltree, events=self.events )

        title = ""
        text = ""
        txtid = ""
        foundtitle = False
        foundtext = False
        for action, elem in context:
            if action =="end" and "title" in elem.tag:
                title = elem.text
                foundtitle = True
            if action =="end" and "text" in elem.tag:
                text = elem.text
                foundtext = True
            if foundtitle and foundtext:
                break
        log.debug("Title = " + title)
        if not any([x in title.lower() for x in WordsFilter]):
            result = DataFrame(uuid.uuid4().hex, title, text, ExtractLinks(text))
            self.emit(result)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/docprocessor.log',
        filemode='a',
    )

    DocProcessor().run()