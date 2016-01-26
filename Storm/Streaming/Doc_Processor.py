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

ReLinks = re.compile('\[\[.*?\]\]') # Regex to trap hashtags

WordsFilter = ["template:","file:"]

DataFrame = namedtuple("WikiPage", "Id, Title, Text, Links, RefBy")

def ExtractLinks(text):
    m = ReLinks.finditer(text)
    links = []
    for k in m:
        link = k.group(0).rstrip(']').lstrip('[')
        if '|' in link:
            splitlink = link.split('|')
            link = splitlink[0]
        if ":" in link:
            splitlink = link.split(':')
            if any([x in splitlink[0].lower() for x in WordsFilter]):
                continue
        links.append(link)
    return links

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
        foundtitle = False
        foundtext = False
        for action, elem in context:
            if action =="end" and "title" in elem.tag:
                title = elem.text.lower()
                foundtitle = True
            if action =="end" and "text" in elem.tag:
                text = elem.text
                foundtext = True
            if foundtitle and foundtext:
                break
        log.debug("Title = " + title)
        if not any([x in title.lower() for x in WordsFilter]):
            result = DataFrame(uuid.uuid4().hex, title, text, ExtractLinks(text), [])
            self.emit(result)

if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        filename='/tmp/docprocessor.log',
        filemode='a',
    )

    DocProcessor().run()