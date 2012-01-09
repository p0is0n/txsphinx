# -*- coding: utf-8 -*-

import sys

from twisted.internet import reactor
from twisted.python import log
from twisted.internet.address import IPv4Address

from txsphinx.client import SphinxConnectionPool

sphinx = SphinxConnectionPool(IPv4Address('127.0.0.1', 9315), pollsize=2)

# Get sphinx client, api as on original sphinx api. Only method names on first-lower characters. 
client = sphinx.getClient()
client.addQuery('test')
client.runQueries().addCallbacks(log.msg, log.err)

reactor.run()

