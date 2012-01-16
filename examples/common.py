# -*- coding: utf-8 -*-

# Copyright (c) knigobaza.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

from twisted.internet import reactor
from twisted.python import log
from twisted.internet.address import IPv4Address

from txsphinx.client import SphinxConnectionPool

sphinx = SphinxConnectionPool(IPv4Address('TCP', '127.0.0.1', port=9315), poolsize=2)

# Get sphinx client, api as on original sphinx api. Only method names on first-lower characters. 
client = sphinx.getClient()
client.addQuery('test')
client.runQueries().addCallbacks(log.msg, log.err)

reactor.run()

