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
import collections

from struct import pack, unpack
from types import UnicodeType
from time import time

from twisted.internet.defer import Deferred, fail, TimeoutError, DeferredList
from twisted.internet.protocol import ReconnectingClientFactory, Protocol
from twisted.internet.task import cooperate
from twisted.internet import reactor
from twisted.internet.address import IPv4Address, UNIXAddress
from twisted.python import log

from qbuf.twisted_support import MultiBufferer, MODE_STATEFUL

# known searchd commands
SEARCHD_COMMAND_SEARCH		= 0
SEARCHD_COMMAND_EXCERPT		= 1
SEARCHD_COMMAND_UPDATE		= 2
SEARCHD_COMMAND_KEYWORDS	= 3
SEARCHD_COMMAND_PERSIST		= 4
SEARCHD_COMMAND_STATUS		= 5
SEARCHD_COMMAND_FLUSHATTRS	= 7

# current client-side command implementation versions
VER_COMMAND_SEARCH		= 0x119
VER_COMMAND_EXCERPT		= 0x104
VER_COMMAND_UPDATE		= 0x102
VER_COMMAND_KEYWORDS	= 0x100
VER_COMMAND_STATUS		= 0x100
VER_COMMAND_FLUSHATTRS	= 0x100

# known searchd status codes
SEARCHD_OK				= 0
SEARCHD_ERROR			= 1
SEARCHD_RETRY			= 2
SEARCHD_WARNING			= 3

# known match modes
SPH_MATCH_ALL			= 0
SPH_MATCH_ANY			= 1
SPH_MATCH_PHRASE		= 2
SPH_MATCH_BOOLEAN		= 3
SPH_MATCH_EXTENDED		= 4
SPH_MATCH_FULLSCAN		= 5
SPH_MATCH_EXTENDED2		= 6

# known ranking modes (extended2 mode only)
SPH_RANK_PROXIMITY_BM25	= 0 # default mode, phrase proximity major factor and BM25 minor one
SPH_RANK_BM25			= 1 # statistical mode, BM25 ranking only (faster but worse quality)
SPH_RANK_NONE			= 2 # no ranking, all matches get a weight of 1
SPH_RANK_WORDCOUNT		= 3 # simple word-count weighting, rank is a weighted sum of per-field keyword occurence counts
SPH_RANK_PROXIMITY		= 4
SPH_RANK_MATCHANY		= 5
SPH_RANK_FIELDMASK		= 6
SPH_RANK_SPH04			= 7
SPH_RANK_EXPR			= 8
SPH_RANK_TOTAL			= 9

# known sort modes
SPH_SORT_RELEVANCE		= 0
SPH_SORT_ATTR_DESC		= 1
SPH_SORT_ATTR_ASC		= 2
SPH_SORT_TIME_SEGMENTS	= 3
SPH_SORT_EXTENDED		= 4
SPH_SORT_EXPR			= 5

# known filter types
SPH_FILTER_VALUES		= 0
SPH_FILTER_RANGE		= 1
SPH_FILTER_FLOATRANGE	= 2

# known attribute types
SPH_ATTR_NONE			= 0
SPH_ATTR_INTEGER		= 1
SPH_ATTR_TIMESTAMP		= 2
SPH_ATTR_ORDINAL		= 3
SPH_ATTR_BOOL			= 4
SPH_ATTR_FLOAT			= 5
SPH_ATTR_BIGINT			= 6
SPH_ATTR_STRING			= 7
SPH_ATTR_MULTI			= 0X40000001L
SPH_ATTR_MULTI64		= 0X40000002L

SPH_ATTR_TYPES = (SPH_ATTR_NONE,
				  SPH_ATTR_INTEGER,
				  SPH_ATTR_TIMESTAMP,
				  SPH_ATTR_ORDINAL,
				  SPH_ATTR_BOOL,
				  SPH_ATTR_FLOAT,
				  SPH_ATTR_BIGINT,
				  SPH_ATTR_STRING,
				  SPH_ATTR_MULTI,
				  SPH_ATTR_MULTI64)

# known grouping functions
SPH_GROUPBY_DAY	 		= 0
SPH_GROUPBY_WEEK		= 1
SPH_GROUPBY_MONTH		= 2
SPH_GROUPBY_YEAR		= 3
SPH_GROUPBY_ATTR		= 4
SPH_GROUPBY_ATTRPAIR	= 5

DEBUG = True


class SphinxError(Exception):
	"""Base protocol errors"""


class SphinxProtocol(MultiBufferer):

	mode = MODE_STATEFUL

	def cancelCommands(self, reason):
		"""
		Cancel all the outstanding commands, making them fail with C{reason}.
		"""
		while self._current:
			deferred, command, client = self._current.popleft()
			deferred.errback(reason)

	def timeoutConnection(self):
		"""Timeout connection"""

	def connectionMade(self):
		if DEBUG:
			log.msg('Sphinx protocol connection made', self, self.factory)

		self._current = collections.deque()
		self._appends = self._current.append
		self._lastreq = None

		self.factory.resetDelay()

	def connectionClosed(self, reason):
		"""Connection closed"""

	def connectionLost(self, reason):
		if DEBUG:
			log.msg('Sphinx protocol connection lost', self, self.factory, reason)

		self.cancelCommands(reason)

	def readHeader(self, data):
		version = int(unpack('>L', data)[0])

		if version != 1:
			log.err(ValueError('expected searchd protocol version, got %s' % (
				version)))

			self.transport.loseConnection()

			# Fail, return
			return

		# all ok, send my version
		self.write(pack('>L', 1))
		self.write(pack('>hhII', SEARCHD_COMMAND_PERSIST, 0, 4, 1))

		if DEBUG:
			log.msg('Sphinx protocol connection established', self, self.factory)

		# Update last request time
		self._lastreq = time()

		if self.factory.deferred is not None:
			self.factory.deferred.callback(self)

			# Clean deferred
			self.factory.deferred = None
		else:
			# pool pendings
			self.factory.pool.delPendings(self.factory)

			# Will is reconnect action
			self.factory.pool.clientFree(self)

		# Ok, wait
		return self.readResponse, 8

	def readResponse(self, data):
		# Unpack response
		status, version, length = unpack('>2HL', data)

		try:
			if not self._current:
				raise RuntimeError('Wow! No current request for server response')

			deferred, command, client, request = self._current.popleft()

			if DEBUG:
				log.msg('Sphinx protocol response', command, time() - request, self, self.factory)

			if length <= 0:
				deferred.errback(RuntimeError('received zero-sized searchd response'))

				# Fail response
				return

			if version < client:
				deferred.errback(RuntimeError('searchd command v.%d.%d older than client\'s v.%d.%d, some options might not work' % (
					version >> 8, version & 0xff, client >> 8, client & 0xff)))

				# Fail response
				return

			self.read(length).addCallbacks(self.command, log.err,
				(deferred, command, status))
		except:
			log.err()

	def getInitialState(self):
		return self.readHeader, 4

	def addCurrent(self, *args):
		self._appends(args)

	def status(self):
		if DEBUG:
			log.msg('Sphinx protocol request status', self, self.factory)

		# Update last request time
		self._lastreq = time()

		deferred = Deferred()

		# Request status
		self.write(pack('>2HLL', SEARCHD_COMMAND_STATUS,
			VER_COMMAND_STATUS, 4, 1))

		self.addCurrent(deferred, 'status', VER_COMMAND_STATUS, self._lastreq)

		# Push current
		return deferred

	def updateAttributes(self, index, attrs, values, mva=False):
		if DEBUG:
			log.msg('Sphinx protocol request update-attributes', self, self.factory)

		# Update last request time
		self._lastreq = time()

		deferred = Deferred()

		# Request
		request = [pack('>L', len(index)), index]
		appends = request.append

		appends(pack('>L', len(attrs)))

		for attr in attrs:
			appends(pack('>L', len(attr)) + attr)
			appends(pack('>L', 1 if mva else 0))

		appends(pack('>L', len(values)))

		for docid, entry in values.items():
			appends(pack('>Q', docid))

			for val in entry:
				appends(pack('>L', len(val) if mva else val))

				if mva:
					for vals in val:
						appends(pack('>L', vals))

		request = ''.join(request)

		self.write(pack('>2HL', SEARCHD_COMMAND_UPDATE,
			VER_COMMAND_UPDATE, len(request)) + request)

		self.addCurrent(deferred, 'update-attributes', VER_COMMAND_UPDATE, self._lastreq)

		# Push current
		return deferred

	def runQueries(self, queries):
		if DEBUG:
			log.msg('Sphinx protocol request run-queries', self, self.factory)

		# Update last request time
		self._lastreq = time()

		deferred = Deferred()

		# Request queries
		request = ''.join(queries)

		self.write(pack('>HHLLL', SEARCHD_COMMAND_SEARCH,
			VER_COMMAND_SEARCH, len(request) + 8, 0, len(queries)) + request)

		self.addCurrent(deferred, 'run-queries', VER_COMMAND_SEARCH, self._lastreq)

		# Push current
		return deferred

	def command(self, data, deferred, command, status):
		if DEBUG:
			log.msg('Sphinx protocol command', command, self, self.factory)

		if status != SEARCHD_OK:
			deferred.errback(ValueError('searchd error'))

			# Fail
			return

		if command == 'status':
			try:
				rows = []
				data = data[8:]

				while data:
					length = unpack('>L', data[:4])[0]

					if not length > 0:
						data = data[4:]

						# Force iteration
						continue

					key, data = data[4:length + 4], data[length + 4:]

					if not key:
						raise ValueError('Wow! Empty key in "%s" command' % (
							command))

					length = unpack('>L', data[:4])[0]

					if length >= 0:
						value, data = data[4:length + 4], data[length + 4:]

						# Add value to rows list
						rows.append((key,
							value))

				deferred.callback(rows)
			except Exception, exception:
				deferred.errback(exception)

		elif command == 'run-queries':
			try:
				rows = []

				# Cursor
				cursor = 0
				buffer = len(data)

				while cursor < buffer:
					row = dict()

					# Add
					rows.append(row)

					row['status'] = unpack('>L',
						data[cursor:cursor + 4])[0]

					# Update
					cursor += 4

					if row['status'] != SEARCHD_OK:
						length = unpack('>L', data[cursor:cursor + 4])[0]
						cursor += 4

						message = data[cursor:cursor + length]
						cursor += length

						if row['status'] == SEARCHD_WARNING:
							row['warning'] = message
						else:
							row['error'] = message

							# Ok, continue to next iteration
							continue

					row.update(matches=[], fields=[],
						attrs=[])

					# Short
					append = row['fields'].append
					counts = unpack('>L', data[cursor:cursor + 4])[0]

					# Update
					cursor += 4

					for i in xrange(0, counts):
						length = unpack('>L', data[cursor:cursor + 4])[0]
						cursor += 4

						# Add
						append(data[cursor:cursor + length])
						cursor += length

					# Short
					append = row['attrs'].append
					counts = unpack('>L', data[cursor:cursor + 4])[0]

					# Update
					cursor += 4

					for i in xrange(0, counts):
						length = unpack('>L', data[cursor:cursor + 4])[0]
						cursor += 4

						k = data[cursor:cursor + length]
						cursor += length

						t = unpack('>L', data[cursor:cursor + 4])[0]
						cursor += 4

						# Add
						append([k, t])

					# read match count
					count, id64 = unpack('>2L', data[cursor:cursor + 8])
					cursor += 8

					# Short
					append = row['matches'].append

					for i in xrange(0, count):
						if id64:
							doc, weight = unpack('>QL', data[cursor:cursor + 12])
							cursor += 12
						else:
							doc, weight = unpack('>2L', data[cursor:cursor + 8])
							cursor += 8

						match = dict(id=doc, weight=weight, attrs=dict())

						for i, (attr, type) in enumerate(row['attrs']):
							if type == SPH_ATTR_FLOAT:
								match['attrs'][attr] = unpack('>f', data[cursor:cursor + 4])[0]
							elif type == SPH_ATTR_BIGINT:
								match['attrs'][attr] = unpack('>q', data[cursor:cursor + 8])[0]
								cursor += 4
							elif type == SPH_ATTR_STRING:
								count = unpack('>L', data[cursor:cursor + 4])[0] / 2
								cursor += 4

								if count > 0:
									match['attrs'][attr] = data[cursor:cursor + count]
								else:
									match['attrs'][attr] = ''

								cursor += count - 4
							elif type == SPH_ATTR_MULTI:
								match['attrs'][attr] = []

								count = unpack('>L', data[cursor:cursor + 4])[0]
								cursor += 4

								for n in xrange(0, count, 1):
									match['attrs'][attr].append(unpack('>L', data[cursor:cursor + 4])[0])
									cursor += 4

								# Update
								cursor -= 4
							elif type == SPH_ATTR_MULTI64:
								match['attrs'][attr] = []

								count = unpack('>L', data[cursor:cursor + 4])[0] / 2
								cursor += 4

								for n in xrange(0, count, 1):
									match['attrs'][attr].append(unpack('>q', data[cursor:cursor + 8])[0])
									cursor += 8

								# Update
								cursor -= 4
							else:
								match['attrs'][attr] = unpack('>L', data[cursor:cursor + 4])[0]

							# Strip data
							cursor += 4

						# Add
						append(match)

					row['total'], row['total_found'], row['time'], count = unpack('>4L', data[cursor:cursor + 16])

					# Convert time to seconds
					row['time'] = '%.3f' % (row['time'] / 1000.0)

					# Update
					cursor += 16

					row['words'] = []

					# Short
					append = row['words'].append

					for i in xrange(0, count):
						length = unpack('>L', data[cursor:cursor + 4])[0]
						cursor += 4

						word = data[cursor:cursor + length]
						cursor += length

						docs, hits = unpack('>2L', data[cursor:cursor + 8])
						cursor += 8

						append(dict(word=word, docs=docs, hits=hits))

				deferred.callback(rows)
			except Exception, exception:
				deferred.errback(exception)

		elif command == 'update-attributes':
			try:
				deferred.callback(unpack('>L', data[0:4])[0])
			except Exception, exception:
				deferred.errback(exception)

		else:
			deferred.errback(ValueError('Unknown command "%s"' % (
				command)))


class SphinxFactory(ReconnectingClientFactory):

	noisy = DEBUG
	maxDelay = 15

	protocol = SphinxProtocol
	protocolInstance = None

	def __init__(self):
		self.deferred = Deferred()

	def clientConnectionLost(self, connector, reason):
		"""
		Notify the pool that we've lost our connection.
		"""
		if self.protocolInstance is not None:
			if self.protocolInstance._lastreq and ((time() - self.protocolInstance._lastreq) >= 100):
				self.stopTrying()

		if self.continueTrying:
			ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

			# pool pendings
			self.pool.addPendings(self)
		else:
			if DEBUG:
				log.msg('Sphinx factory stop', self)

			if self.deferred:
				self.deferred.errback(reason)

				# Clean deferred
				self.deferred = None
			else:
				# pool pendings
				self.pool.delPendings(self)

		if self.protocolInstance is not None:
			self.pool.clientGone(self.protocolInstance)

		# Clean
		self.protocolInstance = None

	def clientConnectionFailed(self, connector, reason):
		"""
		Notify the pool that we're unable to connect
		"""
		if self.protocolInstance is not None:
			if self.protocolInstance._lastreq and ((time() - self.protocolInstance._lastreq) >= 100):
				self.stopTrying()

		if self.continueTrying:
			ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

			# pool pendings
			self.pool.addPendings(self)
		else:
			if DEBUG:
				log.msg('Sphinx factory stop', self)

			if self.deferred:
				self.deferred.errback(reason)

				# Clean deferred
				self.deferred = None
			else:
				# pool pendings
				self.pool.delPendings(self)

		if self.protocolInstance is not None:
			self.pool.clientGone(self.protocolInstance)

		# Clean
		self.protocolInstance = None

	def buildProtocol(self, addr):
		"""
		Attach the C{self.pool} to the protocol so it can tell it, when we've connected.
		"""
		if self.protocolInstance is not None:
			self.pool.clientGone(self.protocolInstance)

		self.protocolInstance = self.protocol()
		self.protocolInstance.factory = self

		return self.protocolInstance

	def __str__(self):
		return '<SphinxFactory %02d instance at 0x%X>' % (
			self.counter, id(self))

	__repr__ = __str__


class SphinxConnectionPool(object):

	def __init__(self, address, poolsize=1):
		assert isinstance(address,
			(UNIXAddress, IPv4Address)), 'Address type [%s] not supported!' % type(address)

		self._address = address
		self._counter = 0

		self._busyClients = set([])
		self._freeClients = set([])

		self._pendingsList = set([])

		self._commands = []
		self._poolsize = poolsize

	def addPendings(self, factory):
		if not factory in self._pendingsList:
			# Add factory to pendings list
			self._pendingsList.add(factory)

	def delPendings(self, factory):
		if factory in self._pendingsList:
			# Delete factory from pendings list
			self._pendingsList.remove(factory)

	def clientConnect(self):
		"""
		Create a new client connection.

		@return: A L{Deferred} that fires with the L{IProtocol} instance.
		"""
		self._counter += 1

		factory = SphinxFactory()
		factory.pool = self
		factory.counter = self._counter

		self.addPendings(factory)

		if isinstance(self._address, IPv4Address):
			reactor.connectTCP(self._address.host, self._address.port,
				factory=factory, timeout=5
			)
		elif isinstance(self._address, UNIXAddress):
			reactor.connectUNIX(self._address.name,
				factory=factory, timeout=5
			)
		else:
			raise RuntimeError('Sphinx unknown address type!')

		def _cbConnected(client):
			self.delPendings(factory)

			if client is not None:
				self.clientFree(client)

		def _ebConnected(reason):
			self.delPendings(factory)

		deferred = factory.deferred
		deferred.addCallbacks(_cbConnected, _ebConnected)

		# Wait for new connection
		return deferred

	def pendingCommand(self, method, args, kwargs):
		if DEBUG:
			log.msg('Sphinx pool pending', method, args, kwargs)

		deferred = Deferred()

		if deferred:
			self._commands.append((deferred, method, args, kwargs))

		# Wait in this deferred
		return deferred

	def performRequestOnClient(self, client, method, args, kwargs):
		if DEBUG:
			log.msg('Sphinx pool use connection', client, client.factory)

		self.clientBusy(client)

		# Request
		request = getattr(client, method, None)

		if request is not None:
			def _cbRequest(result, client=client):
				self.clientFree(client)

				# Got result on next callback
				return result

			return request(*args, **kwargs).addBoth(_cbRequest)

		# Fail, method not exists
		self.clientFree(client)

		return fail(AttributeError(
			client, method))

	def performRequest(self, method, *args, **kwargs):
		"""
		Select an available client and perform the given request on it.

		@parma method: The method to call on the client.

		@parma args: Any positional arguments that should be passed to C{command}.
		@param kwargs: Any keyword arguments that should be passed to C{command}.

		@return: A L{Deferred} that fires with the result of the given command.
		"""
		if len(self._freeClients):
			return self.performRequestOnClient(
				self._freeClients.pop(), method, args, kwargs)

		if not (len(self._busyClients) + len(self._pendingsList)) >= self._poolsize:
			self.clientConnect()

		# Wait for request
		return self.pendingCommand(method, args, kwargs)

	def clientGone(self, client):
		"""
		Notify that the given client is to be removed from the pool completely.

		@param client: An instance of a L{Protocol}.
		"""
		if DEBUG:
			log.msg('Sphinx pool gone connection', client, client.factory)

		if client in self._busyClients:
			self._busyClients.remove(client)

		if client in self._freeClients:
			self._freeClients.remove(client)

	def clientBusy(self, client):
		"""
		Notify that the given client is being used to complete a request.

		@param client: An instance of a L{Protocol}.
		"""
		if DEBUG:
			log.msg('Sphinx pool busy connection', client, client.factory)

		if client in self._freeClients:
			self._freeClients.remove(client)

		self._busyClients.add(client)

	def clientFree(self, client):
		"""
		Notify that the given client is free to handle more requests.

		@param client: An instance of a L{Protocol}.
		"""
		if DEBUG:
			log.msg('Sphinx pool free connection', client, client.factory)

		if client in self._busyClients:
			self._busyClients.remove(client)

		self._freeClients.add(client)

		if len(self._commands):
			(deferred, method,
				args, kwargs) = self._commands.pop(0)

			# With chain deferred
			self.performRequest(method, *args, **kwargs).chainDeferred(deferred)

	def getClient(self):
		return SphinxClient(self)


class SphinxClient(object):

	def __init__(self, pool):
		self.performRequest = pool.performRequest

		self._queries = []
		self._offset		= 0								# how much records to seek from result-set start (default is 0)
		self._limit			= 20							# how much records to return from result-set starting at offset (default is 20)
		self._mode			= SPH_MATCH_ALL					# query matching mode (default is SPH_MATCH_ALL)
		self._weights		= []							# per-field weights (default is 1 for all fields)
		self._sort			= SPH_SORT_RELEVANCE			# match sorting mode (default is SPH_SORT_RELEVANCE)
		self._sortby		= ''							# attribute to sort by (defualt is "")
		self._ranker		= SPH_RANK_PROXIMITY_BM25		# ranking mode
		self._rankexpr		= ''							# ranking expression for SPH_RANK_EXPR
		self._min_id		= 0								# min ID to match (default is 0)
		self._max_id		= 0								# max ID to match (default is UINT_MAX)
		self._filters		= []							# search filters
		self._groupby		= ''							# group-by attribute name
		self._groupfunc		= SPH_GROUPBY_DAY				# group-by function (to pre-process group-by attribute value with)
		self._groupsort		= '@group desc'					# group-by sorting clause (to sort groups in result set with)
		self._groupdistinct	= ''							# group-by count-distinct attribute
		self._maxmatches	= 1000							# max matches to retrieve
		self._cutoff		= 0								# cutoff to stop searching at
		self._retrycount	= 0								# distributed retry count
		self._retrydelay	= 0								# distributed retry delay
		self._anchor		= {}							# geographical anchor point
		self._indexweights	= {}							# per-index weights
		self._ranker		= SPH_RANK_PROXIMITY_BM25		# ranking mode
		self._rankexpr		= ''							# ranking expression for SPH_RANK_EXPR
		self._maxquerytime	= 0								# max query time, milliseconds (default is 0, do not limit)
		self._timeout 		= 1.0							# connection timeout
		self._fieldweights	= {}							# per-field-name weights
		self._overrides		= {}							# per-query attribute values overrides
		self._select		= '*'							# select-list (attributes or expressions, with optional aliases)

	def setLimits(self, offset, limit, maxmatches=0, cutoff=0):
		"""
		Set offset and count into result set, and optionally set max-matches and cutoff limits.
		"""
		assert (type(offset) in [int,long] and 0 <= offset < 16777216)
		assert (type(limit) in [int,long] and 0 < limit < 16777216)
		assert(maxmatches >= 0)

		self._offset = offset
		self._limit = limit

		if maxmatches > 0:
			self._maxmatches = maxmatches

		if cutoff >= 0:
			self._cutoff = cutoff

	def setMaxQueryTime(self, maxquerytime):
		"""
		Set maximum query time, in milliseconds, per-index. 0 means 'do not limit'.
		"""
		assert(isinstance(maxquerytime, int) and maxquerytime > 0)

		self._maxquerytime = maxquerytime

	def setMatchMode(self, mode):
		"""
		Set matching mode.
		"""
		assert(mode in [SPH_MATCH_ALL, SPH_MATCH_ANY, SPH_MATCH_PHRASE, SPH_MATCH_BOOLEAN, SPH_MATCH_EXTENDED, SPH_MATCH_FULLSCAN, SPH_MATCH_EXTENDED2])

		self._mode = mode

	def setRankingMode(self, ranker, rankexpr=''):
		"""
		Set ranking mode.
		"""
		assert(ranker >= 0 and ranker < SPH_RANK_TOTAL)

		self._ranker = ranker
		self._rankexpr = rankexpr

	def setSortMode(self, mode, clause=''):
		"""
		Set sorting mode.
		"""
		assert (mode in [SPH_SORT_RELEVANCE, SPH_SORT_ATTR_DESC, SPH_SORT_ATTR_ASC, SPH_SORT_TIME_SEGMENTS, SPH_SORT_EXTENDED, SPH_SORT_EXPR])
		assert (isinstance ( clause, str ))

		self._sort = mode
		self._sortby = clause

	def setFieldWeights(self, weights):
		"""
		Bind per-field weights by name; expects (name,field_weight) dictionary as argument.
		"""
		assert(isinstance(weights, dict))

		for key, val in weights.items():
			assert(isinstance(key, str))

		self._fieldweights = weights

	def setIndexWeights(self, weights):
		"""
		Bind per-index weights by name; expects (name,index_weight) dictionary as argument.
		"""
		assert(isinstance(weights,dict))

		for key, val in weights.items():
			assert(isinstance(key, str))

		self._indexweights = weights

	def setIDRange(self, minid, maxid):
		"""
		Set IDs range to match.
		Only match records if document ID is beetwen $min and $max (inclusive).
		"""
		assert(isinstance(minid, (int, long)))
		assert(isinstance(maxid, (int, long)))
		assert(minid <= maxid)

		self._min_id = minid
		self._max_id = maxid

	def setFilter(self, attribute, values, exclude=0):
		"""
		Set values set filter.
		Only match records where 'attribute' value is in given 'values' set.
		"""
		assert(isinstance(attribute, str))
		assert iter(values)

		self._filters.append({'type': SPH_FILTER_VALUES, 'attr': attribute, 'exclude': exclude, 'values': values})

	def setFilterRange(self, attribute, min_, max_, exclude=0):
		"""
		Set range filter.
		Only match records if 'attribute' value is beetwen 'min_' and 'max_' (inclusive).
		"""
		assert(isinstance(attribute, str))
		assert(min_<=max_)

		self._filters.append({'type': SPH_FILTER_RANGE, 'attr': attribute, 'exclude': exclude, 'min': min_, 'max': max_ })

	def setFilterFloatRange(self, attribute, min_, max_, exclude=0):
		assert(isinstance(attribute, str))
		assert(isinstance(min_, float))
		assert(isinstance(max_, float))
		assert(min_ <= max_)

		self._filters.append({'type': SPH_FILTER_FLOATRANGE, 'attr': attribute, 'exclude': exclude, 'min': min_, 'max': max_})

	def setGroupBy(self, attribute, func, groupsort='@group desc'):
		"""
		Set grouping attribute and function.
		"""
		assert(isinstance(attribute, str))
		assert(func in [SPH_GROUPBY_DAY, SPH_GROUPBY_WEEK, SPH_GROUPBY_MONTH, SPH_GROUPBY_YEAR, SPH_GROUPBY_ATTR, SPH_GROUPBY_ATTRPAIR] )
		assert(isinstance(groupsort, str))

		self._groupby = attribute
		self._groupfunc = func
		self._groupsort = groupsort

	def setGroupDistinct(self, attribute):
		assert(isinstance(attribute, str))

		self._groupdistinct = attribute

	def setRetries(self, count, delay=0):
		assert(isinstance(count, int) and count >= 0)
		assert(isinstance(delay, int) and delay >= 0)

		self._retrycount = count
		self._retrydelay = delay

	def setOverride(self, name, type, values):
		assert(isinstance(name, str))
		assert(type in SPH_ATTR_TYPES)
		assert(isinstance(values, dict))

		self._overrides[name] = {'name': name, 'type': type, 'values': values}

	def setSelect(self, select):
		assert(isinstance(select, str))

		self._select = select

	def resetOverrides(self):
		self._overrides = {}

	def resetFilters(self):
		"""
		Clear all filters (for multi-queries).
		"""
		self._filters = []
		self._anchor = {}

	def resetGroupBy(self):
		"""
		Clear groupby settings (for multi-queries).
		"""
		self._groupby = ''
		self._groupfunc = SPH_GROUPBY_DAY
		self._groupsort = '@group desc'
		self._groupdistinct = ''

	def status(self):
		"""
		Get the status
		"""
		return self.performRequest('status')

	def updateAttributes(self, index, attrs, values, mva=False):
		"""
		Update given attribute values on given documents in given indexes.
		Returns amount of updated documents (0 or more) on success, or -1 on failure.

		'attrs' must be a list of strings.
		'values' must be a dict with int key (document ID) and list of int values (new attribute values).
		optional boolean parameter 'mva' points that there is update of MVA attributes.
		In this case the 'values' must be a dict with int key (document ID) and list of lists of int values
		(new MVA attribute values).

		Example:
			res = cl.updateAttributes ( 'test1', [ 'group_id', 'date_added' ], { 2:[123,1000000000], 4:[456,1234567890] } )
		"""
		assert (isinstance(index, str))
		assert (isinstance(attrs, list))
		assert (isinstance(values, dict))

		for attr in attrs:
			assert (isinstance (attr, str ))

		for docid, entry in values.items():
			assert (isinstance(entry, list))
			assert (len(attrs) == len(entry))

			for val in entry:
				if mva:
					assert (isinstance(val, list))

		return self.performRequest('updateAttributes', index, attrs, values, mva)

	def addQuery(self, query, index='*', comment=''):
		"""
		Add query to batch.
		"""
		try:
			request = [pack('>4L', self._offset, self._limit, self._mode, self._ranker)]
			appends = request.append

			if self._ranker == SPH_RANK_EXPR:
				appends(pack('>L', len(self._rankexpr)))
				appends(self._rankexpr)

			appends(pack('>L', self._sort))
			appends(pack('>L', len(self._sortby)))
			appends(self._sortby)

			if isinstance(query, UnicodeType):
				query = query.encode('utf-8')

			assert(isinstance(query, str))

			appends(pack('>L', len(query)))
			appends(query)

			if self._weights:
				appends(pack('>L', len(self._weights)))

				for weight in self._weights:
					appends(pack('>L', weight))
			else:
				appends('\x00\x00\x00\x00')

			appends(pack('>L', len(index)))
			appends(index)
			appends(pack('>L', 1)) # id64 range marker
			appends(pack('>Q', self._min_id))
			appends(pack('>Q', self._max_id))

			if self._filters:
				appends(pack('>L', len(self._filters)))

				for f in self._filters:
					# Short type
					ftype = f['type']

					appends(pack('>L', len(f['attr'])) + f['attr'])
					appends(pack('>L', ftype))

					if ftype == SPH_FILTER_VALUES:
						appends(pack('>L', len(f['values'])))

						for val in f['values']:
							appends(pack ('>q', val))

					elif ftype == SPH_FILTER_RANGE:
						appends(pack('>2q', f['min'], f['max']))

					elif ftype == SPH_FILTER_FLOATRANGE:
						appends(pack('>2f', f['min'], f['max']))

					appends(pack('>L', f['exclude']))
			else:
				appends('\x00\x00\x00\x00')

			# group-by, max-matches, group-sort
			appends(pack('>2L', self._groupfunc, len(self._groupby)))
			appends(self._groupby)
			appends(pack('>2L', self._maxmatches, len(self._groupsort)))
			appends(self._groupsort)
			appends(pack('>LLL', self._cutoff, self._retrycount, self._retrydelay))
			appends(pack('>L', len(self._groupdistinct)))
			appends(self._groupdistinct)

			# anchor point
			if self._anchor:
				"""
				@TODO add support anchor points
				"""
			else:
				appends('\x00\x00\x00\x00')

			# per-index weights
			if self._indexweights:
				appends(pack('>L', len(self._indexweights)))

				for i, weight in self._indexweights.items():
					appends(pack('>L', len(i)) + i + pack('>L', weight))
			else:
				appends('\x00\x00\x00\x00')

			# max query time
			appends(pack('>L', self._maxquerytime))

			# per-field weights
			if self._fieldweights:
				appends(pack('>L', len(self._fieldweights)))

				for i, weight in self._fieldweights.items():
					appends(pack('>L', len(i)) + i + pack ('>L', weight))
			else:
				appends('\x00\x00\x00\x00')

			# comment
			appends(pack('>L', len(comment)) + comment)

			# attribute overrides
			if self._overrides:
				appends(pack('>L', len(self._overrides)))

				for v in self._overrides.values():
					request.extend((pack('>L', len(v['name'])), v['name']))
					request.append(pack('>LL', v['type'], len(v['values'])))

					for id, value in v['values'].iteritems():
						appends(pack('>Q', id))

						if v['type'] == SPH_ATTR_FLOAT:
							appends(pack('>f', value))
						elif v['type'] == SPH_ATTR_BIGINT:
							appends(pack('>q', value))
						else:
							appends(pack('>l', value))
			else:
				appends('\x00\x00\x00\x00')

			# select-list
			appends(pack('>L', len(self._select)))
			appends(self._select)

			# send query, get response
			self._queries.append(''.join(request))
		except:
			log.err()

	def runQueries(self):
		if not self._queries:
			return fail(ValueError('no queries defined, issue addQuery() first'))

		# Copy queries
		queries, self._queries = (
			self._queries, [])

		return self.performRequest('runQueries', queries)

	def __getattr__(self, name):
		return getattr(self, name[0].lower() + name[1:])

