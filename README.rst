=======
TxSphinx
=======
:Info: See `the sphinx site <http://sphinxsearch.com/>`_ for more information. See `github <http://github.com/p0is0n/txsphinx/tree>`_ for the latest source.
:Author: Arkadiy Levin aka p0is0n <poisonoff@gmail.com>

About
=====
An asynchronous Python driver for the Sphinx search engine, based on Twisted.
The ``txsphinx`` package is an alternative to the original ``sphinxapi`` implemented on python.

Because the original ``sphinxapi`` package has it's own connection pool and
blocking low-level socket operations, it is hard to fully implement
network servers using the Twisted framework.
Instead of deferring all operations to threads, now it's possible
to do it asynchronously, as easy as using the original API.

