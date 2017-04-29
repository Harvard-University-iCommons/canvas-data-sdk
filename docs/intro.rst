===============
Getting Started
===============

Installation
============

Install from PyPi using pip:

  ``pip install canvas-data-sdk``


Usage
=====

First, create a CanvasDataAPI object::

  from canvas_data.api import CanvasDataAPI

  API_KEY = os.environ['API_KEY']
  API_SECRET = os.environ['API_SECRET']

  cd = CanvasDataAPI(api_key=API_KEY, api_secret=API_SECRET)

Now you can use this object to interact with the API as detailed below.

Schemas
-------

Instructure occasionally updates the Canvas Data schema, and each change has a version
number. To retrieve all of the schema versions that are available::

  schema_versions = cd.get_schema_versions()

which will return a list similar to the following::

  [ {u'createdAt': u'2016-03-29T21:35:23.215Z', u'version': u'1.9.1'},
    {u'createdAt': u'2016-03-11T17:38:01.877Z', u'version': u'1.9.0'},
    {u'createdAt': u'2016-03-10T20:10:16.361Z', u'version': u'1.8.0'},
    {u'createdAt': u'2016-02-18T23:52:56.214Z', u'version': u'1.6.0'},
    ...
  ]

You can retrieve a specific version of the schema::

  schema = cd.get_schema('1.6.0', key_on_tablenames=True)

Or you can retrieve the latest version of the schema::

  schema = cd.get_schema('latest', key_on_tablenames=True)

Dumps
-----

Instructure produces nightly dumps of gzipped data files from your Canvas instance.
Each nightly dump will contain the full contents of most tables, and incremental data
for others (currently just the requests table). To retrieve a list of all of the nightly
dumps that are available::

  dumps = cd.get_dumps()

which will return a list similar to the following::

  [{u'accountId': u'9999',
    u'createdAt': u'2017-04-29T02:03:38.247Z',
    u'dumpId': u'125a3cb0-2cf3-11e7-84a8-784f4352af0c',
    u'expires': 1498615418247,
    u'finished': True,
    u'numFiles': 79,
    u'schemaVersion': u'1.16.2',
    u'sequence': 560,
    u'updatedAt': u'2017-04-29T02:03:39.663Z'},
 {u'accountId': u'9999',
    u'createdAt': u'2017-04-28T02:03:05.520Z',
    u'dumpId': u'1ab0aacc-2cf3-11e7-8299-784f4352af0c',
    u'expires': 1498528985520,
    u'finished': True,
    u'numFiles': 79,
    u'schemaVersion': u'1.16.2',
    u'sequence': 559,
    u'updatedAt': u'2017-04-28T02:03:07.373Z'},
 {u'accountId': u'9999',
    u'createdAt': u'2017-04-27T01:58:08.551Z',
    u'dumpId': u'24f4d347-2cf3-11e7-b1fa-784f4352af0c',
    u'expires': 1498442288551,
    u'finished': True,
    u'numFiles': 79,
    u'schemaVersion': u'1.16.2',
    u'sequence': 558,
    u'updatedAt': u'2017-04-27T01:58:11.533Z'},
    ...
  ]

Files
-----

You can get details on all of the files contained in a particular dump::

  dump_contents = cd.get_file_urls(dump_id='125a3cb0-2cf3-11e7-84a8-784f4352af0c')

Usually you'll just want to get the latest dump::

  dump_contents = cd.get_file_urls(dump_id='latest')

The complete data for each table can be quite large, so Instructure chops it into
fragments and gzips each fragment file. You can download all of the gzipped fragments
for a particular dump::

  files = cd.download_files(dump_id='latest',
                            include_requests=False,
                            directory='./downloads')

The ``requests`` data is very large and needs to be handled differently from the rest
of the tables since it's an incremental dump.  If you want to download everything but
the ``requests`` data, set the ``include_requests`` parameter to ``False`` as above.

Typically you'll want to download the dump files for a particular table, uncompress them,
and re-assemble them into a single data file that can be loaded into a table in your local data
warehouse.  To do this::

  local_data_filename = cd.get_data_for_table(table_name='course_dim')

This will default to download and re-assemble files from the latest dump, but you
can optionally specify a particular dump::

  local_data_filename = cd.get_data_for_table(table_name='course_dim',
                                              dump_id='125a3cb0-2cf3-11e7-84a8-784f4352af0c')
