===============
Getting Started
===============

Installation
============

Install from PyPi using pip:

  ``pip install canvas-data-sdk``


Usage
=====

There are two ways this library can be used: you can call the API in your own code,
allowing highly customized workflows, or you can perform basic operations using the
included command line utility.

Using the command line utility
------------------------------

Installing the module via pip should have also installed a command-line utility
called ``canvas-data``.  You can get help by typing::

  canvas-data --help

which will print out basic help like:

| Usage: canvas-data [OPTIONS] COMMAND [ARGS]...
|
| A command-line tool to work with Canvas Data. Command-specific help is
  available at: canvas-data COMMAND --help
|
| Options:
|   -c, --config FILENAME
|   --api-key TEXT
|   --api-secret TEXT
|   --help                 Show this message and exit.
|
| Commands:
|   get-ddl            Gets DDL for a particular version of the...
|   get-dump-files     Downloads the Canvas Data files for a...
|   get-schema         Gets a particular version of the Canvas Data...
|   list-dumps         Lists available dumps
|   unpack-dump-files  Downloads, uncompresses and re-assembles the...

The utility has several commands which you can see listed in the help output above.
You can get more details on each command by typing::

  canvas-data COMMAND --help

For example::

  canvas-data get-schema --help


Configuring the command line utility
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are two global options that are needed for all of the commands. You can
include them as command line options by placing them before the command::

  canvas-data --api-key=XXXXX --api-secret=YYYYY COMMAND [command options]

Alternatively you can create a YAML-formatted config file and specify that instead. Several of the
commands need to know where to store downloaded file fragments and where to
store the re-assembled data files. You can specify these locations in the config
file too. For example, create a config file called ``config.yml`` containing::

  api_secret: XXXXX
  api_key: YYYYY
  download_dir: ./downloads
  data_dir: ./data

Now you can use it like::

  canvas-data -c config.yml COMMAND [command options]

Setting Up Your Database
^^^^^^^^^^^^^^^^^^^^^^^^

Before you can load any data into your database, you first need to create all of
the tables. You also may need to re-create tables if portions of the schema change
in the future.

You can use the ``get-ddl`` command to generate a Postgres or Amazon Redshift compatible
DDL script based on the JSON-formatted schema definition provided by the Canvas
Data API. It will default to use the latest version of the schema, but you can
specify a different version if needed::

  canvas-data -c config.yml get-ddl > recreate_tables.sql

Note that this script will contain a ``DROP TABLE`` and a ``CREATE TABLE`` statement for
every table in the schema. Please be very careful when running it -- it will
remove all of the data from your database and you'll need to reload it.

Listing the Available Dumps
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Instructure typically creates one dump per day containing the full contents of most of the
tables, and incremental data for the ``requests`` table. Occasionally Instructure will produce
a full dump of the ``requests`` table containing data going back to the start of your instance.

You can use the ``list-dumps`` command to see the dumps that are available::

  canvas-data -c config.yml list-dumps

Details for each dump will be displayed, including the sequence and dump ID. Full-requests-table dumps will
be highlighted.

Getting and Unpacking Data Files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can fetch all of the files for a particular dump (besides the requests files -
more on that later), decompress them, and re-assemble them into a single file for
each table by using this command::

  canvas-data -c config.yml unpack-dump-files

This command will default to fetch data from the latest dump, but you can choose
a specific dump by passing the ``--dump-id`` parameter. You can limit the command
to just fetch and reassemble the data files for a single table by passing the ``--table``
parameter.

The command will create a sub-directory underneath your data directory named after
the dump sequence number, and all of the data files will be stored under that.

A SQL script called ``reload_all.sql`` (or ``reload_<table_name>.sql`` if you're just unpacking
the data for a single table) will also be stored inside the dump
sequence directory. It contains SQL statements that will truncate all of the tables (besides
the requests table) and will load each of the data files into a database. This can be used as
part of a daily refresh process to keep all of your tables up to date. The SQL
commands are known to be compatible with Postgres and Amazon Redshift databases;
YMMV with other databases.

Downloading Data File Fragments
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can just download the compressed file fragments like this::

  canvas-data -c config.yml get-dump-files

Note that if you later run the ``unpack-dump-files`` command, it won't need to re-download
files that you've already fetched using ``get-dump-files``.

Using the API in your own code
------------------------------

First, create a CanvasDataAPI object. You need to supply your API key and secret.
Here we assume that those are available in environment variables, but you could
read them from configuration, too::

  import os
  from canvas_data.api import CanvasDataAPI

  API_KEY = os.environ['API_KEY']
  API_SECRET = os.environ['API_SECRET']

  cd = CanvasDataAPI(api_key=API_KEY, api_secret=API_SECRET)

Now you can use this object to interact with the API as detailed below.

Schemas
^^^^^^^

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
^^^^^

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
^^^^^

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
