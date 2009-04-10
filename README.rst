
===========================
MongoDB AppEngine Connector
===========================
:Info: See `the mongodb site <http://www.mongodb.org>`_ for more  information.  See `github <http://github.com/mongodb/mongo-appengine-connector/tree>`_ for the latest source.
:Author: Mike Dirolf <mike@10gen.com>

About
=====
The MongoDB AppEngine Connector provides a drop-in replacement for App Engine's
datastore API, using MongoDB as a backend.

Questions and Support
=====================

Questions about the MongoDB AppEngine Connector should be directed to the `mongodb-user
group <http://groups.google.com/group/mongodb-user>`_.

Usage
=====

The following steps can be used to get Google AppEngine working with MongoDB:

Install MongoDB
---------------

Instructions for installing MongoDB on your platform can be found on `the mongodb site <http://www.mongodb.org>`_.

After installing, start an instance of **mongod** on the default port::

  $ mongod run

Get the Google AppEngine Code
-----------------------------

In order to use the connector, you must patch the AppEngine source distribution, which can
be found `here <http://code.google.com/p/googleappengine>`_ and checked out via::

  $ svn checkout http://googleappengine.googlecode.com/svn/trunk/ googleappengine-read-only

Be sure that you meet all the pre-requisites listed in the *README* in the resulting
directory.

Patch the AppEngine Code
------------------------

Patches are found in the *gae_patches/* directory and are stored by
svn revision number.  To find the revision number of the repository
that you checked out, go into the root of the repository and type::

  $ svn info

This adapter has been tested against revisions 46 from Google
AppEngine's svn repository. To apply the patch, do
the following from a command line::

  $ cd googleappengine-read-only
  $ cd python/ # need to do this if you checked out all the different AE languages
  $ patch -p0 < gae_patch_r46.txt

you should see output similar to::

  patching file dev_appserver.py
  patching file google/appengine/tools/dev_appserver.py

If you see no error messages, your patching was successful.

Set the MongoDB Connector Directory
-----------------------------------

In the AppEngine distribution, we patched *dev_appserver.py* in the previous step, and now we
need to edit it by hand.  Open the file in your favorite editor, and look for the line::

  "PATH/TO/MONGO-APPENGINE-CONNECTOR/DIRECTORY"

and change it to the appropriate value.  Remember to keep the double quotes.

Install PyMongo, the Python Driver for MongoDB
----------------------------------------------

If you've already installed the MongoDB python driver - PyMongo - you can skip this step.

To install PyMongo::

  $ easy_install pymongo

Run the Tests
-------------

Once AppEngine is patched, pymongo is installed, and MongoDB is up and running, you can test
the installation by running the adapter's unit tests.

To do so, simply start AE via the SDK's start script **dev_appserver.py** with the full path to the
*test/test_site* directory in the adapter distribution.  So, starting in the root of the
AppEngine SDK::

  $ ./dev_appserver.py /my/path/to/mongodb-appengine-connector/test/test_site

Then, if you direct your browser to *http://localhost:8080* you should see a single test
page that begins with::

  Datastore API
  Test a simple db example...
  Test that ids get incremented properly between sessions...
  Slightly less simple db test...
  Test db exceptions...
  Test a delete...
  Test a delete on an unsaved object...
  ...

As long as you don't see the word **FAIL** all tests have passed.

Other Notes
===========

- Right now, the Connection cannot be configured. It attempts to
  connect to a standalone MongoDB instance on localhost:27017.

- Transactions are unsupported. When any operation requiring
  transactions is performed a warning will be logged and the operation
  will be performed transaction-less.

- DateTime values get rounded to the nearest millisecond when saved to
  MongoDB. This is a limitation of MongoDB's date representation, and is
  not specific to this adaptor.

- In order to actually create indexes the dev_appserver must be run with
  the --require-indexes option. Running with this option will probably
  add significant overhead, since each time the dev_appserver checks to
  see if it should create an index a query is performed.

- Index creation ignores the "Ancestor" option. This option would just create an
  index on '_id', which (soon) MongoDB creates automatically anyway.
