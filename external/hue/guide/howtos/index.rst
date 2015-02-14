=======
How Tos
=======

Introduction
============

After you have become familiar with the `Hue UI <../ui>`_ and worked through
`Getting Started <getting_started>`_, you may want to find the fastest way
to learn how to complete a task with Hue. This is precisely the purpose
of this chapter. 

Hue Basics
==========

About Hue
---------

Version
#######

Getting Help
------------

Documentation
#############

Tutorials
#########


Changing User Information
-------------------------

Credentials
###########

Name/Email
##########


Signing In / Signing Out
------------------------


Managing Databases, Tables, and Partitions
==========================================

Selecting a Database 
--------------------

Viewing Table Metadata/Data
---------------------------

View File Location
------------------

Show Partitions 
---------------


Hive Queries in Hue
===================

Creating Queries
----------------

Loading Queries
---------------

Running Queries
---------------

Saving Queries
--------------

Downloading Results
-------------------

Viewing the Query History
-------------------------

Viewing/Editing/Copying/Deleting Saved Queries
----------------------------------------------


Accessing HDFS
==============

Browsing Files
--------------


Creating Directories
--------------------

Creating Files
--------------

Uploading Files
---------------


Copying Files
-------------

Downloading Files
-----------------

Moving/Renaming/Deleting Files
------------------------------

Changing Permissions for Files
------------------------------

Deleting Files
--------------

Job Information
===============

Filtering Jobs
--------------

Viewing Job Logs
----------------

Tasks
#####


Viewing Job Output
------------------

Oozie Workflows
===============

Filtering Oozie Workflows
-------------------------

Resuming/Suspending/Killing Oozie Workflows
-------------------------------------------

Creating a Workflow
-------------------

Importing a Workflow
--------------------

Submitting a Workflow
---------------------

Editing a Workflow
------------------

Uploading a Workflow
--------------------

Displaying the History of a Workflow
------------------------------------

Oozie Coordinators
==================

Viewing Coordinators
--------------------

Creating a Coordinator
----------------------

Submitting a Coordinator
------------------------

Editing a Coordinator
---------------------

Displaying the History of a Coordinator
---------------------------------------

Oozie Bundles
=============

Viewing Bundles
---------------


Opening a Bundle
----------------

Creating a Bundle
-----------------

Submitting a Bundle
-------------------

Editing a Bundle
----------------

Displaying the History of a Bundle
----------------------------------


Creating Hue Applications
=========================

Hue SDK
-------

Hue provides SDKs for building brand new and/or custom applications.
The SDK includes Hue libraries for connecting to Hadoop services
and front-end code consisting of views, models, and templates.

Hue libraries::

    libs/
	/hadoop
		/jobtracker
		/webhdfs
		/yarn
	/liboozie
	/rest
	/thrift
	/...

Hue front-end application code::

    apps/
	/jobbrowser
	/oozie



REST API to HDFS
----------------

You have access to HDFS through the NameNode's RESTful service 
`WebHDFS <http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`_.
You can also use HttpFS as well.

URL Syntax
##########

- ``http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE``
- ``http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN``


Python Client Support
#####################

The Hue File Browser applications has a `Python API <https://pypi.python.org/pypi/WebHDFS/0.2.0>`_
that lets you create instances of WebHDFS to access HDFS.

Here is an example of the class API:

.. code-block:: python

   class WebHdfs (Hdfs):
   
      def create(self, path, …):
         ...

      def read(self, path, …):
         ... 

      def download (request, path):
   
         if not request.fs.exists(path) :
            raise Http404(_(“File not found.”))
         if not request.fs.isfile(path):
            raise PopupException(_(“not a file.”))

Middleware
----------

TBD



