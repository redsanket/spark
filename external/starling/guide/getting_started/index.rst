===============
Getting Started
===============

.. 12/09/15 - Copy edited documentation.

Introduction
============

Starling runs on the Axonite Blue (AB) cluster and uses the `HCatalog <https://cwiki.apache.org/confluence/display/Hive/HCatalog>`_
server to  store the meta-data about its tables and the HDFS on this cluster to store all the 
data. The tables used by Starling are stored in the ``starling`` database and have 
the ``starling_`` prefix in their names. 

Accessing Starling
------------------

Because Starling uses HCatalog, you can use Hive, Pig, or even the MapReduce Java API 
to access data.

.. note:: The HCatalog server on CB is secured: it has the URI 
          ``thrift://axoniteblue-hcat.ygrid.vip.gq1.yahoo.com:50513`` 
          and uses the Kerberos principal ``hcat/_HOST@YGRID.YAHOO.COM``.

Prerequisites
=============

- :ref:`On-Board to Axonite Blue <onboard>`_.
- Log onto Axonite Blue.

Querying Data
=============

Using Hive
----------

#. Request a Kerberos ticket: ``$ kinit {your_username}@Y.CORP.YAHOO.COM``
#. Start Hive: ``$ hive``
#. Set the queue that you're going to use. We'll use ``unfunded`` for this tutorial::

       hive> SET mapred.job.queue.name=unfunded;

   .. note:: To avoid having to set the queue manually, you can set the queue in the 
             Hive configuration file ``$HOME/.hiverc``.
             Hive will use the queue set in the configuration file. You can use see a list 
             of queues that you have
             access to by running the following: ``mapred queue -showacls``

#. Use the ``starling`` database: ``hive> use starling;``
#. Run a query against the ``starling_jobs`` table:: 

       hive> SELECT * FROM starling_jobs WHERE grid='AB' and dt='2012_05_03' LIMIT 10;

   .. note:: Unless you know what you're doing, always use the partition keys in your 
             query (e.g., ``grid`` and ``dt``). 
             If you don't, your hive ``sessionA`` will say ``"Error in semantic analysis: 
             org.apache.thrift.transport.TTransportException: java.net.SocketTimeoutException: 
             Read timed out"``, and you will need to restart your Hive client (all other Hive 
             queries issued in that session will fail.)

#. Check out the other tables in the ``starling`` database: ``hive> show tables;``
#. As you do need to specify a partition, it's good to also see the list of available partitions: ``hive> SHOW PARTITIONS starling_jobs;``


Using Pig
---------

Pig can be used to work with tables on HCatalog. See the `HCatalog Getting Started <http://twiki.corp.yahoo.com/view/Grid/HCatalogGettingStarted#Pig>`_
on how to invoke Pig to use HCatalog. You have to specify your MapReduce Job queue 
using the command-line option ``-Dmapred.job.queue.name=unfunded`` (replace ``unfunded``
with the queue you normally use to execute your MapReduce Jobs on CB). 

You can then query the Starling tables in the following way::

    grunt> A = LOAD 'starling.starling_jobs' USING org.apache.hcatalog.pig.HCatLoader();
    grunt> B = LIMIT A 10;
    grunt> DUMP B;
    [...]

Getting Information About Starling
----------------------------------

From Hive, you can run queries to get basic information:

- Tables: ``show tables;``
- Partitions: ``show partitions starling_jobs;``
- Number of jobs run by a user: ``SELECT COUNT(job_id) FROM starling_jobs WHERE user='dfsload' and grid='MG' and dt='2011_12_03';``
- Number of jobs run each day: ``SELECT COUNT(1), dt FROM starling_jobs WHERE grid='MB' and dt>='2011_07_11' and dt <= '2011_07_13' GROUP BY dt;``  


Next Step
=========

See the `Query Bank <../query_bank>`_ for examples of queries of Starling data.
