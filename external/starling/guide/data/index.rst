================
Data in Starling
================

.. 10/16/14 - Performed a spellcheck, cleaned up tables.

Overview
========

.. _data_overview-what:

What Data is Collected?
-----------------------

Starling collects log data from the grid in a variety of formats.
The log data includes information about jobs, HDFS, audit log
for NameNode, HCatalog, and HiveServer 2, as well as Simon metrics. 

The following logs are collected by Starling:

- **Job History** - this provides detailed information on an executed job, including asks and task attempts, counters, etc.
- **Job Configuration** - this provides the configuration information from an executed job.
- **Job Summary** - this provides a summary of an executed job.
- **FSImage** - this is the file-system image for HDFS generated periodically by the Backup Node (or the Secondary
  NameNode in less-recent versions of Hadoop). This is collected every month by default.
- **Audit Logs** - these contain records of accesses and modifications to file-system objects.
- **Aggregator Dumps** - this provides periodic dumps of different metrics related to various sub-systems for a given cluster.


.. MapReduce JobHistory (Avro format)
   MapReduce Job Configuration (XML)
   MapReduce JobSummary (text files)
   HDFS fsimage (binary format, hadoop specific)
   HDFS NameNode audit logs (text files)
   Hadoop Simon metrics (JMX metrics serialized to text files)
   Hcatalog audit logs (text files)
   HiveServer2 audit logs – both encryped and unencrypted (text files)


When is it Collected?
---------------------

Data is collected every night, but trends are computed initially on a monthly 
basis. This may improve in the future.

How is it Collected?
--------------------

A predefined workflow triggers a series of steps, the first of which 
copies log files from Grid clusters into the Starling
warehouse cluster; the specific log files and clusters are defined by configuration. A subsequent 
step extracts information from the log files and stores it into Hive.
This workflow is executed by Oozie on a recurring basis.

Users can execute ad hoc queries against the data via HCatalog.

Data Retention Policy
---------------------

The retention policy depends on the dataset. We retain most data between one and two years.

Data Warehouse
==============

What is the Data Warehouse?
---------------------------

The Data Warehouse for Starling is simply the Hive database
``starling``. The various source logs are mapped to Hive tables through HCatalog. Thus, if you wanted to analyze
Job History logs, you could run a query against the ``starling_job_summary``
table.  


Where is the Data Warehouse?
----------------------------

You can access the Hive ``starling`` database on Axonite Blue (axonite-gw.blue.ygrid.yahoo.com).


How is the Data Warehouse Accessed?
-----------------------------------

Interactive access to Starling data is through Hive queries, but Pig can use
the library ``org.apache.hcatalog.pig.HCatLoader();`` to access
the tables through HCatalog. You can also use MapReduce 
through HCatalog.

To learn how, see `Getting Started <../getting_started/>`_ and
for examples in Hive and Pig, see `Query Bank <../query_bank>`_.


HCatalog Schemas
================

Introduction
------------

HCatalog is a metadata management layer for all Hadoop grids. All datasets and 
their schemas are registered in HCatalog, allowing engines such as Hive, Pig, 
or MapReduce to use this layer to get to the data on HDFS and the schema used to 
represent it.

How Does It Work?
#################

Raw and processed logs are stored using HCatalog and partitioned by cluster and date. 
These partitioning keys are grid (of type string) and within that ``dt`` (of type string 
and format YYYY_MM_DD) respectively. Partitioning drastically reduces the amount 
of data that Hive has to process to generate the results for most queries. 
If a log is collected more frequently than once per day, there will be another partition 
``tm`` (of type string and format HH_MM) within the ``dt`` partition.

 

..  Raw logs are stored after maximal compression to reduce storage requirements. 
    Processed logs are stored as compressed tables using columnar-storage provided by 
    the RCFile storage-format in order to maximize the potential for compression (as 
    many columns have the same values). Processed logs are accessed via Hive using 
    HiveQL to produce both canned and ad hoc reports. Apart from the primary tables 
    corresponding to the processed logs, Starling will also have secondary tables derived 
    from these primary tables in order to speed up the execution of common queries and 
    the generation of common reports. The retention of both raw and processed logs is 
    determined by an appropriate configuration of HCatalog.


.. important:: All tables reside in the ``starling`` database - enter ``use
               starling;`` in hive to select the right database. Tables have a
               prefix of ``starling_`` in their names to distinguish
               them from other tables in the database.

Schemas
-------

The schemas of the tables are visible in hive via ``describe
TABLE_NAME;``. The full set of current schemas is described in the
`SupportSHOP table browser <http://yo/StarlingTables>`_.

Notes
-----

- FSImage tables: Unlike the data in other tables, the tables created from an FSImage (``fs_namespaces``, ``fs_entries``, and ``fs_blocks``) 
  represent a snapshot rather than incremental information for each period. You must 
  use a partition key with these tables to get the correct snapshot - otherwise your 
  queries will return incorrect results, not to mention scan a lot of data unnecessarily.

- Make sure you convert ``mod_ts`` and ``act_ts`` before calling any
  of the Hive date time functions - otherwise, you'll get a nasty
  surprise. For instance, ``select E.path``, ``from_unixtime(E.acc_ts)``, ``E.size``, ``E.user``, ``E.grid``, ``E.dt``, ``datediff(to_date(from_unixtime(round(E.acc_ts/1000)))``, 
  ``to_date(from_unixtime(unix_timestamp()))) as DAYS_OLD? from starling_fs_entries E where E.dir and datediff(to_date(from_unixtime(round(E.acc_ts/1000)))``, 
  ``to_date(from_unixtime(unix_timestamp()))) > 90 and grid='DG' and DT='2011_11_08' limit 10;``

- The values of ``acc_ts`` should not be used at Yahoo. Most name nodes don't set this value when files 
  are read due to performance issues. This value will always be set to the create time for 
  the file or it will be set to epoch (epoch for files created before 0.20 hadoop was released).

- Simon tables: The Simon aggregator dumps are processed on a "best-effort" basis due to the way 
  the metrics are collected and the dumps captured and made available to Starling. 
  It is quite possible therefore to see missing or duplicate metrics in this table. 
  If you want a unique row for a given metric for a given time-stamp, you must put the 
  appropriate ``DISTINCT`` clauses in your queries.

  Simon collects at least 15 different types of reports are collected; be sure
  to select on the appropriate report_item value to avoid commingling disparate data:
  FSNamesystem status, by node name, by process name, by session, HDFS
  throughput, individual datanode throughput, JobTracker, JobTracker
  totals, NameNode operations, perCluster, perDisk, perNode, shuffle
  output by host, tasktracker, tasktracker totals


