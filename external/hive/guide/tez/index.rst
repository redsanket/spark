==================
Hive on Apache Tez
==================

.. _hive_intro-tez:

Overview
========

Tez, an open source Apache incubator project, is a native Hadoop service built on 
`Yarn <http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/YARN.html>`_ 
and the eventual successor of the map-reduce framework. Tez runs only on Hadoop 2 and
has been designed to execute DAG (directed acyclic graph) of tasks instead of
batch processing data like MapReduce. This makes Tez better for interactive queries
used by Hive.

Tez also has made improvements over MapReduce that allows for 
faster response times and extreme throughput at petabyte 
scale. Let's discuss some of the improvements made that make Tez
ideal framework for Hive and Pig.

Pluggable Inputs
----------------

Tez does not impose a data format on user applications. MapReduce applications
can use key-value pairs; Hive and Pig can use tuple formats that are native
to each respectively.


Simplified Map and Reduce
-------------------------

In a typical MapReduce execution without Tez, 
MapReduce executes a map and then a reduce function, writes to HDFS, runs
possibly more maps and reduce functions, etc., before getting the final result.
You could think of this model as something like MRMRMR. 

Because Tez executes a DAG of tasks (also called flexible DAG), this execution model simplifies into MRRR
and leads us to the next reason for better performance.

I/O Access to HDFS
------------------

With Tez, results aren't written to HDFS until the last reduce function is executed, which
decreases network latency, disk usage, and ultimately leads to faster processing.
Reads are reduced as well because hot data is kept in RAM for faster access (in-memory cache). 


Reduced Launch Latency
----------------------

In the Map-Reduce engine, query startup is very expensive. The latency when 
launching jobs and tasks launch latencies can add up to 5 to 30 seconds to
the execution time for queries, which is especially bad for short queries. 

Tez has a feature called Tez Sessions. A Tez Session that maps to an instance 
of an Tez Application Master (AM), which is similar to a MapReduce 
job having a corresponding MapReduce Master being launched, except with one important distinction: 
Each MapReduce job launches its own MapReduce AM, whereas, in one Tez Session, 
many DAGs can be submitted to the Tez AM without the overhead of launching new AMs.

Highly Customizable
-------------------

Tez is targeted towards data-processing applications and bases computation as data-flow graphs with reusable 
primitives, such as sort, merge, etc. This allows Tez to meet the needs of a broad range of use cases (Hive,
Pig, Cascading, MapReduce, etc.).


Why Tez Helps
=============

Hive submits its query plan directly to Tez Service, so 
jobs are executed on an AM from the pool instead of starting a new AM, removing job launch overhead.
When maps and  reduces complete, they can still pick up more work (container re-use) 
rather than exiting, reducing latency and eliminating difficult split-size tuning.

Running on Tez, Hive can provide run-time query tuning by picking aggregation 
parallelism using online query statistics (run-time reconfiguration of DAG).
Tez's simplification of the map-reduce pattern becomes that much more important when
process complex Hive queries that at a large scale and require a high throughput.


Trying Out Tez
==============

You can try Tez with Hive on Axonite Red sandbox by specifying it as the Hive execution engine

Turn on hive.execution.engine = tez. (You can always switch it back to MapReduce by setting
``hive.execution.engine = mr``.)

The look and feel of Hive looks on both MR and Tez is the same as none of the following change:

- the Hive CLI
- interfacing with JDBC,
- user-defined functions (UDF)
- HiveQL
- metastore

The Hive command ``EXPLAIN`` and reporting with Tez is similar to MR, but you will notice that 
the small differences reflect how the execution underneath differs.
Deployment is simple as well because Tez comes as a client-side library.


Performance Improvement
=======================

To illustrate the performance improvement, we'll benchmark the performance of Hive queries run
on Hive 0.10 with MapReduce and Hive 0.13 with Tez. 

Benchmark Overview
------------------

The table below describes the scope and specification of the benchmark:


.. csv-table::  Hive 0.10 (MapReduce) vs. Hive 0.13 (Tez) Benchmark
   :header: "", "Hive 0.10", "Hive 0.13"
   :widths: 20, 30, 30

   "Benchmark Standard", "TPC-H", "TPC-H"
   "Cluster Size", "300", "300"
   "Data set Sizes", "100 GB, 1 TB, 10 TB", "100 GB, 1 TB, 10 TB"
   "Hadoop Version", "0.23", "0.23"
   "File Format", "RCFile", "ORC File"
   "Security", "None", "None"
   "Engine", "MapReduce", "Tez"
   "Vectorizaton", "No", "Yes"



.. note:: TCP-H consists of a suite of business-oriented ad-hoc queries and 
          concurrent data modifications.

Results for 100 GB
------------------

.. image:: images/100gb_benchmark.jpg
         :height: 557 px
         :width: 850 px
         :scale: 95%
         :alt:  Hive 0.10 v Hive 0.13 Benchmark Results for 100 GB Data set
         :align: left


Results for 1 TB
----------------


.. image:: images/1tb_benchmark.jpg
         :height: 572 px
         :width: 850 px
         :scale: 95%
         :alt:  Hive 0.10 v Hive 0.13 Benchmark Results for 1 TB Data set
         :align: left


Results for 10 TB
-----------------

.. image:: images/10tb_benchmark.jpg
         :height: 565 px
         :width: 850 px
         :scale: 95%
         :alt:  Hive 0.10 v Hive 0.13 Benchmark Results for 10 TB Data set
         :align: left

