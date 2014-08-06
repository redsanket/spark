========
Overview
========


Why Use Storm?
==============

Application Scalability
-----------------------


..

    Add more workers and parallelism to Spouts/Bolts with rising throughput needs

    High throughput with low latency with multiple workers
    e.g. One million 100 byte messages/sec/supervisor


Infrastructure Scalability
--------------------------

.. 

    Add more supervisors to run more number of workers
    Run supervisors with different number of slots

Resource Guarantees
-------------------

..

    The topologies are guaranteed to resources based on Isolation scheduling

    Two Modes – 1) At topology level 
                            2) User level

Fault Tolerance
---------------

.. csv-table:: Fault Tolerance
   :header: "Scenario", "Built-In Tolerance"
   :widths: 15, 40

+--------------------------------+---------------------------------------------------------------------------------------+
| Scenario                       | Built-In Tolerance                                                                    |
+================================+=======================================================================================+
| Worker Dies                    | - Either Supervisor will restart it.                                                  |
|                                | - If it fails on startup, then Nimbus will reassign task to another node.             |
+--------------------------------+---------------------------------------------------------------------------------------+
| Node Dies                      | The task on the node will time out and be assigned to new nodes by Nimbus.            |
+--------------------------------+---------------------------------------------------------------------------------------+
| Nimbus/Supervisor daemons dies | - Daemons restart as if nothing happened.                                             |
|                                | - No worker process is affected.                                                      |
+--------------------------------+---------------------------------------------------------------------------------------+
| Nimbus dies (SPOF)             | - Workers will continue to function and supervisors will restart workers if they die. |
|                                | - Workers WON’T be assigned to other nodes when needed.                               |
+--------------------------------+---------------------------------------------------------------------------------------+

Stream Processing
-----------------

Problem: Process data in stream and write to view/DB in real time

Example: Processing twitter feeds and writing aggregates to Sherpa

Distributed RPC
---------------

Problem: A function that is too intense to compute on single m/c

Example: Regression analysis of user profile vs. buying behavior in real-time


Continuous Compute
------------------

Problem: A function that needs to compute the value all time

Example: 

- Computing the Ads click metrics for specific campaigns
- Computing user metrics for media property page

Guaranteed Message Processing
-----------------------------


+--------------------------------+--------------------------------------------------------------------------------------------------------------+
| Options                        | Overhead vs. Performance                                                                                     |
+================================+==============================================================================================================+
| None                           | - Low overhead, very fast.                                                                                   |
|                                | - Anything where the answer does not have to be exact, and too little is better then too much..              |
+--------------------------------+--------------------------------------------------------------------------------------------------------------+
| At Least Once                  | - Relatively low overhead.                                                                                   |
|                                | - Anything where the answer does not have to be exact, and double counting is better then missing something. |
|                                | - Requires an input source that can do replay.                                                               |
+--------------------------------+--------------------------------------------------------------------------------------------------------------+
| Exactly Once                   | - Higher overhead, but still fairly fast.                                                                    |
|                                | -  Requires input source to support replay, and storage to be able to store batch.
+--------------------------------+--------------------------------------------------------------------------------------------------------------+

Important Concepts and Terminology
==================================

Streams
-------

Unbounded sequence of tuples

Spouts
------

Source of Stream
E.g. Read from Twitter streaming API


Bolts
-----

Processes input streams and produces new streams
E.g. Functions, Filters, Aggregation, Joins


Topologies
----------

Network of spouts and bolts


Who is Using Storm?
===================

Internal Use
------------

Slingstone
##########

The owned and operated content is analyzed/categorized and further streamed to other serving systems using Storm and HBase.

Content Agility
###############

SIPPER is the inline processing engine to process the Ingested content through HBase, CMS etc in parallel to TIPSY (Batch Layer).


RMX/NGD
#######

RMX Fast Feedback Loop will be a new data pipeline (in addition to current Hadoop pipeline), and enables campaign budgets to be 
adjusted within x minutes.


Ads and Data
############

A low latency, real-time, or near real-time reporting platform built on top of a stream/ low latency data 
processing solution that perpetually transforms and aggregates data. 

Sponsored Search
################

Migrating stream pipeline for search to Storm. Getting search events from DH 
Rainbow, do some in-memory calculation and push the results to http servers.
 

Flickr
######

Flickr is auto tagging the photos using the Deep machine learning algorithm. Storm 
reads data from Redis server and processes them on the fly. The results are written 
to the Vespa for search and Sherpa to store auto-tags.


Search (Commerce and Shopping)
##############################

Grid reporting UI that directly exposes data on grid with a simple UI, minimum 
data SLA and report response time - Allows users to build their own reports, 
choose to compute non-additive metrics (UUs) across various dimension combinations 
defined at run time.

SMILE
#####

Smile is a scalable machine learning platform built on top of Storm. While Smile 
emphasizes online machine learning, it also provides hooks to update and produce 
bulk models via a batch training phase. One can run algorithms both in batch and 
online mode.


External Use
------------

Twitter
#######


Discovery, real-time analytics, personalization, search, revenue optimization, and many more

Groupon
#######

Real-time data integration systems  


Infochimps
##########

Data Delivery Services (DDS) uses Storm to provide a fault-tolerant and linearly 
scalable enterprise data collection, transport, and complex in-stream processing cloud service.

Flipboard
#########

Using Storm across a wide range of services - content search, real-time analytics, 
generating custom magazine feeds.

Ooyala
######

Giving customers real-time streaming analytics on consumer viewing behavior and digital content 
trends

Baidu
#####

Storm to process the searching logs to supply real-time stats for accounting pv, ar-time and so on.

Alibaba
#######

Use storm to process the application log and the data change in database to 
supply realtime stats for data apps.

Rocketfuel
##########

Tracks impressions, clicks, conversions, bid requests etc. in real time

Other Stream Processing Solutions
=================================

Samza
-----

Spark
-----

S4
--

Kinesis
-------

Millwheel
---------

StreamInsight
-------------

DataTorrent
-----------

SQLstream
---------




