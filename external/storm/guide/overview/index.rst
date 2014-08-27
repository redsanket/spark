========
Overview
========

.. Status: Still need to edit starting from "Who is Using..." and on (08/27/14).

What is Storm?
==============

Apache™ Storm is a distributed computation system for the real-time processing of streaming
data. The real-time data processing of Storm complement the batch-oriented processing
model of MapReduce in Hadoop. 

Basic Concepts
==============

Before you can even start understanding the benefits of Storm, you should understand
a few core concepts. First, let's discuss how data is structured in Storm. We mentioned
streamed data earlier, but what is the underlying data structure if there is one?
The **streams** in Storm consist of tuples, which are a list of elements such as ``(3, 5, 2, 4)``.
The sources of streams in Storm are called **spouts**, which are written in code and can
draw from any data source (i.e., HBase, Hive tables, REST API, etc.). Code that 
processes these input streams and produces output streams (results) is collectively
known as **bolts**. Bolts can filter, aggregate, and join data as well as communicate
with databases. The network of spouts and bolts in your Storm application is called your
**topology**, which describes how data flows through your application.  

To learn how your Storm application is executed, allocated, and distributed in a Storm cluster,
we direct you to the `Architecture <../architecture/>`_ chapter.

Use Cases
=========

Storm has many use cases: 

- realtime analytic - Example: analyzing Twitter feeds and writing results to Sherpa.
- online machine learning
- continuous computation - Example: computing the click metrics for an ads for specific campaigns or user metrics for the media property page.
- distributed RPC -  Example: regression analysis of user profile vs. buying behavior in real-time.
- ETL

Why Use Storm?
==============

Besides expanding the capabilities of Hadoop to now process real-time data, Storm
offers the following benefits: 





.. Storm is simple and developers can write Storm topologies using any programming language.

- **Application Scalability** -  Storm clusters are designed so that workers that execute spouts and bolts can be increased to meet rising throughput needs.
- **Infrastructure Scalability** - You can also increase the number of supervisors to run more workers and run supervisors with a different number of slots, which define
  the ports on a machine are open for workers to use.
- **Resource Guarantees** - Topologies are guaranteed resources based on scheduling at both the topology and user levels.
- **Fault Tolerance** -   When faults occur during the execution of your computation, Storm reassigns tasks as needed. Storm ensures that computations can 
  run forever (or until you manually kill the computation).


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

- **Stream Processing** - Streamed data can be processed and written to a view or database in real time. 
- **Distributed RPC** - Allows you to execute computations on many machines that would be difficult to do on one machine. 
- **Continuous Compute** - Functions that needs to continously compute the value are allowed to run perpetually.
- **Guaranteed Message Processing** - You can configure Storm to guarantee message processing based on your overhead and performance needs as shown
  in the table below.

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




