===============
Getting Started
===============

.. _hbase_getting_started-installation:

Installation
============

In the `HBase documentation <http://hbase.apache.org/0.94/book.html>`_, follow 
the instructions in `Download and unpack the latest stable release <http://hbase.apache.org/book/quickstart.html#d2417e128>`_.


.. _hbase_getting_started-run:

Run Modes
=========

It important to understand that you can run HBase in two different modes.
The modes are standalone and distributed, and these modes represent the environment
and the filesystems that HBase is run against. In standalone mode, HBase
is run against the local filesystem. In distributed mode, HBase is run on
the `Hadoop distributed file system (HDFS) <http://en.wikipedia.org/wiki/HDFS#Hadoop_distributed_file_system>`_.
 
To learn more about the two modes, see 
`HBase run modes: Standalone and Distributed <http://hbase.apache.org/book/standalone_dist.html>`_.

.. _hbase_getting_started-onboard:

On-Boarding at Yahoo
====================


.. _gs_onboard-overview:

Overview
--------

The Grid team provides two different environments for using the HBase service. Users
can onboard for the development/research environment or onboard for the production
environment. We have separate on-boarding processes for the two environments, which 
we look at next.

.. _gs_onboard-devel:

Development/Research On-Boarding
--------------------------------

To on-board for the development/research environment, you need to file
a bugzilla ticket and provide the following information:

- project information: design doc, Twiki links
- colo preference (tan, blue, red)

Ready? Great, `file a ticket <http://bug.corp.yahoo.com/enter_bug.cgi?product=kryptonite>`_ 
with the information listed above.

.. _gs_onboard-prod:

Production On-Boarding
----------------------

Production onboarding requires review and approval of the application usage 
(i.e., schema, access patterns, etc.) as well as planning for capacity required to support 
the application. Please `file a request <http://supportshop.cloud.corp.yahoo.com/ydrupal/?q=grid-services-request>`_ with the following information:

- Table names and schema
- Design doc describing how the table will be used:
     - application-level schema 
     - row key schema
     - different Access patterns
- Throughput (and latency) requirements for the access patterns mentioned (for the next ~6 mos)
- Storage Requirements (for the next ~6 mos)
- Justification for resource requirements (ie customer usage estimates, workload simulation/performance runs)
- colo
- user/group access privileges

.. _gs_onboard-envs:

Environments
------------

To support the varying requirements during the course of a development cycle, 
HBase provides a number of environments. A service level is tied to a namespace 
that will be created on a given HBase instance. 

See the `HBase tech talk slides <http://twiki.corp.yahoo.com/pub/Grid/HBaseHome/HBase_as_a_Service_Mar_2013_Talk_Final.pptx>`_
on more detail about namespace and resource guarantees.

#. Instant (WIP)
   - Development tier
   - A self-help service will provide requesting users with a namespace
   - Namespace created in this level will be deployed on a shared set of region 
     servers. No resource isolation guarantees are provided.
   - Namespace will have a quota on the number of tables (5) and number of total 
     regions (20). These limits are under observation.
   - Loose guarantees on uptime and data storage.

#. Development
   - Development/Research tier.
   - Unlike #1, a Research/Project namespace may be provided a dedicated set of 
     region servers with a quota appropriately set.
   - HBase capacity requirements needs to be provided.
   - HBase team to review and approve usage and schema.

#. On Demand (WIP)
   - Development/Research tier
   - A self-help service where users can temporarily have exclusive access to a 
     set of region servers. Current limit is one week.
   - Pool of region servers available is limited.

#. Production
   - Production tier
   - HBase capacity requirements needs to be provided.
   - HBase team to review and approve usage and schema. Please engage with HBase 
     team early to avoid any delays.
   - Highest level of guarantees on uptime and data integrity.

