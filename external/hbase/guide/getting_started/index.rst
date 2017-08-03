===============
Getting Started
===============

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

Ready? Great, `file a ticket <https://supportshop.cloud.corp.yahoo.com:4443/doppler/hbase>`_ 
with the information listed above.

.. _gs_onboard-prod:

Production On-Boarding
----------------------

Production onboarding requires review and approval of the application usage 
(i.e., schema, access patterns, etc.) as well as planning for capacity required to support 
the application. Please `file a request <https://supportshop.cloud.corp.yahoo.com:4443/doppler/hbase>`_ with the following information:

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
on more detail about namespace and resource guarantees.  (Note that this talk is from 2013 and out of date at this point.)

#. Shared Sandbox

   - Development/Research tier
   - Namespace created in this level will be deployed on a set of region servers shared with other users
   - No resource isolation guarantees are provided
   - Loose guarantees on uptime and data storage
   - Namespace will have a quota on the number of tables (5) and number of total regions (200)
   - Users create and update their own table schemas with relatively little supervision

#. Development Environment

   - Development/Research tier
   - Unlike #1, uses a dedicated set of region servers
   - More generous table/region quotas
   - Capacity requirements need to be provided to HBase team
   - HBase team to review and approve usage and schema changes
   - Requires CAR approval

#. Perf Environment

   - Development/Research tier
   - A small testing environment 
   - Exclusive access to a set of region servers for a limited period of time
   - This is similar to #2 except that it is temporary and will be reclaimed after the performance test is complete
   - Lifetime of 1-3 months

#. Production

   - Production tier.
   - HBase capacity requirements needs to be provided
   - Dedicated servers and very high quotas
   - HBase team to review and approve usage and schema. 
   - Please engage with HBase team early to avoid any delays
   - Highest level of guarantees on uptime and data integrity
   - Requires CAR approval

Support/Help at Yahoo
=====================

At Yahoo there are various ways to get help/support with HBase.

Hipchat / Slackchat
-------------------

   - This is the quickest way to get assistance with minor questions.
   - The name of the public Yahoo HBase Hipchat room is ``yhbase``.  Later on, we will migrate to Slackchat with a similar name.
   - This room is generally only for quick questions about APIs, use cases, and so on.

Support E-mails
---------------

   - There are 2 e-mails: ``yahoo-hbase-dev@yahoo-inc.com`` and ``yahoo-hbase-users@yahoo-inc.com``
   - The former e-mail address contacts the HBase team only while the latter contacts everyone who uses Yahoo at HBase (who has subscribed to that list)
   - If you have a complicated question that is specific to your circumstances, ask the former list.
   - If you have a general question about HBase, we recommend you ask the latter list first.
   - HBase team monitors both lists.

Announcement E-mails
--------------------

   - Whenever there are upgrades or incidents that impact users, grid-ops team will send announcements
   - Please subscribe to ``ygrid-production-announce@yahoo-inc.com`` and ``ygrid-sandbox-announce@yahoo-inc.com``

Jira
----

   - If you are running into a problem with your HBase environment (poor performance, unavailability, etc) you should file a Jira
   - File the Jira under the `HADOOPPF project <https://jira.corp.yahoo.com/servicedesk/customer/hadooppf/create/support%20request>`_.
   - Provide all details up front

      - what is not working for you
      - the affected colo, namespace, and table name
      - the approximate start time of the incident
      - links to failed job(s) and logs
      - links to any client-side yamas graphs

Doppler
-------
   - If you need to create a new environment or update an existing one 
   - For example, requests for more machines, schema alterations, enabling replication, adding user perms
   - Doppler can be found `here <https://supportshop.cloud.corp.yahoo.com:4443/doppler/hbase>`_.
