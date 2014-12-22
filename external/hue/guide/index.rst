==============
Hue User Guide 
==============

.. toctree::
   :maxdepth: 2
   :hidden:

   overview/index
   ui/index
   getting_started/index
   howtos/index
   reference/index


This document is **Yahoo Proprietary/Confidential**. Do not release the contents of this document outside the company.

About This Guide
================

The goal of this guide is to help developers use a custom version of Hue for Yahoo. 

Keep in the mind that Cloudera version of Hue has some different features, and that some similar features described in the `Hue User Guide <http://cloudera.github.io/hue/docs-3.7.0/user-guide/index.html>`_ behave differently in Yahoo's Hue UI.

.. _onboard:

On-Boarding
===========

You do not need to on-board to use Hue. At Yahoo,
Hue is a service hosted on different clusters. You just need
the URL to the Hue UI on the cluster of your choice to 
get started. 

Getting Help
============

General Questions
-----------------

- yahoo-hue-users@yahoo-inc.com 

Request Support
---------------

TBD

Product/Engineering 
-------------------

TBD


Filing Tickets
--------------

Bugzilla Links
##############

- Dev: http://bug.corp.yahoo.com/enter_bug.cgi?product=Low%20Latency
- Grid Ops: http://bug.corp.yahoo.com/enter_bug.cgi?product=kryptonite

Ticket Filing Process
#####################

If you have identified an issue with Starling withn your environment or 
while testing, you may file a Bugzilla Ticket. When filing the ticket, 
include the following if relevant:

- Grid on which the issue was seen. For example, append [ebony-red] to the description of the ticket.
- The topology name, and component names if specific components have problems.
- Gateway used to launch the topology, if applicable.
- Relevant log messages with the error (Please include the entire stack trace).
- If possible, save the output of around 5 complete stack traces (via jstack) of a JVM with the issue.
- If possible, save the heap dump (via jmap) of a JVM with the issue.
- How you observed the issue (Steps to Reproduce)


Environment
===========

Each Grid cluster has a running Hue instance. You have access to the Hue instance on any cluster,
but you can only access and modify data that you have access permission.

To access the Hue Web UI of a particular instance, use an URL with the 
following syntax: ``https://{cluster_name}-hue.{color}.ygrid.yahoo.com:{port}``

For example, to access the Hue UI on Cobalt Blue, you would go to
``https://cobaltblue-hue.blue.ygrid.yahoo.com:9999/``.

