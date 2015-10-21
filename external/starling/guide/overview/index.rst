========
Overview
========

.. 10/17/14 Reorganized the sections, rewrote the sections "Who Should Use Starling?" and "Starling Users"
.. Need information about how application developers use Starling, Howl, an architecture diagram, info about Rumen, and 
.. a list of competing tecnologies with comparisons (see TBDs).

What is Starling?
=================  

Starling (aka "Hadoop Analytics Warehouse" or HAW) collects and processes various 
types of logs and configurations from Yahoo grids and populates a central 
data-warehouse with this information. This data can then be analyzed for 
generating various reports (key performance indicators for the grids, utilization 
of the grids, metering grid-usage, etc.) and for deriving insights useful for 
research and development.

Why Use Starling?
=================

Starling offers the following:

- insights into the Hadoop Clusters usage to allow us to tune 
  better and  prioritize features.
- key information to manage the data better such as archiving or 
  management of replicas, etc.
- capability to compute the window between a data set's retention and the last access time trends.
- data from multiple logs in one place holistically.
- capability to find the "Query of Death"--jobs that cause clusters to go down.
- capability to add additional logs using pluggable integration points.
- logs for analyzing the following:

  - NameNode static file system images
  - NameNode audit logs
  - Hadoop JobHistory logs
  - `Simon metrics <http://twiki.corp.yahoo.com/view/Yst/ProjectSimon#Simon_Overview>`_
  - Howl (TBD)

Who Should Use Starling?
========================

Starling serves a wide audience with each using Starling in a different way.
Let's look at three key audiences and consider how representatives
from these groups might use Starling.

- **Product Managers** - to get product metrics, usage reports,
  find the top users, and generate reports.
- **Service Engineers** - to debug and improve the efficiency of jobs, 
  storage, and compression.
- **Application Developers** - to optimize their jobs, monitor counters, time take, utilization, etc. 
  They could use counters to understand how the data has changed over a period of time, too.

How It Works
============

Starling uses an Oozie coordinator job and work-flows running on a central warehouse 
cluster to periodically pull logs from various source clusters, process these logs 
and store the processed logs as tables in a central data-warehouse backed by HCatalog. 
These tables can then be queried by developers, performance analysts, etc., using a 
Query Server, Hive, or Pig. Starling does not have a separate stand-alone server--all 
the collection and processing happens as a part of Oozie work-flows and an Oozie 
coordinator job executes these work-flows at an appropriate frequency.

Starling has an extensible architecture that allows it to handle different services 
and their logs. It allows easy configuration to handle different source clusters 
or frequencies of log-collection without requiring any modification or addition of code.

Ways to Access Starling
=======================

- **Hive** - Starling data is stored in the ``starling`` database on the Axonite Blue cluster. The database contains 
  many tables for auditing and getting filesystem, job, queue, and task information.
- **Pig** - You can access the tables through Pig through HCatalog.  
- **HCatalog** - 
- **MapReduce** - You can use the MapReduce Java API to access data through HCatalog as well.


Starling Uses
=============

We've already discussed how different audiences might use Starling and its benefits,
but now we'd like to describe in more detail a few more formalized uses.

Audit Log Tool
--------------

In many circumstances, you will want to capture who accessed which feed 
or identify a set of feeds which haven't been accessed for a specified period of time.
Starling lets you access `Watch Dog (the NameNode/Audit Logs tool) <http://twiki.corp.yahoo.com/view/Grid/NameNodeAuditLogsTool
>`_, so you capture key information from the NameNode audit log of each user-facing grid 
and to store it in a way that allows flexible reporting and trouble-shooting. 

Oozie Usage Report
------------------

Starling allows you generate Oozie usage reports, so you can analyze and identify the following:

- volume and peak usage
- latency
- feature usage

You can also capture the status and starting/ending time of Oozie actions such as bundles/coord,
jobs/coord, actions/workflow, jobs/workflow. This information can help you 
to do the following:

- load balance and plan for capacity when on-boarding, or after on-boarding. 
- analyze the average/deviation/distribution of job execution latency
  to detect abnormality and debug slowness (e.g., why SLAs were breached). 
- determing which features that the customer's coord/workflow jobs are using,
  which helps evaluate the impace of an outage/decommission/update of a product.


Utilization of Grid
-------------------

You can inspect how you are using HDFS by looking at ``jobsummary`` logs from JobTracker
as well as Y organizaton charts and the owners of logical space.

