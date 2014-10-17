========
Overview
========

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
- key information to manage the data better such as Archival decisions or 
  management of replicas, etc.
- capability to compute the window between a data set’s retention and the last access time trends.
- data from multiple logs at one place holistically.
- capability to find the "Query of Death"--jobs that cause the clusters to go down.
- capability to add additional logs using pluggable integration points.
- logs for analyzing the following:

  - Name node Static File System Images
  - Name node audit logs
  - Hadoop Job History logs
  - DAQ/GDM Metrics
  - DAQ/GDM Configuration files
  - Howl

You can also use tools like Rumen to gather data from Starling.


Who Should Use Starling?
========================

Starling serves a wide audience with each using Starling in a different way.

**Product Managers** will want to use Starling to get product metrics, usage reports,
find the top users, and generate reports.

**Service Engineers** will use Starling to debug and improve the efficiency of jobs, 
storage, and compression.

**Application Developers** 

Starling Uses
=============


Audit Log Tool
--------------

In many circumstances, you will want to capture who accessed which feed 
or identify a set of feeds which haven't been accessed for a specified period of time.

data source:  Namenode, Auditlog data
schema: http://twiki.corp.yahoo.com/view/Grid/NameNodeAuditLogsTool

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
as well as Y Org charts and the owners of logical space.


Architecture
============  

Starling connects to several source clusters spread across different data-centers, 
collects relevant logs from these clusters into a central warehouse cluster, processes 
these logs, and stores the processed logs in a data warehouse. This warehouse can 
then be queried using various tools in order to determine KPIs for the clusters 
and for performing other such analyses.

TBD: Need diagram

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

- **Hive** - Starling data is stored in the ``starling`` database on the Cobalt Blue cluster. The database contains 
  many tables for auditing and getting filesystem, job, queue, and task information.
- **Pig** - You can access the tables through Pig through HCatalog.  
- **HCatalog** - 
- **MapReduce** - You can use the MapReduce Java API to access data through HCatalog as well.
- **Query Server** - This is a REST-based API for accessing Starling data. (TBD: auth/read-only?/query params?)


HCatalog
--------

Alternatives to Starling
========================


pros/cons vs. other solution

