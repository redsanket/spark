========
Overview
========

What is Starling?
=================  

Starling (a.k.a. "Hadoop Analytics Warehouse" or HAW) collects and processes various 
types of logs and configurations from Yahoo grids and populates a central 
data-warehouse with this information. This information can then be analyzed for 
generating various reports (key performance indicators for the grids, utilization 
of the grids, metering grid-usage, etc.) and for deriving insights useful for 
research and development.


When to Use It?
===============  

Use Cases
=========

Audit Log Tool
--------------

want to capture who accessed which feed. or identify a set of feeds which haven't not been accessed during last 3 months

data source:  Namenode, Auditlog data
schema: http://twiki.corp.yahoo.com/view/Grid/NameNodeAuditLogsTool

Oozie Usage Report
------------------

Solution would do 1) volume/peak analysis, 2) performance(latency) analysis, 3) feature-usage analysis. For 1). Capture status of how many Oozie bundles/coord jobs/coord actions/workflow jobs/workflow actions are running at any given time period (month, day, hour). This allows for identifying peak time where most coord jobs/actions are running, which could help proper load-balancing, and capacity-planning at onboarding time. For 2), Capture starting/ending time of coord jobs/cood actions/workflow jobs/workflow actions. This enables to analyze average/deviation/distribution of job execution latency, used to detect abnormality and debug slowness (e.g. why SLAs were breached). For 3), Capture features that customer's coord/workflow jobs are using. This could help analyze the full impact of outage/decomission/update of product.


Architecture
============  

Starling connects to several source clusters spread across different data-centers, collects relevant logs from these clusters into a central warehouse cluster, processes these logs and stores the processed logs in a data warehouse. This warehouse can then be queried using various tools in order to determine KPIs for the clusters and for performing other such analyses.




How It Works
============

Starling uses an Oozie coordinator job and work-flows running on a central warehouse cluster to periodically pull logs from various source clusters, process these logs and store the processed logs as tables in a central data-warehouse backed by HCatalog. These tables can then be queried by developers, performance analysts, etc. using a Query Server, Hive or Pig. Starling does not have a separate stand-alone server - all the collection and processing happens as a part of Oozie work-flows and an Oozie coordinator job executes these work-flows at an appropriate frequency.
Starling has an extensible architecture that allows it to handle different services and their logs. It allows easy configuration to handle different source clusters or frequencies of log-collection without requiring any modification or addition of code.

Ways to Access Starling
=======================

Hive
----

Pig
---

HCatalog
--------

Alternatives to Starling
========================


pros/cons vs. other solution

