==================
Performance Tuning
==================

This section is very much a WIP that we will fill in as we get more time.

Job Configuration Optimizations
===============================

Speculative Execution Configuration
-----------------------------------

Note that the following recommendations are based on Hadoop's 2.8.0 speculator implementation.

- Speculative execution can be enabled for HBase jobs but the settings need to be conservative
- Before enabling speculative execution, make sure that if you write to HBase in your job that your writes are idempotent and will work correctly even if more than 1 attempt is running, or that tasks that perform writes do not have speculation enabled (eg oftentimes only reducers write to HBase)
- If you only read from HBase in your job speculative execution is usually pretty safe
- ``mapreduce.job.speculative.speculative-cap-total-tasks`` and ``mapreduce.job.speculative.speculative-cap-running-tasks`` need to be set such that the absolute maximum number of total/running tasks that are speculated number in the 2 or 3 range, as bad nodes in the compute cluster are not that common.
- Currently, ``mapreduce.job.speculative.speculative-cap-total-tasks`` looks to default to 0.01 (1%) which seems fair.  ``mapreduce.job.speculative.speculative-cap-running-tasks`` defaults to 0.1 (10%) which seems a bit too aggressive. Probably 1 or at most 5% is better here for HBase. But better would be to tune this percentage according to the user's job based on the absolute number of tasks.
- maxSimultaneousSpeculations = max(``mapreduce.job.speculative.minimum-allowed-tasks``, numRunningTasks * ``mapreduce.job.speculative.speculative-cap-running-tasks``) so need to also set ``mapreduce.job.speculative.minimum-allowed-tasks`` to a lower number, maybe like 1 or 2 at most.
- ``mapreduce.job.speculative.slowtaskthreshold`` defaults to 1.0 standard deviation which might be too little for HBase and may make certain optimistic assumptions about how balanced/similar different map or reduce tasks are. If 1 task has much more data than the others, its progress rate climbs relatively slowly compared to others and thus it could be at risk of getting re-run in an additional speculative attempt even though the 2nd attempt wouldn't be any faster (and in HBase, would likely be slower.) For HBase purposes it may be worth increasing ``mapreduce.job.speculative.slowtaskthreshold`` from its default of 1 standard deviation to more like 2 standard deviations from the mean unless a user's job doesn't exhibit data skew.
- See more about settings at https://hadoop.apache.org/docs/r2.8.0/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml

Other Job Configs
-----------------

(To Be Added)

Other topics may include setting max map task limits, appropriate timeouts etc

Client Optimizations
====================

(To Be Added)

Topics may include batching puts, sync vs async flushes, concurrency and htable vs hconnection, increment and append APIs, producer consumer patterns, using the block cache, using the row cache, gets vs scans, avoiding OOMs, etc.

Data Schema Optimizations
=========================

(To Be Added)

Topics may include links to common topics like hotspots, bucketing/hashing to ensure uniform load, different ways of breaking entities down into rows/cells in hbase, famous schemas and common use cases like TSDB, avoiding common pitfalls like overuse of strings or json or text in general, variable encodings for numbers, versioning recommendations, common 'tricks' like secondary index tables, common TTL use cases, brief discussion of how to break rows into 1 or more families, etc.

