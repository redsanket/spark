================
Data in Starling
================

.. 10/16/14 - Performed a spellcheck, cleaned up tables.

Overview
========

.. _data_overview-what:

What Data is Collected?
-----------------------

Starling collects log data from the grid in a variety of formats.
The log data includes information about jobs, HDFS, audit log
for NameNode, HCatalog, and HiveServer 2, as well as Simon metrics. 

The following logs are collected by Starling:

- **Job History** - this provides detailed information on an executed job, including asks and task attempts, counters, etc.
- **Job Configuration** - this provides the configuration information from an executed job.
- **Job Summary** - this provides a summary of an executed job.
- **FSImage** - this is the file-system image for HDFS generated periodically by the Backup Node (or the Secondary
  NameNode in less-recent versions of Hadoop). This is collected every month by default.
- **Audit Logs** - these contain records of accesses and modifications to file-system objects.
- **Aggregator Dumps** - this provides periodic dumps of different metrics related to various sub-systems for a given cluster.


.. MapReduce JobHistory (Avro format)
   MapReduce Job Configuration (XML)
   MapReduce JobSummary (text files)
   HDFS fsimage (binary format, hadoop specific)
   HDFS NameNode audit logs (text files)
   Hadoop Simon metrics (JMX metrics serialized to text files)
   Hcatalog audit logs (text files)
   HiveServer2 audit logs – both encryped and unencrypted (text files)


When is it Collected?
---------------------

Data is collected every night, but trends are computed initially on a monthly 
basis. This may improve in the future.

How is it Collected?
--------------------

A predefined workflow triggers a series of steps, the first of which 
copies log files from Grid clusters into the Starling
warehouse cluster; the specific log files and clusters are defined by configuration. A subsequent 
step extracts information from the log files and stores it into Hive.
This workflow is executed by Oozie on a recurring basis.

Users can execute ad hoc queries against the data via HCatalog.

Data Retention Policy
---------------------

The retention policy depends on the dataset. We retain most data between one and two years.

Data Warehouse
==============

What is the Data Warehouse?
---------------------------

The Data Warehouse for Starling is simply the Hive database
``starling``. The various source logs are mapped to Hive tables through HCatalog. Thus, if you wanted to analyze
Job History logs, you could run a query against the ``starling_job_summary``
table.  


Where is the Data Warehouse?
----------------------------

You can access the Hive ``starling`` database on Axonite Blue (axonite-gw.blue.ygrid.yahoo.com).


How is the Data Warehouse Accessed?
-----------------------------------

Interactive access to Starling data is through Hive queries, but Pig can use
the library ``org.apache.hcatalog.pig.HCatLoader();`` to access
the tables through HCatalog. You can also use MapReduce 
through HCatalog.

To learn how, see `Getting Started <../getting_started/>`_ and
for examples in Hive and Pig, see `Query Bank <../query_bank>`_.


HCatalog Schemas
================

Introduction
------------

HCatalog is a metadata management layer for all Hadoop grids. All datasets and 
their schemas are registered in HCatalog, allowing engines such as Hive, Pig, 
or MapReduce to use this layer to get to the data on HDFS and the schema used to 
represent it.

How Does It Work?
#################

Raw and processed logs are stored using HCatalog and partitioned by cluster and date. 
These partitioning keys are grid (of type string) and within that ``dt`` (of type string 
and format YYYY_MM_DD) respectively. Partitioning drastically reduces the amount 
of data that Hive has to process to generate the results for most queries. 
If a log is collected more frequently than once per day, there will be another partition 
``tm`` (of type string and format HH_MM) within the ``dt`` partition.

 

..  Raw logs are stored after maximal compression to reduce storage requirements. 
    Processed logs are stored as compressed tables using columnar-storage provided by 
    the RCFile storage-format in order to maximize the potential for compression (as 
    many columns have the same values). Processed logs are accessed via Hive using 
    HiveQL to produce both canned and ad hoc reports. Apart from the primary tables 
    corresponding to the processed logs, Starling will also have secondary tables derived 
    from these primary tables in order to speed up the execution of common queries and 
    the generation of common reports. The retention of both raw and processed logs is 
    determined by an appropriate configuration of HCatalog.


.. important:: All tables reside in the default database (since HCatalog doesn't properly support 
               multiple databases yet) and have a prefix of ``starling_`` in their names to distinguish 
               them from other tables in the database. For example, the jobs table described below 
               will actually be accessible as the ``starling_jobs`` table in the default database in HCatalog.

Schemas for MapReduce
---------------------

Starling collects and processes the following logs related to MapReduce on a source cluster:


starling_jobs
#############

**Source Log:** Job History

The ``starling_jobs`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):

.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td><code>job_id</code></td>
   <td><code>string</code></td>
   <td> The identifier for the job within the cluster. </td>
   </tr>
   <tr>
   <td> <code>job_name</code> </td>
   <td> <code>string</code> </td>
   <td> The name of the job. </td>
   </tr>
   <tr>
   <td> <code>user</code> </td>
   <td> <code>string</code> </td>
   <td> The user who submitted the job. </td>
   </tr>
   <tr>
   <td> <code>queue</code> </td>
   <td> <code>string</code> </td>
   <td> The queue to which the job was submitted. </td>
   </tr>
   <tr>
   <td> <code>conf_loc</code> </td>
   <td> <code>string</code> </td>
   <td> The location on HDFS for the job configuration. </td>
   </tr>
   <tr>
   <td> <code>view_acl</code> </td>
   <td> <code>string</code> </td>
   <td> The access-control list for viewing the job. This is either empty, a <code>'*'</code> or space-separated lists of comma-separated users and groups respectively. </td>
   </tr>
   <tr>
   <td> <code>modify_acl</code> </td>
   <td> <code>string</code> </td>
   <td> The access-control list for modifying the job. This is either empty, a <code>'*'</code> or space-separated lists of comma-separated users and groups respectively. </td>
   </tr>
   <tr>
   <td> <code>priority</code> </td>
   <td> <code>string</code> </td>
   <td> The priority of the job (e.g., <code>NORMAL</code>). </td>
   </tr>
   <tr>
   <td> <code>status</code> </td>
   <td> <code>string</code> </td>
   <td> The final status of the job (e.g., <code>SUCCESS</code>, <code>FAILED</code>, <code>KILLED</code>, etc.). </td>
   </tr>
   <tr>
   <td> <code>submit_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time when the job was submitted in UTC as milliseconds since the UNIX epoch. </td>
   </tr>
   <tr>
   <td> <code>wait_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time in milliseconds spent by the job waiting to be launched.</td>
   </tr>
   <tr>
   <td> <code>run_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time in milliseconds spent by the job running after being launched. (The total time taken by the job is therefore <code>wait_time</code> + <code>run_time</code>.) </td>
   </tr>
   <tr>
   <td> <code>total_maps</code> </td>
   <td> <code>int</code> </td>
   <td> The total number of Map Tasks launched by the job. </td>
   </tr>
   <tr>
   <td> <code>total_reduces</code> </td>
   <td> <code>int</code> </td>
   <td> The total number of Reduce Tasks launched by the job. </td>
   </tr>
   <tr>
   <td> <code>finished_maps</code> </td>
   <td> <code>int</code> </td>
   <td> The number of Map Tasks that finished successfully. </td>
   </tr>
   <tr>
   <td> <code>finished_reduces</code> </td>
   <td> <code>int</code> </td>
   <td> The number of Reduce Tasks that finished successfully. </td>
   </tr>
   <tr>
   <td> <code>failed_maps</code> </td>
   <td> <code>int</code> </td>
   <td> The number of Map Tasks that failed. </td>
   </tr>
   <tr>
   <td> <code>failed_reduces</code> </td>
   <td> <code>int</code> </td>
   <td> The number of Reduce Tasks that failed. </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</a></td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td>The The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>

starling_job_counters
#####################

**Source Log:** Job History

The ``starling_job_counters`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):

	
.. raw:: html

   <table>
		<thead>
			<tr>
				<th>Column Name</th>
				<th>Type</th>
				<th>Description</th>
			</tr>
		</thead>
		<tbody>
			<tr>
				<td> <code>job_id</code> </td>
				<td> <code>string</code> </td>
				<td> The identifier for a job within the cluster. </td>
			</tr>
			<tr>
				<td> <code>map_counters</code> </td>
				<td> <code>map&lt;string,string&gt;</code> </td>
				<td> The aggregated Counters for Map Tasks for the job with the name of a Counter mapping to its value. </td>
			</tr>
			<tr>
				<td> <code>reduce_counters</code> </td>
				<td> <code>map&lt;string,string&gt;</code> </td>
				<td> The aggregated Counters for Reduce Tasks for the job with the name of a Counter mapping to its value. </td>
			</tr>
			<tr>
				<td> <code>total_counters</code> </td>
				<td> <code>map&lt;string,string&gt;</code> </td>
				<td> The overall Counters for the job with the name of a Counter mapping to its value. </td>
			</tr>
			<tr>
				<td> <code>grid</code> </td>
				<td> <code>string</code> </td>
				<td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</a></td>
			</tr>
			<tr>
				<td> <code>dt</code> </td>
				<td> <code>string</code> </td>
				<td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
			</tr>
       </tbody>
   </table>
		


starling_tasks
##############

**Source Log:** Job History

The ``starling_tasks`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):


.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>job_id</code> </td>
   <td> <code>string</code> </td>
   <td> The identifier for a job within the cluster. </td>
   </tr>
   <tr>
   <td> <code>task_id</code> </td>
   <td> <code>string</code> </td>
   <td> The identifier for a Task for the job. </td>
   </tr>
   <tr>
   <td> <code>type</code> </td>
   <td> <code>string</code> </td>
   <td> The type of the Task (e.g., <code>SETUP</code>, <code>MAP</code>, <code>REDUCE</code>, <code>CLEANUP</code>, etc.). </td>
   </tr>
   <tr>
   <td> <code>status</code> </td>
   <td> <code>string</code> </td>
   <td> The final status of the Task (e.g., <code>SUCCESS</code>, <code>FAILED</code>, <code>KILLED</code>, etc.). </td>
   </tr>
   <tr>
   <td> <code>splits</code> </td>
   <td> <code>string</code> </td>
   <td> The splits created for the Task. </td>
   </tr>
   <tr>
   <td> <code>start_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time when the Task started in UTC as milliseconds since the UNIX epoch. </td>
   </tr>
   <tr>
   <td> <code>run_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time in milliseconds taken by the Task to finish, if available, else <code>-1</code>. </td>
   </tr>
   <tr>
   <td> <code>error_msg</code> </td>
   <td> <code>string</code> </td>
   <td> The error-message for the Task, if any, else an empty string. </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</a></td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>
   



starling_task_counters
######################

**Source Log:** Job History

The ``starling_task_counters`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):


.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type/th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>task_id</code> </td>
   <td> <code>string</code> </td>
   <td> The identifier for a Task for a job. </td>
   </tr>
   <tr>
   <td> <code>counters</code> </td>
   <td> <code>map&lt;string,string&gt;</code> </td>
   <td> The Counters for the Task with the name of a Counter mapping to its value. </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>


starling_task_attempts
######################

**Source Log:** Job History

The ``starling_task_attempts`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):


.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>task_id</code> </td>
   <td> <code>string</code> </td>
   <td> The identifier for a Task for a job. </td>
   </tr>
   <tr>
   <td> <code>task_attempt_id</code> </td>
   <td> <code>string</code> </td>
   <td> The identifier for a Task Attempt for the Task. </td>
   </tr>
   <tr>
   <td> <code>type</code> </td>
   <td> <code>string</code> </td>
   <td> The type of the Task Attempt (e.g., <code>SETUP</code>, <code>MAP</code>, <code>REDUCE</code>, <code>CLEANUP</code>, etc.). </td>
   </tr>
   <tr>
   <td> <code>tracker_name</code> </td>
   <td> <code>string</code> </td>
   <td> The name of the Task Tracker for the Task Attempt. </td>
   </tr>
   <tr>
   <td> <code>http_port</code> </td>
   <td> <code>string</code> </td>
   <td> The HTTP port number for the Task Tracker for the Task Attempt. </td>
   </tr>
   <tr>
   <td> <code>host_name</code> </td>
   <td> <code>string</code> </td>
   <td> The host-name for the Task Attempt. </td>
   </tr>
   <tr>
   <td> <code>rack_id</code> </td>
   <td> <code>string</code> </td>
   <td> The rack-id, if available, for the Task Attempt. </td>
   </tr>
   <tr>
   <td> <code>status</code> </td>
   <td> <code>string</code> </td>
   <td> The final status of the Task Attempt (e.g., <code>SUCCESS</code>, <code>FAILED</code>, <code>KILLED</code>, etc.). </td>
   </tr>
   <tr>
   <td> <code>state</code> </td>
   <td> <code>string</code> </td>
   <td> The final state of the Task Attempt. </td>
   </tr>
   <tr>
   <td> <code>start_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time when the Task Attempt was started in UTC as milliseconds since the UNIX epoch. </td>
   </tr>
   <tr>
   <td> <code>shuffle_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time in milliseconds spent by the Task Attempt in the shuffle phase (valid only for Reduce Task Attempts, <code>0</code> otherwise). </td>
   </tr>
   <tr>
   <td> <code>sort_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time in milliseconds spent by the Task Attempt in the sort phase (valid only for Reduce Task Attempts, <code>0</code> otherwise). </td>
   </tr>
   <tr>
   <td> <code>finish_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time in milliseconds spent by the Task Attempt after being started (for a Map Task Attempt) or after the end of sort phase (for a Reduce Task Attempt). The total time taken by the Task Attempt is therefore <code>shuffle_time</code> + <code>sort_time</code> + <code>finish_time</code>. </td>
   </tr>
   <tr>
   <td> <code>error_msg</code> </td>
   <td> <code>string</code> </td>
   <td> The error-message for the Task Attempt, if any, else an empty string. </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</a></td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>
   


starling_task_attempt_counters
##############################


**Source Log:** Job History

The ``starling_task_attempt_counters`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):

.. raw:: html

   <table>
	<thead>
		<tr>
			<th>Column Name</th>
			<th>Type</th>
			<th>Description</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td> <code>task_attempt_id</code> </td>
			<td> <code>string</code> </td>
			<td>The identifier for a Task Attempt for a Task.</td>
		</tr>
		<tr>
			<td><code>counters</code></td>
			<td> <code>map&lt;string,string&gt;</code> </td>
			<td>The Counters for the Task Attempt with the name of a Counter mapping to its value. </td>
		</tr>
		<tr>
			<td><code>grid</code> </td>
			<td> <code>string</code> </td>
			<td> The partition variable. Grid job was run on 'AB' for AxoniteBlue.</td>
		</tr>
		<tr>
			<td><code>dt</code> </td>
			<td><code>string</code> </td>
			<td>The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
		</tr>
   </tbody></table>



starling_job_confs
##################

**Source Log:** Job Configuration 

The ``starling_job_confs`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):


.. raw:: html


   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>job_id</code> </td>
   <td> <code>string</code> </td>
   <td> The identifier for a job within the cluster. </td>
   </tr>
   <tr>
   <td> <code>params</code> </td>
   <td> <code>map&lt;string,string&gt;</code> </td>
   <td> The configuration parameters for the job with the name of a parameter mapping to its value. If a value has embedded tab or new-line characters, they are represented as <code>\t</code> and <code>\n</code> respectively (in order to prevent Hive from getting confused). </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</a></td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>
   


starling_job_summary
####################

**Source Log:** Job Summary

The ``starling_job_summary`` table (see MAPREDUCE-740) has the following schema (apart from the partitioning keys ``grid`` and ``dt``):

.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>job_id</code> </td>
   <td> <code>string</code> </td>
   <td> The identifier for the job within the cluster. </td>
   </tr>
   <tr>
   <td> <code>submit_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time when the job was submitted in UTC as milliseconds since the UNIX epoch. </td>
   </tr>
   <tr>
   <td> <code>wait_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time in milliseconds spent by the job waiting to be launched. </td>
   </tr>
   <tr>
   <td> <code>first_job_setup_task_launch_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time taken, in milliseconds, for the first job setup task to be initiated after the job launch. </td>
   </tr>
   <tr>
   <td> <code>first_map_task_launch_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time taken, in milliseconds, for the first map task to be initiated after the job launch. </td>
   </tr>
   <tr>
   <td> <code>first_reduce_task_launch_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time taken, in milliseconds, for the first reduce task to be initiated after the job launch. </td>
   </tr>
   <tr>
   <td> <code>first_job_cleanup_task_launch_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time taken, in milliseconds, for the first job cleanup to be initiated after the job launch. </td>
   </tr>
   <tr>
   <td> <code>run_time</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time taken in milliseconds by the job to complete after being launched. (The total time taken by the job is therefore wait_time + run_time.) </td>
   </tr>
   <tr>
   <td> <code>num_maps</code> </td>
   <td> <code>int</code> </td>
   <td> The number of Map Tasks spawned for the job. </td>
   </tr>
   <tr>
   <td> <code>num_slots_per_map</code> </td>
   <td> <code>int</code> </td>
   <td> The number of slots per Map Task for the job. </td>
   </tr>
   <tr>
   <td> <code>num_reduces</code> </td>
   <td> <code>int</code> </td>
   <td> The number of Reduce Tasks spawned for the job. </td>
   </tr>
   <tr>
   <td> <code>num_slots_per_reduce</code> </td>
   <td> <code>int</code> </td>
   <td> The number of slots per Reduce Task for the job. </td>
   </tr>
   <tr>
   <td> <code>user</code> </td>
   <td> <code>string</code> </td>
   <td> The user who submitted the job. </td>
   </tr>
   <tr>
   <td> <code>queue</code> </td>
   <td> <code>string</code> </td>
   <td> The queue to which the job was submitted. </td>
   </tr>
   <tr>
   <td> <code>status</code> </td>
   <td> <code>string</code> </td>
   <td> The final status of the job (e.g., <code>SUCCEEDED</code>, <code>FAILED</code>, <code>KILLED</code>, etc.). </td>
   </tr>
   <tr>
   <td> <code>map_slot_seconds</code> </td>
   <td> <code>bigint</code> </td>
   <td> The total Slot-time in seconds taken by Map Tasks for this job. </td>
   </tr>
   <tr>
   <td> <code>reduce_slots_seconds</code> </td>
   <td> <code>bigint</code> </td>
   <td> The total Slot-time in seconds taken by Reduce Tasks for this job. </td>
   </tr>
   <tr>
   <td> <code>cluster_map_capacity</code> </td>
   <td> <code>int</code> </td>
   <td> The cluster-wide capacity of Map Task Slots at the time the job finished. </td>
   </tr>
   <tr>
   <td> <code>cluster_reduce_capacity</code> </td>
   <td> <code>int</code> </td>
   <td> The cluster-wide capacity of Reduce Task Slots at the time the job finished. </td>
   </tr>
   <tr>
   <td> <code>job_name</code> </td>
   <td> <code>string</code> </td>
   <td> The name for the job. Populated only for Hadoop 1.0.2 clusters. Value would be NULL for Hadoop 0.20.205 clusters </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>
   


Schemas for HDFS
----------------

Starling collects and processes the following logs related to HDFS on a source cluster:


.. warning:: Unlike the data in other tables, the tables created from an FSImage (``fs_namespaces``, ``fs_entries``, and ``fs_blocks``) 
             represent a snapshot rather than incremental information for each period. You must 
             use a partition key with these tables to use the correct snapshot - otherwise your 
             queries will return incorrect results, not to mention scan a lot of data unnecessarily.


starling_fs_namespaces
######################

**Source Log:** FSImage

The ``starling_fs_namespaces`` table has following schema and describes the FSImage details and is partitioned by keys ``grid`` and ``dt`` :


.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>version</code> </td>
   <td> <code>int</code> </td>
   <td> The FSImage version (e.g., <code>-19</code>). </td>
   </tr>
   <tr>
   <td> <code>ns_id</code> </td>
   <td> <code>int</code> </td>
   <td> The ID of the FSImage Namespace. </td>
   </tr>
   <tr>
   <td> <code>gen_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> Generation stamp of the Namespace. </td>
   </tr>
   <tr>
   <td> <code>compressed</code> </td>
   <td> <code>boolean</code> </td>
   <td> If the FSImage was compressed when written. </td>
   </tr>
   <tr>
   <td> <code>codec</code> </td>
   <td> <code>string</code> </td>
   <td> Compression codec used in FSImage. </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>


starling_fs_entries
###################

**Source Log:** FSImage


The ``starling_fs_entries`` table describe the name space listing and has the following schema and is partitioned by keys ``grid`` and ``dt``:


.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>path</code> </td>
   <td> <code>string</code> </td>
   <td> The path of the INode (e.g., <code>/foo/bar/snafu</code>). </td>
   </tr>
   <tr>
   <td> <code>dir</code> </td>
   <td> <code>boolean</code> </td>
   <td> If given path is a directory. </td>
   </tr>
   <tr>
   <td> <code>replicas</code> </td>
   <td> <code>int</code> </td>
   <td> The number of times each block in the file is replicated. </td>
   </tr>
   <tr>
   <td> <code>ns_id</code> </td>
   <td> <code>int</code> </td>
   <td> The name-space identifier for the INode. </td>
   </tr>
   <tr>
   <td> <code>mod_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> The last modification time of the file in UTC format. In milliseconds since Epoch <code>let d=1278543204209/1000; date --date='1970-01-01 UTC '$d' seconds'</code> </td>
   </tr>
   <tr>
   <td> <code>acc_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> The last access time of the file in UTC format. In milliseconds since Epoch. </td>
   </tr>
   <tr>
   <td> <code>block_size</code> </td>
   <td> <code>bigint</code> </td>
   <td> The size of blocks that store the data for the file. </td>
   </tr>
   <tr>
   <td> <code>size</code> </td>
   <td> <code>bigint</code> </td>
   <td> The size of the file in bytes. </td>
   </tr>
   <tr>
   <td> <code>ns_quota</code> </td>
   <td> <code>bigint</code> </td>
   <td> The NS Quota of the file. </td>
   </tr>
   <tr>
   <td> <code>ds_quota</code> </td>
   <td> <code>bigint</code> </td>
   <td> The DS Quota of the file. </td>
   </tr>
   <tr>
   <td> <code>symlink</code> </td>
   <td> <code>String</code> </td>
   <td> Link target if the INode is a symlink. </td>
   </tr>
   <tr>
   <td> <code>user</code> </td>
   <td> <code>string</code> </td>
   <td> The user-name of the owner of this file (e.g., <code>dfsload</code>). </td>
   </tr>
   <tr>
   <td> <code>groupname</code> </td>
   <td> <code>string</code> </td>
   <td> The group-name of the owner of this file (e.g., <code>users</code>). </td>
   </tr>
   <tr>
   <td> <code>perms</code> </td>
   <td> <code>string</code> </td>
   <td> The permissions for the file as a 3-letter octal string (e.g., <code>755</code> for <code>rwxr-xr-x</code>). </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>


Notes
*****

Make sure you convert ``mod_ts`` and ``act_ts`` before calling any of the Hive date time functions otherwise, you'll get a nasty surprise.
e.g., ``select E.path``, ``from_unixtime(E.acc_ts)``, ``E.size``, ``E.user``, ``E.grid``, ``E.dt``, ``datediff(to_date(from_unixtime(round(E.acc_ts/1000)))``, 
``to_date(from_unixtime(unix_timestamp()))) as DAYS_OLD? from starling_fs_entries E where E.dir and datediff(to_date(from_unixtime(round(E.acc_ts/1000)))``, 
``to_date(from_unixtime(unix_timestamp()))) > 90 and grid='DG' and DT='2011_11_08' limit 10;``

The ``acc_ts`` should not be used at Yahoo. Most name nodes don't set this value when files 
are read due to performance issues. This value will always be set to the create time for 
the file or it will be set to epoch (epoch for files created before 0.20 hadoop was released).


starling_fs_blocks
##################

**Source Log:** FSImage

The ``starling_fs_blocks`` table has following schema and describes 
the Block details and is partitioned by keys ``grid`` and ``dt``:


.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>path</code> </td>
   <td> <code>string</code> </td>
   <td> The path of the INode (e.g., <code>/foo/bar/snafu</code>). </td>
   </tr>
   <tr>
   <td> <code>block_id</code> </td>
   <td> <code>bigint</code> </td>
   <td> The ID of the block representing the file. </td>
   </tr>
   <tr>
   <td> <code>size</code> </td>
   <td> <code>bigint</code> </td>
   <td> Size of the block representing the file in bytes. </td>
   </tr>
   <tr>
   <td> <code>gen_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> Generation of the block representing the file. </td>
   </tr>
   <tr>
   <td> <code>position</code> </td>
   <td> <code>int</code> </td>
   <td> Index position of the block for a given Inode, position of 0 says it is the first block and so on. </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>


.. note:: The version of the FSImage parser used in the Starling processor deliberately 
          omits information pertaining to INodeUnderConstruction and DelegationToken, which 
          are maintained by the NameNode.

starling_fs_audit
#################

**Source Log:** Name Node Audit

The ``starling_fs_audit`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):

.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>src_path</code> </td>
   <td> <code>string</code> </td>
   <td> Path of the source file/directory. </td>
   </tr>
   <tr>
   <td> <code>cmd_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time when the command was executed on the file in UTC as milliseconds since the UNIX epoch. </td>
   </tr>
   <tr>
   <td> <code>cmd</code> </td>
   <td> <code>string</code> </td>
   <td> The command that was executed ( <code>open</code>, <code>create</code>, <code>delete</code>, <code>liststatus</code>, <code>mkdirs</code>, <code>rename</code>, <code>setOwner</code>, <code>setPermission</code>, <code>setReplication</code>). </td>
   </tr>
   <tr>
   <td> <code>ugi</code> </td>
   <td> <code>string</code> </td>
   <td> The user-group information (UGI) on whose behalf the command was executed (e.g., <code>gmetrics@YGRID.YAHOO.COM</code>). </td>
   </tr>
   <tr>
   <td> <code>ip_addr</code> </td>
   <td> <code>string</code> </td>
   <td> The IP address from where the command was received (e.g., <code>98.137.112.252</code>). </td>
   </tr>
   <tr>
   <td> <code>dest_path</code> </td>
   <td> <code>string</code> </td>
   <td> Path of the destination file/directory </td>
   </tr>
   <tr>
   <td> <code>user</code> </td>
   <td> <code>string</code> </td>
   <td> The user-name of the <em>owner</em> of this file (e.g., <code>dfsload</code>). Note that this is <em>not</em> the user who executed the command (see <code>ugi</code> instead). </td>
   </tr>
   <tr>
   <td> <code>groupname</code> </td>
   <td> <code>string</code> </td>
   <td> The group-name of the owner of this file (e.g., <code>users</code>). </td>
   </tr>
   <tr>
   <td> <code>perms</code> </td>
   <td> <code>string</code> </td>
   <td> String representation of the file permissions (e.g., <code>rwx--r---</code>) </td>
   </tr>
   <tr>
   <td> <code>grid</code> </td>
   <td> <code>string</code> </td>
   <td>The abbreviation of the grid cluster. For example, the value for Axonite Blue would be 'AB'.</td>
   </tr>
   <tr>
   <td> <code>dt</code> </td>
   <td> <code>string</code> </td>
   <td> The partition variable. Date when job was run e.g., <code>YYYY_MM_DD</code> </td>
   </tr>
   </tbody></table>


Schemas for Simon
-----------------

Starling collects and processes the following logs related to Simon on a source cluster:



starling_simon_reports
######################

**Source Log:** Aggregator Dumps

The ``simon_reports`` table has the following schema (apart from the partitioning keys ``grid`` and ``dt``):

.. raw:: html

   <table>
   <thead>
   <tr>
   <th>Column Name</th>
   <th>Type</th>
   <th>Description</th>
   </tr>
   </thead>
   <tbody>
   <tr>
   <td> <code>app_name</code> </td>
   <td> <code>string</code> </td>
   <td> The application corresponding to the report (e.g., <code>jvm</code>). </td>
   </tr>
   <tr>
   <td> <code>report_name</code> </td>
   <td> <code>string</code> </td>
   <td> The name of the report (e.g., <code>JVM</code>). </td>
   </tr>
   <tr>
   <td> <code>report_cluster</code> </td>
   <td> <code>string</code> </td>
   <td> The cluster for the report (e.g., <code>jvm.mithrilgold</code>). Note that this is <em>not</em> the same as the value of the <code>grid</code> partitioning key. </td>
   </tr>
   <tr>
   <td> <code>report_version</code> </td>
   <td> <code>string</code> </td>
   <td> The version of the report (e.g., <code>0.1.0.0</code>). </td>
   </tr>
   <tr>
   <td> <code>report_period</code> </td>
   <td> <code>int</code> </td>
   <td> The period after which the report is generated (e.g., <code>60</code>). </td>
   </tr>
   <tr>
   <td> <code>report_ts</code> </td>
   <td> <code>bigint</code> </td>
   <td> The time when the report was generated in UTC as milliseconds since the UNIX epoch. </td>
   </tr>
   <tr>
   <td> <code>report_item</code> </td>
   <td> <code>string</code> </td>
   <td> The name of an item within the report (e.g., <code>by node name</code>). </td>
   </tr>
   <tr>
   <td> <code>tags</code> </td>
   <td> <code>map&lt;string,string&gt;</code> </td>
   <td> The tag-values for a particular row within a report-item with the name of a tag mapping to its value. </td>
   </tr>
   <tr>
   <td> <code>metrics</code> </td>
   <td> <code>map&lt;string,string&gt;</code> </td>
   <td> The reported metrics for a particular row within a report-item with the name of a metric mapping to its value. </td>
   </tr>
   </tbody></table>


Notes
*****

The Simon aggregator dumps are processed on a "best-effort" basis due to the way 
the metrics are collected and the dumps captured and made available to Starling. 
It is quite possible therefore to see missing or duplicate metrics in this table. 
If you want a unique row for a given metric for a given time-stamp, you must put the 
appropriate ``DISTINCT`` clauses in your queries.

There are at least 15 different types of reports recorded: 

- FSNamesystem status 
- by node name 
- by process name
- by session
- HDFS throughput
- individual datanode throughput
- JobTracker
- JobTracker totals
- NameNode operations
- perCluster
- perDisk 
- perNode 
- shuffle output by host 
- tasktracker and tasktracker totals

Be sure to select the right report type to avoid commingling disparate data.
