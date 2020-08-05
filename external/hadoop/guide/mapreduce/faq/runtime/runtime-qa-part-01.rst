Writing MapReduce? code with ToolsRunner? that accepts `-D` and other options through `GenericParser`
=====================================================================================================

- Please follow example in :ref:`Getting Started Sec. <getting_started_yhadoop>`.
- ``ToolRunner`` can be used to run classes implementing Tool interface. It works in conjunction with ``GenericOptionsParser`` to parse the generic hadoop command line arguments and modifies the Configuration of the ``Tool``. The application-specific options are passed along without being modified.

All hadoop commands are invoked by the bin/hadoop script. Running the hadoop script without any arguments prints the description for all commands.

**Usage:**

  .. code-block:: bash

    hadoop [--config confdir] [--loglevel loglevel] [COMMAND] \
    	[GENERIC_OPTIONS] [COMMAND_OPTIONS]


Hadoop has a set of generaic options that honor a common set of configuration options to alter their behavior:

+--------------------------------------------------+-------------------------------------------------------------------------------------------------+
|                  GENERIC_OPTION                  | Description                                                                                     |
+==================================================+=================================================================================================+
| ``-archives <comma separated list of archives>`` | Specify comma separated archives to be unarchived on the compute machines. Applies only to job. |
+--------------------------------------------------+-------------------------------------------------------------------------------------------------+
| ``-conf <configuration file>``                   | Specify an application configuration file.                                                      |
+--------------------------------------------------+-------------------------------------------------------------------------------------------------+
| ``-D <property>=<value>``                        | Use value for given property.                                                                   |
+--------------------------------------------------+-------------------------------------------------------------------------------------------------+
| ``-files <comma separated list of files>``       | Specify comma separated files to be copied to the map reduce cluster. Applies only to job.      |
+--------------------------------------------------+-------------------------------------------------------------------------------------------------+
| ``-fs <file:///> or <hdfs://namenode:port>``     | Specify a ResourceManager. Applies only to job.                                                 |
+--------------------------------------------------+-------------------------------------------------------------------------------------------------+
| ``-libjars <comma seperated list of jars>``      | Specify comma separated jar files to include in the classpath. Applies only to job.             |
+--------------------------------------------------+-------------------------------------------------------------------------------------------------+

All of these commands are executed from the hadoop shell command.


Command to interact with Map Reduce Jobs.

  .. code-block:: bash

    mapred job | [GENERIC_OPTIONS] | [-submit <job-file>] | [-status <job-id>] \
    	| [-counter <job-id> <group-name> <counter-name>] | [-kill <job-id>] \
    	| [-events <job-id> <from-event-#> <#-of-events>] \
    	| [-history [all] <jobHistoryFile|jobId> [-outfile <file>] \
    	| [-format <human|json>]] [-list [all]] | [-kill-task <task-id>] \
    	| [-fail-task <task-id>] \
    	| [-set-priority <job-id> <priority>] | [-list-active-trackers] \
    	| [-list-blacklisted-trackers] \
    	| [-list-attempt-ids <job-id> <task-type> <task-state>] \
    	| [-logs <job-id> <task-attempt-id>] [-config <job-id> <file>]


+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                 COMMAND_OPTION                |                                                                                                                                                                                       Description                                                                                                                                                                                       |
+===============================================+=========================================================================================================================================================================================================================================================================================================================================================================================+
| ``-submit job-file``                          | Submits the job.                                                                                                                                                                                                                                                                                                                                                                        |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-status job-id``                            | Prints the map and reduce completion percentage and all job counters.                                                                                                                                                                                                                                                                                                                   |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-counter job-id group-name counter-name``   | Prints the counter value.                                                                                                                                                                                                                                                                                                                                                               |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-kill job-id``                              | Kills the job.                                                                                                                                                                                                                                                                                                                                                                          |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-events job-id from-event-# #-of-events``   | Prints the events’ details received by jobtracker for the given range.                                                                                                                                                                                                                                                                                                                  |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|  | ``-history [all] jobHistoryFile|jobId \``  | Prints job details, failed and killed task details. More details about the job such as successful tasks, task attempts made for each task, task counters, etc can be viewed by specifying the ``[all]`` option. An optional file output path (instead of stdout) can be specified. The format defaults to human-readable but can also be changed to JSON with the ``[-format]`` option. |
|  |   ``[-outfile file] [-format human|json]`` |                                                                                                                                                                                                                                                                                                                                                                                         |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-list [all]``                               | Displays jobs which are yet to complete. ``-list all`` displays all jobs.                                                                                                                                                                                                                                                                                                               |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-kill-task task-id``                        | Kills the task. Killed tasks are NOT counted against failed attempts.                                                                                                                                                                                                                                                                                                                   |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-fail-task task-id``                        | Fails the task. Failed tasks are counted against failed attempts.                                                                                                                                                                                                                                                                                                                       |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-set-priority job-id priority``             | Changes the priority of the job. Allowed priority values are ``VERY_HIGH``, ``HIGH``, ``NORMAL``, ``LOW``, ``VERY_LOW``                                                                                                                                                                                                                                                                 |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-list-active-trackers``                     | List all the active ``NodeManagers`` in the cluster.                                                                                                                                                                                                                                                                                                                                    |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-list-blacklisted-trackers``                | List the black listed task trackers in the cluster. This command is not supported in MRv2 based cluster.                                                                                                                                                                                                                                                                                |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|  | ``-list-attempt-ids job-id task-type \``   | List the attempt-ids based on the task type and the status given. Valid values for task-type are ``REDUCE``, ``MAP``. Valid values for task-state are running, pending, completed, failed, killed.                                                                                                                                                                                      |
|  |    ``task-state``                          |                                                                                                                                                                                                                                                                                                                                                                                         |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-logs job-id task-attempt-id``              | Dump the container log for a job if ``taskAttemptId`` is not specified, otherwise dump the log for the task with the specified ``taskAttemptId``. The logs will be dumped in system out.                                                                                                                                                                                                |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``-config job-id file``                       | Download the job configuration file.                                                                                                                                                                                                                                                                                                                                                    |
+-----------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

.. seealso:: :hadoop_rel_doc:`Hadoop Commands Guide <hadoop-project-dist/hadoop-common/CommandsManual.html>` and :hadoop_rel_doc:`MapReduce Commands Guide
 <hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapredCommands>` for Hadoop |HADOOP_RELEASE_VERSION|.



How to determine the Hadoop version
===================================

To find out which version of Hadoop is running on a cluster, use ``hadoop version``
(Note: ``hadoop -version`` gets you the java version instead)


Which MapReduce APIs to use
===========================

* "MRv1" MapReduce APIs - ``org.apache.hadoop.mapred.*``
* "MRv2" MapReduce APIs - ``org.apache.hadoop.mapreduce.*``

It is recommended to use the new API (MRv2). 

In Apache Hadoop 2.x we have spun off resource management capabilities into Apache Hadoop YARN, a general purpose, distributed application management framework while Apache Hadoop MapReduce (aka MRv2) remains as a pure distributed computation framework.
MRv2 is able to ensure satisfactory compatibility with MRv1 applications.

+---------------------------------------------------+---------------------------------------------------+
| Problematic Function                              | Incompatibility Issue                             |
+===================================================+===================================================+
| ``org.apache.hadoop.util.ProgramDriver#drive``    | Return type changes from ``void`` to ``int``      |
+---------------------------------------------------+---------------------------------------------------+
|  | ``org.apache.hadoop.mapred.jobcontrol.Job``    | Return type changes from ``String`` to ``JobID``  |
|  | ``#getMapredJobID``                            |                                                   |
+---------------------------------------------------+---------------------------------------------------+
| ``org.apache.hadoop.mapred.TaskReport#getTaskId`` | Return type changes from ``String`` to ``TaskID`` |
+---------------------------------------------------+---------------------------------------------------+
|  | ``org.apache.hadoop.mapred.ClusterStatus``     | Data type changes from ``long`` to ``int``        |
|  | ``#UNINITIALIZED_MEMORY_VALUE``                |                                                   |
+---------------------------------------------------+---------------------------------------------------+
|  | ``org.apache.hadoop.mapreduce.filecache``      |  | Return type changes from ``long[]``            |
|  | ``.DistributedCache#getArchiveTimestamps``     |  | to ``String[]``                                |
+---------------------------------------------------+---------------------------------------------------+
|  | ``org.apache.hadoop.mapreduce.filecache``      |  | Return type changes from ``long[]``            |
|  | ``.DistributedCache#getFileTimestamps``        |  | to ``String[]``                                |
+---------------------------------------------------+---------------------------------------------------+
| ``org.apache.hadoop.mapreduce.Job#failTask``      | Return type changes from ``void`` to ``boolean``  |
+---------------------------------------------------+---------------------------------------------------+
| ``org.apache.hadoop.mapreduce.Job#killTask``      | Return type changes from ``void`` to ``boolean``  |
+---------------------------------------------------+---------------------------------------------------+
|  | ``org.apache.hadoop.mapreduce``                |  | Return type changes from                       |
|  | ``.Job#getTaskCompletionEvents``               |  | ``o.a.h.mapred.TaskCompletionEvent[]`` to      |
|                                                   |  | ``o.a.h.mapreduce.TaskCompletionEvent[]``      |
+---------------------------------------------------+---------------------------------------------------+


What queue(s) should I use?
===========================

Users submit jobs to Queues. Queues, as collection of jobs, allow the system to provide specific functionality. For example, queues use ACLs to control which users who can submit jobs to them.

- In order to display the queue name and associated queue operations allowed for the current user. The list consists of only those queues to which the user has access.
  
  .. code-block:: bash

    mapred queue -showacls

- To request new queue access, please request through `Hadoop Queue Slingshot on doppler <https://supportshop.cloud.corp.yahoo.com:4443/doppler/hadoop/cluster/DR/queue/slingshot>`_.


How to access job status
========================

I want to get job stats like number of map input records, reduce output records etc after the job is complete.

**Ans:** To access your jobs stats after the job is completed, try the following.

* Go to the `YGrid Versions <https:/yo/ygridversions>`_ page and click the cluster configuration link for your cluster.
* On the Cluster RM page, click the Job Tracker link. (i.e., Tracking UI column).
* click on the row equivalent to your Job. It will take you to the details of the Job

* You can see the history summary of job using the command line:

  .. code-block:: bash

  	# find the status of a job
  	mapred job -status [jobId]
  	mapred job -history [jobId]


How to access log files
=======================

The logs are available through the Hadoop GUI.

* Go to the `YGrid Versions <https:/yo/ygridversions>`_ page and click the cluster configuration link for your cluster.
* On the Cluster RM page, click the Job Tracker link. (i.e., Tracking UI column).
* At the top corner of the page click :menuselection:`Tools --> Local Logs`.
* You can also view the logs of an application from the commad line

  .. code-block:: bash

    mapred logs job-id [task-attempt-id]


How to kill a job
=================

Use this command: ``mapred job -kill job-id``


How to fail or kill a task
==========================

``mapred job –fail-task task-id`` or ``mapred job -kill-task task-id``


The first form adds to the tasks fail count, the second does not.
A task is allowed to fail four times before hadoop kills the job.
The task ID should look something like: ``task_1594783077330_219187_m_000000`` (for a map task).


How to give colleagues access to your online job logs
=======================================================

When an application runs, generates logs, and then places the logs into HDFS.
By default, job logs are only readable by the person who submitted the logs.
To provide read access to the MapReduce history and the YARN logs, list the user names using the following configuration: ``-D mapreduce.job.acl-view-job="user1,user2"``. To give access only to group gridpe, set ``-D mapreduce.job.acl-view-job=" gridpe"``.

Note the space before the gridpe. To give access to all users, set ``-D mapreduce.job.acl-view-job="*".``, but please understand that you're making the jobconf wide open. (could become security issue depending on the application you run).