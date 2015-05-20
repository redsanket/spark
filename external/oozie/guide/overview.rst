.. _overview:

Overview
========

.. 04/23/15: Rewrote.
.. 05/15/15: Edited.

.. _overview-what:

What is Oozie?
--------------

Oozie is a Java Web application running in a Java servlet-container
that executes Workflow jobs consisting of one or more actions. 
It is an extensible, scalable, and reliable system that allows you to 
define, manage, schedule, and execute complex Hadoop workloads 
through Web services. 

.. _overview-why:

Why Use Oozie?
--------------

Most work on the grid requires executing many actions in
a series of steps. For example, you may need to
execute the following actions in the given order:

- MapReduce
- Pig
- Streaming
- HDFS operation such as ``mkdir``, ``chmod``

Without a workflow system such as Oozie, you would have to use
your own custom system based on shell scripts and cron jobs.
Building and running custom systems is expensive, 
and you may not have the operations, development, and hardware support.

.. _overview-features:

Oozie Features
~~~~~~~~~~~~~~

Oozie provides the following:

- An XML-based declarative framework to specify a job or a complex workflow of dependent jobs.
- Support for different types of jobs such as Hadoop MapReduce, Pipe, Streaming, Pig, Hive and custom java applications.
- Workflow scheduling based on frequency and/or data availability.
- Monitoring capability, automatic retry, and failure handling of jobs.
- Extensible and pluggable architecture to allow arbitrary grid programming paradigms.
- Authentication, authorization, and capacity-aware load throttling to allow multi-tenant software as a service.
- HBase access through Oozie with credentials
- HCatalog access through Oozie with credentials
- Email actions
- DistCp actions (intra as well as inter-cluster copy)
- Shell actions (run any script, e.g., Perl, Python, Hadoop CLI)
- Workflow dry-run & fork-join validation
- Bulk monitoring (REST API)
- Workflow Expression Language (EL) functions and Coordinator EL functions
  for parameterized Workflows
- Job DAGs

.. Left off here on 04/23/15.

.. _overview-concepts:

Basic Concepts
--------------

Before you get started, you should be familiar with some basic concepts
that will help you understand the documentation and better use Oozie.

.. _concepts-actions:

Actions
~~~~~~~

Oozie actions are execution or computation tasks such as a MapReduce job, a Pig 
job, or a shell command. Actions are also referred to as tasks or 'action nodes'.

.. _concepts-workflows:

Workflows
~~~~~~~~~

Oozie Workflows are blueprints for executing jobs. More specifically, these
blueprints are `directed acyclic graphs <http://en.wikipedia.org/wiki/Directed_acyclic_graph>`_
that structure the execution of Hadoop actions such as MapReduce, Pig, Hive, shell script, 
custom Java code, etc.

.. _concepts-coordinators:

Coordinators
~~~~~~~~~~~~

Oozie Coordinator jobs are recurring Oozie Workflow jobs 
triggered by time (frequency) and data availability.

.. _concepts-bundles:

Bundles
~~~~~~~

Bundles are a set of Coordinator applications and often known has data pipelines. 
Users can start, stop, suspend, resume, and rerun Bundles.
Although there is no explicit dependency among Coordinators of a Bundle, one of 
the primary reasons for using Bundles is to execute Coordinator applications that 
share a data dependency. For example, one Coordinator may need to wait for 
data created by another Coordinator before running, and Bundles allow users to 
define and control this data dependency between the Coordinators.


.. _overview-accessing:

Accessing Oozie
---------------

To access Oozie, users and client programs need one URL to
connect to the following:

- Web UI,
- REST/Java API
- JobTracker/ResourceManager callbacks

You can also use the load balancer, VIP/DNS round-robin 
to provide one entry point to the Oozie servers
See :ref:`Oozie Servers on Clusters <references-oozie_servers>` table
for the URLs to the Oozie UIs on the different clusters.

.. _overview-logging:

Oozie Logs
----------

Oozie log files are not stored in a database, and jobs are not specified
to a specific Oozie server. Although each Oozie server only has access 
to its own logs, you can request an Oozie server to retrieve the logs
of another Oozie server. If an Oozier server goes down, however, you cannot
retrieve logs from that server.

To view logs, you can either go to one of the :ref:`Oozie UIs <references-oozie_servers>` 
and click a job ID or run the following Oozie command with your job ID: 
``$ oozie job -log <oozie-jobID> -auth kerberos``


