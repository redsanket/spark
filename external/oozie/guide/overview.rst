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
- Support for different types of jobs such as Hadoop Map-Reduce, Pipe, Streaming, Pig, Hive and custom java applications.
- Workflow scheduling based on frequency and/or data availability.
- Monitoring capability, automatic retry, and failure handling of jobs.
- Extensible and pluggable architecture to allow arbitrary grid programming paradigms.
- Authentication, authorization, and capacity-aware load throttling to allow multi-tenant software as a service.
- HBase access through Oozie with credentials
- HCatalog access through Oozie with credentials
- Email actions
- DistCP actions (intra as well as inter-cluster copy)
- Shell actions (run any script, e.g., Perl, Python, Hadoop CLI)
- Workflow dry-run & fork-Join validation
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

Oozie actions are execution or computation tasks such as a Map-Reduce job, a Pig 
job, or a shell command. Actions are also referred to as tasks or 'action nodes'.

.. _concepts-workflows:

Workflows
~~~~~~~~~

Oozie Workflows are blueprints for executing jobs. More specifically, these
blueprints are `directed acyclic graphs <http://en.wikipedia.org/wiki/Directed_acyclic_graph>`_
that structure execution of Hadoop actions such as MapReduce, Pig, Hive, shell script, 
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
have share a data dependency. For example, one Coordinator may need to wait for 
data created by another Coordinator before running, and Bundles allow users to 
define and control this data dependency between the Coordinators.

.. _overview-use_cases:

Use Cases 
---------

To help you understand how you might use Oozie, we will
describe the most common use cases in the following sections.

.. _use_cases-time:

Time Triggers
~~~~~~~~~~~~~

One of the most common uses for Oozie is to execute a Workflow 
at certain intervals. To do this, you would define a :ref:`Coordinator <concepts-coordinators>` 
through a Coordinator XML file. In the  
Coordinator XML (``coordinator.xml``) below, the start time, end time, 
and the frequency to run the applications is defined as attributes
in the ``<coordinator-app>`` element.

.. code-block:: xml

   <coordinator-app name=“my_coord” start="2009-01-07T23:59Z" end="2010-01-01T00:00Z" 
     frequency=${coord:minutes(15)} 
     xmlns="uri:oozie:coordinator:0.4">
     <action>
       <workflow>
         <app-path>hdfs://bar:9000/usr/abc/logsprocessor-wf</app-path>
         <configuration>
           <property> <name>key</name><value>val</value></property>
         </configuration>
       </workflow>
     </action>      
   </coordinator-app>


.. _use_cases-time_data:

Time and Data Triggers
~~~~~~~~~~~~~~~~~~~~~~

Sometimes you may only want to run Workflows when input data is available.
Oozie lets you *materialize* (create but not run) a Workflow at a specified
interval, but only run the Workflow when input data is ready.

The ``coordinator.xml`` below defines input events and then uses
the built-in expression language (EL) function ``dataIn`` to
check if the input data (``inputLogs``) is available.

.. code-block:: xml

   <coordinator-app name=“coord1” frequency=“${coord:hours(1)}”…> 
     <datasets>
       <dataset name="logs" frequency=“${coord:hours(1)}” initial-instance="2009-01-01T23:59Z">
         <uri-template>hdfs://bar:9000/app/logs/${YEAR}/${MONTH}/${DAY}/${HOUR}</uri-template>
       </dataset>
     </datasets>
     <input-events>
       <data-in name=“inputLogs” dataset="logs">
         <instance>${coord:current(0)}</instance>
       </data-in>
     </input-events>
     <action>
       <workflow>
         <app-path>hdfs://bar:9000/usr/abc/logsprocessor-wf</app-path>
         <configuration>
           <property> <name>inputData</name><value>${dataIn(‘inputLogs’)}</value> </property>
         </configuration>
       </workflow>
     </action>
     ...
   ...


.. _use_cases-rolling:

Rolling Window
~~~~~~~~~~~~~~

You can also access datasets at a smaller interval and then roll them
up at a larger frequency. 

For example, in the ``coordinator.xml`` below, but the Coordinator itself runs
every hour, so you can roll the 15-minute datasets into hourly datasets.

.. code-block:: xml

   <coordinator-app name=“coord1” frequency=“${coord:hours(1)}”…> 
     <datasets>
       <dataset name="logs" frequency=“${coord:minutes(15)}” initial-instance="2009-01-01T00:00Z">
         <uri-template>hdfs://bar:9000/app/logs/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}</uri-template>
       </dataset>
     </datasets>
     <input-events>
       <data-in name=“inputLogs” dataset="logs">
         <start-instance>${current(-3)}</start-instance>
         <end-instance>${current(0)}</end-instance>
       </data-in>
     </input-events>
     <action>
       <workflow>
         <app-path>hdfs://bar:9000/usr/abc/logsprocessor-wf</app-path>
         <configuration>
           <property> <name>inputData</name><value>${dataIn(‘inputLogs’)}</value> </property>
         </configuration>
       </workflow>
     </action>      
   </coordinator-app>

.. _use_cases-sliding:

Sliding Window
~~~~~~~~~~~~~~

Another less common use case is when you need to frequently access past data and
then roll it up. 

For example, the ``coordinator.xml`` below every hour accesses the past 24 hours of data and rolls 
that data up. You can see that the frequency for the Coordinator is every hour but that the input 
event is for 24 hours (``${current(0)} to ``${current(-23)}``).

.. code-block:: xml

   <coordinator-app name=“coord1” frequency=“${coord:hours(1)}”…> 
     <datasets>
       <dataset name="logs" frequency=“${coord:hours(1)}” initial-instance="2009-01-01T00:00Z">
         <uri-template>hdfs://bar:9000/app/logs/${YEAR}/${MONTH}/${DAY}/${HOUR}</uri-template>
       </dataset>
     </datasets>
     <input-events>
       <data-in name=“inputLogs” dataset="logs">
         <start-instance>${current(-23)}</start-instance>
         <end-instance>${current(0)}</end-instance>
       </data-in>
     </input-events>
     <action>
       <workflow>
         <app-path>hdfs://bar:9000/usr/abc/logsprocessor-wf</app-path>
         <configuration>
           <property> <name>inputData</name><value>${dataIn(‘inputLogs’)}</value> </property>
         </configuration>
       </workflow>
     </action>      
   </coordinator-app>

.. _overview-use_patterns:

Use Patterns 
------------

We discussed the common use cases, which typically deal with time and data dependencies.
In this section, we'll look at Workflows from the perspective of data: management, modeling, and
flow. 

.. _use_patterns-data_management:

Simple Data Management
~~~~~~~~~~~~~~~~~~~~~~

The following are some of the basic data management tasks
that you might use Oozie for:

- data transformation/filtering/Ybeacon
- data metrics
- directory management
- copying input data 
- data replication
- clean up feed/data cleanup
- generate data

For example, you might have a Oozie workflow that
copies an input feed, transforms the data, writes
the resulting data to HDFS, and then deletes
the copied input feed.

.. _use_patterns-data_modeling:

Data Modeling
~~~~~~~~~~~~~

You can also use Oozie to process and analyze multiple
streams of data. The following are examples
of how you might perform data modeling with Oozie:

- process logs in parallel
- parse ad events and train data (Moneyball)
- consolidate Tweets
- process Moneyball bids
- process user engagement
- check retention rate

As you can see from the list above, many uses
of Oozie for data modeling are useful for user
and ad data. For example, you could create 
a Workflow/Coordinator to extract ad events, join
them, compute derived features, and then send
out email notifications containing these features. 

.. _use_patterns-data_pipeline:

Complete Data Pipeline 
~~~~~~~~~~~~~~~~~~~~~~

The data pipeline is a complex set of actions and interdependencies. As we
said earlier, in Oozie, Bundles are also known as data pipelines. Thus,
your data pipeline will generally involve a set of Coordinators, each
Coordinator with one Workflow job that may contain multiple actions.
Often data dependencies will exist between Coordinators and at the Workflow level. 
Thus, you might need use a complete data pipeline for
the following:

- stream video pipeline
- complete data transformation pipeline
- data ingestion

The following diagram shows a simplified 
flow of streamed data. Keep in mind that
each task represented by a gray box
could involve multiple Coordinators and Workflows.

.. image:: images/data_pipeline.jpg
   :height: 502px
   :width: 507 px
   :scale: 95 %
   :alt: Data Pipeline Work Flow
   :align: left

.. _use_patterns-end_data_processing:

End-to-End Data Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~

The end-to-end data processing 
involves a pipeline but also 
closes the process by generally
writing or storing results.

For example, you may need end-to-end data
processing for the following:

- ingesting data
- processing links with Slingstone 

The diagram below shows how data is analyzed
based on conditions and later joined before 
being ultimately written, in this case, to HBase.

.. image:: images/end-to-end-processing.jpg
   :height: 513px
   :width: 506 px
   :scale: 95 %
   :alt: End-to-end processing.
   :align: left

.. _overview-architecture:

Architecture 
------------

From the diagram below, you can see that Oozie is a Java Web application
that provides a Web service and internally uses a DAG engine to process
Workflows, Coordinators, and Bundles.  Oozie also stores
state in an Oracle database (submitted jobs, workflow definitions, etc.)

.. image:: images/architecture_overview.jpg
   :height: 462px
   :width: 760 px
   :scale: 95 %
   :alt: Oozie Architectural Diagram
   :align: left

The diagram does fail to show two important aspects of the architecture:

- Instead of a fail-over model, many Oozie servers access the same database.
- ZooKeeper handles the coordination of Hadoop jobs, thus to access
  HBase tables on different clusters, you need to provide information
  about the ZooKeeper znodes and quorums.

.. _architecture-stack:

Technology Stack
~~~~~~~~~~~~~~~~

Oozie relies on the following technologies:

- **Apache Tomcat** - used for the REST Web service and for communicating
  with HDFS, Hive, and Pig.
- **Workflow Lite Library** - parses Oozie configuration files, creating instances, 
  controlling logic and execution of Workflows
- **HDFS** - used to store deployed applications (Hadoop distributed cache)
- **Oracle DB** - used for persisting Workflow jobs state 

.. image:: images/oozie_stack.jpg
   :height: 277px
   :width: 685 px
   :scale: 95 %
   :alt: Oozie Technology Stack
   :align: left

.. _architecture-abstraction_layer:

Abstraction Layer
~~~~~~~~~~~~~~~~~

The abstraction layer represents the structure of how Hadoop actions
are organized and the execution flow.

.. image:: images/oozie_layers.jpg
   :height: 791px
   :width: 950 px
   :scale: 90 %
   :alt: Oozie Abstraction Layer
   :align: left

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


