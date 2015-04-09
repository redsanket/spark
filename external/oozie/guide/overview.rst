Overview
========

What is Oozie?
--------------

Oozie is an extensible, scalable and reliable system to define, manage, schedule, 
and execute complex Hadoop workloads via web services. More specifically, this includes:

  * XML-based declarative framework to specify a job or a complex workflow of dependent jobs.
  * Support different types of job such as Hadoop Map-Reduce, Pipe, Streaming, Pig, Hive and custom java applications.
  * Workflow scheduling based on frequency and/or data availability.
  * Monitoring capability, automatic retry and failure handing of jobs.
  * Extensible and pluggable architecture to allow arbitrary grid programming paradigms.
  * Authentication, authorization, and capacity-aware load throttling to allow multi-tenant software as a service.

Oozie is a server based Workflow Engine specialized in running workflow jobs with actions that run Hadoop Map/Reduce and Pig jobs.

Oozie is a Java Web-Application that runs in a Java servlet-container.

Why Use Oozie?
--------------

Most work on the grid requires executing many actions in
a series of steps. For example, you may need to
execute the following actions in the given order:

- MapReduce
- Pig
- Streaming
- HDFS operation such as ``mkdir``, ``chmod``

With a workflow system such as Oozie, you would have to use
your own custom system based on shell scripts, cron jobs, etc.
The problem with custom systems is the cost of building and running 
applications is higher, and you don't have the 
operations, development, and hardware support.

Oozie Features
--------------

- HBase access through Oozie, via credentials
- HCatalog access through Oozie, via credentials
- Email action
- DistCP action (intra as well as inter-cluster copy)
- Shell action (run any script e.g. perl, python, hadoop CLI)
- Workflow dry-run & Fork-Join validation
- Bulk monitoring (REST API)
- Coordinator EL functions for parameterized workflows
- Job DAG


Use Cases 
---------

Time Triggers
~~~~~~~~~~~~~

Execute your workflow every 15 minutes

Start-time, end-time 
and frequency


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


Time and Data Triggers
~~~~~~~~~~~~~~~~~~~~~~

Materialize your workflow every hour, but only run them when the input data is 
ready (that is loaded to the grid every hour)


.. code-block:: xml

   <coordinator-app name=“coord1” frequency=“${1*HOURS}”…> 
     <datasets>
       <dataset name="logs" frequency=“${1*HOURS}” initial-instance="2009-01-01T23:59Z">
         <uri-template>hdfs://bar:9000/app/logs/${YEAR}/${MONTH}/${DAY}/${HOUR}</uri-template>
       </dataset>
     </datasets>
     <input-events>
       <data-in name=“inputLogs” dataset="logs">
         <instance>${current(0)}</instance>
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


Rolling Window
~~~~~~~~~~~~~~

Access 15 minute datasets and roll them up into hourly datasets


.. code-block:: xml

   <coordinator-app name=“coord1” frequency=“${1*HOURS}”…> 
     <datasets>
       <dataset name="logs" frequency=“15” initial-instance="2009-01-01T00:00Z">
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

Sliding Window
~~~~~~~~~~~~~~

Access last 24 hours of data, and roll them up every hour

.. code-block:: xml

   <coordinator-app name=“coord1” frequency=“${1*HOURS}”…> 
     <datasets>
       <dataset name="logs" frequency=“${1*HOURS}” initial-instance="2009-01-01T00:00Z">
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




Basic Concepts
--------------

Oozie Workflows
~~~~~~~~~~~~~~~

- Oozie executes workflow defined as DAG of jobs
- The job type includes MapReduce, Pig, Hive, shell script, custom Java code etc.
- Introduced in Oozie 1.x





Coordinators
~~~~~~~~~~~~



Bundles
~~~~~~~


Use Patterns 
------------


Simple Data Management
~~~~~~~~~~~~~~~~~~~~~~


Cases
+++++

- Data transformation/filter/Ybeacon
- Data calculation – metrics
- Directory management
- Copy input feed
- Data replication
- Remove feed/data cleanup
- Generate data

Example
+++++++

Start -> (Copy input feed) -> End


Data Modeling
~~~~~~~~~~~~~

Cases
+++++

- Process logs in parallel
- Parse ad events and train data – Moneyball
- Consolidate tweets
- Moneyball bid processor
- Process user engagement
- Check retention rate

Example
+++++++

Start -> Extract ad events -> Join ad events -> Compute derived features -> End
                                                          |
                                                          --> Email features

Complete Data Pipeline 
~~~~~~~~~~~~~~~~~~~~~~

Cases
+++++

- Stream video pipeline
- Complete data transformation pipeline
- Data ingestion

Example
+++++++


.. image:: images/data_pipeline.jpg
   :height: 502px
   :width: 507 px
   :scale: 95 %
   :alt: Data Pipeline Work Flow
   :align: left

End-to-End Data Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~

Cases
+++++

- Data Ingestion
- Slingstone Processing links

Example
+++++++

.. image:: images/end-to-end-processing.jpg
   :height: 513px
   :width: 506 px
   :scale: 95 %
   :alt: End-to-end processing.
   :align: left





Architecture Overview
---------------------


.. image:: images/architecture_overview.jpg
   :height: 462px
   :width: 760 px
   :scale: 95 %
   :alt: Oozie Architectural Diagram
   :align: left


Technology Stack
~~~~~~~~~~~~~~~~

- Server based, Java web-app (Tomcat)
- One Oozie-server instance per cluster
- Workflow Library (wfLite…)
- HDFS for storing deployed applications (Hadoop distributed cache)
- DB for persisting workflow jobs state (Oracle)

.. image:: images/oozie_stack.jpg
   :height: 277px
   :width: 685 px
   :scale: 95 %
   :alt: Oozie Technology Stack
   :align: left


Abstraction Layer
~~~~~~~~~~~~~~~~~

.. image:: images/oozie_layers.jpg
   :height: 791px
   :width: 950 px
   :scale: 90 %
   :alt: Oozie Abstraction Layer
   :align: left


State Transitions
~~~~~~~~~~~~~~~~~


Workflow Engine
~~~~~~~~~~~~~~~

Oozie executes workflow defined as DAG of jobs
The job type includes MapReduce, Pig, Hive, shell script, custom Java code etc.
Introduced in Oozie 1.x


.. image:: images/oozie_stack.jpg
   :height: 392px
   :width: 761 px
   :scale: 90 %
   :alt: Workflow Engine
   :align: right





Limitations/Restrictions
------------------------

