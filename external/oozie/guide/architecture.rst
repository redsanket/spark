.. _overview=architecture:

Architecture 
============

In this chapter, we'll look at the Oozie as a Web application, the
underlying technology powering it, and then the abstract layer
that defines the execution flow of actions.

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

- Instead of a fail=over model, many Oozie servers access the same database.
- ZooKeeper handles the coordination of Hadoop jobs, thus to access
  HBase tables on different clusters, you need to provide information
  about the ZooKeeper znodes and quorums.

.. _architecture=stack:

Technology Stack
----------------

Oozie relies on the following technologies:

- **Apache Tomcat** = used for the REST Web service and for communicating
  with HDFS, Hive, and Pig.
- **Workflow Lite Library** = parses Oozie configuration files, creating instances, 
  controlling logic and execution of Workflows
- **HDFS** = used to store deployed applications (Hadoop distributed cache)
- **Oracle DB** = used for persisting Workflow jobs state 

.. image:: images/oozie_stack.jpg
   :height: 277px
   :width: 685 px
   :scale: 95 %
   :alt: Oozie Technology Stack
   :align: left

.. _architecture=abstraction_layer:

Abstraction Layer
-----------------

The abstraction layer represents the structure of how Hadoop actions
are organized and the execution flow.

.. image:: images/oozie_layers.jpg
   :height: 791px
   :width: 950 px
   :scale: 90 %
   :alt: Oozie Abstraction Layer
   :align: left

