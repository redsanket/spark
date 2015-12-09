======
Hue UI
======

This chapter will give you an overview of the components of the Yahoo Hue Web interface.
We'll first look at the organization and function of the components on the landing page
before covering each component from left to right in more detail.

.. note:: We recommend using Firefox for reliability and optimal performance.

Location of Hue UI
==================

To use the Hue Web UI, you will need to know which cluster you are using. 
The URL to each Hue instance has the following syntax: ``https://{cluster_name}-hue.{color}.ygrid.yahoo.com:{port}``

For example, to access the Hue UI on Axonite Red, you would go to
``https://axonitered-hue.red.ygrid.yahoo.com:9999/``.

We also have ``yo`` links to each cluster instance. The syntax is
``http://yo/hue.{cluster_initials}``. Thus, the ``yo`` link to
the Axonite Red cluster would be ``http://yo/hue.ar``. See 
:ref:`Hue Instances <reference-hue_instances>` for the list
of clusters, URLs, and ``yo`` links.

Navigation Overview
===================

The screenshot of the **Hue Web UI** below explains the purpose of the components
of the top navigation bar. Click the figure to enlarge it.

.. image:: images/hue_ui2.jpg
   :height: 888px
   :width: 1450px
   :scale: 55%
   :alt: Hue UI with annotations.
   :align: left

About Hue
=========

By clicking the **Hue** icon in the top left-hand corner, you can quickly see the Hue version being used
and the link to our documentation.

.. image:: images/hue_about.jpg
   :height: 556px
   :width: 1450px
   :scale: 55%
   :alt: About Hue 
   :align: left

Home
====

The **Home** icon brings you to **My documents**, where you can view and share your past queries,
create new documents or projects, and view projects that were shared with you.  


.. image:: images/hue_home.jpg
   :height: 558px
   :width: 1450px
   :scale: 55%
   :alt: Hue Home
   :align: left

Query Editors
=============

The **Query Editors** drop-down menu lets you quickly navigate to one of the following
available editors:

- **Hive:** Also known as Beeswax, lets you write queries, execute them, get logs, query
  output, chart data, and download results.
- **Pig:** Lets you write and execute scripts, get lets and script output.
- **Job Designer:** Create Oozie actions that you can build, execute, and test.

We'll give you a high-level overview of each editor in the following sections
to help orient you.

Hive Editor
-----------

The **Hive Editor** has the following components for writing, executing, and saving queries,
viewing the results, logs, and even charts. 
You can create Hive databases, tables and partitions, load data, create, run, and 
manage queries, and download the results in a Microsoft Office Excel worksheet file 
or a comma-separated values file. Queries are executed asynchronously, and results
are cached for future reference. You can also re-run saved and archived queries.

- **Query Editor**
- **My Queries**
- **Saved Queries**
- **History**

.. note:: The **Hive Editor** does not have support for emailing results yet. Also,
          Beeswax, the application behind the **Hive Editor**, is only 
          available on Hive 0.13 (AR, MR, AB, MB, NB).

We'll look at each component, which is accessed through a tab, in the next
sections.

Query Editor
############

From the screenshot of the **Query Editor** below, you can see there is
a text field for entering queries that you can then execute, save, or even
have Hue give you an explanation.


.. image:: images/hue_hive_query_editor.jpg
   :height: 872px
   :width: 1439px
   :scale: 55%
   :alt: Hue Hive Editor
   :align: left

The default tabs shown in in the left-hand and bottom navigation bars
are **Assist** and **Recent queries**. We'll look at those
and the other tabs next.

Assist
******

The **Assist** shown below is called *assist* because it helps you
find databases and tables quickly. The pull-down menu lets you quickly
select tables you have access to. The available tables in the selected
database will automatically be displayed.

.. image:: images/hue_hive_query_editor_assist.jpg
   :height: 872px
   :width: 1439px
   :scale: 55%
   :alt: Hive Editor: Assist
   :align: left

Settings
********

From the **Settings** tab, you can add key-value pairs by clicking **Add** under **Settings** and
entering a key such as ``mapred.reduce.tasks`` with a value. You can also point
to a JAR or other files by adding file resources, or defining a UDF by providing
a named function from a class such as the function ``myFunction`` in the class ``com.acme.example``.

.. image:: images/hue_hive_editor_setting.jpg
   :height: 866px
   :width: 1450px
   :scale: 55%
   :alt: Hive Editor: Settings
   :align: left

Recent Queries
**************

The **Recent queries** tab lists your past queries. You can click on the queries
to add them to the **Query Editor** text field.

.. image:: images/recent_queries_tab.jpg
   :height: 447px
   :width: 950px
   :scale: 90%
   :alt: Recent Queries Tab
   :align: left


Query
*****

The **Query** tab simply shows the HQL query that has been executed.

.. image:: images/query_tab.jpg
   :height: 123px
   :width: 822px
   :scale: 90%
   :alt: Query Tab
   :align: left



Log
***

The **Log** table shows the log messages for the execution of the latest Hive query.

.. image:: images/log_tab.jpg
   :height: 660px
   :width: 950px
   :scale: 90%
   :alt: Log Tab
   :align: left

Columns
*******

The **Columns** tab shows the columns of the results for a query.

.. image:: images/cols_tab.jpg
   :height: 121px
   :width: 842px
   :scale: 92%
   :alt: Log Tab
   :align: left



Results
*******

The **Results** table shows the row IDs and the data for each column.

.. image:: images/results_tab.jpg
   :height: 455px
   :width: 950px
   :scale: 90%
   :alt: Results Tab
   :align: left

Chart
*****

The **Chart** tab allows you to see a graphical representation of the data
as a bar chart, line graph, pie chart, or map.

.. image:: images/chart_tab.jpg
   :height: 253px
   :width: 950px
   :scale: 90%
   :alt: Chart Tab
   :align: left



My Queries
##########

The **My Queries** tab shown below lets you view recently saved and run queries. The other features
such as editing, copying, creating new queries, etc., simply take you to the other components.

.. image:: images/hue_hive_my_queries.jpg
   :height: 940px
   :width: 1489px
   :scale: 55%
   :alt: Hive Editor: My Queries
   :align: left

Recent Saved Queries
********************

The **Recent Saved Queries** tab shown below displays the queries that you saved recently. Again,
you have the option of selecting queries and copying, editing, viewing usage history, which will
take you to a different UI component to complete the task.

.. image:: images/hue_hive_my_queries-recent.jpg
   :height: 939px
   :width: 1455px
   :scale: 55%
   :alt: 
   :align: left


Recent Run Queries
******************

The **Recent Run Queries** shown below displays information about recently run queries such as the time, name, query, and
whether it failed or succeeded. 

.. image:: images/hue_hive_query_editor_run_queries.jpg
   :height: 939px
   :width: 1455px
   :scale: 55%
   :alt: Hue Hive Query Editor: Recently Run Queries
   :align: left

Saved Queries
#############

The **Saved Queries** tab seen below is like **Recent Saved Queries** from **My Queries**, but it shows all of the queries
you recently saved.

.. image:: images/hue_hive_query_editor_run_queries.jpg
   :height: 939px
   :width: 1455px
   :scale: 55%
   :alt: Hue Hive Query Editor: Recently Run Queries
   :align: left

History
#######

The **History** tab seen here simply shows the time, name, user, state, and link to results for a query. You are also
shown the actual query. 

.. image:: images/hue_hive_history.jpg
   :height: 913px
   :width: 1450px
   :scale: 55%
   :alt: Hue Hive Query Editor: History
   :align: left

Pig Editor
----------

The **Pig Editor** shown below has a left-hand and top navigation bars to access different components.
The left-hand navigation menu is used mostly to do tasks like saving, submitting, sharing, and copying
scripts. The top navigation components lets you view saved scripts and information about scripts
that have been run.

In summary, the **Pig Editor** gives you the following:

- **Editor:** lets you create, edit, run (executed through Oozie), save, copy and delete scripts as well as set scripts property.
- **Scripts Manager:** lets you create, open, run, copy, and delete scripts.
- **Status Dashboard:** lets you view running and completed scripts.

.. note:: Pig scripts are executed via Oozie server (Hue needs a server).
          No live logs right now like Hive Beeswax.



.. image:: images/hue_pig_editor.jpg
   :height: 915px
   :width: 1450px
   :scale: 55%
   :alt: Pig Editor
   :align: left

**Top Navigation Components**

- **Editor**
- **Scripts**
- **Dashboard**

**Left-Hand Navigation Components**

- **Pig**
- **Properties**
- **Save**
- **New Script**
- **Submit**
- **Logs**


Properties
##########

The **Properties** menu shown below allows you to do a number of things:

- Name or rename a Pig script
- Add parameters such as input (i.e., a path to a file), configurations such as ``optimizer_off`` or ``verbose``.
- Define configurations for Hadoop such as specifying a queue for running a job or assigning a value to a configuration.
- Provide resources for the Pig script such as a path to a HDFS file.

.. image:: images/hue_pig_editor_properties.jpg
   :height: 915px
   :width: 1450px
   :scale: 55%
   :alt: Pig Editor: Properties
   :align: left

Save
####

Clicking **Save** simply saves the Pig script so that you can refer to it at a later time.

.. image:: images/hue_pig_editor_save.jpg
   :height: 870px
   :width: 1450px
   :scale: 55%
   :alt: Pig Editor: Save
   :align: left

New Script
##########

The **New Script** menu simply opens a new text field to write a Pig script. The
text field has a sample line to help you get started.

Submit
######

The **Submit** menu allows you to submit your script to be executed. 

Logs
####

From **Logs**, you can view the Oozie workflow information by clicking on the status link.

Help
####

The **Help** icon just gives you tips for writing Pig scripts.

Scripts
#######

The **Scripts** tab displays your past saved scripts. You can run, copy, or delete your scripts from here.

.. image:: images/hue_pig_scripts.jpg
   :height: 556px
   :width: 1450px
   :scale: 55%
   :alt: Pig Scripts
   :align: left


Dashboard
#########

The **Dashboard** shown below displays the name, status, and creation date of your executed and running Pig scripts.

.. image:: images/hue_pig_scripts.jpg
   :height: 556px
   :width: 1450px
   :scale: 55%
   :alt: Pig Scripts
   :align: left

Job Designer
------------

The **Job Designer** allows you to create and submit jobs to Hadoop cluster. The jobs are the same as Oozie actions.

In addition, the **Job Designer** supports the following:

- including variables with jobs to enable you and others to enter values when running the job.
- MapReduce, Java, Streaming, Hive, Pig, Fs, Ssh, Shell, Email, DistCp.
- specifying several meta-level properties of a job, including name, description, the executable scripts or classes, and parameters for those scripts or classes.
- search, filter, create, delete, restore, copy, edit, and submit a job design.
- using Oozie ShareLib for DistCp, Streaming, Pig, and Hive jobs.
- importing actions into workflows through the Oozie editor.

.. note:: Pig with HCatalog and Hive actions do not carry credential (should be fixed shortly).
          Also, logs may not be complete at this point.

Job Designer Fields
###################

.. csv-table:: Job Designer Fields
   :header: "Fields", "Description", "Notes"
   :widths: 15, 30


   "Name", "Identifies the job and its collection of properties and parameters.", ""
   "Description", "A description of the job.", ""
   "Advanced", "Is shared: indicates whether to share the action with all users. Oozie parameters passed to Oozie.", ""
   "Prepare", "Specifies paths to create or delete before starting the workflow job.", ""
   "Params", "Parameters to supply.", ""
   "Job Properties", "Job properties.", ""
   "Files", "Files to pass to the job. Equivalent to the Hadoop ``-files`` option.", ""
   "Archives", "Archives to pass to the job. Equivalent to the Hadoop ``-archives`` option.", ""
   "MapReduce", "MR functions in Java.", "Jar path to the JAR file containing the Mapper and 
                Reducer function (can be from existing MR classes without having to write a main Java class)."
   "Streaming", "MapReduce functions in non-Java that reads/writers standard UNIX.", "Mapper/Reducer path to 
                the mapper/reducer script or class. Use Files option to pass it as part of job 
                submission if not on HDFS. Equivalent to the Hadoop ``–mapper`` / ``-reducer`` option."
   "Java", "Main class written in Java.", "Jar path to a JAR file containing the main class used to invoke the program
           the arguments to pass to the main class, and options to the JVM."
   "Pig", "Pig script.", "The script name and path to the Pig script."
   "Hive, "Hive script.", "The script name and the path to the Hive script."
   "Shell", "Shell command.", "The shell command and output of the command to capture."
   "Ssh", "SSH command.", "The **user** to run the command, the **host** where to run the command, the ssh **command**,
          the **output** to capture."
   "DistCp", "DistCp command.", ""
   "Fs", "HDFS commands.", "The **path to delete** (directory gets deleted recursively), **source** and **destination** paths to move, 
         **path of permissions** to change, and whether to change recursively."
   "Email", "Email message.", ""

Metastore Manager
=================

The **Metastore Manager** shown below gives you access to HCatalog. You can browse 
the columns, partitions, sample data, and metadata for tables. 

From the **Database** drop-down menu, select a database to see the available tables in the right panel. 
You can then browse the data for a table by clicking the table name or checking the checkbox next to a table 
and then clicking **View**. Clicking **Browse Data** will open the **Query Editor** and show the results.


You can also do the following:

- manage the databases, tables, and partitions of the Hive Metastore or HCatalog.
- select, create, or drop databases.
- create, browse, import data into, drop, view location of tables
- create table from wizards or manually
- list available databases and tables
- browse table data (limited set) and metadata (can be slow: optimization on its way)

.. note:: Metastore Manager is only available on clusters with Hive 0.13 (AR, MR, AB, MB, NB).

.. image:: images/hue_metastore_manager.jpg
   :height: 912px
   :width: 1450px
   :scale: 55%
   :alt: Metastore Manager
   :align: left

.. note:: You may not be able to view data for a table because you don't have permission. 


Workflows
=========

.. Apache™ Oozie is a Java Web application used to schedule Apache Hadoop jobs. Oozie 
.. combines multiple jobs sequentially into one logical unit of work. It is integrated 
.. with the Hadoop stack and supports Hadoop jobs for Apache MapReduce, Apache Pig, 
.. Apache Hive, and Apache Sqoop. It can also be used to schedule jobs specific to a system, like Java programs or shell scripts.

.. Oozie Workflow jobs are Directed Acyclical Graphs (DAGs), specifying a sequence of actions to execute. The Workflow job has to wait
.. Oozie Coordinator jobs are recurrent Oozie Workflow jobs that are triggered by time and data availability.
.. Oozie Bundle provides a way to package multiple coordinator and workflow jobs and to manage the life cycle of those jobs.

The **Workflows** drop-down menu lets you schedule and view Oozie jobs.


Dashboard
---------

The **Dashboard** lets you view the status of Oozie Workflow, Coordinator, and Bundle jobs. 
The **Dashboard** also displays SLAs for jobs and general information about Oozie, such as the
version, configuration file, timers, and counters. 

Before we look closer at each component of the dashboard, let's summarize the features of
the dashboard as a whole:

- Detailed status of running and completed workflow, coordinator, bundle, SLA jobs 
  and information about Oozie instrumentation and configuration
- Summary of the running and completed workflow, coordinator, and bundle jobs
- You can view jobs for a period up to the last 30 days
- You can filter the list by date (1, 7, 15, or 30 days) or status (Succeeded, Running, or Killed). Note that the date and status buttons are toggles

Notes
#####

Logs may not be complete

SLA dashboard isn't working yet

Filters only operates on latest 100 displayed entries



Workflows
#########

The Workflow application is a collection of actions arranged in a directed acyclic 
graph (DAG).


The **Workflows** tab shown below as you might imagine displays the running and completed 
`Oozie Workflow jobs <http://oozie.apache.org/docs/4.1.0/WorkflowFunctionalSpec.html>`_.
You can choose how many jobs to display and sort the jobs by status through the **Show only** or the **days
with status**. 


.. image:: images/hue_oozie_dashboard_workflows.jpg
   :height: 912px
   :width: 1450px
   :scale: 55%
   :alt: Hue Oozie Dashboard: Workflows
   :align: left

If you click on a job, you can view the execution details in the **Actions** tab, the application path and start/creation time
in the **Details** tab, the Oozie configurations used in the **Configuration** tab, the Oozie logs in the **Log** tab,
and the Oozie job configuration file in the **Definition** tab.

.. _oozie_db_wf-details:

Job Details
***********

You can see examples of the **Actions**, **Details**, **Configuration**, **Log**, and **Definition** tabs below for
the Oozie Workflow for the **StarlingProcessor** job.

+-------------------------------------------------------------+------------------------------------------------------------------------+
| **Actions**                                                 | **Details                                                              |
+=============================================================+========================================================================+
| .. image:: images/hue_oozie_dashboard_workflows_actions.jpg | .. image:: images/hue_oozie_dashboard_workflows_details.jpg            |
|    :height: 243px                                           |    :height: 251px                                                      |
|    :width: 800px                                            |    :width: 800px                                                       |
|    :scale: 55%                                              |    :scale: 55%                                                         |
|    :alt: Hue Oozie Dashboard: Workflow Actions              |    :alt: Hue Oozie Dashboard: Workflow Details                         |
|    :align: left                                             |    :align: left                                                        |
+-------------------------------------------------------------+------------------------------------------------------------------------+


+-------------------------------------------------------------+------------------------------------------------------------------------+
| **Configuration**                                           | **Log**                                                                |
+=============================================================+========================================================================+
| .. image:: images/hue_oozie_dashboard_workflows_config.jpg  | .. image:: images/hue_oozie_dashboard_workflows_log.jpg                |
|    :height: 257px                                           |    :height: 254px                                                      |
|    :width: 800px                                            |    :width: 800px                                                       |
|    :scale: 55%                                              |    :scale: 55%                                                         |
|    :alt: Hue Oozie Dashboard: Workflow Actions              |    :alt: Hue Oozie Dashboard: Workflow Details                         |
|    :align: left                                             |    :align: left                                                        |
+-------------------------------------------------------------+------------------------------------------------------------------------+


+-----------------------------------------------------------------+
| **Definition**                                                  |
+=================================================================+
| .. image:: images/hue_oozie_dashboard_workflows_definition.jpg  | 
|    :height: 249px                                               |
|    :width: 800px                                                |    
|    :scale: 55%                                                  |    
|    :alt: Hue Oozie Dashboard: Workflow Actions                  |    
|    :align: left                                                 |   
+-----------------------------------------------------------------+

Coordinators
############

The Coordinator application allows you to define and execute recurrent 
and interdependent workflow jobs. 

The **Coordinators** tab, like the **Workflows** tab, shows the running, completed, and killed `Oozie
Coordinator jobs <http://oozie.apache.org/docs/4.1.0/CoordinatorFunctionalSpec.html>`_. You can 
also have the option of selecting how many jobs to show per page, sorting the jobs by status, and 
clicking a job to get details (see :ref:`Job Details <Doozie_db_wf-details>` above).

.. image:: images/hue_oozie_dashboard_coordinators.jpg
   :height: 912px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Dashboard: Coordinators
   :align: left


Bundles
#######

The Bundle application allows you to batch a set of coordinator applications.

The **Bundles** tab shows the running, completed, and killed `Oozie
Bundles jobs <http://oozie.apache.org/docs/4.1.0/BundleFunctionalSpec.html>`_.
 You can also have the option of selecting how many jobs to show per page,
sorting the jobs by status, and clicking a job to get details 
(see :ref:`Job Details <Doozie_db_wf-details>` above).

.. image:: images/hue_oozie_dashboard_bundles.jpg
   :height: 914px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Dashboard: Bundles
   :align: left

SLA
###

The **SLA** tab shown below displays the Oozie jobs that have SLAs. You can search by the job name or ID and then filter by date.

.. image:: images/hue_oozie_dashboard_sla.jpg
   :height: 913px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Dashboard: SLA
   :align: left

Oozie
#####

The **Oozie** tab is a panel with two of its own tabs: **Instrumentation** and **Configuration**. 
The **Instrumentation** tab shows data used for `Oozie monitoring <http://oozie.apache.org/docs/3.3.0/AG_Monitoring.html>`_
so that runtime, performance, and health metrics can be collected.

Instrumentation
***************

variables
^^^^^^^^^

The default for the **Instrumentation** panel shown below is **variables**. The variables include information
such as the Oozie information, logging settings, libraries being used, JVM memory information, and more.



.. image:: images/hue_oozie_dashboard_oozie_variables.jpg
   :height: 914px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Dashboard: Instrumentation Variables
   :align: left

samplers
^^^^^^^^

The **samplers** tab shown below displays performance statistics based on polls for data. The default
time interval for polling is one minute.

.. image:: images/hue_oozie_dashboard_instrumentation_samplers.jpg
   :height: 912px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Dashboard: Instrumentation Samplers
   :align: left

timers
^^^^^^

The **timers** tab shown below displays how much time is spent in different operations.

.. image:: images/hue_oozie_dashboard_oozie_instrumentation_timers.jpg
   :height: 912px
   :width: 1450px
   :scale: 55%
   :alt:  Oozie Dashboard: Instrumentation Timers
   :align: left

counters
^^^^^^^^

The **counters** tab shown below displays statistics about the number of times an event has occurred.

.. image:: images/hue_oozie_dashboard_instrumentation_counters.jpg
   :height: 914px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Dashboard: Instrumentation Counters
   :align: left


Configuration
*************

The **Configuration** tab shows the `Oozie configuration <http://oozie.apache.org/docs/3.3.0/AG_Install.html#Oozie_Configuration>`_ 
being used when running jobs.

.. image:: images/hue_oozie_dashboard_oozie_configuration.jpg
   :height: 915px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Dashboard: Configuration
   :align: left

Editors
-------

The **Oozie Editor** has three tabs for creating, importing, scheduling, exporting, copying, deleting, and submitting Oozie
Workflow, Coordinator, and Bundle jobs.

From the **Oozie Editor**, you can do the following:

- view available workflows, coordinators with workflows and frequencies, and bundles with coordinators and kickoff .
- create, import, export, schedule and submit workflows, coordinators, and bundles.
- delete, copy, and restore workflows, coordinators, and bundles

.. note:: Workflows, coordinators, and bundles can only be viewed, submitted, and modified by their owner.
          The **Workflow Export** function does not work right now.



Workflows
#########

The  **Workflows** tab lets you first create or import an Oozie Workflow. 

.. _oozie_workflow-editor:

Editor
******

To start creating an Oozie Workflow, from the **Workflows** tab, click the **Create** button. This
will open a **Properties** panel seen below that asks for a name and
description of your new workflow and then click **Save**.

.. image:: images/hue_oozie_editor_workflow.jpg
   :height: 825px
   :width: 1450px
   :scale: 55%
   :alt: Hue Oozie: Editor Workflow
   :align: left

Workspace
^^^^^^^^^

From the **Workflow** sidebar menu, you can drag one of the tasks on the workspace, such as **Hive**
to the workspace area between **start** and **end**. 

Edit Node
^^^^^^^^^

After you have dragged a task to the workspace, the **Edit Node:** page shown
below will open. You enter the name of the task, a description, SLA configuration,
credentials, any scripts, as well as parameters, job properties, files, and archives.

.. image:: images/hue_oozie_editor_workflow.jpg
   :height: 825px
   :width: 1450px
   :scale: 55%
   :alt: Hue Oozie: Editor Workflow
   :align: left

Properties
^^^^^^^^^^

The **Properties** menu shows a summary of your workflow: name, description, parameters, job properties, SLA configuration, HDFS deployment directory, and Oozie workflow XML file.

.. image:: images/hue_oozie_workflows_editor-properties.jpg
   :height: 950px
   :width: 1450px
   :scale: 55%
   :alt: Hue Oozie: Editor Properties
   :align: left

Workspace
^^^^^^^^^

Clicking **Workspace** takes you to the :ref:`File Browser <hue_ui-file_browser>`.

Advanced
********

The **Advanced** menu in the sidebar navigation allows you to import actions, kill a node, and view the history of your workflows.

Import action
^^^^^^^^^^^^^

The **Import action** menu lets you import an action from the **Job Designer** or **Oozie**. 


+-------------------------------------------------------------+------------------------------------------------------------------------+
| **Job Designer Import**                                      | **Oozie Import**                                                       |
+=============================================================+========================================================================+
| .. image:: images/hue_oozie_editor-import_job_designer.jpg  | .. image:: images/hue_oozie_editor-import_oozie.jpg                    |
|    :height: 288px                                           |    :height: 288px                                                      |
|    :width: 800px                                            |    :width: 800px                                                       |
|    :scale: 55%                                              |    :scale: 55%                                                         |
|    :alt: Hue Oozie Dashboard: Import Job Designer           |    :alt: Hue Oozie Dashboard: Oozie Import                             |
|    :align: left                                             |    :align: left                                                        |
+-------------------------------------------------------------+------------------------------------------------------------------------+



Actions
*******

From the **Actions** menu items, you can submit, schedule, copy, or export a workflow.


Submit
^^^^^^

The **Submit** menu option will prompt you to confirm that you want to submit a Workflow and then
submit it to the cluster.

Schedule
^^^^^^^^

The **Schedule** menu option will open the :ref:`Coordinator Editor <>`, where you
can enter basic information, specify a frequency to run the workflow, define outputs,
and make any advanced settings such as Oozie parameters, timeouts, concurrency, etc.


Coordinators
############

Coordinator Manager
*******************

The **Coordinator Manager** shown below lets you create, import, delete, submit, copy, and delete Oozie Coordinators.

.. image:: images/hue_oozie_editor_coordinators.jpg
   :height: 826px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Editor: Coordinators
   :align: left

By clicking on one of the Oozie Coordinator jobs or the **Create** button, you open the **Coordinator Editor**. 

Coordinator Editor
^^^^^^^^^^^^^^^^^^

The **Coordinator Editor** has a series of UI elements for each step from basic information to advanced settings.

.. image:: images/hue_oozie_editor_coordinator-properties.jpg
   :height: 692px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Editor: Coordinator
   :align: left

Workflow
^^^^^^^^

The **Workflow** sidebar menu takes you to the :ref:`Oozie Workflows Editor <oozie_workflow-editor>`.

Datasets
^^^^^^^^

Under the **Datasets** menu, you will see two options: **Create new** and **Show existing**.
Clicking the **Create new** option will open the **Create a new dataset** panel below that allows
you to define a dataset.

.. image:: images/hue_oozie_coordinators_editor-create.jpg
   :height: 864px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Editor: Create a dataset
   :align: left

The **Show existing** menu option will display your existing datasets.

History
^^^^^^^

The **History** menu has the one option **Show history** that shows you
the history of your Oozie Coordinators running.

Actions
^^^^^^^

The **Actions** menu has the two options **Submit** and **Copy**. As with the
**Coordinator Manager**, you can submit an Oozie Coordinator Job by clicking
**Submit**. 

If you want to create another Oozie Coordinator similar to an existing
Oozie Coordinator, you click **Copy**, which creates another Coordinator
with the same name with the string "-copy" appended to the original
Coordinator name.

Bundles
#######

Bundle Manager
**************

The **Bundle Manager** shown below lets you create, import, delete, submit, copy, and delete `Oozie Bundles <http://oozie.apache.org/docs/3.3.0/BundleFunctionalSpec.html>`_.
Oozie Bundles allow you to define and execute multiple coordinator applications and are often called a data pipelines.

.. image:: images/hue_oozie_editor_bundles.jpg
   :height: 855px
   :width: 1450px
   :scale: 55%
   :alt: Oozie Editor: Bundles
   :align: left





By clicking on one of the Oozie Bundle jobs or clicking the **Create** button, you open the **Bundle Editor**. 

Bundle Editor
^^^^^^^^^^^^^

The **Bundle Editor** has a simpler editor than the **Coordinator Editor**. The main panel
has the tab **Step 1: General** for entering general information about a Bundle and
the tab **Step 2: Advanced setting** for adding Oozie parameters. The two tabs
are shown in the screenshot below.


+-------------------------------------------------------------+------------------------------------------------------------------------+
| **Step 1: General**                                         | **Step 2: Advanced setting**                                           |
+=============================================================+========================================================================+
| .. image:: images/hue_oozie_bundle_editor-general.jpg       | .. image:: images/hue_oozie_bundle_editor-advanced.jpg                 |
|    :height: 347px                                           |    :height: 348px                                                      |
|    :width: 800px                                            |    :width: 800px                                                       |
|    :scale: 55%                                              |    :scale: 55%                                                         |
|    :alt: Hue Oozie Dashboard: Bundle Editor - General       |    :alt: Hue Oozie Dashboard: Bundle Editor - Advanced                 |
|    :align: left                                             |    :align: left                                                        |
+-------------------------------------------------------------+------------------------------------------------------------------------+


Coordinators
^^^^^^^^^^^^

From the **Coordinators** menu, you have the option **+Add** and **Show selected**. Clicking
**+Add** opens the pane **Add coordinator to the Bundle** shown below, where you can add a 
Coordinator with or without parameters to a Bundle.

.. image:: images/hue_oozie_bundle_editor-add.jpg
   :height: 542px
   :width: 1439px
   :scale: 55%
   :alt: Oozie Bundle Editor: Add Coordinator
   :align: left


History
^^^^^^^

The **History** menu has the one option **Show history** that shows you
the history of your Oozie Bundles running.

Actions
^^^^^^^

The **Actions** menu has the two options **Submit** and **Copy**. 
You can submit an Oozie Bundle Job by clicking
**Submit** or create another Oozie Bundle similar to an existing
by clicking **Copy**, which creates another Bundle
with the same name with the string "-copy" appended to the original
Bundle name.

.. _hue_ui-file_browser:

File Browser
============

The **File Browser** allows you to access the Hadoop Distributed File System (HDFS). 

From the **File Browser**, you can do the following:

- upload  and download files or zipped archives
- create new files or directories
- rename, move, delete, or copy files and directories
- browse files and directories
- change the owner, group, or permissions for files and directories
- search for files, directories, owners, and groups   
- view and edit files as text or binary


.. image:: images/hue_file_browser.jpg
   :height: 914px
   :width: 1450 px
   :scale: 55 %
   :alt: Hue File Browser   
   :align: left

Job Browser
===========

The **Job Browser** lets you monitor MapReduce jobs,
Tez sessions, and provides links to the ResourceManager logs. 

With the **Job Browser**, you can do the following:

- examine all Hadoop MapReduce jobs running on the cluster.
- filter based on the state of the jobs (Succeeded, Running, Failed, or Killed), username, or job name.
- access links to the cluster's ResourceManager page (no direct log support) 

.. note:: Currently, there is no support for direct link to job output directory in the **File Browser**,
          killing jobs, or views for Tez DAGs, Tez jobs, or the Tez ATS UI.


.. image:: images/hue_job_browser.jpg
   :height: 556px
   :width: 1450 px
   :scale: 55 %
   :alt: Hue Job Browser   
   :align: left

If you click one of the job IDs, you open the **Hadoop Application Overview** shown below that
summarizes the job, gives you a tracking URL, the node URL and port, as well as a link
to logs.

.. image:: images/hadoop_app_overview.jpg
   :height: 634px
   :width: 1438 px
   :scale: 55 %
   :alt: Hue Job Browser   
   :align: left


 
Documentation
=============

The **Documentation** navigation links to this document. 


Demo Tutorials
==============

The **Demo** navigation provides links to external information about Hue and Hadoop.

Sign Out
========

When you sign out, Hue just reuses your Backyard credentials to sign you back in or
redirects you to Bouncer.

Administrator Features
======================

Admins only : Hue logs (access.log) all requests against the Hue web server, (supervisor.log) 
information for the supervisor process, (supervisor.out) stdout and stderr for 
the supervisor process, (.log) logs for each supervised process, and (.out) stdout 
and stderr for each supervised process

Admins only : Hue requires a SQL database to store small amounts of data, including 
user account information as well as history of job submissions and Hive queries
