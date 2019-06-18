===============
Getting Started
===============

**Time Required:** 30-40 minutes

.. _hue_getting_started-intro:

Introduction
============

In this getting started, you will be getting familiar with the Hue
interface and learn how to use Hue to use Hadoop 
technology such as HDFS, Hive, Pig, Oozie, Spark, and Job Browser.


We'll be doing the following in this tutorial:

- View data and work with files/directories with the **File Browser**. 
- Create a database and table, and view data with the **Table Browser**.
- Run Hive queries on data with the **Hive Query Editor**.
- Process data with Pig through the **Pig Editor**.
- Create a simple Oozie Workflow through the **Scheduler** to 
  execute a Hadoop job with several tasks. 
- Monitor Hadoop jobs in the **Job Browser**.


.. _hue_getting_started-prereq:

Prerequisites
-------------

- If you're a newbie to Hadoop, read the
  following:

  - `What is Hadoop? <http://hadoop.apache.org/#What+Is+Apache+Hadoop%3F>`_
  - `Hive Wiki <https://cwiki.apache.org/confluence/display/Hive/Home>`_
  - `Pig introduction <http://pig.apache.org/>`_
  - `Oozie Overview <http://oozie.apache.org/>`_
  - `Getting Started on the Oath Grid <https://yahoo.jiveon.com/docs/DOC-46590>`_

.. _hue_getting_started-about:

About the Tutorial 
------------------

.. _about-org:

Organization
~~~~~~~~~~~~

The tutorial is divided into several sections, starting 
from the simplest uses such as navigating HDFS and 
then gradually becoming more complex until you're 
creating an Oozie Workflow to automate several
jobs. For each section, we also offer additional 
tips for using Hue.

.. _about-queues:

Hadoop Queues
~~~~~~~~~~~~~

We'll be using the ``default`` queue for running jobs, but we suggest
that you use your team's queue because for faster job processing. We'll
show you how to set the queue in the tutorial.

.. _hue_getting_started-disc_data:


1. Navigating HDFS With File Browser
====================================

#. From Hue (visit yo/hue.tt which is the Hue instance on Tiberium Tan), 
select the menu icon in the top left most corner of Hue.  In the dropdown that
appears, under "Browsers", choose "Files". 

   .. image:: images/click_file_browser.jpg
      :height: 310px
      :width: 950 px
      :scale: 90%
      :alt: Click File Browser
      :align: left 
 
#. You should see your home directory with a long-format listing of files.

   .. image:: images/home_directory.jpg
      :height: 355px
      :width: 950 px
      :scale: 90%
      :alt: Hue Home Directory
      :align: left 


#. Click the **/user** path. 
#. Click the empty space next to the **/user** path, type **/rbernota**, and then the press the **enter** key.

   .. image:: images/rbernota_dir.jpg
      :height: 276px
      :width: 950 px
      :scale: 90%
      :alt: Hue Rick Bernotas Directory
      :align: left 

#. From the **File Browser**, navigate to the directory ``/user/rbernota/HueTalk/``.

   .. tip:: You can view the contents of files by double-clicking the
            file name. Hue doesn't allow you to view ``bz2`` compressed
            files as text (hex is displayed instead), but you can view 
            ``gz`` compressed files as text. For example, double-click
            one of the gzipped files in 
            ``/user/rbernota/HueTalk/superbowl2014_tweets/``.
         
#. Go to your home directory by clicking **File Browser**.
#. Click **+ New->Directory** and enter the
   directory name **hue_tutorial** in the **Directory Name** text 
   field and click **Create**.

   .. image:: images/create_tutorial_dir.jpg
      :height: 171px
      :width:  831 px
      :scale: 100%
      :alt: Hue Tutorial Directory 
      :align: left 


.. tip:: The **File Browser** also lets you do the 
         following with files and directories:

         - change permissions
         - rename 
         - delete and create 
         - upload 

#. From the **File Browser**, navigate back to the directory ``/user/rbernota/HueTalk/``.
#. Select the checkbox to the left of the directory ``superbowl2014``.
#. From the **Actions** dropdown menu, select **Copy**.
#. Navigate to, and select the **hue_tutorial** directory that you previously created.
#. Click the **Copy** button and ensure that the data was copied to your directory.


.. _hue_getting_started-create_db_tables:

2. Creating Database/Tables
===========================

Creating a Database and Table With the Table Browser                                                                 
-------------------------------------------------------------------

Creating the Database
~~~~~~~~~~~~~~~~~~~~~

#. Click the top-left menu nav icon and navigate to **Browsers->Tables** in the top navigation bar.
#. Click the **Databases** link.
#. On the right side, click the **Create a new database** icon, which looks like a plus sign.
#. Enter **superbowl_{your_user_name}_database** in the **Database Name** text field.
#. Uncheck the **Default location** checkbox, specify a location **/user/{your_user_name}/superbowl_database** for the database in HDFS
under your user directory, and click **Submit**.
#. Your task history will show that the database was successfully created.
#. To verify, return to the **Table Browser** and click **Databases**, and scroll down to see your database.

Creating the Table
~~~~~~~~~~~~~~~~~~

#. From the **Databases** panel, find and then click the database you just created. Hint: It's
   easier to find through the search text field.
#. On the right side, click the **Create a new table** icon, which looks like a plus sign.
#. For **Source->Type**, choose **File**.
#. For **Path**, use the HDFS file chooser to locate the data file for the table, like **/user/{your_user_name}/hue_tutorial/superbowl2014/superbowl2014_tweets/20140202_041903_f34a1395-862a-410a-b663-c8be258349a9.csv.gz**
#. Under **Format->Field Separator**, choose **Pipe**.  For **Record Separator**, choose **New line**.
#. Uncheck the **Has Header** checkbox.
#. At this point, you should see a preview of the dataset.  Click **Next**.
#. Specify the name of the table, including the database name, like **superbowl_{your_user_name}_database.superbowl_tweets**.
#. Under **Properties->Format**, specify **Text**. Make sure the **Store in default location** checkbox is checked.
#. Specify **Fields** like:

   - ``username`` type string
   - ``tweettime`` type timestamp
   - ``tweet`` type string
   - ``retweetcount`` type bigint
   - ``ondbl`` type double
   - ``atdbl`` type double
   - ``country`` type string
   - ``name`` type string
   - ``address`` type string
   - ``type`` type string
   - ``placeURL`` type string

#. Click **Submit**.
#. Your task history will show that the table was successfully created. As the table is displayed to you in the table browser,
you should see the columns definition, as well as sample data from the table.


.. _hue_getting_started-view_metadata:

3. Viewing Metadata and Data from the Table Browser 
===================================================

#. From the top left of the navigation bar, select the left-most menu icon, 
and click the **Tables** option under **Browsers** to open the **Table Browser**.

   .. image:: images/open_metastore_manager.jpg
      :height: 255 px
      :width: 921 px
      :scale: 90%
      :alt: Opening Table Browser
      :align: left 

#. From the **Table Browser**, click the **Databases** link at the top.
#. Scroll down and click the link for your database.
#. Check the checkbox next to the table 
   **superbowl_tweets** and click **View**.
#. You'll see the **Columns** tab showing  column names with the type. 
#. Click the **Sample** tab to see example data from your table.
#. To see properties of the table, such as the owner, when it was created, table type, etc., click **Details**.
#. You can also view the file location for the database by clicking **Location** on the **Overview** tab.

.. tip:: If you're not familiar with HiveQL, you can use
         the **Table Browser** to create or drop tables.
         

.. _hue_getting_started-query_data:

4. Querying Data With Hive
==================================

.. _query_data-hive:

Using Hive
----------

In this section, we're going to use the **Hive Query Editor** to execute queries on the
table you created.

#. Go to the **Hive Query Editor**. (Click **Query->Editor->Hive**.)
#. From the SQL **Assist** panel on the left-hand side, find your database.
   You should see the one table we created on the **Assist** panel.
#. Click the **superbowl_tweets** to see the available fields.
#. Enter the following query to **Query Editor** window to see the tweet data:

   ``select username, tweettime, tweet from superbowl_{your_user_name}_database.superbowl_tweets;``

#. Click **Execute**. From the **Results** tab, you'll see the tweet data.
#. Click the **Chart** to see the options you have for graphic representation of the results.
#. Click the **Export Results** icon to see the options you have for exporting the data to XLS, CSV, Clipboard, or HDFS.
#. Try exporting your result data in the various formats.

5. Saving Scripts to Files
==========================

In this section, we're going to be creating a directory 
and saving Hive scripts to files, so that we
can automate everything we've done through actions
and Oozie Workflows later.

#. Use the **File Browser** to go to your home directory.
#. Click **New->Directory**.

   .. image:: images/create_new_dir.jpg
      :height: 302 px
      :width: 950 px
      :scale: 90%
      :alt: Creating New Directory
      :align: left 	
   
#. In the **Create Directory** dialog, enter **hue_scripts** 
   in the **Directory Name** text field for the directory name
   and click **Create**.
  
   .. image:: images/create_new_dir.jpg
      :height: 162 px
      :width: 481 px
      :scale: 100%
      :alt: Creating the Hue Scripts Directory
      :align: left 	

#. Navigate to the new directory **hue_scripts** and click **New->File**.
#. Create the file **create_db_tables.hql** 
   the following code:

   .. code-block:: sql

      create database superbowl_{your_user_name}_script_db location '/user/{your_user_name}/superbowl_scripts_database';
      use superbowl_{your_user_name}_script_db;

      create external table superbowl_tweets (
        username string,
        tweettime timestamp,
        tweet string,
        retweetcount bigint,
        ondbl double,
        atdbl double,
        country string,
        name string,
        address string,
        type string,
        placeURL string
      )
      row format delimited
      fields terminated by '|'
      lines terminated by '\n';

      load data inpath 'hdfs:/user/{your_user_name}/hue_tutorial/superbowl2014/superbowl2014_tweets/20140202_063803_f34a1395-862a-410a-b663-c8be258349a9.csv.gz' into table superbowl_tweets;
 
   Once again, be sure to replace ``{your_user_name}`` with your
   user name.

#. Create another file **tweets_query.hql** with the following: 
   
   .. code-block:: sql

      use superbowl_{your_user_name}_script_db;
      SET hive.exec.compress.output=false;

      CREATE TABLE superbowl_script_tweets row format delimited fields terminated by ","
      STORED AS TEXTFILE AS select username, tweet from superbowl_tweets;

   This will create a smaller table stored as text, with fewer columns from 
   our original tweets table.

#. Great, we have our scripts. We're still going to need to 
   do a few more things for our Oozie Workflow,
   but we're going to use the **Scheduler** next to complete the 
   process. 


6. Creating Workflows With the Scheduler
===========================================

With the **Scheduler**, you're configuring Oozie to
run tasks in a job. This lets you create Oozie workflows,
coordinators (set of workflows), and bundles (set of coordinators).
We're just going to create an Oozie Workflow to automate
what we've done thus far. 

.. note:: As with the steps before, replace 
          ``{your_user_name}`` in the given user input 
          with your actual user name.
          Henceforth, we're going to omit 
          any prompts or reminders to do so.   

#. Click **Query->Scheduler->Workflow**.

   .. image:: images/open_oozie_editor.jpg
      :height: 194 px
      :width: 663 px
      :scale: 93%
      :alt: Open Scheduler
      :align: left 	

#. Click **My Workflow** to open a dialog, enter **hue_tutorial_workflow** in the text field,
   and click the **√** symbol.

   .. image:: images/name_workflow.jpg
      :height: 405 px
      :width: 950 px
      :scale: 90%
      :alt: Name the Workflow for the Hue Tutorial
      :align: left

#. Drag the **Hive Script** object to the gray dotted box.
#. In the dialog, do the following: 

   #. In the **Script** text box, enter the path **/user/{your_user_name}/hue_scripts/create_db_tables.hql**.
   #. In the **Hive XML** text box, enter the path **/user/rbernota/HueTalk/hive-site.xml**.
   #. Click **Add**.

   .. note:: To run Hive queries in Oozie, you need to provide a ``hive-site.xml``. 
             If you're not working on the Tiberium Tan Hue instance, you'll have to copy the file from a cluster gateway node to your home directory, and reference it there.

#. Click the gear icon in the dialog to open the Hive Script settings.
#. Click **PROPERTIES** to open two text fields.
#. In the two text fields, enter the value **hive.querylog.location** in the left-hand text field (name) and
   the value **hivelogs** in the right-hand text field (value).

   .. image:: images/hivelogs_property.jpg
      :height: 295 px
      :width: 466 px
      :scale: 98%
      :alt: Hive Logs Property
      :align: left 

#. Open another **PROPERTIES** key-value pair by clicking the plus sign again, and add the property **oozie.action.sharelib.for.hive** and the value **hcat_current,hive_current**.

#. Create another **Hive** action for your Oozie Workflow:

   #. In the **Script** text box, enter the path **/user/{your_user_name}/hue_scripts/tweets_query.hql**. 
   #. In the **Hive XML** text box, enter the path **/user/rbernota/HueTalk/hive-site.xml**.
   #. Click the **Properties** icon and then **PROPERTIES**. In the two text boxes, 
      enter **hive.querylog.location** for and **hivelogs** for the name and value.
   #. Open another **PROPERTIES** key-value pair by clicking the plus sign again, and add the property **oozie.action.sharelib.for.hive** and the value **hcat_current,hive_current**.

#. At this point, you may add any of the other Oozie workflow actions to your Oozie workflow.
#. Save the Oozie workflow.
#. From the right-hand side of the **Scheduler** navigation bar, click |arrowhead| to submit your Oozie job.
#. While your Oozie Workflow is running, let's move to the next section to learn about the
   **Job Browser**. 


.. tip:: We've already looked at importing actions, creating tasks, and submitting the Oozie Workflow from the **Scheduler**, but
         there are a lot more features. You can copy your Oozie Workflow, look at the list of past Oozie Workflows that
         were submitted, and schedule Oozie Workflows (with Oozie Coordinators, which we cover later).


.. _viewing_jobs:

7. Viewing and Managing Jobs
============================

From the **Job Browser**, you can view  your jobs and
other jobs. You can sort jobs by status, search for jobs 
by a user or key term, also look at the cluster and ResourceManager logs.

#. Let's first look for our jobs by clicking **Browsers->Jobs** from the left assist panel.

   .. image:: images/open_job_browser.jpg
      :height: 165 px
      :width: 950 px
      :scale: 90%
      :alt: Open Job Browser
      :align: left   

#. Sort your jobs by clicking the green **Succeeded**. (Depending how far 
   your job has progressed, you may only see one or two successful jobs.)

   .. image:: images/successful_jobs.jpg
      :height: 216 px
      :width: 950 px
      :scale: 90%
      :alt: Successful Jobs
      :align: left   

#. You can view the cluster log by clicking the log ID of a job. Try clicking the 
   job ID of the first job in the list.

   .. image:: images/log_id.jpg
      :height: 216 px
      :width: 950 px
      :scale: 90%
      :alt: Link to Job.
      :align: left   

#. The **Hadoop Cluster** page gives you the user, application type, state, start time, tracking URL,
   and a link to the log. 

   .. image:: images/application_logs.jpg
      :height: 351 px
      :width: 950 px
      :scale: 90%
      :alt: Hadoop Application Log Page
      :align: left 

#. Click on the **Tracking URL** in another tab to
   see **Job** log that gives detailed information about
   the Map and Reduce jobs.

   .. image:: images/map_reduce_jobs.jpg
      :height: 364 px
      :width: 950 px
      :scale: 90%
      :alt: MapReduce Logs
      :align: left 
  
#. From the **MapReduce Job** page, click **logs** to open the **Hadoop Logs** page
   that contains logs for *stderr*, *stdout*, and *syslog*.
   You can also click the **here** link for any of those log types to see the full log.

   .. image:: images/map_reduce_jobs.jpg
      :height: 364 px
      :width: 950 px
      :scale: 90%
      :alt: MapReduce Logs
      :align: left 


   If you have an error in one of the jobs of your Oozie Workflow, the logs are the
   best place to find out what went wrong. 
#. Go back to the **Hadoop Cluster** page and click on the **Scheduler** link. This
   shows you the cluster metrics and the free and used capacity for each queue, which
   will sometimes explain why it's taking a long time to run your Oozie Workflow. 

   .. image:: images/cluster_metrics_queues.jpg
      :height: 348 px
      :width: 950 px
      :scale: 90%
      :alt: Scheduler Showing Cluster Metrics and Queue Capacity
      :align: left 
 


#. From the **Application Queues** section, click **default** to see the available capacity for the
   *default* queue. This is the queue your jobs use if you do not specify one. We recommend
   using the queue allotted to your team for your production Oozie Workflows. Your jobs will generally finish faster.

   .. image:: images/default_queue.jpg
      :height: 309 px
      :width: 950 px
      :scale: 90%
      :alt: The Capacity for the Default Queue
      :align: left 
   
#. Another useful metric is the **Nodes of the cluster** page, which you
   can get to by clicking the **Node** link. The page has detailed information about the
   container, such as the virtual memory allocated, Pmem enforced, virtual cores, etc.

   .. image:: images/nodes_of_cluster.jpg
      :height: 517 px
      :width: 950 px
      :scale: 90%
      :alt: The Node Manager
      :align: left 


#. Okay, our Oozie Workflow should be about done. Go back to the **Scheduler** to see the progress of your Oozie Workflow.
   Hopefully, you see green **OK** icons for all the jobs in the Oozie Workflow as seen below.

   .. image:: images/successful_workflow.jpg
      :height: 517 px
      :width: 950 px
      :scale: 90%
      :alt: Oozie Dashboard: Successful Workflow
      :align: left 

#. Congratulations if your Oozie Workflow successfully completed. 

.. note:: Once you are done with the tutorial and 
          experimenting with the data,
          please drop the databases and tables you created
          during the tutorial to free up grid resources
          for others.

.. See the :ref:`How Tos <gs-troubleshooting>` chapter to learn more.
  

.. _gs-troubleshooting:

Troubleshooting
=============== 

.. _gs_troubleshooting-general:

General
-------

- Replaced the string ``{your_user_name}`` with your
  actual user name.
- Check the ResourceManager logs. Go to **Job Browser**, click
  the job ID link, and then the logs link. 
 

.. _gs_troubleshooting-hive_pig:

Hive/Pig Jobs 
-------------

Confirm that you have down the following:

- Make sure that ``hcat`` is checked.
- The **Job XML** points to a ``hive-site.xml`` file.
- For Hive jobs, the job property ``oozie.action.sharelib.for.hive`` 
  has ``hcat_current,hive_current`` (no spaces between the values).
  For Pig jobs, the job property ``oozie.action.sharelib.for.pig`` has the values 
  ``pig_current``. 
- Hive jobs also need a log file, so you'll need to make sure you
  specified the job property ``hive.querylog.location`` and a directory
  name. We use ``hivelogs`` in the tutorial, but any directory name is
  sufficient.
- If you are running queries on large datasets, you should specify filters and partitions 
  as much as possible because Hive will by default run queries on the largest set of data
  unless filters or partitions are specified.
- If your job is just taking a long time to complete, check the **Scheduler** page to
  see what is the available capacity is for your queue. You may want to use 
  a different queue.

.. _gs_troubleshooting-shell:

Shell Scripts
-------------

Make sure you have specified the path with the script (i.e., ``/user/{your_user_name}/script.sh``)
in the **Files** field.


.. *Home page* - shows your project and your history, queries, could share possibly.

.. Hive
.. Pig
.. Job Designer - Oozie Flow
.. => Dashboard is the Oozie Dashboard

.. Execute from Property page by clicking on arrow icon. Notification is shown in Job Browser.
.. You'll see your job in the Job Browser.
.. Can kill jobs with "Kill" button.


.. Name: oozie.actions.sharelib.for.pig
.. Value:  (pig_current, hcat_current - if you're going through HCat)

.. |arrowhead| image:: images/arrow_head.jpg
.. |files| image:: images/files.jpg
