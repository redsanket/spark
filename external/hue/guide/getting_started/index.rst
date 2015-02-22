===============
Getting Started
===============


Introduction
============

In this getting started, you will be using a Flickr data set
to explore the different features of Hue. 

We'll use the **Data Discovery** tool to find the location on the grid,
then use the **File Browser** to look at the data, the **Hive Query Editor**
to create a database and table. We'll look at the table data with the
**Metastore Manager** and then run Pig scripts and Hive queries on the data
through the **Pig Editor** and **Hive Query Editor**, respectively. 
While our queries are running, we'll take a look at the Hadoop jobs
in the **Job Browser**, and finally, we'll create a simple Oozie workflow
through the **Workflows** tool.

About the Tutorial
------------------

You'll notice throughout the tutorial that we are asking you to
use your user name as part of database and table names 
such as ``flickr_cc_100mb_{your_user_name}_db``. This is to
avoid name collisions and for the convenience of referencing later.
Once you are done with the tutorial and experimenting with the data,
we would appreciate it if you dropped the databases and tables you created
during the tutorial.

.. 0. Home? My Queries - saved queries, results, edits, copy, usage, trash


1. Discovering Data
===================

In addition to Hue, Yahoo provides you with the **Data Discovery**
tool that allows you to search for databases and tables in Hive
and then view the results in Hue.

#. Go to https://supportshop.cloud.corp.yahoo.com:4443/data-discovery/ to get to the **Data Discovery** tool.
#. Enter **Flickr** in the **Search** text field and click **Search**.
#. You'll see the database ``flickr_cc_100m_db`` in the Cobalt Blue cluster.
#. Click on the database and you'll be taken to a page giving general information and the schema.
#. This is the database we're going to be creating and using in Hue. Let's get going 
.. TBD: Once you find a database or table you can access, click **View in Hue**
..        to view the results in Hue.


2. Navigating HDFS With File Browser
====================================

#. Go to the Hue instance for Cobalt Blue: https://yo/hue.cb
#. Click the **File Browser** tab. You should see your home directory on Cobalt Blue that gives a long format listing of files.
#. Click ``/user`` and enter  
#. From the **File Browser**, navigate to the directory ``/user/sumeetsi/HueTalk/Flickr100cc``.
#. Double-click the file ``yfcc100m_dataset_copy_1.bz2``. This is the data we'll be using throughout this tutorial.
#. You'll see the data in hex. As of now, Hue doesn't allow you to view bz2 files as text. 

   .. note:: You can view gzipped files as text. As an example, see one of the gzipped files 
             in ``/user/sumeetsi/HueTalk/superbowl2014_tweets/``.

3. Getting Data
===============

We're going to be using the Flickr data we just looked at. Because you
can't download files right now, we're going to copy the data into
your home directory. 

#. Using **File Browser**, navigate to ``/user/sumeetsi/HueTalk/Flickr100cc`` if you are not there.
#. Check the checkbox next to the file ``yfcc100m_dataset_copy_1.bz2``.
#. From the **Actions** drop-down menu, select **Copy**.
#. A **Copy to** dialogue window will appear. From it, choose your home directory. 
#. Go to home directory by clicking the **Home** icon shown to the left of the directory path you're in. 


.. note:: You can also download files. If we were able to download files, we'd
          have used the upload feature to demonstrate. 

Other Ways
----------

- Upload data through **File Browser**
- Use the command line: ``$ hadoop fs -text {file_name}``
- Create an Oozie job in **Job Designer** that uses `DistCP <http://oozie.apache.org/docs/4.1.0/DG_DistCpActionExtension.html>`_.
 

4. Manipulating Files
=====================

#. From your home directory in **File Browser**, click **+ New->Directory** and enter the
   directory name ``hue_tutorial`` in the **Directory Name** text field and click **Create**.
#. Select the file ``yfcc100m_dataset_copy_1.bz2`` that 
   you just copied from ``/user/sumeetsi/HueTalk/Flickr100cc``.
#. From the **Actions** drop-down menu, select **Move**.
#. From the **Move to** dialogue window, enter the path ``/user/{your_user_name}/hue_tutorial/``.
#. Click the directory ``hue_tutorial``.
#. Check the checkbox next to the file ``yfcc100m_dataset_copy_1.bz2`` and select **Actions->Rename**.
#. Enter a simpler name like **flickr100m_dataset.bz2** and click **Rename**.
#. Finally, check the checkbox next to the file you renamed and click **Actions->Change Permissions**.
#. Uncheck **Group->Write** and **Other->Write**, and then click **Submit**.

Note
----

TBD: MIME type detection and pagination


5. Creating Database/Tables
===========================

We're going to use the **Hive Editor** to write a query that creates a table with the data
you copied to your home directory.

#. Click **Query Editors->Hive** to open the **Hive Editor->Query Editor**.
#. To create a database, in the **Query Editor** text area, enter the following query, replacing ``{user_name}`` with your own, and
   clicking **Execute**: ``create database flickr_{user_name}_100m_db comment 'Flickr Creative Commons 100M data dump' location '/user/{user_name}/hue_tutorial/'``;
#. Confirm that your database was created by clicking the **Database** drop-down menu and scrolling down or entering the name in the text.
#. Select the database you just created and run the following query to create an external table with the data you copied earlier to your home directory.
   (Be sure to replace the string ``{your_user_name}`` with your user name.)

   .. code-block:: sql

      create external table flickr_{your_user_name}_100m_db.flickr_{your_user_name}_100m_table (
         photoid bigint, 
         usernsid string, 
         userhandle string, 
         date_taken string, 
         date_imported bigint,
         camera string, 
         name string, 
         description string, 
         tags string, 
         machinetags string,
         longitude double, 
         latitude double, 
         accuracy int,
         photopage string, 
         photopixels string, 
         licensename string, 
         licenseurl string, 
         server int, 
         farm int, 
         secret string, 
         secreto string, 
         extension string,
         isvideo int
      )
      row format delimited
      fields terminated by '\t'
      lines terminated by '\n'
      location '/user/{your_user_name}/hue_tutorial/';

#. The **Log** pane will show you progress, and when the query has been executed, the **Results**
   pane will automatically open. The message will only say, however, that "The operation has no results."
#. To confirm the table has been created, click the **Refresh** icon next to **Database** in the left **Assist** pane.
   You should see your table displayed.
#. Confirm that your table has data by entering the following query (replacing ``{your_user_name}`` again) and clicking **Execute** again.

   .. code-block:: sql

      select count(1) as count, licensename from flickr_{your_user_name}_100m_table group by licensename sort by count;



   **Error:** Your query has the following error(s):

#. Before we look at data, click **Save as...**, enter **Count Flickr Licenses** in the **Name** and **Description** fields, and click **Save**.

6. Viewing Metadata and Data from Metastore Manager
===================================================

#. Click the **Metastore Manager** to open the **Metastore Manager**.
#. From the **Metastore Manager**, select your database from thee **DATABASE** drop-down menu.
#. Check checkbox next to the table ``flickr_{your_user_name}_100m_table`` and click **View**.
#. You'll see the **Columns** tab showing  column names with the type. 
#. Click the **Sample** tab to see example data from your table.
#. To see properties of the table, such as the owner, when it was created, table type, etc., click **Properties**.
#. You can also view the file location for the database by clicking **View File Location**.


Creating a Database and Table With the Metastore Manager (Optional)
-------------------------------------------------------------------

We created our Hive database and table earlier through the **Hive Query Editor**, but you
can do the same thing through the **Metastore Manager**. This is useful
for those not as familiar with HQL or want to import data into Hive.

Creating the Database
~~~~~~~~~~~~~~~~~~~~~

#. Click **Metastore Manager** in the top navigation.
#. Click the **Databases** link.
#. From the **Actions** pane on the left-hand side, click **Create a new database**.
#. Enter **sb2014_{your_user_name}** in the **Database Name** text field and click **Next**.
#. With the **Location** checkbox checked, click **Create database**.

Creating the Table
~~~~~~~~~~~~~~~~~~

#. From the **Databases** panel, find and then click the database you just created. Hint: It's
   easier to find through the search text field.
#. From the **ACTIONS** menu on the left-hand panel, click **Click a new table from a file**.
#. In the **Name Your Table and Choose A File** panel, enter the table name **sb2014_{your_user_name}_tb**
   in the **Table Name** text field and for the **Input File**, navigate to 
   **/user/sumeetsi/HueTalk/superbowl2014/superbowl2014_tweets/20140202_014112_e97baf5d-42b8-4d91-8b61-017afdbd4b89.csv.gz**.
#. With **Import data from file** checked, click **Next**.
#. From the **Choose a Delimiter** panel, use the **Delimiter** drop-down menu to choose **Other**, enter
   the vertical bar character **|**, and click **Preview**.

   Your data in the **Table preview** should look more normalized, but the column names are obviously 
   just autogenerated. We'll fix this soon.
#. Click **Next**.
#. In another tab, use the **File Browser** to navigate to ``/user/sumeetsi/HueTalk/superbowl2014/header.csv``.
#. You should see the column names for our table:

   - ``username``
   - ``timestamp``
   - ``tweet``
   - ``retweetcount``
   - ``on``
   - ``at``
   - ``country``
   - ``name``
   - ``address``
   - ``type``
   - ``placeURL``

#. Going back to the **Metastore Manager**, in the **Define your columns**, enter the column names
   listed in the previous step to replace the column names from ``col_0`` to ``col_10``. 
#. Click **Create Table**.
#. You'll see the **Log** file until the results are available, at which time, you'll be taken
   to the **Databases > sb2014_{your_user_name} > sb2014_{your_user_name}_tb** panel, where you
   can view the columns (names and types), sample data, and table properties.

7. Querying Data With Hive and Pig
==================================

Using Hive
----------

We have our Flickr database and table, and if you used the **Metastore Manager**, you also
have a database and table for tweets for Superbowl 2014. In this section,
we're going to use the **Hive Query Editor** to execute queries on the
Flickr table. We recommend that you try your own queries for the Superbowl table if
you created one.

#. Go to the **Hive Query Editor**. (Click **Query Editors->Hive**.)
#. From the **Assist** panel on the left-hand side, find your Flickr database from the **Database** drop-down menu.
   You should see the one table we created on the **Assist** panel.
#. Click the **flickr_{your_user_name}_100mb_table** to see the available fields.
#. Double-click the table name to have the name automatically added to the **Query Editor**.
#. Enter the following query to **Query Editor** window to see the location of different cameras:

   ``select camera, longitude, latitude from flickr_jcatera_100m_table;``
#. From the **Results** tab, you'll see the list of cameras and their location.
#. Click the **Chart** to see a graphic representation of the results.
#. The default **Chart type** is **Bars** with the **X-Axis** containing the
   cameras, and the **Y-Axis** containing the longitude.
#. Click the **Map** icon and select **latitude** from the **Latitude** drop-down menu,
   **longitude** from the **Longitude** drop-down menu, and **camera** for the **Label**
   drop-down menu.
#. You should see a map with map markers. If you click on the map markers, you'll
   see the camera used at the marked location.
#. In the top-right corner of the bottom pane, you'll see four icons. Click the
   the third icon to save the results to HDFS. 
#. In the **Save Query Results** dialog window, enter the path **/user/jcatera/hue_tutorial/flickr_camera_location.csv**
   in the **In an HDFS file** text field and click **Save**. (We're going to use this file later
   when we look at the **Pig Editor**.)

#. Click **Explain** to see an analysis of the stages, operators, stages of execution,
   the output columns, which you can use to troubleshoot or optimize your queries.
   TBD: Ask whether this needs to be in tutorial or ask for more of an explanation.
#. Click **Explain** to see an analysis of the stages, operators, stages of execution,
   the output columns, which you can use to troubleshoot or optimize your queries.
#. Let's save our query by clickng **Save as...**, entering **Flickr Camera Location Query**, and clicking **Save**.


..  Hive Editor: query log, results, fullscreen result, save results to HDFS, download to Excel (csv,xls). 
.. Setting panel:  Key-Value, File Resources - JAR, UDFS - name/class
.. Question icon: shows an "Assist" window that will assist you in writing Pig scripts, operators,
.. relational operators, 
.. Easy Query Settings: configs, parameters, etc.

Using Pig
---------

#. From the top-navigation bar, click  **Query Editors** and select **Pig**.
#. In the **Pig Editor** window, enter the following code, replacing ``{your_user_name}`` with
   your own user name.
   
   .. code-block:: pig
  
      -- Load the CSV you downloaded from the Query Editor.
      raw = LOAD '/user/{your_user_name}/hue_tutorial/flickr_camera_location.csv' USING PigStorage(',') AS (camera, longitude, latitude);

      -- Filter out the rows that do not have values for camera or null values for the longitude/latitude.
      has_camera = FILTER raw BY camera is not null;
      has_long = FILTER has_camera BY not longitude matches 'NULL';
      has_lat = FILTER has_long BY not latitude matches 'NULL';

      -- Store the results to a file.
      STORE has_lat into 'flickr_camera_location' USING PigStorage (',');
      
#. Click **Save** in the right-hand **Editor** panel, enter the text **Flickr Camera Location Script**
   in the text field and click **Save**.
#. Click **Properties** from the left-hand **Editor** pane.
#. From **Hadoop properties** on the right-hand panel, click **+ Add**.
#. For the **Name** field, enter the value **oozie.action.sharelib.for.pig**.
#. For the **Value** field, enter the value **pig_current, hcat_current**.
#. From **Resources**, click **+ Add**.
#. With the value **File** in the **Type** drop-down menu, enter **/user/sumeetsi/HueTalk/hive-site.xml**
   for the **Value** text field.
#. Click the arrowhead icon in the top-right corner to run your script.

   The script should save only rows that have a camera name, longitude, and latitude, 
   and write results to the directory ``flickr_camera_location``. 
#. After your script has finished running, use **File Browser** to view the results
   in the HDFS path ``/user/{your_user_name}/flickr_camera_location/part-m-00000``.


Tips
----

The **Assist** panel helps you write Pig scripts. To see completed jobs, click **Dashboard**. 
The **Scripts** tab lists your past scripts for your reference.

.. Uses Oozie to execute Pig.

8. Creating Actions With the Job Designer
=========================================

Hue lets you create workflows in two ways: as an
action or through Oozie workflows, coordinators,
and bundles. The **Job Designer** makes it create a simple Oozie workflow to execute
one action without worrying about the configuration.

We're going to use the **Job Designer** to create an action in this
section and then use the **Oozie Workflows Editor** to create an Oozie workflow
in the next section.


#. From top navigation bar, click the **Query Editors** and select **Job Designer**.
#. From the **Designs** panel, click **New action** and select **Fs** as your action.
#. Enter **delete_pig_output** in the **Name** text field and **Removing results from Pig script
   Flickr Camera Location Script** for the **Description** text field.
#. Click **Add path** next to **Delete path** and enter the path ``/user/{your_user_name}/flickr_camera_location/``.
   We're deleting the path so we can run our Pig script again in an Oozie job that we 
   create through the **Workflows Editor** in the next section.
#. Click **Save**.
#. From the **Designs** pane, select the action that you just saved and click **Submit**.

#. Once your job has completed, you'll be taken to the **Workflow** pane has tabs 
   to view the action progress, details (time, application path),
   configuration (jobTracker,nameNode, Oozie path, etc.), log, and definition (workflow XML).


9. Creating Workflows With the Oozie Editor
===========================================

With the **Workflows Editor**, you're configuring Oozie to
run tasks in a job. This lets you create Oozie workflows,
coordinators (set of workflows), and bundles (set of coordinators).
We're just going to create an Oozie job to do the work we have
been doing with Hue up until now.

#. From the top-navigation bar, click **Workflows** and select **Editors->Workflows**.
#. Click **+ Create** to start creating a new workflow.
#. Enter **hue_tutorial_workflow** in the **Name** field and click **Save**.

#. Delete Pig results
#. Create a new directory for results.
#. DistCp file to directory
#. Create new Hive table.
#. Run Hive query and save results to file.
#. Run Pig script to clean up results.

TBD: 


Notes
-----

TBD: Kill or supend jobs.



.. _viewing_jobs:

10. Viewing and Managing Jobs
=============================

From the **Job Browser**, you can view  your jobs and
other jobs. You can sort jobs by status, search for jobs 
by a user or key term, also look at the cluster and ResourceManager logs.

#. Let's first look for our jobs by clicking **Job Browser**.
#. By default, the **Job Browser** shows Oozie jobs sorted by your username, so 
   you should see the job that executed your Pig script as Pig scripts are run by Oozie.
#. 
#. You many not see any jobs at first because the **Job Browser** by default
   looks for jobs you own. Delete your user name from the **Username** text
   field. You should see all the jobs owned by others.
#. Sort by failed jobs by clicking **Failed**. 
#. You can view the cluster log by clicking the log ID of a job. Try clicking the 
   job ID of the first job in the list.
#. The cluster log gives you the user, application type, state, start time, tracking URL,
   and a diagnotic message. Click on the **Tracking URL** in another tab to
   see **Job** log.
#. The **Job** log gives you more detailed information such as the total
   number of successful, completed, and failed Map and Reduce tasks.
#. From the **Application Master** table, click the **Node** link to
   view the **NodeManager** to see detailed information about the
   container, such as the virtual memory allocated, Pmem enforced, virtual cores, etc.

Let's start a job now and take a look at the job in the **Job Browser**.

#. Open up the **Hive Query Editor** in another tab. 
#. From your **Recent Queries** tab, double-click your last Hive query.
#. With the query in the **Query Editor** window, click **Execute**.
#. Now go back to the **Job Browser** and enter your username  in the **Username** text field.
   You should see your job with the **Running** status.
#. Take a look at the cluster, **Job**, and **NodeManager** logs.  
  






*Home page* - shows your project and your history, queries, could share possibly.

.. Hive
.. Pig
.. Job Designer - Oozie Flow
.. => Dashboard is the Oozie Dashboard

.. Execute from Property page by clicking on arrow icon. Notification is shown in Job Browser.
.. You'll see your job in the Job Browser.
.. Can kill jobs with "Kill" button.


.. Name: oozie.actions.sharelib.for.pig
.. Value:  (pig_current, hcat_current - if you're going through HCat)

.. For Using HCat:

.. Under every cluster, you add /sharelib/v1/hive/hive-0.13.0.3.1411171801/libexec/hive/conf/hive-site.xml
.. as the resource.















