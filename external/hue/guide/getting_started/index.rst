===============
Getting Started
===============

**Time Required:** 30-40 minutes

.. _hue_getting_started-intro:

Introduction
============

In this getting started, you will be getting familiar with the Hue
interface and learn how to use Hue to use Hadoop 
technology such as HDFS, Hive, Pig, Oozie, and Job Browser.


We'll be using Flickr data to do the following in this tutorial:

- Use the **Data Discovery** tool to find the location of data on the grid.
- View data and work with files/directories with the **File Browser**. 
- Create a database and table with the **Hive Query Editor**.
- View data with the **Metastore Manager**.
- Run Hive queries on data with the **Hive Query Editor**.
- Process data with Pig through the **Pig Editor**.
- Create a simple Oozie Workflow through the **Oozie Editor** to 
  execute a Hadoop job with several tasks. 
- Monitor Hadoop jobs in the **Job Browser**.


.. _hue_getting_started-prereq:

Prerequisites
-------------

- Configure your browser to :ref:`use the SOCKS proxy <using_socks_proxy>` to connect to the Internet.
  (We recommend using Mozilla's Firefox browser.)
- If you're a newbie to Hadoop, read the
  following:

  - `What is Hadoop? <http://hadoop.apache.org/#What+Is+Apache+Hadoop%3F>`_
  - `Hive Wiki <https://cwiki.apache.org/confluence/display/Hive/Home>`_
  - `Pig introduction <http://pig.apache.org/>`_
  - `Oozie Overview <http://oozie.apache.org/>`_
  - `Getting Started on the Yahoo Grid <https://yahoo.jiveon.com/docs/DOC-46590>`_

.. _hue_getting_started-about:

About the Tutorial 
------------------

.. _about-org:

Organization
~~~~~~~~~~~~

The tutorial is divided into nine sections, starting 
from the simplest uses such as navigating HDFS and 
then gradually becoming more complex until you're 
creating an Oozie Workflow to automate several
jobs. For each section, we also offer additional 
tips for using Hue.


.. _about-conventions:

Conventions
~~~~~~~~~~~

You'll notice throughout the tutorial that we are 
asking you to use your user name as part of database 
and table names as well as file names. For example,
the database you'll be creating will have the naming syntax 
``flickr_{your_user_name}_db``. This is to avoid name 
collisions and for the convenience of referencing later.


Hue UI components and text you are to enter in dialog windows will be indicated 
in bold. Code, however, for scripts that you'll be using will be in monospaced font.

For icons, we will refer to them by name rather than 
use an inline image. For example, to run Pig scripts,
you be told to click the **arrowhead** icon rather
than shown a shrunken image of the icon.


.. _about-queues:

Hadoop Queues
~~~~~~~~~~~~~

Also, we'll be using the ``default`` queue for running jobs, but we suggest
that you use your team's queue because for faster job processing. We'll
show you how to set the queue in the tutorial.

.. _hue_getting_started-disc_data:

1. Discovering Data
===================

In addition to Hue, Yahoo provides you with the **Data Discovery**
tool that allows you to search for databases and tables in Hive
and then view the results in Hue.

#. Go to https://supportshop.cloud.corp.yahoo.com:4443/data-discovery/ to get to the **Data Discovery** tool.
#. The first time going to the tool, you'll need to click **Add Exception..** and then **Confirm Security Exception** as shown below:

   .. image:: images/certificate.jpg
      :height: 389px
      :width: 950 px
      :scale: 90%
      :alt: Getting Certificates for the Data Discovery Tool  
      :align: left      

#. From the **Data Discovery** tool, enter **Flickr** in the **Search** text field and click **Search**.

   .. image:: images/dd_search_flickr.jpg
      :height: 508px
      :width: 950 px
      :scale: 90%
      :alt: Data Discovery Tool
      :align: left      

#. From the **Cluster** drop-down menu, select **Tiberium Tan**.

   .. image:: images/select_cluster.jpg
      :height: 551px
      :width: 950 px
      :scale: 90%
      :alt: Select Cluster From Data Discovery Tool

#. From the **Search** page, with **Tiberium Tan** selected as the cluster,
   enter **flickr_rbernota_db** in the **Database** field. The database **flickr_rbernota_db**
   and the table **flickr_rbernota_table** will be displayed.

   .. image:: images/select_db_table.jpg
      :height: 298px
      :width: 950 px
      :scale: 90%
      :alt: Enter the Database and Table From Tiberium Tan

#. Click on the database and you'll be taken to a page giving general information and the schema.
   Click **View in Hue**.

   .. image:: images/general_info_flickr_db.jpg
      :height: 603px
      :width: 950 px
      :scale: 90%
      :alt: Flickr Database Info
      :align: left 
    
#. Add the exception and accept the certificate for Hue as you did for the **Data Discovery**.
   You should see the **flickr_rbernota_db** database in **Hue** as shown below:
   
   .. image:: images/hue_flickr_db.jpg
      :height: 490px
      :width: 950 px
      :scale: 90%
      :alt: Flickr Database Info in Hue
      :align: left 

#. Next, we'll use Hue to browse the data in **Hue**.

.. tip::  From the `Data Discovery <https://supportshop.cloud.corp.yahoo.com:4443/data-discovery/>`_ 
          tool, you can also click **Browse** to select a cluster, 
          database, and table. From the **General Info** page,
          you can view the table in **Hue**.

.. _hue_getting_started-file_browser:

2. Navigating HDFS With File Browser
====================================

#. From the **Metastore Manager**, click **File Browser** located in the the top navigation bar. 

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
#. Click the **pen** symbol next to the **/user** path, type **/rbernota**, and then the press the **enter** key.

   .. image:: images/rbernota_dir.jpg
      :height: 276px
      :width: 950 px
      :scale: 90%
      :alt: Hue Rick Bernotas Directory
      :align: left 

#. From the **File Browser**, navigate to the directory ``/user/rbernota/HueTalk/Flickr100cc``.

   .. image:: images/hue_talk_dataset.jpg
      :height: 205px
      :width: 950 px
      :scale: 90%
      :alt: Hue Talk Dataset 
      :align: left 

   The file ``flickr100m_dataset.bz2`` contains the data we'll be using throughout this tutorial.  

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


.. _hue_getting_started-create_db_tables:

3. Creating Database/Tables
===========================

We're going to use the **Hive Editor** to write a query that creates a table with the data
you copied to your home directory.

#. Click **Query Editors->Hive** to open the **Hive Query Editor**.

   .. image:: images/start_hive_editor.jpg
      :height: 445 px
      :width: 912 px
      :scale: 90%
      :alt: Starting Hive Editor
      :align: left 
   
#. To create a database, in the **Query Editor** 
   text area, enter the query below, replace
   ``{user_name}`` with your own, and
   click **Execute**::

       create database flickr_{user_name}_db comment 'Flickr Creative Commons 100M data dump' location '/user/rbernota/HueTalk/Flickr100cc/';
       

   .. image:: images/hive_editor.jpg
      :height: 162 px
      :width: 950 px
      :scale: 90%
      :alt: Creating a Database With the Hive Editor
      :align: left 

   The **Log** pane will show you progress, and when 
   the query has been executed, the **Results**
   pane will automatically open. The message will only 
   say, however, that "The operation has no results."

   .. image:: images/db_log_no_results.jpg
      :height: 266 px
      :width: 950 px
      :scale: 90%
      :alt: Database Created
      :align: left 

#. Confirm that your database was created by clicking 
   the **Database** drop-down menu and scrolling down 
   or entering the name in the text field.
   (You may need to click the **Refresh** icon next to 
   the **Database** label to see your new database.)
   
   .. image:: images/refresh_database.jpg
      :height: 316 px
      :width: 202 px
      :scale: 100%
      :alt: Refresh Databases
      :align: left 

#. With your database selected, run the following query to create an external 
   table with the data you copied earlier to your home directory.
   (Be sure to replace the string ``{your_user_name}`` with your user name.)

   .. code-block:: sql

      create external table flickr_{your_user_name}_db.flickr_{your_user_name}_table (
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
      location '/user/rbernota/HueTalk/Flickr100cc/';

#. Once again, you should get a log page saying that "The operation has no results." To confirm the table 
   has been created, click the **Refresh** icon next to **Database** in the left **Assist** pane.
   You should see your table displayed.
#. Confirm that your table has data by entering the following query (replacing ``{your_user_name}`` again) and clicking **Execute** again.

   .. code-block:: sql

      select count(1) as count, licensename from flickr_{your_user_name}_table group by licensename sort by count;


#. After the Hadoop job has completed, you should see results similar to the following:

   .. image:: images/flickr_query_results.jpg
      :height: 199 px
      :width: 950 px
      :scale: 90%
      :alt: Flickr Query Results
      :align: left 

#. Click the **file** icon shown below to save the results to HDFS.

   .. image:: images/save_results_button.jpg
      :height: 207 px
      :width: 950 px
      :scale: 90%
      :alt: Save Results Button
      :align: left 

#. In the **Save Query Results** dialog box, enter the 
   path **/user/{your_user_name}/hue_tutorial/flickr_licenses.csv**,
   and click **Save**.

   .. image:: images/flickr_licenses_csv.jpg
      :height: 185 px
      :width: 478 px
      :scale: 90%
      :alt: Save Results as a CSV File
      :align: left 


#. Once the file has been saved, you will be shown the contents in the **File Browser**.
   Notice on the left-hand side, you can modify the file by clicking **Edit file**.

   .. image:: images/file_browser_view_file.jpg
      :height: 404 px
      :width: 709 px
      :scale: 90%
      :alt: Viewing File in File Browser
      :align: left 

.. tip::  The **Query Editor** provides a couple of ways to help you.
          
          - Mousing over the **Question Mark** icon on the 
            top-right corner of the editing field tells you 
            how to use autocomplete, run multiple statements,
            or run a partial statement.
          - You can also save a query by clicking **Save as...**, 
            entering a name, and clicking **Save**. 
          - Click **Explanation** to see the dependencies, the edges and
            vertices of the Tez directed acyclic graph (DAG) as well as 
            the operations for the maps and reducers.
            


.. _hue_getting_started-view_metadata:

4. Viewing Metadata and Data from Metastore Manager
===================================================

#. From the top navigation bar, click the **Metastore Manager** to open the **Metastore Manager**.

   .. image:: images/open_metastore_manager.jpg
      :height: 255 px
      :width: 921 px
      :scale: 90%
      :alt: Opening Metastore Manager
      :align: left 

#. From the **Metastore Manager**, select your database from the **DATABASE** drop-down menu.
#. Check the checkbox next to the table 
   **flickr_{your_user_name}_table** and click **View**.

   .. image:: images/metastore_view_data.jpg
      :height: 229 px
      :width: 840 px
      :scale: 92%
      :alt: Viewing Data in the Metastore Manager
      :align: left 

#. You'll see the **Columns** tab showing  column names with the type. 

   .. image:: images/metastore_cols.jpg
      :height: 663 px
      :width: 643 px
      :scale: 92%
      :alt: Metastore Manager Columns
      :align: left 
 
#. Click the **Sample** tab to see example data from your table.

   .. image:: images/sample_data.jpg
      :height: 553 px
      :width: 950 px
      :scale: 90%
      :alt: Sample Data
      :align: left 
   
#. To see properties of the table, such as the owner, when it was created, table type, etc., click **Properties**.

   .. image:: images/table_properties.jpg
      :height: 738 px
      :width: 830 px
      :scale: 90%
      :alt: Table Properties
      :align: left 

#. You can also view the file location for the database by clicking **View File Location**.

.. tip:: If you're not familiar with HiveQL, you can use
         the **Metastore Manager** to create or drop tables.
         See the next optional section to learn how to 
         create a table.
         

(Optional) Creating a Database and Table With the Metastore Manager 
-------------------------------------------------------------------

We created our Hive database and table earlier through the 
**Hive Query Editor**, but you can do the same thing through 
the **Metastore Manager**. This is useful
for those not as familiar with HQL or who want to import data 
into Hive.

Creating the Database
~~~~~~~~~~~~~~~~~~~~~

#. Click **Metastore Manager** in the top navigation bar.
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
   **/user/rbernota/HueTalk/superbowl2014/superbowl2014_tweets/20140202_045947_e97baf5d-42b8-4d91-8b61-017afdbd4b89.csv.gz**.
#. With **Import data from file** checked, click **Next**.
#. From the **Choose a Delimiter** panel, use the **Delimiter** drop-down menu to choose **Other**, enter
   the vertical bar character **|**, and click **Preview**.

   Your data in the **Table preview** should look more normalized, but the column names are obviously 
   just autogenerated. We'll fix this soon.
#. Click **Next**.
#. In another tab, use the **File Browser** to navigate to ``/user/rbernota/HueTalk/superbowl2014/header.csv``.
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

#. Going back to the **Metastore Manager**, in the 
   **Define your columns**, enter the column names
   listed in the previous step to replace the column 
   names from **col_0** to **col_10**. 
#. Click **Create Table**.
#. You'll see the **Log** file until the results are available, at which time, you'll be taken
   to the **Databases > sb2014_{your_user_name} > sb2014_{your_user_name}_tb** panel, where you
   can view the columns (names and types), sample data, and table properties.


.. _hue_getting_started-query_data:

5. Querying Data With Hive and Pig
==================================

.. _query_data-hive:

Using Hive
----------

We have our Flickr database and table, and if you used the **Metastore Manager**, you also
have a database and table for tweets for Superbowl 2014. In this section,
we're going to use the **Hive Query Editor** to execute queries on the
Flickr table. We recommend that you try your own queries for the Superbowl table if
you created one.

#. Go to the **Hive Query Editor**. (Click **Query Editors->Hive**.)
#. From the **Assist** panel on the left-hand side, find your Flickr database from the **DATABASE** drop-down menu.
   You should see the one table we created on the **Assist** panel.
#. Click the **flickr_{your_user_name}_table** to see the available fields.

   .. image:: images/assist_panel.jpg
      :height: 533 px
      :width: 213 px
      :scale: 90%
      :alt: Table Fields
      :align: left 

#. Enter the following query to **Query Editor** window to see the location of different cameras:

   ``select camera, longitude, latitude from flickr_{your_user_name}_table;``
#. Click **Execute**. From the **Results** tab, you'll see the 
   list of cameras and their location.
#. Click the **Chart** to see a graphic representation of the results.

   .. image:: images/basic_chart.jpg
      :height: 245 px
      :width: 950 px
      :scale: 90%
      :alt: Basic Chart
      :align: left 

   The default **Chart type** is **Bars** with the **X-Axis** containing the
   cameras, and the **Y-Axis** containing the longitude.
#. Click the **Map** icon and select **latitude** from the **Latitude** drop-down menu,
   **longitude** from the **Longitude** drop-down menu, and **camera** for the **Label**
   drop-down menu.

   .. image:: images/map_chart.jpg
      :height: 358 px
      :width: 950 px
      :scale: 90%
      :alt: Map Chart
      :align: left 

   You should see a map with map markers. If you click on the map markers, you'll
   see the camera used at the marked location.

#. In the top-right corner of the bottom pane, you'll see four icons. Click the
   the **disk** icon to save the results to HDFS. 

   .. image:: images/save_csv.jpg
      :height: 358 px
      :width: 950 px
      :scale: 90%
      :alt: Save CSV files.
      :align: left 


#. In the **Save Query Results** dialog window, enter the path **/user/{your_user_name}/hue_tutorial/flickr_camera_locations.csv**
   in the **In an HDFS file** text field and click **Save**. (We're going to use this file later
   when we look at the **Pig Editor**.)
#. Use the **File Browser** to verify the file has been saved.

.. _using_pig:

Using Pig
---------

#. From the top-navigation bar, click  **Query Editors** and 
   select **Pig**.

   .. image:: images/start_pig.jpg
      :height: 252 px
      :width: 724 px
      :scale: 92%
      :alt: Starting Pig Editor
      :align: left 


#. In the **Pig Editor** window, enter the following code, replacing ``{your_user_name}`` with
   your own user name.
   
   .. code-block:: pig
  
      -- Load the CSV you downloaded from the Query Editor.
      
      raw = LOAD '/user/{your_user_name}/hue_tutorial/flickr_camera_locations.csv' USING PigStorage(',') AS (camera:chararray, longitude:long, latitude:long);
      data = FOREACH raw GENERATE camera, longitude, latitude;
      has_camera = FILTER data  BY camera is not null;
      has_long = FILTER has_camera BY longitude is not null;
      has_lat = FILTER has_long BY latitude is not null;
      
      STORE has_lat into '/user/{your_user_name}/hue_tutorial/flickr_camera_locations_sanitized' USING PigStorage(',');
      
#. Click **Save** in the left-hand **Editor** panel.

   .. image:: images/editor_save.jpg
      :height: 297 px
      :width: 207 px
      :scale: 100%
      :alt: Pig Editor: Save
      :align: left 

#. In the **Save script** dialog window, enter 
   the text **Flickr Camera Location Script**
   in the text field and click **Save**.

   .. image:: images/save_pig_script.jpg
      :height: 204 px
      :width: 478 px
      :scale: 95%
      :alt: Saving Pig Script 
      :align: left 

#. To run a Pig script, you'll need to add some configuration. 
   Click **Properties** from the left-hand **Editor** pane.

   .. image:: images/pig_properties.jpg
      :height: 407 px
      :width: 671 px
      :scale: 92%
      :alt: Pig Properties
      :align: left 

#. From **Hadoop properties** on the right-hand panel, click **+ Add**.
#. For the **Name** field, enter **oozie.action.sharelib.for.pig**, and for the 
   **Value** field, enter **pig_current**.

   .. image:: images/pig_hadoop_properties.jpg
      :height: 349 px
      :width: 950 px
      :scale: 90%
      :alt: Hadoop Properties for Pig 
      :align: left 

#. Click **Save**.
#. Run your script by clicking the **arrowhead** icon in the top-right corner. 
   (It may take a few minutes to complete.)

   .. image:: images/run_pig_button.jpg
      :height: 199 px
      :width: 950 px
      :scale: 90%
      :alt: Run Pig Button
      :align: left 


   The script should save only rows that have a camera name, longitude, and latitude, 
   and write results to the directory ``flickr_camera_location``. 
#. After your script has finished running, use **File Browser** to view the results
   in the HDFS path ``/user/{your_user_name}/hue_tutorial/flickr_camera_location_sanitized/``.


.. tip:: The **Assist** sidebar helps you write Pig scripts. You 
         can click functions to add them to the editing field.

         The **Scripts** tab lists your past scripts for your reference.
         You can also share your scripts with others with the 
         **Share** tab. 


6. Saving Scripts to Files
==========================

In this section, we're going to be creating a directory 
and saving the HQL and Pig scripts to files, so that we
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
   
#. In the **Create Directory** dialog window, enter **hue_scripts** 
   in the **Directory Name** text field for the directory name
   and click **Create**.
  
   .. image:: images/create_new_dir.jpg
      :height: 162 px
      :width: 481 px
      :scale: 100%
      :alt: Creating the Hue Scripts Directory
      :align: left 	

   We're creating a new directory to include scripts because our Oozie Workflow will be removing and recreating 
   the directory **hue_tutorial**.
#. Navigate to the new directory **hue_scripts** and click **New->File**.
#. In the **Create File** dialog box, enter **del_db_tables.hql**.

   .. image:: images/del_db_tables_file.jpg
      :height: 180 px
      :width: 480 px
      :scale: 100%
      :alt: Creating the Script to Delete Database Tables
      :align: left 	

   We're creating a script that deletes the Flickr database 
   and tables. 
#. Click **del_db_tables.hql**.
#. From the **Actions** panel, click **Edit file** to open an editing pane.

   .. image:: images/edit_file.jpg
      :height: 372 px
      :width: 950 px
      :scale: 90%
      :alt: Edit the File
      :align: left 	
   
#. Enter the following text in the editing field and click **Save**. (Be sure to replace ``{your_user_name}`` with your user name.)

   .. code-block:: sql

      drop table if exists flickr_{your_user_name}_db.flickr_{your_user_name}_table;
      drop table if exists flickr_{your_user_name}_db.flickr_camera_location;
      drop database flickr_{your_user_name}_db;
 

#. In the same directory, create the file **create_db_tables.hql** 
   to create the database and tables for the Flickr data with 
   the following code:

   .. code-block:: sql

       create database flickr_{your_user_name}_db  comment 'Flickr Creative Commons 100M data dump' location '/user/{your_user_name}/hue_tutorial/';
       use flickr_{your_user_name}_db; 

       create external table flickr_{your_user_name}_table (
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
 
   Once again, be sure to replace ``{your_user_name}`` with your
   user name.

#. Create another file **camera_location_query.hql** with the following: 
   
   .. code-block:: sql

      use flickr_{your_user_name}_db;
      SET hive.exec.compress.output=false;

      CREATE TABLE flickr_camera_location row format delimited fields terminated by ','  
      STORED AS TEXTFILE AS select camera, longitude, latitude from flickr_{your_user_name}_table;

   This will create a smaller table with only three columns from 
   our original Flickr table.

#. To merge all of the CSV data into one file, in the same directory, create the file
   **create_camera_location_csv.sh** with the following:

   .. code-block:: bash

      #!/bin/bash

      hdfs dfs -cat /user/{your_user_name}/hue_tutorial/flickr_camera_location/\* | hdfs dfs -put - /user/{your_user_name}/hue_tutorial/flickr_camera_locations.csv

#. Finally, we want to create the Pig script **remove_null_locations.pig** in the **hue_scripts** directory with the
   code below:

   .. code-block:: pig

      -- Load the CSV you downloaded from the Query Editor.
      
      raw = LOAD '/user/{your_user_name}/hue_tutorial/flickr_camera_locations.csv' USING PigStorage(',') AS (camera:chararray, longitude:long, latitude:long);
      data = FOREACH raw GENERATE camera, longitude, latitude;
      has_camera = FILTER data  BY camera is not null;
      has_long = FILTER has_camera BY longitude is not null;
      has_lat = FILTER has_long BY latitude is not null;
      
      STORE has_lat into '/user/{your_user_name}/hue_tutorial/flickr_camera_locations_sanitized' USING PigStorage(',');
      
   This is the Pig script we used before: it removes rows that 
   do not have a value for the camera, longitude, or latitude.  

#. Great, we have our scripts. We're still going to need to 
   do a few more things for our Oozie Workflow,
   but we're going to use the **Oozie Editor** next to complete the 
   process. 


.. 7. Creating Actions With the Job Designer
.. =========================================
.. 
.. Hue lets you create workflows in two ways: as an
.. action or through Oozie workflows.
.. The **Job Designer** makes it create a simple Oozie 
.. workflow to execute one action without worrying 
.. about the configuration.
.. 
.. We're going to use the **Job Designer** to create 
.. a couple of actions that we'll import from 
.. Oozie workflow we create in the next section.
.. 
.. #. From the top navigation bar, click **Query Editors->Job Designer**.
.. 
..    .. image:: images/open_job_designer.jpg
..       :height: 137 px
..       :width: 499 px
..       :scale: 95%
..       :alt: Opening Job Designer
..       :align: left 	
.. 
.. #. From the **Designs** panel, click **New action** and select **Fs** as your action.
.. 
..    .. image:: images/jd_refresh_tutorial.jpg
..       :height: 276 px
..       :width: 950 px
..       :scale: 90%
..       :alt: Create Fs Job
..       :align: left 	
.. 
.. #. Enter **hue_tutorial_refresh** in the **Name** text field and **Cleaning up HDFS for Hue tutorial** 
..    for the **Description** text field.
.. 
..    .. image:: images/hue_tutorial_refresh_desc.jpg
..       :height: 202 px
..       :width: 950 px
..       :scale: 90%
..       :alt: Refresh Tutorial Description
..       :align: left 	
.. 
.. 
.. #. Specify the paths to delete and create by doing the following:
..    
..    #. Click **Add path** next to **Delete path** and enter the path **/user/{your_user_name}/hue_tutorial/**.
..    #. To recreate the directory for the latest results, in the **Create directory** field, enter the directory **/user/{your_user_name}/hue_tutorial/**.
..    #. Click **Save**.
.. 
.. 
..       .. image:: images/hue_tutorial_delete_paths.jpg
..          :height: 429 px
..          :width: 789 px
..          :scale: 92%
..          :alt: Specify Delete Paths
..          :align: left 	
.. 
..    We're deleting the path so we can run our scripts 
..    again in an Oozie job that we 
..    create through the **Workflows Editor** in the 
..    next section.
.. 
.. #. From the **Designs** panel, click **New action** and select **Email** as your action.
.. 
..    .. image:: images/create_mail_notification.jpg
..       :height: 282 px
..       :width: 950 px
..       :scale: 90%
..       :alt: Create a Mail Notification
..       :align: left 	
.. 
.. #. Enter **hue_tutorial_notification** in the **Name** text field and **Email Notification for the Hue Tutorial**
..    for the **Description** text field.
.. 
..    .. image:: images/email_notification_desc.jpg
..       :height: 273 px
..       :width: 734 px
..       :scale: 92%
..       :alt: Add Description for Notification Mail
..       :align: left 	
..     
.. #. In the **TO addresses**, enter your email address. In the **Subject** field, enter **Hue Tutorial Oozie Workflow Has Completed**.
..    Finally, in the **Body** text area, enter the 
..    following and be sure to replace ``{your_user_name}`` with your user name:: 
.. 
..        See the sanitized 
..        CSV file with the Flickr camera locations at the 
..        following URL: 
..        https://cobaltblue-hue.blue.ygrid.yahoo.com:9999/filebrowser/#/user/{your_user_name}/hue_tutorial/flickr_camera_locations_sanitized
.. 
..    .. image:: images/workflow_email_notification.jpg
..       :height: 253 px
..       :width: 950 px
..       :scale: 90%
..       :alt: Email Address and Body for Notification
..       :align: left 	
..    
.. #. Click **Save**.
.. #. From the **Designs** pane, check the **hue_tutorial_notification** checkbox and click **Submit**.
.. 
..    .. image:: images/submit_email_notification_job.jpg
..       :height: 441 px
..       :width: 812 px
..       :scale: 91%
..       :alt: Submit Job
..       :align: left 	
..    
.. #. You'll be taken to the **Workflow** pane and quickly see that the **Status** indicate **Succeeded** and
..    the **Progress** bar reach **100%**. You should receive the notification email in a few minutes, too.
.. 
..    .. image:: images/job_successful.jpg
..       :height: 493 px
..       :width: 950 px
..       :scale: 91%
..       :alt: Successful Job
..       :align: left 	
..     
.. #. We're going to create an Oozie Workflow next, which will use 
..    the actions that we just created.


7. Creating Workflows With the Oozie Editor
===========================================

With the **Workflows Editor**, you're configuring Oozie to
run tasks in a job. This lets you create Oozie workflows,
coordinators (set of workflows), and bundles (set of coordinators).
We're just going to create an Oozie Workflow to automate
what we've done thus far. 

.. note:: As with the steps before, replace 
          ``{your_user_name}`` in the given user input 
          with your actual user name.
          Henceforth, we're going to omit 
          any prompts or reminders to do so.   

#. From the top-navigation bar, click **Workflows** and then click
   **Editors->Workflows**.

   .. image:: images/open_oozie_editor.jpg
      :height: 194 px
      :width: 663 px
      :scale: 93%
      :alt: Open Oozie Editor
      :align: left 	

#. Click **+ Create** to start creating a new workflow.

   .. image:: images/create_workflow.jpg
      :height: 152 px
      :width: 950 px
      :scale: 90%
      :alt: Create Oozie Workflow
      :align: left 	

#. Click **My Workflow** to open a dialog window, enter **hue_tutorial_workflow** in the text field,
   and click the **√** symbol.

   .. image:: images/name_workflow.jpg
      :height: 405 px
      :width: 950 px
      :scale: 90%
      :alt: Name the Workflow for the Hue Tutorial
      :align: left

#. From the **ACTIONS** panel, drag the **HDFS Fs** icon to the **Drop your action here** section. (We're
   going to use an HDFS command to do some setting up.)

   .. image:: images/drop_hdfs_fs_action.jpg
      :height: 428 px
      :width: 583 px
      :scale: 100%
      :alt: Create Fs Action 
      :align: left 	

#. In the pop-up dialog that opens, click **Add**.
#. Specify the paths to delete and create by doing the following:
  
   #. Click the dialog title **HDFS Fs** to open a text box. Enter the string **hue_tutorial_refresh** and click the **√** symbol.
   #. Click **DELETE PATH** and enter the path **/user/{your_user_name}/hue_tutorial/** in the adjacent text field.
   #. To recreate the directory for the latest results, click **CREATE DIRECTORY** and enter the directory 
      **/user/{your_user_name}/hue_tutorial/** in the adjacent text field.
   #. Click the **Save** icon.

   .. image:: images/hdfs_fs_action-add_dirs.jpg
      :height: 586 px
      :width: 950 px
      :scale: 90%
      :alt: Specifying Delete and Create Directories.
      :align: left 	

   We're deleting and recreating the path for our results. 

#. Drag the **DistCp** object to the gray dotted box below **hue_tutorial_refresh**. We're going to 
   use `DistCp <http://hadoop.apache.org/docs/r1.2.1/distcp2.html>`_ to copy the Flickr dataset 
   to our home directories in an Oozie task.
#. In the dialog window, do the following: 
 
   #. Click the dialog title **Distcp**, enter the text **copy_flickr_data** in the text box, and then click **√**.
   #. In the **Source** text field, enter **/user/rbernota/Flickr100cc/flickr100m_dataset.bz2**.
   #. In the **Destination** text field, enter **/user/{your_user_name}/hue_tutorial/**.
   #. Click **Add**.

   .. image:: images/copy_flickr_data.jpg
      :height: 144px
      :width: 359 px
      :scale: 100%
      :alt: Create DistCp action.
      :align: left 	

   For the rest of the steps in this section, we'll be omitting screenshots unless the interface changes for a step.
   We feel you have figured out the basic mechanics of creating actions.

#. Drag the **Hive Script** object to the gray dotted box below **copy_flickr_data**.
#. In the dialog window, do the following: 

   #. Click the dialog title **Hive Script** to open a text box. Enter the text **del_db_tables** and click the **√** symbol.
   #. In the **Script** text box, enter the path **/user/{your_user_name}/hue_scripts/del_db_tables.hql**.
   #. In the **Hive XML** text box, enter the path **/user/rbernota/HueTalk/hive-site.xml**.
   #. Click **Add**.

   .. note:: To run Hive queries in Oozie, you need to provide a ``hive-site.xml``. 
             If you're not working on the Tiberium Tan Hue instance, you'll have to copy the file from 
             https://tiberiumtan-hue.tan.ygrid.yahoo.com:9999/filebrowser/view/user/rbernota/HueTalk/hive-site.xml
             to your home directory and enter the path **/user/{your_user_name}/hive-site.xml**.

#. Click **PROPERTIES** to open two text fields.
#. In the two text fields, enter the value **hive.querylog.location** in the left-hand text field (name) and
   the value **hivelogs** in the right-hand text field (value).

   .. image:: images/hivelogs_property.jpg
      :height: 295 px
      :width: 466 px
      :scale: 98%
      :alt: Hive Logs Property
      :align: left 

#. Click the **Save** icon.
#. Create another **Hive** action for your Oozie Workflow:

   #. Use the name **create_db_tables** for the Hive action.
   #. In the **Script** text box, enter the path **/user/{your_user_name}/hue_scripts/create_db_tables.hql**. 
   #. In the **Hive XML** text box, enter the path **/user/rbernota/HueTalk/hive-site.xml**.
   #. Click the **Properties** icon and then **PROPERTIES**. In the two text boxes, 
      enter **hive.querylog.location** for and **hivelogs** for the name and value.
   #. Click the **Save** icon.

#. We still need to create the Hive table with just the camera 
   and location data, so create the last Hive action with the 
   script **/user/{your_user_name}/hue_scripts/camera_location_query.hql**. 
   Use the name **camera_location_query** for the Hive action.

   .. important:: Remember to add **hive.querylog.location** and **hivelogs** as job propertes as well as
                  to enter **/user/rbernota/HueTalk/hive-site.xml** in the **Hive XML** field.


#. To create a **Shell** task that creates a CSV file from the Hive table the last
   task creates, from the **ACTIONS** panel, drag the **Shell** icon to the next empty dotted box under the **camera_location_query** action. 

#. In the pop-up dialog box, do the following:

   #. Click the **Shell** link and enter the title **create_camera_location_csv**.
   #. For the **Shell command** text field, enter **/user/{your_user_name}/hue_scripts/create_camera_location_csv.sh**.
   #. Click **Add**.
 
   .. image:: images/create_shell_action.jpg
      :height: 139 px
      :width: 449 px
      :scale: 98%
      :alt: Creating a Shell Script Action
      :align: left   

#. From the **hue_tutorial_workflow** pane, drag the **Pig** object to the next gray empty dotted box.
#. Create the Pig action by doing the following:

   #. In the pop-up dialog box, click the link **Pig Script** and enter **remove_null_camera_locations** as the 
      name of the Pig action.
   #. Enter the path **/user/{your_user_name}/hue_scripts/remove_null_locations.pig** 
      in the **Script** text field.
   #. Click **Add**.

   .. image:: images/create_pig_action.jpg
      :height: 138 px
      :width: 449 px
      :scale: 98%
      :alt: Creating a Pig Action

#. From the **remove_null_camera_locations** Pig action, click the **Properties** icon to
   open the **Properties** tab.

#. In the **Properties** tab, click **PROPERTIES** and enter 
   **oozie.action.sharelib.for.pig** in the name text field and 
   **pig_current** in the value text field.

   .. image:: images/set_pig_properties.jpg
      :height: 379 px
      :width: 567 px
      :scale: 96%
      :alt: Setting Properties for Pig Action


   .. note:: Notice that we don't specify **hcat_current** because Pig
             is accessing a CSV file, not a Hive table, which would
             require access to HCatalog. The Job XML
             ``hive-site.xml`` file is as you might have guessed: 
             only needed for Hive.

#. Click **Save**.


#. Finally, we want the job to notify us when we're done. So, go ahead and create an **Email** action
   by dragging the **Email** icon to the gray empty dotted box under the **remove_null_camera_locations** action. 
#. In the pop-up dialog box, do the following:

   #. Click the **Email** link and enter the text **hue_tutorial_notification** in the text field, and click **√**. 
   #. In the **To addresses** text field, enter your email address.
   #. In the **Subject** text field, enter **Hue Tutorial's Oozie Workflow Has Completed**.
   #. In the **Body** text area, enter the following::

          See the sanitized CSV file with the Flickr camera locations at the
          following URL (replace {your_user_name} with your own):
          https://tiberiumtan-hue.tan.ygrid.yahoo.com:9999/filebrowser/#/user/{your_user_name}/hue_tutorial/flickr_camera_locations_sanitized

   #. Click **Add**.

#. Click **Save**.
#. From the right-hand side of the **Oozie Editor** navigation bar, click |arrowhead| to submit your Oozie job.
#. While your Oozie Workflow is running, let's move to the next section to learn about the
   **Job Browser**. 


.. tip:: We've already looked at importing actions, creating tasks, and submitting the Oozie Workflow from the **Oozie Editor**, but
         there are a lot more features. You can copy your Oozie Workflow, look at the list of past Oozie Workflows that
         were submitted, and schedule Oozie Workflows (with Oozie Coordinators, which we cover later).


.. _viewing_jobs:

8. Viewing and Managing Jobs
============================

From the **Job Browser**, you can view  your jobs and
other jobs. You can sort jobs by status, search for jobs 
by a user or key term, also look at the cluster and ResourceManager logs.

#. Let's first look for our jobs by clicking **Job Browser** from the top navigation bar.

   .. image:: images/open_job_browser.jpg
      :height: 165 px
      :width: 950 px
      :scale: 90%
      :alt: Open Job Browser
      :align: left   


#. By default, the **Job Browser** shows Oozie jobs sorted by your username, so 
   you should two jobs: the parent (or launcher) **hue_tutorial_workflow** and the 
   child job that is still running. (The parent will stay at 5% until its
   children have been completed.)

   .. image:: images/parent_child_job.jpg
      :height: 141 px
      :width: 950 px
      :scale: 90%
      :alt: Parent/Child Jobs
      :align: left   

#. Sort your jobs by clicking the green **Succeed**. (Depending how far 
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


#. Okay, our Oozie Workflow should be about done. Go back to the **Oozie Editor** to see the progress of your Oozie Workflow.
   Hopefully, you see green **OK** icons for all the jobs in the Oozie Workflow as seen below.

   .. image:: images/successful_workflow.jpg
      :height: 517 px
      :width: 950 px
      :scale: 90%
      :alt: Oozie Dashboard: Successful Workflow
      :align: left 

#. Congratulations if your Oozie Workflow successfully completed. 
   Use the **File Browser** to navigate to 
   ``/user/{your_user_name}/hue_tutorial/flickr_camera_locations_sanitized``
   to see your sanitized Flickr data in CSV.  If one of your jobs failed, see 
   :ref:`Troubleshooting <gs-troubleshooting>`. 


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
- For Hive jobs, the job property ``oozie.sharelib.for.hive`` 
  has ``hcat_current,hive_current`` (no spaces between the values).
  For Pig jobs, the job property ``oozie.sharelib.for.pig`` has the values 
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

.. |arrowhead| image:: arrow_head.jpg
