===============
Getting Started
===============


Introduction
============

In this getting started, you will be using one data set
to explore the different features of Hue. We'll 
use the File Browser to upload, view, and modify data.
Next, we'll use the Pig Editor in Hue to write
Pig scripts that extract and clean up data so that
it's suitable to Hive.


Prerequisites
=============

You'll need to ask for access to one of the clusters through
the `Grid Support Shop <http://yo/supportshop>`_. Once you
have been given access, you can access the Hue UI through one
of the following URLs:

- 
-

Viewing Data
============


Querying Data
=============

Using Hive
----------

Using Pig
---------


Viewing Jobs
============

Submitting Jobs
===============




*Home page* - shows your project and your history, queries, could share possibly.


Hive
Pig
Job Designer - Oozie Flow

Description of Query Page
-------------------------

Once you select a database, you'll be able to see the tables. You can preview by clicking the 'eye' icon. You 
will then see the metastore table (Shows schema and partition column). The table icon will show the sample of the data.

(You can't get all the data because of the size of data) 


Writing Queries
===============

Shows logs automatically and then goes to results when the job is done. A notification will
also be displayed will discuss the job that completed. Click on the Job ID to get the Hadoop cluster
ARUM (Job Tracker). 

Bug: it will say RUNNING even though it's  


Chart: 



What Needs Screenshots
----------------------

Hive
####

- Hive Editor: query log, results, fullscreen result, save results to HDFS, download to Excel (csv,xls). 
- Explain Query
- Recent query


Setting
-------

Key-Value
File Resources - JAR

UDFS - name/class

----


My Queries - saved queries, results, edits, copy, usage, trash



Pig
===

Uses Oozie to execute Pig.

Properties
----------

Sets up Oozie job.

**Need** - need to add (pig_current, hcat_current - if you're going through HCat)

Hadoop properties:

Name: oozie.actions.sharelib.for.pig
Value:  (pig_current, hcat_current - if you're going through HCat)

For Using HCat:

Under every cluster, you add /sharelib/v1/hive/hive-0.13.0.3.1411171801/libexec/hive/conf/hive-site.xml
as the resource.
Use

Execute from Property page by clicking on arrow icon. Notification is shown in Job Browser.
You'll see your job in the Job Browser.


Question icon: shows an "Assist" window that will assist you in writing Pig scripts, operators,
relational operators, 


Pig Dashboard
-------------



Oozie Dashboard
---------------


Job Designer
============

Actions are Oozie actions


Metastore Manager
-----------------

Shows HCat info, Hive, shows actual location of project file



What Doesn't Work
-----------------


Workflow
========

Shows the actual running jobs

=> Dashboard is the Oozie Dashboard


Can edit new files, but not existing files.


Job Browser
-----------

Can kill jobs with "Kill" button.

