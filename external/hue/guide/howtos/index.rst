=======
How Tos
=======

Introduction
============

This chapter is for users who have completed the  `Getting Started <getting_started>`_
and need to complete a specific task. See the `Overview <../overview>`_ for
general information about Hue and `Hue UI <../ui>`_ to learn about the Hue UI components.

Creating a Coordinator
======================

Coordinators allow you to schedule Oozie Workflows to run. You can think of
Coordinators as Cron jobs on the grid. Coordinators also allow you
set inputs and outputs, so you can dynamically run queries with new
parameters. 

You'll be creating a simple Hive query that will be used in a 
Oozie Workflow, and then scheduled with a Coordinator.

1. Create a Hive Query With Parameterized Inputs
------------------------------------------------

Go to the **Hive Query Editor** and enter the following in the 
text field::

   set hive.exec.compress.output=false;

   INSERT OVERWRITE DIRECTORY '${OUTPUT}'

   select count(1) as views, mobile_app_name as app from benzene.daily_data 
   where dt =SUBSTR('${DATE}', 76) and network="on" and pty_family="news" 
   and pty_device="mobile" and pty_experience="app" and event_family="view" group 
   by mobile_app_name order by views desc;



, test the query runs successfully in the Hive editor, and save it to HDFS as a hive script




Creating a Bundle
=================

Overview
--------

Prerequisites
-------------


