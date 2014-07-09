===============
Getting Started
===============

.. _hive_getting_started-overview:

Overview
========

This chapter presents an overview of HDFS commands and a Hive command-line tutorial.

.. _hive_overvew-shell_cmds:

Basic HDFS Shell Commands
-------------------------

Before you start using the Hive CLI, you should be familiar
with some basic ``hdfs`` commands that you'll need to use often
when working on the grid. For a complete reference, see 
the `File System Shell Guide <http://archive.cloudera.com/cdh/3/hadoop/file_system_shell.html>`_.

Listing HDFS Files
##################

To get a stat for files::

    -bash-4.1$ hadoop fs -ls <args>

Copying Files
#############

The following allows you to copy files from a source to a destination.

::

    -bash-4.1$ hadoop fs -cp URI [URI …] <dest>

You can also copy files from local to HDFS or from HDFS to local::

    -bash-4.1$ hadoop fs -copyFromLocal <localsrc> URI 

    -bash-4.1$ hadoop fs -copyToLocal [-ignorecrc] [-crc] URI <localdst>

Making Directories
##################

You can create HDFS directories by specifying paths or URIs:

    -bash-4.1$ hadoop fs -mkdir <paths|URIs> 

Moving Files and Directories
############################

You can use the following to move files from source to destination::

    -bash-4.1$ hadoop fs -mv URI [URI …] <dest>


The above command allows multiple sources as well, in which case ``<dest>`` needs to be directory. 
Note, moving files across file systems is not permitted. 

As with copying files, you can move local files to HDFS and vice versa with the following::

    -bash-4.1$ hadoop fs -moveToLocal [-crc] <src> <dst>
    -bash-4.1$ hadoop fs -moveFromLocal <localsrc> <dst>


Deleting Files/Directories
##########################

You can use the following to remove non-empty directories and files::

    -bash-4.1$ hadoop fs -rm [-skipTrash] URI [URI …]

To recursively delete files, use the following::

    -bash-4.1$ hadoop fs -rmr [-skipTrash] URI [URI …]

Viewing File Content
####################

Using the following command, you view the content by copying the source to ``stdout``::

    -bash-4.1$ hadoop fs -cat URI [URI …]


.. _hive_overvew-security:

Security Model
--------------

The Yahoo grid contains sensitive data from many different properties. 
To preserve the integrity and confidentiality of that data, users are restricted
by ACLs that define access permissions. For example, you cannot
create tables over the data that you do not have write permissions. For 
data that you do not have read permissions, you will not be able to view the
data or its metadata.


.. _hive_getting_started-using_hive:

Using the Hive Shell
====================

**Audience:** Developers

The focus of following section is not to teach you how to use the Hive CLI, but how
to set up and use the Hive CLI at Yahoo.

.. _gs_hive_sh-user_acct:

Create a User Account
---------------------

#. Subscribe to the following iLists: 

   - ygrid-sandbox-announce@yahoo-inc.com
   - ygrid-research-announce@yahoo-inc.com
   - ygrid-production-announce@yahoo-inc.com
#. Go to the `Grid Services Request Forms <http://adm005.ygrid.corp.bf1.yahoo.com:9999/grid_forms/main.php>`_.
#. Select the **User Account** tab.
#. Fill out the form with the following information:

   - **Request Type** - Choose **New User Account**.
   - **Section 1: Clusters** - Check **kryptonite.red (HadoopDev)** from **Hadoop 0.23 Sandbox Clusters**
     and **axonite.red** from the **Hadoop 2.0 Sandbox Clusters**.
#. From **Section 4: Prerequisites for Approval**, check the checkboxes stating that you
   have read and agree to the terms.
#. Enter the purpose of requesting access in the text field for **Section 5: Justification**.
#. Click **Submit**.

For grid clusters other than Kryptonite Red, check `Grid Versions <http://wiki.corp.yahoo.com/view/Grid/GridVersions>`_
to see what clusters have ``hive-client`` running on them and then make another
request with your existing user account for access to different clusters.

.. _gs_hive_sh-confirm_access:

Confirm Access to Hive on Kryptonite Red
----------------------------------------

#. Log onto the Kryptonite cluster with SSH: ``$ ssh kryptonite-gw.red.ygrid.yahoo.com``
#. Obtain a Kerberos ticket-granting ticket: ``$ kinit <username>@Y.CORP.YAHOO.COM``
#. Start the Hive CLI: ``$ hive``
#. View the existing databases: ``$ show databases;``
#. You should see a fairly long list of database names. 

.. _gs_hive_sh-proj_space:

Create a Project Space
----------------------

To create a database to house your Hive tables, you'll need to create a project space (``/projects``) 
on the grid (HDFS) or a dedicated, mountable Unix space (NFS) that can be set up. The project
acts as a namespace for tables. As Hive uses HDFS for storing its table's data, the quota 
that applies to the project space is automatically used. 

.. note:: If you would like to experiment with Hive without creating a project space, you can 
          use your home directory on HDFS (``/user/<userid>``).

.. _proj_space-req:

Requirements
############

- Your user account or headless account must already exist on the grid, which you
  obtained by completing  :ref:`Create a User Account <gs_hive_sh-user_acct>`.

.. _proj_space-req:

Request a Project Space
#######################

To request a project space, follow these steps:

- Determine a directory name for the project space: ``/projects/<project_space>``
- Determine the amount of space in GBs (see :ref:`Space Quotas for Projects <gs_appendix-space_quotes>`)
- Determine the number of files to be stored.
- Fill out the `Grid Services Request Form: Project Space <http://adm005.ygrid.corp.bf1.yahoo.com:9999/grid_forms/main.php>`_
  providing the information above.

.. _gs_hive_sh-db:

Create a Database
-----------------

To create your table, you specify a location that points to your project space.

::

    hive> CREATE DATABASE <database_name> LOCATION '/projects/<project_space>/';

Example::

    hive> CREATE DATABASE hive_team_db LOCATION '/projects/hive_warehouse';

Your team may have a team space for your project as well::

    hive> CREATE DATABASE <database_name> LOCATION '/projects/<team_space>/<project_space>/';

Thus, the following might also be an example::

    hive> CREATE DATABASE hive_team_db LOCATION '/projects/hive_team/hive_warehouse';

To use your database::

    hive> USE <database_name>;

.. _gs_hive_sh-config:

Create a Hive Configuration File
--------------------------------

We recommend that you create the Hive configuration file ``.hiverc`` that contains
your database and other default configurations. For example: ``mapred.job.queue.name``.
The ``.hiverc`` file is generally stored in ``$HOME/.hiverc``.

The following example ``.hiverc`` shows you some of the configuration you can set:

:: 

   add jar /homes/<your_user_name>/hive-some-jar-file.jar;
   set hive.exec.mode.local.auto=true;
   set hive.cli.print.header=true;
   set hive.cli.print.current.db=true;
   set hive.auto.convert.join=true;
   set hive.mapjoin.smalltable.filesize=30000000;

.. _gs_hive_sh-next:


Next Steps
----------

- Read the `Hive LanguageManual <https://cwiki.apache.org/confluence/display/Hive/LanguageManual>`_.
- If you haven't received a project space, see :ref:`Create a Project Space <gs_hive_sh-proj_space>`.
- Ask for access to other grid clusters if needed.

