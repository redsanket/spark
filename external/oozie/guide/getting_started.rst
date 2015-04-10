.. _getting_started:

Getting Started
===============

In the quick starts in this chapter, you will 
learn how to create Oozie Workflows, Coordinators, and
Bundles. 

#. Create a simple grid applications with Oozie configuration
files (``workflow.xml``, ``coordinator.xml``, and ``bundle.xml``). 
We'll also be using the ``job.properties`` file to pass
values to parameterized Workflow

Prerequisites
-------------

- Complete the :ref:`On-boarding <onboard>`.

Creating a Workflow
-------------------

In this quick start, we'll be doing the following:

#. Create a simple grid application.
#. Create a ``workflow.xml`` file.
#. Store a custom JAR in a ``lib`` directory.
#. Create a ``job.properties`` file to pass values to the
   parameterized Worklow.
#. Submit the Oozie job from a gateway.
#. Check the job status with the Oozie client (CLI/Java) and the Web Console.

Creating a Coordinator
----------------------

#. Copy the `oozie_examples.tar <http://twiki.corp.yahoo.com/pub/CCDI/OozieCoordinator/oozie-examples-4.1.0-SNAPSHOT-examples.tar.gz>`_ 
   file to your home directory.
#. Untar the file to your home directory. The directory structure should be as follows::

       $ cd
       $ tar xvf oozie_examples.tar
       $ ls -al examples
       total 8
       drwxr-xr-x  5 angeloh  users  170 Jan 19 15:51 .
       drwxr-xr-x  3 angeloh  users  102 Jan 19 15:51 ..
       -r-xr-xr-x  1 angeloh  users  402 Jan 13 20:29 apps
       drwxr-xr-x  5 angeloh  users  170 Jan 19 15:51 input-data
       drwxr-xr-x  3 angeloh  users  102 Jan 13 20:29 src

#. There is one coordinator example called aggregator under ``examples/apps``. Edit the file ``job.properties``.

   .. code-block:: bash

      # replace the key to oozie.coord.application.path
      # replace the value to the coordinator app you want to run
      oozie.coord.application.path=hdfs://localhost:9000/tmp/examples/apps/aggregator

#. Copy the ``examples`` directory to your home directory in HDFS::

       $ cd ..
       $ hadoop fs -put examples /tmp/examples
       $ hadoop fs -ls /tmp/examples

#. Submit an Oozie Coordinator Job::

       #Set OOZIE_URL environment property
       $ export OOZIE_URL=http://SERVERNAME:PORT/oozie
    
       #Submit workflow
       $ cd
       $ cd examples
       $ oozie job -run -config job.properties
       Backyard Password:  <enter your Backyard password>
       job: 0000000-100129181121546-oozie-ange-C

#. Check the status of your job::

       $ oozie job -info 0000000-100129181121546-oozie-ange-C
       
       --------------------------------------------------------------------------------------------------------
       Job Name      :  MY_APP                                                                  
       App Path      :  hdfs://localhost:9000/tmp/examples/apps/aggregator            
       Status        :  SUCCEEDED                                                               
       --------------------------------------------------------------------------------------------------------
       Action Number           external statusStatus     Tracker URI  Ext. Id               Ext. Status     Error Code    created                 Last Check             
       actions list check null 
       actions list size is 1
       1                       null        SUCCEEDED  -            0000000-100129181121546-oozie-ange-C-               -             2010-01-30 02:17 +0000  -                  
       --------------------------------------------------------------------------------------------------------
       
       
       #The "Status" will change from RUNNING to SUCCEEDED when the job has completed successfully.


Creating a Bundle
-----------------

Next Steps
----------

See the `Oozie Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Cookbooks>`_ for
Java, MapReduce, and Pig examples.
