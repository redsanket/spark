.. _getting_started:

Getting Started
===============

.. 04/22/15: Rewrote.
.. 05/15/15: Edited.

In the quick starts in this chapter, you will 
learn how to create Oozie Workflows, Coordinators, and
Bundles. 

Setting Up
----------

.. 04/30/15: Tested.

#. Request access to Kryptonite Red (or other cluster) by completing the :ref:`On-Boarding <onboard>` steps.
#. SSH to the Kryptonite Red gateway (i.e., ``kryptonite-gw.red.ygrid.yahoo.com``).
#. Obtain and cache a Kerberos ticket for the purpose of authentication: ``$ kinit $USER@Y.CORP.YAHOO.COM``
#. Get the Oozie examples: ``$ hdfs dfs -copyToLocal hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/jcatera/oozie_examples $HOME/oozie_examples``
#. Set the global variables to the values below::

       export HADOOP_HOME=/home/gs/hadoop/current; PATH=/home/y/var/yoozieclient/bin:$HADOOP_HOME/bin/:$PATH;
       OOZIE_URL=http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie;OOZIE_AUTH=kerberos

   We recommend adding the above export statements to the configuration file ``.bashrc``. 


Working on Different Clusters
*****************************

To complete the quick starts below on a cluster other than Kryptonite Red,
be sure to modify some configuration for the example to successfully run.

In ``oozie_examples/apps``, you'll find different example applications, each with
its own ``job.properties`` file. You'll need to modify the the following configurations,
replacing ``{cluster}`` and ``{color}`` with the appropriate values::

    nameNode=hdfs://{cluster}{color}-nn1.{color}.ygrid.yahoo.com:8020
    jobTracker={cluster}{color}-jt1.{color}.ygrid.yahoo.com:8032

For example, the ``nameNode`` for Cobalt Blue would be 
``hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com:8020``. 


Workflow Quick Start
--------------------

.. 04/30/15: Tested.

In the ``$HOME/oozie_examples/apps/``, you'll find the Workflow example ``map-reduce``.
We're going to configure and run this Workflow in the following steps.

#. SSH to Kryptonite Red (or the cluster that you requested access).
#. Request a Kerberos ticket: ``$ kinit $USER@Y.CORP.YAHOO.COM``
#. Move ``oozie_examples`` directory to HDFS: ``$ hdfs dfs -copyFromLocal $HOME/oozie_examples hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/$USER/oozie_examples``
#. Change to ``$HOME/oozie_examples``.
#. Submit your Oozie job: ``$ oozie job -config examples/apps/map-reduce/job.properties -run``
   
   Oozie will return a job ID.
#. With the returned job ID, request information about the job: ``$ oozie job -info {job_id}`` 

#. To view the generated output: ``$ hdfs dfs -cat hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/$USER/oozie_examples/output-data/map-reduce/part-00000``


Coordinator Quick Start
-----------------------

.. 04/30/15: Tested.

In the ``$HOME/proj/oozie/examples/src/main/apps/``, you'll find the Coordinator example ``aggregator``.
We're going to configure and run this Coordinator in the following steps.

#. SSH to Kryptonite Red (or the cluster that you requested access).
#. Request a Kerberos ticket: ``$ kinit $USER@Y.CORP.YAHOO.COM``
#. Change to the following directory: ``$HOME/oozie_examples/apps/aggregator``
#. Submit the Oozie Coordinator job: ``$ oozie job -run -config job.properties``

   An Oozie job ID will be returned to you.
    
#. With the Oozie job ID, check the status of your job: ``$ oozie job -info <oozie_job_id>``

#. The returned output should look similar to that below::
       
       ------------------------------------------------------------------------------------------------------------------------------------
       Job Name    : aggregator-coord
       App Path    : hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/jcatera/oozie_examples/apps/aggregator/coordinator.xml
       Status      : SUCCEEDED
       Start Time  : 2010-01-01 01:00 GMT
       End Time    : 2010-01-01 03:00 GMT
       Pause Time  : -
       Concurrency : 1
       ------------------------------------------------------------------------------------------------------------------------------------
       ID                                         Status    Ext ID                               Err Code  Created              Nominal Time         
       0079707-150302104108145-oozie_KR-C@1       SUCCEEDED 0079708-150302104108145-oozie_KR-W   -         2015-04-29 23:06 GMT 2010-01-01 01:00 GMT 
       ------------------------------------------------------------------------------------------------------------------------------------
       0079707-150302104108145-oozie_KR-C@2       SUCCEEDED 0079709-150302104108145-oozie_KR-W   -         2015-04-29 23:06 GMT 2010-01-01 02:00 GMT 
       ------------------------------------------------------------------------------------------------------------------------------------
       
   .. note:: The *status* will change from ``RUNNING`` to ``SUCCEEDED`` when the job has completed successfully.

#. After the job is ``SUCCEEDED``, once again, you can view the written output: ``$ hdfs dfs -cat hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/$USER/oozie_examples/output-data/aggregator/aggregatedLogs/2010/01/01/01/part-00000``

Creating a Bundle
-----------------

.. 04/30/15: Tested.

In the ``$HOME/oozie_examples/apps/``, you'll find the Bundle example ``bundle``.
We're going to configure and run this Bundle in the following steps.

#. SSH to Kryptonite Red (or the cluster that you requested access).
#. Request a Kerberos ticket: ``$ kinit $USER@Y.CORP.YAHOO.COM``
#. Change to the following directory: ``$HOME/oozie_examples/apps/bundle``
#. Submit an Oozie Bundle job: ``$ oozie job -run -config job.properties``
#. Check the status of your job with your job ID: ``$ oozie job -info <oozie_job_id>``
#. You should see output similar to that below::

       Job ID : 0079753-150302104108145-oozie_KR-B
       ------------------------------------------------------------------------------------------------------------------------------------
       Job Name : bundle-app
       App Path : hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/jcatera/examples/apps/bundle
       Status   : RUNNING
       Kickoff time   : null
       ------------------------------------------------------------------------------------------------------------------------------------
       Job ID                                   Status         Freq Unit         Started                 Next Materialized       
       ------------------------------------------------------------------------------------------------------------------------------------
       0079754-150302104108145-oozie_KR-C       RUNNING        60   MINUTE       2010-01-01 01:00 GMT    2010-01-01 03:00 GMT    
       ------------------------------------------------------------------------------------------------------------------------------------

       
   .. note:: The *status* will change from ``RUNNING`` to ``SUCCEEDED`` when the job has completed successfully.

#. This particular bundle just runs the Coordinator you looked at in the last section, so you can view the output written
   to the same directory: ``$ hdfs dfs -cat hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/$USER/oozie_examples/output-data/aggregator/aggregatedLogs/2010/01/01/01/part-00000``

   .. note:: Generally, you would use a Bundle to run more than one Coordinator, and those Coordinators will have some type 
             of dependency (time/data). 
             
Next Steps
----------

- Try running the other examples in ``oozie_examples/apps``, look at the configuration files ``job.properties``,
  ``workflow.xml``, and ``coordinator.xml`` and then the Java code in ``oozie_examples/src/org/apache/oozie/example``.
- Go through the examples in the Apache's `Oozie Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Cookbooks>`_. 
- See the :ref:`Cookbook Examples <cookbook>` chapter in this guide.

 
