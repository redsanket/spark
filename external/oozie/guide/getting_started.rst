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
#. Create the directory ``$HOME/proj/oozie/`` for the quick starts: ``$ mkdir -p $HOME/proj/oozie``
#. Get the Oozie examples:
  
   #. Clone the Git ``oozie`` repository: ``$ git clone git@git.corp.yahoo.com:hadoop/oozie.git``
   #. Move the Oozie examples: ``$ mv oozie/examples proj/oozie``
   #. Delete the directory ``oozie``: ``$ rm -rf oozie``
#. Confirm that the global variables are set to the values below:

   - ``JAVA_HOME=/home/gs/java/jdk``
   - ``HADOOP_HOME=/home/gs/hadoop/current``
   - ``HADOOP_CONF_DIR=/home/gs/conf/current``
   - ``PATH=/home/y/var/yoozieclient/bin:$HADOOP_HOME/bin/:$PATH``
   - ``OOZIE_URL=http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie``

   To set the global variables, use the ``export`` command: ``export HADOOP_HOME=/grid/0/gs/hadoop/current``

   .. note:: If you plan on completing the quick starts below on a cluster other than Kryptonite Red,
             be sure to modify the URIs used in the tutorials. The following is the URI syntax: 
             ``{scheme}://{cluster}{color}-{server}.{color}.ygrid.yahoo.com:{port}/{path}`` 
             For example, the ``OOZIE_URL`` for Cobalt Blue would be ``http://cobaltblue-oozie.blue.ygrid.yahoo.com:4080/oozie``
             See :ref:`Oozie Servers on Clusters <references-oozie_servers>` as a reference.

#. Move ``examples`` directory to HDFS: ``hdfs dfs -put $HOME/proj/oozie/examples hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/$USER/examples``



Workflow Quick Start
--------------------

.. 04/30/15: Tested.

In the ``$HOME/proj/oozie/examples/src/main/apps/``, you'll find the Workflow example ``map-reduce``.
We're going to configure and run this Workflow in the following steps.

#. SSH to Kryptonite Red (or the cluster that you requested access).
#. Request a Kerberos ticket: ``$ kinit $USER@Y.CORP.YAHOO.COM``
#. Make the following edits to ``$HOME/proj/oozie/examples/src/main/apps/map-reduce/job.properties``::

       nameNode=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020
       jobTracker=kryptonitered-jt1.red.ygrid.yahoo.com:8032
       queueName=default

#. Change to ``$HOME/proj/oozie``.
#. Submit your Oozie job: ``$ oozie job -config examples/src/main/apps/map-reduce/job.properties -run -auth kerberos``
   Oozie will return a job ID.
#. With the returned job ID, request information about the job: ``$ oozie job -info {job_id} -auth kerberos`` 

#. To view the generated output: ``hdfs dfs -cat hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/$USER/examples/output-data/map-reduce/part-0000``


Coordinator Quick Start
-----------------------

.. 04/30/15: Tested.

In the ``$HOME/proj/oozie/examples/src/main/apps/``, you'll find the Coordinator example ``aggregator``.
We're going to configure and run this Coordinator in the following steps.

#. SSH to Kryptonite Red (or the cluster that you requested access).
#. Request a Kerberos ticket: ``$ kinit $USER@Y.CORP.YAHOO.COM``
#. Change to ``$HOME/proj/oozie/examples/src/main/apps/aggregator``
#. As with the Workflow example, edit the file ``job.properties`` so
   that the configurations have the values shown below::

       nameNode=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020
       jobTracker=kryptonitered-jt1.red.ygrid.yahoo.com:8032
       queueName=default

#. Submit the Oozie Coordinator job: ``$ oozie job -run -config job.properties -auth kerberos``
   An Oozie job ID will be returned to you.
    
#. With the Oozie job ID, check the status of your job: ``$ oozie job -info <oozie_job_id> -auth kerberos``

#. The returned output should look similar to that below::
       
       ------------------------------------------------------------------------------------------------------------------------------------
       Job Name    : aggregator-coord
       App Path    : hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/jcatera/examples/apps/aggregator/coordinator.xml
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


Creating a Bundle
-----------------

.. 04/30/15: Tested.

In the ``$HOME/proj/oozie/examples/src/main/apps/``, you'll find the Bundle example ``bundle``.
We're going to configure and run this Bundle in the following steps.

#. SSH to Kryptonite Red (or the cluster that you requested access).
#. Request a Kerberos ticket: ``$ kinit $USER@Y.CORP.YAHOO.COM``
#. Change to ``$HOME/proj/oozie/examples/src/main/apps/bundle``
#. Again, edit the file ``job.properties`` so that the configurations are
   given the values below::

       nameNode=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020
       jobTracker=kryptonitered-jt1.red.ygrid.yahoo.com:8032
       queueName=default
    
#. Submit an Oozie Bundle job: ``$ oozie job -run -config job.properties -auth kerberos``
#. Check the status of your job with your job ID: ``$ oozie job -info <oozie_job_id> -auth kerberos``
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


Next Steps
----------

See Apache's `Oozie Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Cookbooks>`_ for
Java, MapReduce, and Pig examples. Also, see the :ref:`Cookbook Examples <cookbook>` chapter.
