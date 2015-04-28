.. _getting_started:

Getting Started
===============

.. 04/22/15: Rewrote.

In the quick starts in this chapter, you will 
learn how to create Oozie Workflows, Coordinators, and
Bundles. 

Setting Up
----------

#. Request access to Kryptonite Red (or other cluster) by completing the :ref:`On-boarding <onboard>` steps.
#. SSH to Kryptonite Red.
#. Create the directory ``$HOME/proj/oozie/`` for the quick starts: ``$ mkdir -p $HOME/proj/oozie``
#. Get the Oozie examples:
  
   #. Clone the Git ``oozie`` repository: ``$ git clone git@git.corp.yahoo.com:hadoop/oozie.git``
   #. Move the Oozie examples: ``$ mv oozie/examples proj/oozie``
   #. Delete the directory ``oozie``: ``$ rm -rf oozie``
#. Confirm that the global variables are set to the values below:

   - ``JAVA_HOME=/home/gs/java/jdk``
   - ``HADOOP_HOME=/grid/0/gs/hadoop/current``
   - ``HADOOP_CONF_DIR=/grid/0/gs/conf/current``
   - ``PATH=$HADOOP_HOME/bin/:$PATH``
   - ``OOZIE_URL=http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie``

   To set the global variables, use the ``export`` command: ``export HADOOP_HOME=/grid/0/gs/hadoop/current``

   .. note:: If you plan on completing the quick starts below on a cluster other than Kryptonite Red,
             be sure to modify the URIs used the tutorials. The following is the URI syntax: ``{scheme}://{cluster}{color}-{server}.{color}.ygrid.yahoo.com:{port}/{path}`` 
             For example, the ``OOZIE_URL`` for Cobalt Blue would be ``http://cobaltblue-oozie.blue.ygrid.yahoo.com:4080/oozie``


Workflow Quick Start
--------------------

#. Sign on to Kryptonite Red (or the cluster that you requested access).
#. Request a Kerberos ticket: ``$ kinit $USER@Y.CORP.YAHOO.COM``
#. Move ``examples`` directory to HDFS: ``hdfs dfs -put $HOME/proj/oozie/examples hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/$USER/examples``
#. Make the following edits to ``$HOME/proj/oozie/examples/src/main/apps/map-reduce/job.properties``:

   - ``nameNode=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020``
   - ``jobTracker=kryptonitered-jt1.red.ygrid.yahoo.com:8032``
   - ``queueName=default``

#. Change to ``$HOME/proj/oozie``.
#. Submit your Oozie job: ``$ oozie job -config examples/src/main/apps/map-reduce/job.properties -run -auth kerberos``
   Oozie will return a job ID.
#. With the returned job ID, request information about the job: ``$ oozie job --info {job_id} -auth kerberos`` 
#. To view the generated output: ``hdfs dfs -cat hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/$USER/examples/output-data/map-reduce/part-0000``


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

#. There is one Coordinator example called aggregator under ``examples/apps``. Edit the file ``job.properties``.

   .. code-block:: bash

      # replace the key to oozie.coord.application.path
      # replace the value to the coordinator app you want to run
      oozie.coord.application.path=hdfs://localhost:9000/tmp/examples/apps/aggregator

#. Copy the ``examples`` directory to your home directory in HDFS::

   .. code-block:: bash

      $ cd ..
      $ hadoop fs -put examples /tmp/examples
      $ hadoop fs -ls /tmp/examples

#. Submit an Oozie Coordinator job::

   .. code-block:: bash

      #Set OOZIE_URL environment property
      $ export OOZIE_URL=http://SERVERNAME:PORT/oozie
    
      #Submit workflow
      $ cd
      $ cd examples
      $ oozie job -run -config job.properties
      Backyard Password:  <enter your Backyard password>
      job: 0000000-100129181121546-oozie-ange-C

#. Check the status of your job::

   .. code-block:: bash

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

#. Copy the `oozie_examples.tar <http://twiki.corp.yahoo.com/pub/CCDI/OozieFAQ/oozie_examples.tar>`_ 
   file to your home directory.
#. Untar the file to your home directory. The directory structure should be as follows::

       $ ls -al examples
       total 8
       drwxr-xr-x  5 angeloh users 4096 Apr  7 15:23 .
       drwxr-xr-x  4 angeloh users 4096 Apr  7 15:23 ..
       drwxr-xr-x 16 angeloh users 4096 Apr  7 15:01 apps
       drwxr-xr-x  4 angeloh users 4096 Apr  7 15:23 input-data
       drwxr-xr-x  3 angeloh users 4096 Feb  2 13:59 src

#. The bundle example is under ``apps/bundle/*``.

#. Copy the ``examples`` directory to your home directory in HDFS: ``$ hadoop fs -put examples .``
#. Submit an Oozie Bundle Job:

   #. $ Export the variable ``OOZIE_URL``: ``$ export OOZIE_URL=http://SERVERNAME:PORT/oozie``
   #. Submit the Oozie Bundle: ``$ oozie job -run -config job.properties -auth kerberos``
#. Check the status of your job: ``$ oozie job -info 0000000-110407152927173-oozie-ange-B``
#. You should see output similar to that below::

       Job ID : 0000000-110407152927173-oozie-ange-B
       ------------------------------------------------------------------------------------------------------------------------------------
       Job Name : bundle-test
       App Path : hdfs://localhost:9000/user/angeloh/examples/apps/bundle/bundle.xml
       Status   : RUNNING
       Kickoff time   : null
       ------------------------------------------------------------------------------------------------------------------------------------
       Job ID                                   Status    Freq Unit         Started                 Next Materialized       
       ------------------------------------------------------------------------------------------------------------------------------------
       0000001-110407152927173-oozie-ange-C     RUNNING   60   MINUTE       2010-01-01 01:00        2010-01-01 03:00        
       ------------------------------------------------------------------------------------------------------------------------------
       
       #The "Status" will change from RUNNING to SUCCEEDED when the job has completed successfully.


Next Steps
----------

See the `Oozie Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Cookbooks>`_ for
Java, MapReduce, and Pig examples.
