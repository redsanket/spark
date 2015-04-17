Bundles
=======

.. 04/16/15: Rewrote.
.. TBD: Provide annotations for diagrams.

Overview
--------

Bundle is a higher-level Oozie abstraction that batches a set of Coordinator 
applications. The user can start, stop, suspend, resume, and rerun in the 
Bundle-level, making it easier to control the operation. 

More specifically, the Oozie Bundle system allows the user to define and execute 
a bunch of Coordinator applications often called a data pipeline. No explicit 
dependency exists among the Coordinator applications in a Bundle. A user, however, could 
use the data dependency of Coordinator applications to create an implicit data 
application pipeline.

TBD: Need to annotate diagram below.

.. image:: images/coord_pipeline.jpg
   :height: 334px
   :width: 720 px
   :scale: 95 %
   :alt: Oozie Coordinator Pipeline
   :align: left

State Transitions
-----------------

TBD: Need to annotate the diagram below.

.. image:: images/bundle_state_transitions.jpg
   :height: 432px
   :width: 687 px
   :scale: 95 %
   :alt: Oozie Bundle State Transitions
   :align: left


Running Bundles
---------------

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




