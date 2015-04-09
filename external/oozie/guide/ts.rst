Troubleshooting
===============


Debugging
---------

1. Checking Oozie Job
~~~~~~~~~~~~~~~~~~~~~

::
    
    $ oozie job -oozie http://localhost:11000/oozie -info 14-20090525161321-oozie-joe ----------------------------------------------------------------------------------------------------------------
    Workflow Name : map-reduce-wf
    App Path : hdfs://localhost:8020/user/joe/workflows/map-reduce
    Status : SUCCEEDED
    Run : 0
    User : joe
    Group : users
    Created : 2009-05-26 05:01
    Started : 2009-05-26 05:01
    Ended : 2009-05-26 05:01
    
    Actions 
    --------------------------------------------------------------------------------------
    Action Name Type Status Transition External Id External Status Error Code Start End 
    ----------------------------------------------------------------------------------------------------
    hadoop1  map-reduce  OK  end  job_200904281535_0254  SUCCEEDED  -  2009-05-26 05:01 2009-05-26 05:01 
    ----------------------------------------------------------------------------------------------------
   
2. Checking/Debugging Oozie Jobs 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the Web Console to check your Oozie. For example, for Kryptonite Red,
go to http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie.

The syntax for the Web Console is ``http://{cluster}{color}-oozie.{color}.ygrid.yahoo.com:4080/oozie``.

Coordinator Actions
+++++++++++++++++++

In the current releases, logs are shown for coordinator 'actions' only (and not for the parent job).

Click on the tab **Coord Job Log** once you've clicked on the job from the list
Action logs can be viewed by entering the action numbers in the text box below the 
log area either as a comma-separated list or a range e.g. 1-5,6,9,10. 

Screenshot

Due to large size of logs (for coordinator with large number of actions), loading 
the logs might take upto a few minutes.

In the next release, after opening the tab, the default log displayed will be of 
the parent coordinator job. Accessing actions' logs will be same as current behavior. 
Performance improvements are underway to improve the streaming speed.

TBD: Add screenshot.

For more information about the Web Console, see https://cwiki.apache.org/confluence/display/OOZIE/Map+Reduce+Cookbook.

3. View Oozie Logs
~~~~~~~~~~~~~~~~~~

Logs are located on the Tomcat host at ``/home/y/libexec/yjava_tomcat/logs/{host_name}``.
Logs are available for privileged users only.

You can also use the ``oozie`` client to view the log::

    $ oozie job -log <jobid> -oozie <OOZIE_URL>

  
Errors
------

* :ref:`Where are the log files created? <log_files>`  

Solutions
---------

.. _log_files:
.. topic::  **Where are the log files created?**

   The Hive server log is located at ``/home/y/libexec/hive_server/logs/hive_server.log``. 
   The Hive CLI log is in ``$HADOOP_TOOLS_HOME/var/logs/hive_cli/${userid}/hive.log``.
    
