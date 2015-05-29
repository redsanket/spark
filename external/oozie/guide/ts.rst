.. _ts:

Troubleshooting
===============

.. 05/14/15: Edited.


.. _ts-debugging:

Debugging
---------

.. _debug-check_oozie_job:

1. Checking Oozie Job
~~~~~~~~~~~~~~~~~~~~~

::
    
    $ oozie job -oozie http://localhost:11000/oozie -info 14-20090525161321-oozie-joe
    ---------------------------------------------------------------------------------    
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

.. _debug-debug_oozie_job:
   
2. Checking/Debugging Oozie Jobs 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the Web Console to check your Oozie. For example, for Kryptonite Red,
go to http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie.

The syntax for the Web Console is ``http://{cluster}{color}-oozie.{color}.ygrid.yahoo.com:4080/oozie``.

.. _-debug_oozie_job-coord_actions:

Coordinator Actions
*******************

To view logs for a Coordinator action(s):

#. From the Oozie Web Console, click the **Coordinator Jobs** tab.
#. Click the job ID for your Coordinator to open the **Coord Job Info** tab of the **Job** window.
#. From the **Job** window, click the **Coord Job Log** tab.
#. In the **Enter action list** text field, enter the action number(s) or a range of actions that 
   you'd like to view logs of. For example, you would enter **1,2,4-6** to see the
   logs for the Coordinator actions 1, 2, 4, 5, and 6.
#. Click **Get Logs** to see the raw logs of the job running your Coordinator action(s).
   (Due to large size of logs (for Coordinators with large number of actions), loading
    the logs may take a few minutes.)

.. note::  In the current releases, logs are shown for Coordinator *actions* only (and not 
           for the parent job). Future releases, the default log will show the parent
           Coordinator job.

For more information about the Web Console, 
`Map Reduce Cookbook: HOW TO USE THE OOZIE WEB-CONSOLE <https://cwiki.apache.org/confluence/display/OOZIE/Map+Reduce+Cookbook#MapReduceCookbook-CASE-8:HOWTOUSETHEOOZIEWEB-CONSOLE>`_.

.. _debug-view_oozie_logs:

3. View Oozie Logs
~~~~~~~~~~~~~~~~~~

Logs are located on the Tomcat host at ``/home/y/libexec/yjava_tomcat/logs/oozie/oozie.log``.
Logs are available for **privileged** users only.

Besides the Oozie Web Console, you can also use the ``oozie`` client to view the log::

    $ oozie job -log <jobid> -oozie <OOZIE_URL> -auth kerberos

.. _ts-errors: 

Errors
------

.. _errors-submit_oozie_wf: 

Can't Submit Oozie Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _submit_oozie_wf-internal_server_error: 

Internal Server Error (1)
*************************

:: 

    $ oozie job -run -config map-reduce-job.properties -auth kerberos

    Error: OTHER : Internal Server Error

.. _internal_server_error-solution: 

Possible Solutions
++++++++++++++++++

- Do you have access to a Hadoop queue?
  Check out the queues for the cluster you're using: ``http://ucdev4.yst.corp.yahoo.com/jtqueues/?cluster={cluster_name}``
  For example, if you're using Axonite Blue, you would go to ``http://ucdev4.yst.corp.yahoo.com/jtqueues/?cluster=axonite-blue``.

- Can you access the Hadoop queue?

  ::

     $ mapred queue -showacls


You should see a list of queues. If you don't see ``SUBMIT_APPLICATION`` 
next to the queue that you're using, you don't have access.
To request access to the queue, `submit a request to Grid-Ops <http://yo/supportshop>`_. 
(You need access to a Hadoop queue to submit Workflows to Oozie.)


.. _submit_oozie_wf-internal_server_error2: 

Internal Server Error (2)
*************************

Your ``OOZIE_URL`` environment variable and ``oozie.wf.application.path`` specified in 
your properties file must point to the **same** grid.

.. _internal_server_error2-correct: 

Correct Example
+++++++++++++++

::

    $ oozie job -oozie http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie -config kr-wf.properties -run -auth kerberos

**kr-wf.properties**

::

    oozie.wf.application.path=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/{userid}/workflow/mb

.. _internal_server_error2-incorrect: 

Incorrect Example
+++++++++++++++++

::

    $ oozie job -oozie http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie -config db-wf.properties -run
    -auth kerberos

**db-wf.properties**

::

    oozie.wf.application.path=hdfs://dilithiumblue-nn1.blue.ygrid.yahoo.com:8020/user/userid/workflow/mb


.. _errors-xml_schema: 

E0701: XML Schema Error
~~~~~~~~~~~~~~~~~~~~~~~

E0701: XML schema error, cvc-complex-type.2.4.a: Invalid content was found starting 
with element ``some-element-a``. One of ``{"uri:oozie:workflow:0.5":other-element-b}`` is expected.

.. _xml_schema-solution: 

Possible Solution
*****************

If you encounter above error, you should check that your XML elements are in the correct 
sequence as specified in the `Workflow XSD <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#Appendix_A_Oozie_XML-Schema>`_. (The order of the tags matters here.)


.. _errors-auth: 

Error: AUTHENTICATION : Forbidden
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _auth-wrong_version: 

Wrong Version of Oozie Client
*****************************

You are using the wrong version of the Oozie client. You are probably using the 
Apache Oozie client instead of the Yahoo Oozie client. Install the Yahoo Oozie client from 
``http://dist.corp.yahoo.com/by-package/yoozie_client/``.

::

    $ yinst install yoozie_client

.. _auth-incorrect_path: 

Incorrect Workflow Path
***********************

Your ``OOZIE_URL`` environment variable and ``oozie.wf.application.path`` specified in 
your properties file must point to the **same** grid.

.. _incorrect_path-correct:

Correct Example
+++++++++++++++

::

    $ oozie job -oozie http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie -config kr-wf.properties -run -auth kerberos


**kr-wf.properties**

::

    oozie.wf.application.path=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/{userid}/workflow/kr

.. _incorrect_path-incorrect:

Incorrect Example
+++++++++++++++++

::

    $ oozie job -oozie http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie -config db-wf.properties -run -auth kerberos

**db-wf.properties**

::

    oozie.wf.application.path=hdfs://dilithiumblue-nn1.blue.ygrid.yahoo.com:8020/user/{userid}/workflow/db

.. _errors-unauthorized:

E1001 : unauthorized request user [userid]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``E1001`` error means one of two things:

#. The HDFS path to your Workflow application is incorrect.

   - Verify that the HDFS path is correct.
   - Verify that you're using the correct port number.
   - If you are using a local installation, you may have to use ``localhost`` instead 
     of the hostname in the Workflow path.

#. You don't have user/group read access to the Workflow application path.

   - Is the path correct?
   - In the properties file, there should not be any space after the path.
   - Make sure you define absolute paths to your HDFS paths:

     - Correct path syntax: ``oozie.wf.application.path=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/user/myuserid/workflows/pig``
     - Incorrect path syntax: ``oozie.wf.application.path=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020/workflows/pig``

.. _errors-local_install: 

Oozie Local Installation Problems
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _local_install-not_read_wf_def: 

Error: WF:E1310 : could not read the workflow definition
********************************************************

If you are using the Oozie tarball to run a local installation 
on your machine, then use the Apache build (not the Yahoo build).

This is an example of the Apache build: ``oozie-1.5.2.000.0.18.3-2009-09-15_13-15-53-hudson-apache_1_5_branch_milestone_builds-2-2009-09-15_13-15-53.tar.gz``

This is an example of the Yahoo build (do not use this for local development): ``oozie-0.1.1.000.0.18.3-2660502-2009-08-25_00-53-58-SVN-1246-hudson-yahoo_branch_daily_builds-87-2009-08-25_00-53-58.tar.gz``

If you use the Yahoo build, you may see this error::

    2009-09-23 22:24:15,056 FATAL Services:532 - USER[-] GROUP[-] configuration file could not be read [/Users/nipuns/.keystore], {1}
     org.apache.oozie.service.ServiceException: configuration file could not be read [/Users/nipuns/.keystore], {1}
             at org.apache.oozie.service.LdapAuthorizationService.init(LdapAuthorizationService.java:81)
             at org.apache.oozie.service.Services.setServiceInternal(Services.java:279)
             at org.apache.oozie.service.Services.setService(Services.java:265)
             at org.apache.oozie.service.Services.init(Services.java:181)
             at org.apache.oozie.servlet.ServicesLoader.contextInitialized(ServicesLoader.java:40)
             at org.apache.catalina.core.StandardContext.listenerStart(StandardContext.java:3843)
             at org.apache.catalina.core.StandardContext.start(StandardContext.java:4342)
             at org.apache.catalina.core.ContainerBase.addChildInternal(ContainerBase.java:791)
             at org.apache.catalina.core.ContainerBase.addChild(ContainerBase.java:771)
             at org.apache.catalina.core.StandardHost.addChild(StandardHost.java:525)
             at org.apache.catalina.startup.HostConfig.deployWAR(HostConfig.java:830)
             at org.apache.catalina.startup.HostConfig.deployWARs(HostConfig.java:719)
             at org.apache.catalina.startup.HostConfig.deployApps(HostConfig.java:490)
             at org.apache.catalina.startup.HostConfig.check(HostConfig.java:1217)
             at org.apache.catalina.startup.HostConfig.lifecycleEvent(HostConfig.java:293)
             at org.apache.catalina.util.LifecycleSupport.fireLifecycleEvent(LifecycleSupport.java:117)
             at org.apache.catalina.core.ContainerBase.backgroundProcess(ContainerBase.java:1337)
             at org.apache.catalina.core.ContainerBase$ContainerBackgroundProcessor.processChildren(ContainerBase.java:1601)
             at org.apache.catalina.core.ContainerBase$ContainerBackgroundProcessor.processChildren(ContainerBase.java:1610)
             at org.apache.catalina.core.ContainerBase$ContainerBackgroundProcessor.run(ContainerBase.java:1590)
             at java.lang.Thread.run(Thread.java:637)
     2009-09-23 22:24:15,060  INFO Services:538 - Shutdown{E}
     2009-09-23 22:24:53,630  INFO XLogService:538 -


.. _errors-job_failed: 

My Hadoop Job Failed. Where are the Logs?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Go to the Oozie Web UI.
#. Click on your job. A new window will open.
#. Select the action from the bottom of new window. Another new window will open.
#. Click on the right of **Console URL**. This will load the Hadoop Web UI. It will 
   take you to a URL similar to http://tiberiumtan-jt1.tan.ygrid.yahoo.com:19888/jobhistory/job/job_1416815736267_5393163/
#. Select the link of the completed map task from the “Maps” line. It will take you 
   to a URL such as ``http://tiberiumtan-jt1.tan.ygrid.yahoo.com:19888/jobhistory/attempts/job_1416815736267_5393163/m/SUCCESSFUL``.


Click the logs link in the map attempt shown. It will take you to a URL similar to the 
following:: 

    http://tiberiumtan-jt1.tan.ygrid.yahoo.com:19888/jobhistory/logs/gsta31332.tan.ygrid.yahoo.com:8041/container_1416815736267_5393163_01_000001/attempt_1416815736267_5393163_m_000000_0/headlessusername

That page should list three logs: **stderr**, **stdout**, **syslog**. The **stderr** log 
should contain the stacktraces in case of failure. The **stdout** log usually contains 
the execution of the action (Pig client log, Hive client log, shell output, etc.). 
The **stdout** log also might contain error messages or stacktraces, so be sure to 
always check it.
