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

Can't Submit Oozie Workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Internal Server Error (1)
*************************

:: 

    $ yoozie job -run -config map-reduce-job.properties

    Error: OTHER : Internal Server Error


Solution: Do you have access to a Hadoop queue?

Check out the queues for Axonite Blue here: http://ucdev4.yst.corp.yahoo.com/jtqueues/?cluster=axonite-blue

- You do not have access to the Hadoop queue!

  ::

     $ hadoop queue -showacls


If you don't have access to the queue you are using, submit a request to Grid-Ops; 
You should have access to a Hadoop queue and will be able to submit workflows to Oozie.


Internal Server Error (2)
*************************

Your ``OOZIE_URL`` environment variable and ``oozie.wf.application.path`` specified in 
your properties file must point to the **same** grid.

Correct Example
+++++++++++++++

::

    $ oozie job -oozie http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie -config mb-wf.properties -run

::

    $ cat mb-wf.properties

Incorrect Example
+++++++++++++++++

::

    $ oozie job -oozie http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie -config mb-wf.properties -run
    oozie.wf.application.path=hdfs://mithrilblue-nn1.blue.ygrid.yahoo.com:8020/user/userid/workflow/mb

**mb-wf.properties**

::

    oozie.wf.application.path=hdfs://dilithiumblue-nn1.blue.ygrid.yahoo.com:8020/user/userid/workflow/mb

E0701: XML Schema Error
~~~~~~~~~~~~~~~~~~~~~~~

E0701: XML schema error, cvc-complex-type.2.4.a: Invalid content was found starting 
with element ``some-element-a``. One of ``{"uri:oozie:workflow:0.5":other-element-b}`` is expected.

If you encounter above error, you should check that your xml tags are in the correct 
sequence as specified in the Workflow XSD. Order of the tags matters here.


Error: AUTHENTICATION : Forbidden
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Wrong Version of Oozie Client
*****************************

You are using the wrong version of the oozie client. You are probably using the Apache oozie client instead of the Yahoo oozie client.
Download the yahoo oozie client from dist.corp.yahoo.com

::

    $ yinst install yoozie_client

Incorrect Workflow Path
***********************

Your ``OOZIE_URL`` environment variable and ``oozie.wf.application.path`` specified 
in your properties file must point to the same grid.

Correct Example
+++++++++++++++

::

    $ oozie job -oozie http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie -config mb-wf.properties -run


``mb-wf.properties``

::

    oozie.wf.application.path=hdfs://mithrilblue-nn1.blue.ygrid.yahoo.com:8020/user/userid/workflow/mb

Incorrect Example
+++++++++++++++++

::

    $ oozie job -oozie http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie -config mb-wf.properties -run

``mb-wf.properties``

::

    oozie.wf.application.path=hdfs://dilithiumblue-nn1.blue.ygrid.yahoo.com:8020/user/userid/workflow/mb

E1001 : unauthorized request user [userid]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

the ``E1001`` error means one of two things:

1. The HDFS path to your Workflow application is incorrect.

- Verify that the HDFS path is correct
- Are you using the correct port number?
- If you are using a local installation, you may have to use 'localhost' instead 
  of the hostname in the WF path

2. You don't have user/group read access to the Workflow application path.

- Is the path correct?
- In the properties file, there should not be any space after the path.
- Make sure you define absolute paths to your HDFS paths.

  - Correct path syntax: ``oozie.wf.application.path=hdfs://axoniteblue-nn1.blue.ygrid.yahoo.com:8020/user/myuserid/workflows/pig``
  - Incorrect path syntax: ``oozie.wf.application.path=hdfs://axoniteblue-nn1.blue.ygrid.yahoo.com:8020/workflows/pig``

Oozie Local Installation Problems
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

My Hadoop Job Failed. Where are the Logs?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Go to the Oozie Web UI.
#. Click on your job. A new window will pop up.
#. Select the action from the bottom of new window. It will also pop-up another new window.
#. Click on the right of **Console URL**. This will load the Hadoop Web UI. It will 
   take you to a url like http://tiberiumtan-jt1.tan.ygrid.yahoo.com:19888/jobhistory/job/job_1416815736267_5393163/
#. Select the link of the completed map task from the “Maps” line. It will take you 
   to a URL such as http://tiberiumtan-jt1.tan.ygrid.yahoo.com:19888/jobhistory/attempts/job_1416815736267_5393163/m/SUCCESSFUL


Click on the logs link in the map attempt shown. It will take you to a url like http://tiberiumtan-jt1.tan.ygrid.yahoo.com:19888/jobhistory/logs/gsta31332.tan.ygrid.yahoo.com:8041/container_1416815736267_5393163_01_000001/attempt_1416815736267_5393163_m_000000_0/headlessusername. That page should list three logs - stderr, stdout, syslog. stderr should contain the stacktraces in case of failure. stdout usually contains the execution of the action (pig client log, hive client log, shell output, etc). stdout also might contain error messages or stacktraces. So be sure to always check that.



