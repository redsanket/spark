Oozie Client
============

Overview
--------

TBD: What, why

The Oozie client (``yoozie_client`` package) is installed on all Grid Gateway machines, 
and Oozie can be invoked without any setup. the path ``/home/y/var/yoozieclient/bin`` is 
automatically added to the ``PATH`` global variable through the grid gateway user profile script. If you 
are installing ``yoozie_client`` on your launcher box, ensure you add ``/home/y/var/yoozieclient/bin``
to the ``PATH`` variable or invoke the full path ``/home/y/var/yoozieclient/bin/oozie``.

Installing Oozie Client
-----------------------

If you run the Oozie client from a gateway, you do not need to install it.
The steps below are for those wanting to run the Oozie client from an OpenStack instance.

#. Create a `OpenStack <http://yo/openhouse>`_ instance with YLinux 6.2+ and a medium disk or higher. 
#. Install the Oozie client: ``$ yinst i yoozie_client -br test``
#. Run the Oozie command with the ``-keydb`` option: ``$ oozie jobs -len 1 -keydb``

General
-------

See http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie/docs/DG_CommandLineTool.html

-keydb
~~~~~~

Set up keydb
If keydb is not set up, and -keydb is not specified in command line, BackYard password is required for the Bouncer authentication.
Example, $ oozie jobs -len 1 -keydb

Using -keydb on Gateway
***********************

Put your keydb file in your home, in a dir structure similar to
``/home/myself/oozie/root/conf/keydb/myself.keydb``. 

In this file, the keyname (keyname name=) *must* be your username,
otherwise it will not be found.  Most other ``keydb`` uses can use any
name, but for Oozie it must be the user name.  (The actual filename doesn't
matter as long as it is ``*.keydb``.)

Set either ``$ROOT`` or ``$YINST_ROOT`` to the path containing ``conf``.  In
the example script below, the line ``export ROOT=/home/myself/oozie/root/`` is important.

**Example Script**

:: 

    export ROOT=/home/user/oozie/root
    oozie=/home/y/var/yoozieclient/bin/oozie
    server=http://axoniteblue-oozie.blue.ygrid.yahoo.com:4080/oozie
    user=user
    bouncer=gh
    optsd="-Doozie.bouncer=$bouncer -Doozie.save.cookie=false -Duser.name=$user"
    opts="-keydb -oozie $server"
    oozie jobs $optsd -filter user=$user $opts

**Example keydb File**

.. code-block:: xml

   <keydb>
       <keygroup name="oozie" id="0">
           <keyname name="USERNAME" usage="all" type="transient">
               <key version="0"
                    value = "ACTUALPASSWORDHERE" current = "true"
                    timestamp = "20100704074611"
                    expiry = "20130703074611">
               </key>
           </keyname>
       </keygroup>
   </keydb>








Using keydb on Client
*********************

First, these packages must be installed::

    $ yinst install bouncer_auth_java
    $ yinst install yjava_byauth

After installation, create a ``keydb`` file with your account and password.

::

    $ vim /home/y/conf/keydb/oozie.keydb (you can call it any name you like.)

.. code-block:: xml

   <keydb>
       <keygroup name="oozie_key" id="0">   
           <keyname name="USER" usage="all" type="permanent">
               <key version="0"
               value = "PASSWORD" current = "true"
               timestamp = "20100409011532"
               expiry = "20130408011532">
           </key>
       </keyname>
   </keygroup>


Now, you can use -keydb in Oozie client: ``$ oozie job -run -config xxx.properties -keydb``







-oozie
~~~~~~

Specify the oozie URL. If -oozie is not specified in command line, environment variable OOZIE_URL will be the default oozie URL.
-oozie overwrite OOZIE_URL.
Example, $ oozie jobs -len 1 -keydb -oozie http://cobaltblue-oozie.blue.ygrid.yahoo.com:4080/oozie

-auth (Oozie 2.2+)
~~~~~~~~~~~~~~~~~~

Set up client authentication
Valid authentication types: YCA, KERBEROS. BOUNCER. Authentication type is case insensitive. If -auth is not specified in command line, default is BOUNCER authentication.
Example, $ oozie jobs -len 1 -auth Kerberos

Job Operations
--------------

Submit a workflow job
~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Oozie job will be created, returned with a job ID, but it will not run until a "-start" command.
Not supported for coordinator job. (as of oozie 2.2)
Example, $ oozie job -submit -config job.properties


Start a workflow job
~~~~~~~~~~~~~~~~~~~~

Oozie docs
Oozie job, previously submitted (-submit) with the given job ID, will be executed.
Not supported for coordinator job. (as of oozie 2.2)
Example, $ oozie job -start oozie-wf-jobID

Run a workflow or coordinator job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Oozie job will be created and executed.
Example, $ oozie job -run -config job.properties

Suspend a workflow or coordinator job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Oozie job, and the actions, will be suspended.
Example, $ oozie job -suspend oozie-jobID

Resume a workflow or coordinator job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Resume the previously suspended oozie job, and the actions.
Example, $ oozie job -resume oozie-jobID


Kill a workflow or coordinator job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Oozie job, and the actions, will be killed.
Example, $ oozie job -kill oozie-jobID

Rerun a workflow job
~~~~~~~~~~~~~~~~~~~~

Oozie docs
Workflow jobs in terminal states, SUCCEEDED, FAILED, KILLED, can be rerun.
Specify skipped nodes in job.properties file.

::

     # workflow nodes map_reduce_1, java_1, and hdfs_1 will be skipped, i.e., not rerun.
     oozie.wf.rerun.skip.nodes=map_reduce_1,java_1,hdfs_1

     # all workflow will be rerun, i.e., no skipped nodes.
     oozie.wf.rerun.skip.nodes=,

Example, $ oozie job -config job.properties -rerun oozie-wf-jobID


Rerun coordinator action[s] (Oozie 2.1+)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Coordinator actions in terminal states, TIMEDOUT, SUCCEEDED, KILLED, FAILED.
Example, $ oozie job -rerun oozie-coord-jobID -action 1
Example, $ oozie job -rerun oozie-coord-jobID -date 2010-09-10T01:00Z. The date needs to be in UTC format.
By default, coordinator action rerun will delete all output events before re-run the actions. If the output events need not delete before rerun, apply -nocleanup, e.g.,
$ oozie job -rerun oozie-coord-jobID -action 1 -nocleanup
By default, coordinator action rerun will re-use the previous input events for coord:latest() and/or coord:future().
if there are new input events available, apply -refresh for rerun to re-evaluate input events for coord:latest() and/or coord:future(). e.g, 
$ oozie job -rerun oozie-coord-jobID -action 1 -refresh.
Not supported for coordinator job. (as of oozie 2.2)


Change a coordinator job (Oozie 2.1+)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
To change end time, $ oozie job -change oozie-coord-jobID -value endtime=2010-09-10T01:00Z
the new endtime needs to be later than the nominal time of the last materialized action.
if the coordinator job already completes, changing endtime to a later date will trigger the coordinator job to create and run new actions.
To change concurrency, $ oozie job -change oozie-coord-jobID -value concurrency=10
if change concurrency to -1 or other negative integer, it means no limit in concurrency.
To change pause time, $ oozie job -change oozie-coord-jobID -value pausetime=2010-09-10T01:00Z
the pausetime needs to be later than the nominal time of the last materialized action.
setting pausetime to blank is to remove the previous pausetime, $ oozie job -change oozie-coord-jobID -value pausetime=''

To change multiple values,

$ oozie job -change oozie-coord-jobID -value endtime=2010-09-10T01:00Z\;concurrency=10
$ oozie job -change oozie-coord-jobID -value "endtime=2010-09-10T01:00Z;concurrency=10"



Check job status for workflow or coordinator job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Example, job status, $ oozie job -info oozie-jobID
Example, detailed job status, $ oozie job -info oozie-jobID -verbose
Example, detailed job status for specified actions, $ oozie job -info oozie-jobID -len 10 -offset 60 -verbose
Example, detailed coordinator action status, $ oozie job -info oozie-coord-jobID@2 -verbose
Example, detailed workflow action status, $ oozie job -info oozie-wf-jobID@hadoop1 -verbose

Check job status for workflow or coordinator job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check job definition for workflow or coordinator job
Oozie docs
Example, $ oozie job -definition oozie-jobID



Check job logs for workflow or coordinator job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Example, $ oozie job -log oozie-jobID

Dryrun of a coordinator job
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oozie docs
Coordinator dry run will print the job definition, and all action instances. All parameters, except run time parameters such as ${YEAR}, ${MONTH}, ${DAY}, ${HOUR}, ${MINUTE}, will be resolved.
Example, $ oozie job -dryrun -config job.properties


Jobs Operations
---------------

Check status of workflow jobs
Oozie docs
Example, list 5 workflow jobs from the second job (jobs ordered by Started Time), $ oozie jobs -len 5 -offset 2
Example, list 5 workflow jobs with KILLED status and submitted by strat_ci, $ oozie jobs -len 5 -filter "status=KILLED;user=start_ci"
Workflow job status

Check status of coordinator jobs
--------------------------------

Oozie docs
Example, list 5 coordinator jobs from the second job (jobs ordered by Created Time), $ oozie jobs -len 5 -offset 2 -jobtype coord
Example, list 5 coordinator jobs with KILLED status and application name coord-test, $ oozie jobs -len 5 -filter "status=KILLED;name=coord-test" -jobtype coord
Coordinator job status, Coordinator job status (oozie 3.0+, working in progress)
Coordinator action status


Admin Operations
----------------

Assign admin users (Oozie 2.2+)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

$ yinst set yoozie_conf_<instance>.adminusers='username1,username2'. Then restart yoozie conf package, and restart yjava_tomcat.

Check oozie build version
~~~~~~~~~~~~~~~~~~~~~~~~~

$ oozie admin -version

Change and check system mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Valid system modes: NORMAL, NOWEBSERVICE, SAFEMODE
Example, $ oozie admin -systemmode SAFEMODE
Example, $ oozie admin -status

Validate Operations
-------------------

Oozie docs
It supports workflow xml check only.

SLA Operations
--------------

Oozie docs
Example, list 2 SLA records, sequence-id 101 and sequence-id 102, $ oozie sla -offset 100 -len 2


Pig Operations (Oozie 2.2+)
---------------------------

Oozie docs
Example, $ oozie pig -file multiquery1.pig -config job.properties -X -Dmapred.job.queue.name=grideng -Dmapred.compress.map.output=true -Ddfs.umask=18 -param_file paramfile -p INPUT=/tmp/workflows/input-data
All jar files, including pig.jar and customized udf, need to upload to <oozie.libpath> in advance.
When -param_file option is used, the <parameter file> need to upload to <oozie.libpath> in advance.
-X is the last argument in the command line.
NOT supported pig options: -4 (-log4jconf), -e (-execute), -f (-file), -l (-logfile), -r (-dryrun), -x (-exectype), -P (-propertyFile)

::

    $ cat job.properties
    fs.default.name=hdfs://gsbl91027.blue.ygrid.yahoo.com:8020
    mapred.job.tracker=gsbl91029.blue.ygrid.yahoo.com:50300
    oozie.libpath=hdfs://gsbl91027.blue.ygrid.yahoo.com:8020/tmp/user/workflows/lib
    mapreduce.jobtracker.kerberos.principal=mapred/gsbl91029.blue.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
    dfs.namenode.kerberos.principal=hdfs/gsbl91027.blue.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM



Using Oozie Maven Artifacts
---------------------------

If you have a Java maven project which uses Oozie client or core library, you can 
simply use Oozie maven artifacts. Given below is the maven repository and dependency 
settings for your POM file.

Version numbers: If using oozie.version 4.4.1.3 (production Jan 2015) --> 4.4.1.3.1411122125 . 
Check the version of current version of Oozie deployed in http://twiki.corp.yahoo.com/view/Grid/GridVersions.

POM XML
~~~~~~~

.. code-block:: xml

   <repositories>
     <repository>
       <id>yahoo</id>
         <url>http://ymaven.corp.yahoo.com:9999/proximity/repository/public</url>
         <snapshots>
         <enabled>false</enabled>
         </snapshots>
     </repository>
   </repositories>
   <dependencies>
     <dependency>      
       <groupId>org.apache.oozie</groupId>
       <artifactId>yoozie-client</artifactId>
       <version>${oozie.version}</version>
       <scope>compile</scope>
       </dependency>
     </dependency>
     <dependency>
       <groupId>org.apache.oozie</groupId>
       <artifactId>oozie-core</artifactId>
       <version>${oozie.version}</version>
       <classifier>tests</classifier>   
       <scope>compile</scope>
     </dependency>
     <dependency>
       <groupId>org.apache.oozie</groupId>
       <artifactId>oozie-core</artifactId>
       <version>${oozie.version}</version>
       <scope>compile</scope>
     </dependency>
   <dependency>      
       <groupId>org.apache.oozie</groupId>
       <artifactId>yoozie-auth</artifactId>
       <version>${oozie.version}</version>
       <scope>compile</scope>
       </dependency>
     </dependency>
   </dependencies>         

Getting the Required Yinst Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alternately, you can also install following ``yoozie`` yinst packages to get the Oozie Jars and POM files.

``yinst i yoozie_maven -br stable`` 

.. note:: The ``current`` branch might also contain the version deployed on a research cluster. 
          Package is promoted to stable only when it is deployed on production.


yinst i yoozie_hadooplibs_maven -br stable
yinst i yoozie_hbaselibs_maven -br stable
yinst i yoozie_hcataloglibs_maven -br stable




