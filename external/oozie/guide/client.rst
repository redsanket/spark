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

General
-------

See http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie/docs/DG_CommandLineTool.html

-keydb
~~~~~~

Set up keydb
If keydb is not set up, and -keydb is not specified in command line, BackYard password is required for the Bouncer authentication.
Example, $ oozie jobs -len 1 -keydb

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
