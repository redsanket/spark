.. _oozie_client:

Oozie Client
============

.. 04/17/15: Rewrote.

Overview
--------

The Oozie client is the Oozie program that allows you to execute Workflows, Coordinators,
and Bundles. You can invoke the Oozie client from the command line or through 
`Hue <http://devel.corp.yahoo.com/hue/>`_.

The Oozie client (``yoozie_client`` package) is installed on all grid gateway machines, 
and Oozie can be invoked without any setup. The path ``/home/y/var/yoozieclient/bin`` is 
automatically added to the ``PATH`` global variable through the grid gateway user profile script. If you 
are installing ``yoozie_client`` on your launcher box, ensure you add ``/home/y/var/yoozieclient/bin``
to the ``PATH`` variable or invoke the full path ``/home/y/var/yoozieclient/bin/oozie``.

Installing Oozie Client
-----------------------

If you run the Oozie client from a gateway, you do not need to install it.
The steps below are for those wanting to run the Oozie client from an OpenStack instance.

#. Create a `OpenStack <http://yo/openhouse>`_ instance with YLinux 6.2+ and a medium disk or higher. 
#. Install the Oozie client: ``$ yinst i yoozie_client -br test``
#. Run the Oozie command with the ``-keydb`` option: ``$ oozie jobs -len 1 -keydb -auth kerberos``

General
-------

We have listed the more widely used Oozie commands and those that are
specific to Yahoos. For a complete reference, see
the `Command-Line Interface Utilities documentation <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_CommandLineTool.html>`_.


-keydb
~~~~~~

If ``keydb`` is not set up and ``-keydb`` is specified, the Backyard password is required for the Bouncer authentication.
Because Backyard authentication is no longer used, you should specify ``-auth kerberos`` to use Kerberos authentication::

    $ oozie jobs -len 1 -keydb -auth kerberos

Using -keydb on a Gateway
*************************

Put your ``keydb`` file in your home, in a directory structure similar to
``/home/myself/oozie/root/conf/keydb/myself.keydb``. 

In this file, the key name (key name ``name=``) *must* be your user name,
otherwise, it will not be found.  Most other ``keydb`` uses can use any
name, but for Oozie it must be the user name. (The actual file name doesn't
matter as long as it has the file extension ``keydb``. For example: ``somefile.keydb``

Set either ``$ROOT`` or ``$YINST_ROOT`` to the path containing ``conf``.  In
the example script below, the line ``export ROOT=/home/myself/oozie/root/`` 
is important.

**Example Script**

:: 

    export ROOT=/home/user/oozie/root
    oozie=/home/y/var/yoozieclient/bin/oozie
    server=http://axoniteblue-oozie.blue.ygrid.yahoo.com:4080/oozie
    opts="-keydb -oozie $server -auth kerberos"
    oozie jobs $opts

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


Now, you can use -keydb in Oozie client: ``$ oozie job -run -config xxx.properties -keydb -auth kerberos``


-oozie
~~~~~~

The ``-oozie`` option is used to specify the Oozie URL. If ``-oozie`` is not specified on the command line, 
the environment variable ``OOZIE_URL`` will be the default Oozie URL. If you have not set ``OOZIE_URL``
or specified the Oozie URL with the option ``-oozie``, you will get the following error::

    java.lang.IllegalArgumentException: Oozie URL is not available neither in command option or in the environment
    	at org.apache.oozie.cli.OozieCLI.getOozieUrl(OozieCLI.java:677)
    	at com.yahoo.oozie.cli.YOozieCLI.createXOozieClient(YOozieCLI.java:348)
    	at org.apache.oozie.cli.OozieCLI.jobsCommand(OozieCLI.java:1491)
    	at org.apache.oozie.cli.OozieCLI.processCommand(OozieCLI.java:642)
    	at org.apache.oozie.cli.OozieCLI.run(OozieCLI.java:592)
    	at com.yahoo.oozie.cli.YOozieCLI.main(YOozieCLI.java:170)
    Oozie URL is not available neither in command option or in the environment


The ``-oozie`` option also allows you to overwrite the environment variable ``OOZIE_URL``.

For example: ``$ oozie jobs -len 1 -keydb -oozie http://cobaltblue-oozie.blue.ygrid.yahoo.com:4080/oozie -auth kerberos``

-auth (Oozie 2.2+)
~~~~~~~~~~~~~~~~~~

The ``-auth`` option allows you to specify the authentication type. The default is Backyard, but it is **no longer** supported, so
you should use the ``-auth`` option with the two other valid types: ``YCA`` and ``Kerberos``. (The authentication type
is case insensitive.) 

For example: ``$ oozie jobs -len 1 -auth kerberos``

Job Operations
--------------

Submit a Workflow Job
~~~~~~~~~~~~~~~~~~~~~

The ``-submit`` option creates an Oozie job and returns a job ID, but does not actually run
the job until you use the ``-start`` option.

.. note:: The ``-submit`` option is not supported for Coordinator job as of Oozie 2.2.

For example: ``$ oozie job -submit -config job.properties -auth kerberos``


Start a Workflow Job
~~~~~~~~~~~~~~~~~~~~

After you have submitted your job, you will receive a job ID. You
can start the job with the ``-start`` option and the job ID.

.. note:: Again, the ``-start`` option is not supported for Coordinator jobs as of Oozie 2.2.

For example: ``$ oozie job -start oozie-wf-jobID -auth kerberos``

Run a Workflow or Coordinator Job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``-run`` option to create and execute an Oozie job.

For example: ``$ oozie job -run -config job.properties -auth kerberos``

Suspend a Workflow or Coordinator Job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``-suspend`` option suspends Oozie jobs and their actions.

For example: ``$ oozie job -suspend oozie-jobID -auth kerberos``

Resume a Workflow or Coordinator Job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To resume a suspended job and actions, you use the ``-resume`` option. 

For example: ``$ oozie job -resume oozie-jobID -auth kerberos``


Kill a Workflow or Coordinator Job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To kill an Oozie job and its actions, you use the ``-kill`` option.

For example: ``$ oozie job -kill oozie-jobID -auth kerberos``

Rerun a Workflow Job
~~~~~~~~~~~~~~~~~~~~

You can rerun Workflow jobs with terminal states ``SUCCEEDED``, ``FAILED``, ``KILLED``.

In the ``job.properties`` file, you specify the actions
you want to skip as shown below::

     # workflow nodes map_reduce_1, java_1, and hdfs_1 will be skipped, i.e., not rerun.
     oozie.wf.rerun.skip.nodes=map_reduce_1,java_1,hdfs_1

     # all workflow will be rerun, i.e., no skipped nodes.
     oozie.wf.rerun.skip.nodes=,

For example: ``$ oozie job -config job.properties -rerun oozie-wf-jobID -auth kerberos``

.. _rerun_coords:

Rerun Coordinator Action[s] (Oozie 2.1+)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also rerun Coordinator actions that are in the 
terminal states ``TIMEDOUT``, ``SUCCEEDED``, ``KILLED``, and ``FAILED``.

For example, to rerun the first action in a Coordinator::

    $ oozie job -rerun oozie-coord-jobID -action 1 -auth kerberos

You can also schedule to rerun a Coordinator at a specified time with the ``-date`` 
option (the date needs to be in UTC format)::

    $ oozie job -rerun oozie-coord-jobID -date 2010-09-10T01:00Z -auth kerberos


By default, when Coordinator actions are rerun, they delete all output events before rerunning 
the actions. If you do not want to delete output events, add the option ``-nocleanup``::

    $ oozie job -rerun oozie-coord-jobID -action 1 -nocleanup -auth kerberos

In addition, when Coordinator action are rerun, they will by default reuse the 
previous input events for ``coord:latest()`` and/or ``coord:future()``.
If there are new input events available, rerun the job and specify the ``-refresh`` option 
to re-evaluate input events for ``coord:latest()`` and/or ``coord:future()``::

    $ oozie job -rerun oozie-coord-jobID -action 1 -refresh -auth kerberos

.. note:: The ``-refresh`` option is not supported for the Coordinator job as of Oozie 2.2.


Change a Coordinator Job (Oozie 2.1+)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``-change`` option to change a Coordinator job.

To change the end time, you use the option ``-change`` with the ``-value`` option
and specify the key-value parameter for the end time::

    $ oozie job -change oozie-coord-jobID -value endtime=2010-09-10T01:00Z -auth kerberos

.. note:: The new ``endtime`` needs to be later than the time of the last executed action.
          If the Coordinator job completes, changing the ``endtime`` to a later date will trigger 
          the Coordinator job to create and run new actions.

To change the concurrency, you use the ``-change`` option and the ``-value`` option
with the parameter ``concurrency``::

    $ oozie job -change oozie-coord-jobID -value concurrency=10 -auth kerberos

.. note:: If you change ``concurrency`` to ``-1`` or another negative integer, it signifies no limit to the concurrency.

In the same way, you can change the pause time::

    $ oozie job -change oozie-coord-jobID -value pausetime=2010-09-10T01:00Z -auth kerberos

.. note:: The ``pausetime`` needs to be later than the time of the last executed action.
          Assigning an empty value to``pausetime`` removes the previous ``pausetime``.
          For example: ``$ oozie job -change oozie-coord-jobID -value pausetime='' -auth kerberos``

To change multiple values::

    $ oozie job -change oozie-coord-jobID -value endtime=2010-09-10T01:00Z\;concurrency=10 -auth kerberos
    $ oozie job -change oozie-coord-jobID -value "endtime=2010-09-10T01:00Z;concurrency=10" -auth kerberos


.. Left off here on 04/18/15. 

Check the Job Status for Workflow or Coordinator Jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``-info`` option allows you to view the status of Oozie jobs.

To view the basic job status::

    $ oozie job -info oozie-jobID -auth kerberos

For the detailed job status, you use the ``-info`` option with the ``-verbose`` option::

    $ oozie job -info oozie-jobID -verbose -auth kerberos

You can also get a detailed job status for specified actions::

    $ oozie job -info oozie-jobID -len 10 -offset 60 -verbose -auth kerberos

For a detailed Coordinator status:: 

    $ oozie job -info oozie-coord-jobID@2 -verbose -auth kerberos

For a detailed status of a Workflow, you use the ``@`` symbol to 
specify the Workflow ID::

    $ oozie job -info oozie-wf-jobID@hadoop1 -verbose -auth kerberos

Check the Job Definition for Workflow or Coordinator Jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``-definition`` option to view a job definition for a Workflow or Coordinator.

For example: ``$ oozie job -definition oozie-jobID -auth kerberos``



Check the Job Logs for Workflow or Coordinator Jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``-lob`` option to view job logs.

For example: ``$ oozie job -log oozie-jobID -auth kerberos``


Dry Run of a Coordinator Job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``-dryrun`` option to do a dry run of a Coordinator. This will print the 
job definition and all action instances. 
All parameters, except run time parameters such as ``${YEAR}``, ``${MONTH}``, 
``${DAY}``, ``${HOUR}``, ``${MINUTE}`` will be resolved.

For example: ``$ oozie job -dryrun -config job.properties -auth kerberos``


Filter Jobs
~~~~~~~~~~~

You can view a subset of jobs or filter jobs based on certain parameters.

For example, to view the five Workflow jobs starting from the second job (jobs ordered by start time),
you use the ``-len`` and ``-offset`` options together::

    $ oozie jobs -len 5 -offset 2 -auth kerberos

To filter jobs based on parameters, use the ``-filter`` option followed by the parameter::

    $ oozie jobs -len 5 -filter "status=KILLED;user=start_ci -auth kerberos"

See also `Checking the Status of multiple Workflow Jobs <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_CommandLineTool.html#Checking_the_Status_of_multiple_Workflow_Jobs>`_.



Check the Status of Coordinator Jobs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``-jobtype`` option to view job information for Coordinators.

For example, to list five Coordinator jobs from the second job (jobs ordered by created time):: 

    $ oozie jobs -len 5 -offset 2 -jobtype coord -auth kerberos

To list five Coordinator jobs with ``KILLED`` status and the application name ``coord-test``:: 

    $ oozie jobs -len 5 -filter "status=KILLED;name=coord-test" -jobtype coord -auth kerberos

See also `Coordinator Job <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.1.2._Coordinator_Job>`_
and `Coordinator Action Status <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.1.3.2._Coordinator_Action_Status>`_.


Admin Operations
----------------

Assign Admin Users (Oozie 2.2+)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``yinst`` with the ``set`` command to assign administrators for an Oozie instance.

#. Assign the users as administrators::

       $ yinst set yoozie_conf_<instance>.adminusers='username1,username2' 

#. Restart the ``yoozie`` configuration package. 
#. Restart ``yjava_tomcat``.

Check Oozie Build Version
~~~~~~~~~~~~~~~~~~~~~~~~~

To check the Oozie build version::

    $ oozie admin -version -auth kerberos

Change and Check the System Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The valid system modes are ``NORMAL``, ``NOWEBSERVICE``, and ``SAFEMODE``.
You can check the the system mode with the ``-status`` option and change
the status with the ``-systemmode`` option.

For example, to check the system mode::

    $ oozie admin -status -auth kerberos

To change to ``SAFEMODE``, you would use the following::

    $ oozie admin -systemmode SAFEMODE -auth kerberos


Validate Operations
-------------------

The ``validate`` command allows you to validate your Workflow XML. See `Validating a Workflow 
XML <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_CommandLineTool.html#Validating_a_Workflow_XML>`_.

.. note:: The ``validate`` command currently only supports validating ``workflow.xml``.

SLA Operations
--------------

The ``sla`` command allows you to get a list of SLA events and information about those events.

For example, to list two SLA records with the sequence ID 101 and sequence ID 102:: 

    $ oozie sla -offset 100 -len 2 -auth kerberos

See `SLA Operations <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_CommandLineTool.html#SLA_Operations>`_ 
for more examples.


Pig Operations (Oozie 2.2+)
---------------------------

The Oozie client has a ``pig`` command that provides you with options for Pig operations.

In the following example, all JAR files, including ``pig.jar`` and any customized 
UDF, need to be uploaded to the Oozie library path in advance. 
The parameter ``paramfile`` is a file that also needs to be uploaded to the Oozie 
library path before the command can be executed::

    $ oozie pig -file multiquery1.pig -config job.properties -X -Dmapred.job.queue.name=grideng -Dmapred.compress.map.output=true -Ddfs.umask=18 -param_file paramfile -p INPUT=/tmp/workflows/input-data -auth kerberos


.. note:: The following Pig options are not supported: ``-4 (-log4jconf)``, ``-e (-execute)``, ``-f (-file)``, 
          ``-l (-logfile)``, ``-r (-dryrun)``, ``-x (-exectype)``, ``-P (-propertyFile)``.

The ``job.properties`` file specified in the command above might look similar to the
following::

    fs.default.name=hdfs://gsbl91027.blue.ygrid.yahoo.com:8020
    mapred.job.tracker=gsbl91029.blue.ygrid.yahoo.com:50300
    oozie.libpath=hdfs://gsbl91027.blue.ygrid.yahoo.com:8020/tmp/user/workflows/lib
    mapreduce.jobtracker.kerberos.principal=mapred/gsbl91029.blue.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
    dfs.namenode.kerberos.principal=hdfs/gsbl91027.blue.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM


See `Submitting a pig job through HTTP <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_CommandLineTool.html#Submitting_a_pig_job_through_HTTP>`_
for another example.

