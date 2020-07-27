Writing MapReduce? code with ToolsRunner? that accepts `-D` and other options through `GenericParser`
=====================================================================================================

.. todo:: find page GenericParser

- Please follow example at :ref:`Sec. <getting_started_yhadoop>`
- Options handled by `GenericParser <http://hadoop.apache.org/docs/r0.23.6/hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options>`_


How to determine the Hadoop version
===================================

To find out which version of Hadoop is running on a cluster, use ``hadoop version``
(Note: ``hadoop -version`` gets you the java version instead)


Which MapReduce APIs to use
===================================

* "Old" MapReduce APIs - ``org.apache.hadoop.mapred.*``
* "New" MapReduce APIs - ``org.apache.hadoop.mapreduce.*``

   * Both APIs should work in 0.23 and later.
   * For pig custom input formats and UDFs, you would need to use the new API.


Which MapReduce APIs to use
===================================

* ``mapred queue -showacls`` would show the list of queues you can submit.
* For requesting new queue access, please request through the `form <https://supportshop.cloud.corp.yahoo.com/ydrupal/?q=grid-services-request>`_


How to access log files (Hadoop 0.20)
=======================================

The logs are available through the Job Tracker URL.

* Go the the `Grid Versions <https://yo/GridVersions/>`_ page and click the the Cluster Configuration link for your cluster.
* On the Cluster Configuration page, click the Job Tracker link.
* At the top right corner of the page click :menuselection:`Quick Links --> Local Logs` (which is also located at the bottom of the Job Tracker page).
* Click 'Job Tracker History' (which takes you to the History Viewer page) and then filter by userid, jobid to find links to your jobs.


How to access job status (Hadoop 0.20)
============================================

I want to get job stats like number of map input records, reduce output records etc after the job is complete. On nitroblue the job stats page disappears as soon as the job is finished. I am unable to get the stats using the command hadoop job status  after the job has completed running.
I tried setting ``-Dmapred.job.tracker.persist.jobstatus.hours=24`` but that didn't help either.

**Ans:** To access your jobs stats after the job is completed on Milthril Gold (Hadoop 0.20), try the following.



* When running your job, you should see something like this displayed on the terminal:

  .. code-block:: bash

    09/09/30 20:13:02 INFO streaming.StreamJob: Tracking URL:
    http://jtgd00065.gold.ygrid.yahoo.com:50030/jobdetails.jsp?jobid=job_200909301338_1234

* You need both the URL and jobid from the line above.
  From your browser, access the Hadoop Administration main page via this URL: http://jtgd00065.gold.ygrid.yahoo.com:50030/
* Scroll down to the end of the page.
* Under Local Logs, click Job Tracker History link.
* At the Filter (*username:jobname*) box, enter you username and click Filter!
* You should found your jobid on the result list, click the link with your jobid and you should see your job stats on the page.
* Click the links and you'll see all the detailed information you want.
* Or, rather than scrolling, do this:

   * Click the Quick Links on the top right hand corner and it will show you a pull-down manual.
   * From the pull-down manual, click Local Logs to jump directly to the bottom of the page.


How to kill a job
======================

Use this command: ``hadoop job -kill job_id``


How to fail or kill a task
=================================

``hadoop job –fail-task attemptid or hadoop job -kill-task attemptid``


The first form adds to the tasks fail count, the second does not.
A task is allowed to fail four times before hadoop kills the job.
The attempt ID should look something like: ``attempt_201103101125_12196_m_000025_0`` (for a map task).

How to set 777 Permission as default
============================================

I need the files I create to be readable and writable by my colleagues.
I set the dir to be 777, but every time I delete the files, and recreate them, they become non-readable again by my colleagues.

**Ans**: Add this option definition to your Pig or Map/Reduce commands:

``-Ddfs.umask=0 # makes all files readable and writable``

How to give colleagues access to your online job logs
=======================================================

By default, job logs are only readable by the person who submitted the logs.
To give a specific user an access, set ``-D mapreduce.job.acl-view-job="user1,user2"``.
To give access only to group gridpe, set ``-D mapreduce.job.acl-view-job=" gridpe"``.

Note the space before the gridpe. To give access to all users, set ``-D mapreduce.job.acl-view-job="*".``, but please understand that you're making the jobconf wide open. (could become security issue depending on the application you run)