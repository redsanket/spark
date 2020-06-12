**************
YARN/MapReduce
**************

.. contents:: Table of Contents
  :local:
  :depth: 4

-----------


Writing MapReduce? code with ToolsRunner? that accepts `-D` and other options through `GenericParser`
=====================================================================================================

.. todo:: find page GenericParser

- Please follow example at :ref:`Sec. <guide_getting_started_try_out_mapreduce>`
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

How to give your tasks more memory
============================================

There are a number of mapreduce options that control how much memory your tasks can access. Typically there are separate options for mappers and reducers, but some options allow you to set limits for both at once.


First, ``mapreduce.map.memory.mb`` controls how much memory space is allocated to your map tasks (see Memory Management).
You can increase it up to the value of ``yarn.scheduler.maximum-allocation-mb``, which is currently set to ``8192MB`` and not over-writable. This param is used by the framework in limiting the amount of ``vmem`` and ``pmem`` usage per task.
It adds up everything from the task's process-tree. vmem usage is checked against ``mapreduce.{map,reduce}.memory.mb`` and ``pmem`` usage against ``mapreduce.{map,reduce}.memory.mb``, ``yarn.nodemanager.vmem-pmem-ratio`` (which is set to 2.1 cluster-wide).

This same param is used by the CapacityScheduler in scheduling your tasks. Task space is allocated in increments of ``yarn.scheduler.minimum-allocation-mb`` (512MB) and there is a queue-determined limit on how much resources each user can take at once.
The map/reduce tasks are run in a JVM. Java processes uses heap memory which is specified through -Xmx.
JVM also uses native memory for allocating thread stack space, jvm internal data structures, etc and any JNI in your task code will use native memory too. ``mapreduce.{map,reduce}.memory.mb`` need to be sum of Heap memory and native memory which is the total memory that will be used by the java process.

You will want to increase the heap size if your job has large data structures such as big hashes. You can do this for both mapper and reducer with ``mapred.child.java.opts``, (see task environment) or separately with ``mapreduce.{map,reduce}.java.opts``. For example, to set the mapper heap size to 2GB, use ``-Dmapred.map.child.java.opts=-Xmx2048m``. Other ``java -X`` options let you the initial heap size & stack size. It is a good practice to use ``mapreduce.{map,reduce}.java.opts`` instead of changing ``mapred.map.child.java.opts`` as only reducers usually require more memory and increasing for both map and reducers will waste memory.

By default ``512MB`` is left for native memory. We recommend users maintain the same ratio while increasing memory unless more native memory is required with JNI. So in general, ``mapreduce.{map,reduce}.memory.mb`` should be equal to sum of ``-Xmx`` specified in ``mapreduce.{map,reduce}.java.opts`` and 512 MB. If you set ``mapreduce.{map,reduce}.memory.mb`` to 4096 but only have ``-Xmx1536M`` in your ``mapreduce.{map,reduce}.java.opts``, then you are wasting 2G of memory.

Always ensure that you increase both ``memory.mb`` and ``java.opts`` together and the difference between them is 512MB.
You can check the actual Physical memory usage of the tasks in the Counters page of the job in the UI (:menuselection:`Map-Reduce Framework --> Physical memory (bytes) snapshot`) and tune (increase or reduce) the memory further based on actual usage. If the Counters page shows that there are lot of spill (:menuselection:`Map-Reduce Framework --> Spilled Records`), then increase ``mapreduce.task.io.sort.mb`` to 512 or 768. Default is 256. Reducing spill will also speed up the job.
For Tez, you can go to "All Tasks" in the DAG UI and select the counter of interest in the Column Selector settings on the top right corner. If there are thousands of tasks and UI is slow, you can also query starling by going to Axonite Blue Hue, choosing the database as starling.

.. code-block:: sql

  select task_attempt_id,
         CAST(counters['org.apache.tez.common.counters.TaskCounter']['PHYSICAL_MEMORY_BYTES'] as bigint) as memory
  from starling_vertex_task_attempt_counters
  where dt == '2016_05_22' and
        grid == 'PT' and
        app_id = 'application_1459233834927_12719048'
  ORDER BY memory desc;


Specifying the Number of Mappers
=================================

#. If you have files of size greater than HDFS block size (128 MB on our clusters), use ``mapred.min.split.size``
   The number of mappers is controlled by the number of splits. If your input is split into 2500 pieces, you’ll get 2500 mappers. If you want to reduce the number of mappers, use “mapred.min.split.size=X”, where X is the minimum number of bytes that should be in one split. You can also set this large enough that files will be sent whole to the mappers, which will mean that you will get the same number of mappers as you have inputs.

   If you have more than 50 input files, it will not be possible to reduce your mapper count to 50 (unless you go outside the Hadoop framework and have your mappers pull multiple files from the HDFS directly).

#. If you have files of size less than HDFS block size (128 MB on our clusters), use ``CombineFileInputFormat``.
   The latter packs more than 1 file into a split making sure a single mapper gets to operate on more than one file.
   Also it is intelligent enough to pack files keeping in mind the “data locality” factor.

   So it makes a best effort at combining files together that would have maximum blocks local.
   ``CombineFileInputFormat`` is an abstract class so you will have to do a bit more work for ``SequenceFiles``.
   The primary benefit of ``CombineFileInputFormat`` is that it decouples the amount of data a mapper consumes from HDFS block size.



Example of someone’s class that extends CombineFileInputFormat: http://svn.corp.yahoo.com/view/yahoo/platform/pepper/trunk/log-aggregator-lib/src/main/java/com/yahoo/pepper/log/aggregator/hadoop/CustomCombineFileInputFormat.java?view=markup

Map/Reduce Job with Side-Effects and Speculative Execution
==================================================================

I have a map/reduce job, and both the map and reduce have side-effects. I also want to set speculative execution on for my job.

**Use case**: I write out debug, performance, and exception files during the map phase and the reduce phase of the job. I call 3rd party library code in the map/reduce, and hence these stats are very useful. I am trying to create special output files (side-effect files) on HDFS for both the maps and reduces.

**Investigation**:I looked at this faq which mentions the gotchas for speculative execution & map-reduce job with side-effects (http://wiki.apache.org/hadoop/FAQ#A9).


It seems that the framework provides support in 2 cases:

  #. reduce phase of a map-reduce job
  #. map phase of a map-only job.

But there seems to be no support for the case:c) Map phase in a map-reduce job?

**Ans**: In a map-task of a map/reduce job with speculative execution enabled, you should create the side files in the current working directory of the task. The current working directory can be obtained via API –``FileOutputFormat.getWorkOutputPath(jobConf)``. These side files should be moved to the desired location in the ``close()`` method of your mapper. This ensures that only the side-files from successful task attempt are stored in the desired location.

Some tasks fail but job succeeds?
============================================

In Hadoop 0.20, if a job has its status set to 'SUCCEEDED', but some of the map tasks are listed as 'FAILED', does that mean that the 'FAILED' map tasks were successfully re-executed?

**Ans**: YES

When does the reducer phase start?
============================================

The documentation states that when all mappers are done the reducers start. However, when I run the program, the status on the console shows a few mappers then reducer then some lines for mappers.

**Ans**: Reducers begin copying the data as soon as maps dump it to disk. A map may dump partial results before it completes, and some maps finish before others. So in that sense reducers begin before the map phase completes. But since reducers first do a merge on all the data, they cannot truly start processing (that is, your reduce function is not envoked) until all map processes have finished and their data has been sorted and copied to the reducer.

How are the binary files split by Hadoop programmatically?
==================================================================

**Q**: For a binary file, what kinds of metadata is stored to manage the sequence of the file blocks? Also, how is the split different from text files?

**Ans**: HDFS stores files in blocks, like any other file system. It has no notion of types of files. How data is provided to the maps is done using Hadoop's ``InputSplit``. Often these are written to give a single block to a single map, but this is not required. Handling the splitting of records across block boundaries is the responsibility of the InputSplit.

**Q**: The documentation says that a special sort of marker is used to define the boundary of split. However it does not say more about that marker. Do you have any idea about it?

**Ans**: As I understand it, what they do in ``SequenceFile`` is every so many records they write a sync marker. That way, when an ``InputSplit`` starts in the middle of a record (which in general it will) they can skip to the sync marker and then start reading records. When an ``InputSplit`` comes to the end of its split, it keeping reading past the end until it hits a sync marker.
This is exactly what ``TextRecordReader`` does, except it uses ``\n`` as a sync marker.
Since you can't use any single byte as a sync marker in binary data, it uses a longer string of many bytes that hopefully would not be in the data itself.

How to Keep Jobs Running Even Though Some Tasks Fail
=======================================================

If you want your job to continue running even though some tasks fail (e.g. invalid input records), you can set mapred.max.failures.percent in ``jobconf.xml`` to a low value, from 5% to 10%:

``mapred.max.failures.percent = 5``


How to Handle Very Long Lines of Text
============================================

In Hadoop 0.18 and Hadoop 0.20 there is a config knob for the TextInputFormat that allows you to limit the length of lines returned. In particular, if you set ``mapred.linerecordreader.maxlength`` to 1000000, all of the lines will be at most 1 million characters long with the rest of the line discarded. This can help protect you from an occasional missing newline without the complexities of bad record.


.. todo:: find page Skipping Bad Records

In Hadoop 0.20 you can skip the line. See `Skipping Bad Records <http://twiki.corp.yahoo.com:8080/?url=http%3A%2F%2Fhadoop.apache.org%2Fcore%2Fdocs%2Fr0.20.0%2Fmapred_tutorial.html%23Skipping%2BBad%2BRecords&SIG=123f8udue>`_

Performance tuning guidelines for Map/Reduce jobs
=======================================================

.. todo:: find page MapRedPerfTuningReferenceDocument

Document for performance analysis of Map/Reduce job : `MapRedPerfTuningReferenceDocument <https://twiki.corp.yahoo.com/view/Grid/MapRedPerfTuningReferenceDocument>`_

Data Join Using Map/Reduce
=================================

.. todo:: find page DataJoinUsingMapReduce

Is the join program described in `DataJoinUsingMapReduce <https://twiki.corp.yahoo.com/view/Grid/DataJoinUsingMapReduce>`_ generic for joining any two text files, or is it ULT specific? If it's generic, could the description of it be made generic?



See hadoop datajoin utility. $HADOOP_HOME/src/contrib/data_join

How to separate output into multiple directories
=======================================================

Occasionally there is a need to separate the output of a map-reduce job into multiple directories instead of a single directory. These directories represent the type or class of data they contain which is very case specific. For e.x. Assuming that we are processing data using ``TextInputFormat`` from 3 domains - news, sports, finance and would like the final output to be present in the following directory structure -

* ``/my/home/ouput/news/*``,
* ``/my/home/ouput/sports/*``,
* ``/my/home/ouput/finance/*``, following steps should be followed:-


#. Prefix the type name of the (key,value) followed by a separator of your choice (like '\t') to the 'key' in the map or reduce phase.

  .. code-block:: java

      enum Domain { news, sports, finance };
      public void map(WritableComparable lineNumber, Text line, OutputCollector output, Reporter reporter) {
      String inputLine = line.toString();
      String key = readKey(inputLine);
      String value = readValue(inputLine);
      Domain domain = findDomain(key,value);
      key = domain.toString() + "\t" + key;
      ...
      output.collect(new Text(key), new Text(value));
    }

#. Extend `MultipleTextOutputFormat` and override the APIs as follow.
   Note:- The APIs below are called in MultipleOutputFormat in the order as they appear below. The example below is for Text, Text output.

  .. code-block:: java

    class MyMultipleOutputText extends MultipleTextOutputFormat<Text,Text> {
      /**
        * Read the key/value prefix information here to generate the path for the output.
        * @param key
        *           Input key to the record writer's write() call.
        * @param v
        *           Input value to the record writer's write() call.
        * @param name
        *           Default output path of the form "part-000*".
        * @return
        *           The output file name deduced from key/value
        */

      public String generateFileNameForKeyValue(Text key, Text v, String name) {
        /*
         * default value for "name" that we receive is of the form "part-000*"
         * split the default name (for e.x. part-00000 into ['part', '00000'] )
         */
        String[] nameparts = name.split("-");
        String keyStr = key.toString();
        int typeNameIdx = keyStr.indexOf("\t");
        // get the file name
        name = keyStr.substring(0, typeNameIdx);
        /*
         * return the path of the form 'fileName/fileName-0000'
         * This makes sure that fileName dir is created under job's output dir
         * and all the keys with that prefix go into reducer-specific files under
         * that dir.
         */
        return new Path(name, name + "-" + nameparts[1]).toString();
      }

      /*
       * Strip the type information here to generate the actual key.
       */
        public Text generateActualKey(Text key, Text value) {
        String keyStr = key.toString();
        int idx = keyStr.indexOf("\t") + 1;
        return new Text(keyStr.substring(idx));
      }

     /**
       * This API is called in the end in the MultipleOutputFormat.getRecordWriter().write() call to get a
       * writer corresponding to the path deduced from the previous APIs. The writer is cached so any subsequent
       * write() calls resulting in previously seen path gets the writer from cache.
       * Any job configuration specific initialization should be done here.
       * @param fs
       *          the file system to use
       * @param job
       *          a job conf object
       * @param name
       *          the name of the file over which a record writer object will be
       *          constructed
       * @param reporter
       *          a progressable object
       * @return A RecordWriter object over the given file
       * @throws IOException
       */
       public RecordWriter<Text,Text> getBaseRecordWriter(FileSystem fs, JobConf job,
           String name, Progressable reporter) throws IOException {
           // Initialization code here
           super.getBaseRecordWriter(fs, job, name, reporter);
       }
    }

#. Set the output format in jobConf

  .. code-block:: java

    jobConf.setOutputFormat(MyMultipleOutputText.class);


What is the maximum value one can use for ``mapred.task.timeout``?
==================================================================

Maximum would be 0. That would disable the timeout.

How to run hadoop programs on the gateway machines
=======================================================

Running map-red jobs on the gateways, is now an expert option. (This decision was made because several users had unintentionally run their full-fledged jobs on the gateway machines in the past, when they really wanted to run the jobs on the grid instead.)

Here is how you can test your jobs (on a much smaller dataset, please) on the gateway machine itself:

  .. code-block:: bash

    $ mkdir ~/local
    $ cat > ~/local/hadoop-site.xml
    <configuration>
    <property>
     <name>fs.default.name</name>
     <value>mithrilgold-nn1.inktomisearch.com:8020</value>
     <final>true</final>
     <description>NameNode</description>
    </property>
    <property>
      <name>mapred.job.tracker</name>
      <value>local</value>
      <final>true</final>
      <description>JobTracker</description>
    </property>
    </configuration>
    ^D
    $ export HADOOP_CONF_DIR=~/local
    $ hadoop jar ....

If you want to use the input files from the local file system instead of DFS, you will need to change value for ``fs.default.name`` to ``file:///``

How to authenticate and run jobs across the Grid (Proxy)
========================================================

.. note:: this content has replaced the original legacy `Grid Invoke Web Request Via Proxy <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Sandbox/GridInvokeWebRequestViaProxy.html>`_


HTTP Proxy
----------

Grid `HttpProxy` allows user jobs/processes running on ygrid to access services outside of ygrid.
There are two types of HttpProxy:

* Internal HttpProxy allows access to internal services in `.yahoo.com domain`.
* External HttpProxy allows access to external services. Athenz authentication with External `HttpProxy` is required for accessing external services.

For more information see the `HTTP Proxy confluence page <https://confluence.vzbuilders.com/display/HPROX/HTTP+Proxy>`_



HDFS Proxy
----------

`HDFSProxy` provides authenticated encrypted access to a particular cluster's Hadoop file system (HDFS) from *outside a cluster*:
e.g. cross-colo, or when trying to access HDFS outside of the grid backplane. (Within the grid backplane, use HDFS).

For more information see the `HDFSProxy confluence page <https://confluence.vzbuilders.com/display/HPROX/HDFS+Proxy>`_


HTTPFS Proxy
------------

* `Httpfs Proxy Dev Guide <https://confluence.vzbuilders.com/display/HPROX/Httpfs+Proxy+Dev+Guide>`_
* `Configuring Oath HttpFS for CertificateBased Auth <https://docs.google.com/document/d/1mjLerhHZeiOLChNyP33yZDsCB6AC8X6geqLbjrlxi00>`_


How to find out what input file your mapper task is operating on
=================================================================

Look at the task's environment variable ``map_input_file``. Some other useful environment variables are ``map_input_length``, ``map_input_start`` and ``mapred_task_id`` or ``mapred_tip_id`` (the first identifies the attempt, the second the task being attempted.
More are described in: Configured Parameters in the MapReduce documentation.

For a full list, try running:

  .. code-block:: bash

    hadoop jar $HADOOP_HOME/hadoop-streaming.jar \
           -Dmapred.job.queue.name=unfunded -input x-in -output x_out \
           -mapper 'sh -c "printenv"' -reducer cat


where x_in is any dummy input file in HDFS and ``x_out/part-00000`` will contain the results from the ``printenv`` command.

Hadoop Job History API
======================

Job History API provides users an API for retrieving Job History logs in raw format.

Job History logs can be retrieved from job tracker (before job tracker is restarted) through the following HTTP/GET request.

``http://<<jobtracker-weburl:webport>>/gethistory.jsp?jobid=<<jobid>>``

Ex. ``http://cobaltblue-jt1.blue.ygrid.yahoo.com:50030/gethistory.jsp?jobid=job_201101201556_48716``

This request has to be made with a valid YBY cookie of a user who has view-acls enabled for this job.



The above request is redirected to a dedicated history server
  
  .. code-block:: bash
  
    @url http://<<history-server:historyport>>/historyfile?logFile=<<encoded-job-log-file-in-mapred-history-done>>

, which would stream the contents of the history file (as in ``/mapred/history/done``).

The history contents are returned in raw format (which is JSON format) and hence should allow automation tools to be able to process the history files.

Job History Servlet
======================

Job History Servlet is responsible for serving Job History logs. This servlet is hosted in tomcat container running on JT node in its own process space. Because of running in a separate process space, it is easier to throttle the load that History requests can put on JobTracker node. Overwhelming number of History log retrieval requests will not affect JT.

From Anti-patterns section in `apache_hadoop_best_practices_a <http://developer.yahoo.com/blogs/hadoop/posts/2010/08/apache_hadoop_best_practices_a/...It/>`_ is hightly discouraged to have applications doing screen scraping of JobTracker web-ui for status of queues/jobs or worse, job-history of completed jobs...

With Job History Servlet, you should be able read history files from an Automated process without putting load on Job Tracker.

When ApplicationMaster? is hitting "Split metadata size exceeded beyond 10000000" or OutOfMemory? error
==============================================================================================================

* If there are too many mappers (in the order of thousands) and you actually don't need them (having way too many mappers is actually bad for performance), try reducing the number of total mapper tasks by having bigger splits by setting:
  ``-Dmapreduce.input.fileinputformat.split.minsize=536870912`` (512M or any higher value, default to block size which is 128M on our grid. For ABF feeds 1G or 2G is good).

* If the file sizes are small, say 128MB, setting s``plit.minsize`` to a higher value like 1G does not help.
  In that case, you can try to combine splits using CombinedInputFormat or if you're using pig, you can set:
  ``-Dpig.maxCombinedSplitSize=536870912`` or higher.
  Use that in conjunction with ``split.minsize setting``.

* If first two options does not work for you and it is hitting "Split metadata size exceeded beyond 10000000" error, please try bumping up AM meta info size by setting ``-Dmapreduce.job.split.metainfo.maxsize=___`` to higher value (default is 10,000,000)

* If first two options does not work for you and if Application Master is hitting OOM due to too many tasks, please try bumping up heapsize of the application master by

    .. code-block:: bash

      #default is 1024m and you can go up to 3584 
      -Dyarn.app.mapreduce.am.command-opts= \
          'Xmx2048m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true' \
      #default 1536, Max - 4096. Rule of thumb - 512MB higher than the Xmx value
      -Dyarn.app.mapreduce.am.resource.mb=2560


Running tasks on 64bit jvm
===========================

We now have an option to run mapreduce tasks on 64bit jvm. (32bit jvm is the default)
Please set the following for map task, reduce task and ``ApplicationMaster`` respectively

  .. code-block:: bash
   
    -Dmapreduce.map.env="JAVA_HOME=/home/gs/java/jdk64/current”
    -Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java/jdk64/current”
    -Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java/jdk64/current”
    -Dmapreduce.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
    -Dyarn.app.mapreduce.am.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64

* Maximum container size will remain at 8G for the time being
* Note: this uses the current symlink to the 64bit jvm, if you explicitly want Java7 or Java8 please see the next three topics


Explicitly running tasks on 32bit Java7 jvm (gridjdk-1.7.0_17)
==============================================================

If the default Java has been moved to 64-bit and you temporarily need to move back to 32-bit Java:

* export ``JAVA_HOME="/home/gs/java7/jdk32"``; and
* set the following parameters for your job's AM, mappers and reducers:

  .. code-block:: bash
   
    -Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java7/jdk32"
    -Dmapreduce.map.env="JAVA_HOME=/home/gs/java7/jdk32"
    -Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java7/jdk32"
    -Dmapreduce.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-i386-32"
    -Dyarn.app.mapreduce.am.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-i386-32"

Hadoop 2.7 will only support 64-bit Java and JDK8, so this is a temporary solution. Applications must be migrated to 64-bit Java and JDK8.

Explicitly running tasks on 64bit Java7 jvm (gridjdk64-1.7.0_17)
================================================================

* export ``JAVA_HOME="/home/gs/java7/jdk64"``
* And set the following parameters for your job's AM, mappers and reducers:

  .. code-block:: bash

    -Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java7/jdk64"
    -Dmapreduce.map.env="JAVA_HOME=/home/gs/java7/jdk64"
    -Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java7/jdk64"
    -Dmapreduce.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
    -Dyarn.app.mapreduce.am.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"

Example job use

  .. code-block:: bash

    hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.8.1503021851.jar  wordcount
      -Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java7/jdk64"
      -Dmapreduce.map.env="JAVA_HOME=/home/gs/java7/jdk64"
      -Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java7/jdk64"
      -Dmapreduce.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
      -Dyarn.app.mapreduce.am.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
      /user/patwhite/PigTez/usersales/  /user/patwhite/data/out_java7_64

Explicitly running tasks on 64bit Java8 jvm (yjava_jdk-1.8.0_25 64-bit)
=======================================================================

* export ``JAVA_HOME="/home/gs/java8/jdk64"``
* And set the following parameters for your job's AM, mappers and reducers:

    .. code-block:: bash

      -Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java8/jdk64"
      -Dmapreduce.map.env="JAVA_HOME=/home/gs/java8/jdk64"
      -Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java8/jdk64"
      -Dmapreduce.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
      -Dyarn.app.mapreduce.am.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"


Example job use

 .. code-block:: bash

     hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.8.1503021851.jar  wordcount
        -Dyarn.app.mapreduce.am.env="JAVA_HOME=/home/gs/java8/jdk64"
        -Dmapreduce.map.env="JAVA_HOME=/home/gs/java8/jdk64"
        -Dmapreduce.reduce.env="JAVA_HOME=/home/gs/java8/jdk64"
        -Dmapreduce.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
        -Dyarn.app.mapreduce.am.admin.user.env="LD_LIBRARY_PATH=/home/gs/hadoop/current/lib/native/Linux-amd64-64"
        /user/patwhite/PigTez/usersales/  /user/patwhite/data/out_java8_64