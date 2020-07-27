How to separate output into multiple directories
=================================================

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