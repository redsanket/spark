How do I provide my own input/output format with streaming?
===========================================================

You can specify your own custom class by packing them and putting the custom jar to ``$HADOOP_CLASSPATH``.

How do I get the Job variables in a streaming job's mapper/reducer?
===================================================================

See :hadoop_rel_doc:`Configured Parameters <hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Configured_Parameters>`. During the execution of a streaming job, the names of the `mapred` parameters are transformed. The dots ( . ) become underscores ( _ ). For example, ``mapreduce.job.id`` becomes ``mapreduce_job_id`` and ``mapreduce.job.jar`` becomes ``mapreduce_job_jar``. In your code, use the parameter names with the underscores.


How to use multiple jars to run my job?
=======================================

* Use ``-libjars`` to specify multiple jars on the command line.
* Set environment variable ``HADOOP_CLASSPATH`` to the jars for your jobs main class.


When would I use ``NLineInputFormat``?
======================================

**What’s the difference between ``TextInputFormat`` and ``NLineInputFormat``:**

.. glossary::

   :hadoop_rel_doc:`TextInputFormat <api/index.html?org/apache/hadoop/mapreduce/lib/input/TextInputFormat.html>`
     uses the default implementation of `getSplits()` inherited from `FileInputFormat`. It respects the number of splits determined by the framework based on the size of your input data and the number of nodes available. You can’t control the number of map tasks to spawn.
   
   :hadoop_rel_doc:`NLineInputFormat <api/index.html?org/apache/hadoop/mapreduce/lib/input/NLineInputFormat.html>`
     It overrides `getSplits()` such that every single line in your input file(s) becomes an InputSplit. Thus the number of map tasks spawned will be exactly the number of lines in your input file(s). And each map task is fed with a single line. |br| 
     In many "pleasantly" parallel applications, each process/mapper processes the same input file(s), but with computations are controlled by different parameters.(Referred to as "parameter sweeps"). One way to achieve this, is to specify a set of parameters (one set per line) as input in a control file (which is the input path to the map-reduce application, where as the input dataset is specified via a config variable in JobConf.). The `NLineInputFormat` can be used in such applications, that splits the input file such that by default, one line is fed as a value to one map task, and key is the offset. i.e. (k,v) is (`LongWritable`, `Text`). The location hints will span the whole mapred cluster.

Both inputformat classes use ``LineRecordReader`` to read a line into a <K,V> pair. The byte offset of the first character of that line is to be used as the key. And the rest of the line is to be used as the value. However Hadoop Streaming has made it a special case for ``TextInputFormat`` that it passes only the value part to your pipe program. For other inputformat classes, both key and value will be passed to your pipe program, delimited by `\t`.

**You would use NLineInputFormat**

Some examples from Hadoop the definitive guide:

#. In applications that take a small amount of input data and run an extensive (that is, CPU-intensive) 
computation for it, then emit their output.
#. You create a “seed” input file that lists the data sources, one per line. Then 
each mapper is allocated a single data source, and it loads the data from that source into HDFS.

How to debug streaming jobs?
============================

* User can specify ``stream.non.zero.exit.is.failure`` as true or false to make a streaming task that exits with a non-zero status to be `Failure` or `Success` respectively. By default, streaming tasks exiting with non-zero status are considered to be failed tasks. (See :ref:`Streaming Overview Section <mapreduce_streaming>`).
* Use ``-mapdebug`` and ``-reducedebug`` to configure a script to call when a task fails.
  
In streaming mode, a debug script can be submitted with the command-line options ``-mapdebug`` and ``-reducedebug``, for debugging map and reduce tasks respectively.
The arguments to the script are the task’s stdout, stderr, syslog and jobconf files. The debug command, run on the node where the MapReduce task failed, is:

  .. code-block:: bash

    $script $stdout $stderr $syslog $jobconf
