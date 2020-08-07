.. _mapreduce_streaming:

*********
Streaming
*********

.. admonition:: Readings...
   :class: readingbox

   .. include:: /common/mapreduce/streaming-reading-resources.rst

Although the Hadoop framework is implemented in Java, MapReduce applications need not be written in Java.

.. glossary::

   Hadoop Streaming (:hadoop_rel_doc:`api package <api/org/apache/hadoop/streaming/package-summary.html>`)
     It is a utility which allows users to create and run jobs with any executables (e.g. shell utilities) as the mapper and/or the reducer.
   
   Hadoop Pipes (:hadoop_rel_doc:`api package <api/org/apache/hadoop/mapred/pipes/package-summary.html>`)
     is a SWIG-compatible C++ API to implement MapReduce applications (non JNI based).



Hadoop streaming is a utility that comes with the Hadoop distribution. The utility allows you to create and run Map/Reduce jobs with any executable or script as the mapper and/or the reducer. For example:

  .. parsed-literal::

     hadoop jar hadoop-streaming-|HADOOP_RELEASE_VERSION|.jar
           -input myInputDirs
           -output myOutputDir
           -mapper /bin/cat
           -reducer /usr/bin/wc

In the above example, both the mapper and the reducer are executables that read the input from stdin (line by line) and emit the output to stdout. The utility will create a Map/Reduce job, submit the job to an appropriate cluster, and monitor the progress of the job until it completes.

When an executable is specified for mappers, each mapper task will launch the executable as a separate process when the mapper is initialized. As the mapper task runs, it converts its inputs into lines and feed the lines to the stdin of the process. In the meantime, the mapper collects the line oriented outputs from the stdout of the process and converts each line into a key/value pair, which is collected as the output of the mapper. By default, the prefix of a line up to the first tab character is the key and the rest of the line (excluding the tab character) will be the value. If there is no tab character in the line, then entire line is considered as key and the value is null. However, this can be customized by setting ``-inputformat`` command option

When an executable is specified for reducers, each reducer task will launch the executable as a separate process then the reducer is initialized. As the reducer task runs, it converts its input key/values pairs into lines and feeds the lines to the stdin of the process. In the meantime, the reducer collects the line oriented outputs from the stdout of the process, converts each line into a key/value pair, which is collected as the output of the reducer. By default, the prefix of a line up to the first tab character is the key and the rest of the line (excluding the tab character) is the value. However, this can be customized by setting ``-outputformat`` command option

User can specify ``stream.non.zero.exit.is.failure`` as true or false to make a streaming task that exits with a non-zero status to be `Failure` or `Success` respectively. By default, streaming tasks exiting with non-zero status are considered to be failed tasks.

Streaming supports streaming command options as well as generic command options. The general command line syntax is shown below.

  .. code-block:: bash

    hadoop command [genericOptions] [streamingOptions]

.. table:: `Hadoop streaming command options`
  :widths: auto

  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  |                  Parameter                  |                                                                          Description                                                                         | Optional |
  +=============================================+==============================================================================================================================================================+==========+
  | ``-input`` directory or file                | Input location for mapper                                                                                                                                    | Required |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-output`` directory                       | Output location for reducer                                                                                                                                  | Required |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-mapper`` executable or JavaClass         | Mapper executable. If not specified, `IdentityMapper` is used as the default                                                                                 | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-reducer`` executable or JavaClass        | Reducer executable. If not specified, `IdentityReducer` is used as the default                                                                               | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-file`` filename                          | Make the mapper, reducer, or combiner executable available locally on the compute nodes                                                                      | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-inputformat`` JavaClass                  | Class you supply should return key/value pairs of Text class. If not specified, `TextInputFormat` is used as the default                                     | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-outputformat`` JavaClass                 | Class you supply should take key/value pairs of Text class. If not specified, `TextOutputformat` is used as the default                                      | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-partitioner`` JavaClass                  | Class that determines which reduce a key is sent to                                                                                                          | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-combiner`` command or JavaClass          | Combiner executable for map output                                                                                                                           | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-cmdenv`` name=value                      | Pass environment variable to streaming commands                                                                                                              | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-inputreader``                            | For backwards-compatibility: specifies a record reader class (instead of an input format class)                                                              | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-verbose``                                | Verbose output                                                                                                                                               | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-lazyOutput``                             | Create output lazily. For example, if the output format is based on `FileOutputFormat`, the output file is created only on the first call to `Context.write` | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-numReduceTasks``                         | Specify the number of reducers                                                                                                                               | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-mapdebug``                               | Script to call when map task fails                                                                                                                           | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+
  | ``-reducedebug``                            | Script to call when reduce task fails                                                                                                                        | Optional |
  +---------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------+----------+


.. table:: `Hadoop generic command options used with Streaming`
  :widths: auto

  +------------------------------+---------------------------------------------------------------------------+----------+
  |           Parameter          |                                Description                                | Optional |
  +==============================+===========================================================================+==========+
  | ``-conf`` configuration-file | Specify an application configuration file                                 | Optional |
  +------------------------------+---------------------------------------------------------------------------+----------+
  | ``-D`` property=value        | Use value for given property                                              | Optional |
  +------------------------------+---------------------------------------------------------------------------+----------+
  | ``-fs`` `host:port` or local | Specify a namenode                                                        | Optional |
  +------------------------------+---------------------------------------------------------------------------+----------+
  | ``-files``                   | Specify comma-separated files to be copied to the Map/Reduce cluster      | Optional |
  +------------------------------+---------------------------------------------------------------------------+----------+
  | ``-libjars``                 | Specify comma-separated jar files to include in the classpath             | Optional |
  +------------------------------+---------------------------------------------------------------------------+----------+
  | ``-archives``                | Specify comma-separated archives to be unarchived on the compute machines | Optional |
  +------------------------------+---------------------------------------------------------------------------+----------+


.. admonition:: Related...
   :class: readingbox

   Check the FAQ section in :ref:`mapreduce_streaming_faq`
