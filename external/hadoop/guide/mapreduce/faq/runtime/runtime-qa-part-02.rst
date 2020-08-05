
How to give your tasks more memory
==================================

There are a number of mapreduce options that control how much memory your tasks can access. Users/admins can also specify the maximum virtual memory of the launched child-task, and any sub-process it launches recursively.
The value ``mapreduce.{map|reduce}.memory.mb`` should be specified in mega bytes (MB). Note the following:

* This is a *per-process* limit
* The value must be greater than or equal to the ``-Xmx`` passed to JavaVM, else the VM might not start.
  


The following are the mapreduce configurations option for mapreduce

  .. code-block:: bash

    mapreduce.{CONFIGURATION_OPTION}


:guilabel:`Map Parameters`

+--------------------------------------+-------+--------------------------------------------------------------------------------------------------------------------------------+
|                 Name                 |  Type |                                                           Description                                                          |
+======================================+=======+================================================================================================================================+
| ``task.io.sort.mb``                  | int   | The cumulative size of the serialization and accounting buffers storing records emitted from the map, in megabytes.            |
+--------------------------------------+-------+--------------------------------------------------------------------------------------------------------------------------------+
| ``map.sort.spill.percent``           | float | The soft limit in the serialization buffer. Once reached, a thread will begin to spill the contents to disk in the background. |
+--------------------------------------+-------+--------------------------------------------------------------------------------------------------------------------------------+

:guilabel:`Shuffle/Reduce Parameters`


+---------------------------------------------------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|                        Name                       |  Type |                                                                                                                                                                                                                                              Description                                                                                                                                                                                                                                             |
+===================================================+=======+======================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================+
| ``task.io.soft.factor``                           | int   | Specifies the number of segments on disk to be merged at the same time. It limits the number of open files and compression codecs during merge. If the number of files exceeds this limit, the merge will proceed in several passes. Though this limit also applies to the map, most jobs should be configured so that hitting this limit is unlikely there.                                                                                                                                         |
+---------------------------------------------------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``reduce.merge.inmem.thresholds``                 | int   | The number of sorted map outputs fetched into memory before being merged to disk. Like the spill thresholds in the preceding note, this is not defining a unit of partition, but a trigger. In practice, this is usually set very high (1000) or disabled (0), since merging in-memory segments is often less expensive than merging from disk (see notes following this table). This threshold influences only the frequency of in-memory merges during the shuffle.                                |
+---------------------------------------------------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``reduce.shuffle.merge.percent``                  | float | The memory threshold for fetched map outputs before an in-memory merge is started, expressed as a percentage of memory allocated to storing map outputs in memory. Since map outputs that can’t fit in memory can be stalled, setting this high may decrease parallelism between the fetch and merge. Conversely, values as high as 1.0 have been effective for reduces whose input can fit entirely in memory. This parameter influences only the frequency of in-memory merges during the shuffle. |
+---------------------------------------------------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``reduce.shuffle.input.buffer.percent``           | float | The percentage of memory- relative to the maximum heapsize as typically specified in ``mapreduce.reduce.java.opts`` - that can be allocated to storing map outputs during the shuffle. Though some memory should be set aside for the framework, in general it is advantageous to set this high enough to store large and numerous map outputs.                                                                                                                                                      |
+---------------------------------------------------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``reduce.input.buffer.percent``                   | float | The percentage of memory relative to the maximum heapsize in which map outputs may be retained during the reduce. When the reduce begins, map outputs will be merged to disk until those that remain are under the resource limit this defines. By default, all map outputs are merged to disk before the reduce begins to maximize the memory available to the reduce. For less memory-intensive reduces, this should be increased to avoid trips to disk.                                          |
+---------------------------------------------------+-------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


.. note:: ``mapreduce.{map|reduce}.java.opts`` are used only for configuring the launched child tasks from ``MRAppMaster``. Configuring the memory options for daemons is documented in :hadoop_rel_doc:`Configuring the Environment of the Hadoop Daemons<hadoop-project-dist/hadoop-common/ClusterSetup.html#Configuring_Environment_of_Hadoop_Daemons>`.

First, ``mapreduce.map.memory.mb`` controls how much memory space is allocated to your map tasks (Physical memory).
The physical memory configured for your job must fall within the minimum and maximum memory allowed for containers in your cluster. The maximum value is set as YARN confoguration parameter in the ``mapred-site.xml``, dubbed ``yarn.scheduler.maximum-allocation-mb`` , which is currently set to ``20480MB`` (``131072`` for AxoniteBlue) and not over-writable. See also ``yarn.scheduler.minimum-allocation-mb``.

Next you need to configure the JVM heap size for your map and reduce processes. These sizes need to be less than the physical memory you configured 
ou can do this for both mapper and reducer with  ``mapreduce.{map|reduce}.java.opts``.  For example, to set the mapper heap size to 2GB,
use ``-Dmapreduce.map.java.opts=-Xmx2048m``. Other ``java -X`` options let you the initial heap size & stack size.

By default ``512MB`` is left for native memory. We recommend users maintain the same ratio while increasing memory unless more native memory is required with JNI. So in general, ``mapreduce.{map|reduce}.memory.mb`` should be equal to sum of ``-Xmx`` specified in ``mapreduce.{map|reduce}.java.opts`` and 512 MB. If you set ``mapreduce.{map|reduce}.memory.mb`` to 4096 but only have ``-Xmx1536M`` in your ``mapreduce.{map|reduce}.java.opts``, then you are wasting 2G of memory.
Always ensure that you increase both ``memory.mb`` and ``java.opts`` together and the difference between them is 512MB.
You can check the actual Physical memory usage of the tasks in the Counters page of the job in the UI (:menuselection:`cluster/scheduler --> Tracking UI --> select Application[ID] --> Job --> Counters`) and tune (increase or reduce) the memory further based on actual usage. If the Counters page shows that there are lot of spill (:menuselection:`Map-Reduce Framework --> Spilled Records`), then increase ``mapreduce.task.io.sort.mb`` to 512 or 768. Default is 256. Reducing spill will also speed up the job.

.. note:: Reducers usually require more memory and increasing for both map and reducers will waste memory. So, you may need to set ``mapreduce.reduce.java.opts`` slightly higher than ``mapreduce.map.java.opts`` in order to save memory.


For Tez, you can go to "All Tasks" in the DAG UI and select the counter of interest in the Column Selector settings on the top right corner. If there are thousands of tasks and UI is slow, you can also query starling by going to Axonite Blue Hue, choosing the database as starling.

.. code-block:: sql

  select task_attempt_id,
         CAST(counters['org.apache.tez.common.counters.TaskCounter']['PHYSICAL_MEMORY_BYTES'] as bigint) as memory
  from starling_vertex_task_attempt_counters
  where dt == '2016_05_22' and
        grid == 'PT' and
        app_id = 'application_1459233834927_12719048'
  ORDER BY memory desc;

How are the binary files split by Hadoop programmatically?
==========================================================

**Q**: For a binary file, what kinds of metadata is stored to manage the sequence of the file blocks? Also, how is the split different from text files?

**Ans**: HDFS stores files in blocks, like any other file system. It has no notion of types of files. How data is provided to the maps is done using Hadoop's ``InputSplit``. Often these are written to give a single block to a single map, but this is not required. Handling the splitting of records across block boundaries is the responsibility of the InputSplit.

**Q**: The documentation says that a special sort of marker is used to define the boundary of split. However it does not say more about that marker. Do you have any idea about it?

**Ans**: As I understand it, what they do in ``SequenceFile`` is every so many records they write a sync marker. That way, when an ``InputSplit`` starts in the middle of a record (which in general it will) they can skip to the sync marker and then start reading records. When an ``InputSplit`` comes to the end of its split, it keeping reading past the end until it hits a sync marker.
This is exactly what ``TextRecordReader`` does, except it uses ``\n`` as a sync marker.
Since you can't use any single byte as a sync marker in binary data, it uses a longer string of many bytes that hopefully would not be in the data itself.

Specifying the Number of Mappers
=================================

#. The MapReduce framework relies on the :hadoop_rel_doc:`InputFormat <api/org/apache/hadoop/mapreduce/InputFormat.html>` of the job to:

   * Validate the input-specification of the job.
   * Split-up the input file(s) into logical ``InputSplit`` instances, each of which is then assigned to an individual Mapper.
   * Provide the ``RecordReader`` implementation used to glean input records from the logical ``InputSplit`` for processing by the Mapper.
  
#. The default behavior of file-based ``InputFormat`` implementations, typically sub-classes of FileInputFormat, is to split the input into logical ``InputSplit`` instances based on the total size, in bytes, of the input files.
#. If you have files of size greater than HDFS block size (128 MB on our clusters), use the split size of the input to control that. The number of mappers is controlled by the number of splits. If your input is split into 2500 pieces, you’ll get 2500 mappers. The default behavior of file-based ``InputFormat`` implementations, typically sub-classes of :hadoop_rel_doc:`FileInputFormat <api/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.html>`, is to split the input into logical ``InputSplit`` instances based on the total size, in bytes, of the input files. However, the FileSystem blocksize of the input files is treated as an upper bound for input splits. A lower bound on the split size can be set via ``mapreduce.input.fileinputformat.split.minsize``. You can also set this large enough that files will be sent whole to the mappers, which means that you will get the same number of mappers as you have inputs.
   
   .. code-block:: bash

      mapreduce.input.fileinputformat.split.minsize=X
      # where X is the minimum number of bytes that should be in one split.

#. If logical splits based on input-size is insufficient for many applications since record boundaries must be respected. In such cases, the application should implement a :hadoop_rel_doc:`RecordReader <api/org/apache/hadoop/mapreduce/RecordReader.html>`, that is responsible for respecting record-boundaries and presents a record-oriented view of the logical ``InputSplit`` to the individual task.

#. If you have files of size less than HDFS block size (128 MB on our clusters), use :hadoop_rel_doc:`CombineFileInputFormat <api/org/apache/hadoop/mapreduce/lib/input/CombineFileInputFormat.html>`. The latter packs more than 1 file into a split making sure a single mapper gets to operate on more than one file. Also it is intelligent enough to pack files keeping in mind the “data locality” factor. So it makes a best effort at combining files together that would have maximum blocks local. ``CombineFileInputFormat`` is an abstract class so you will have to do a bit more work for ``SequenceFiles``. The primary benefit of ``CombineFileInputFormat`` is that it decouples the amount of data a mapper consumes from HDFS block size.


How Can I use ``CombineFileInputFormat``?
=========================================

- Hadoop example code :hadoop_github_url:`MultiFileWordCount.java <hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/MultiFileWordCount.java>` shows a class that uses a custom implementation of ``CombineFileInputFormat``.
- There is a step-by-step online article `Process Small Files on Hadoop Using CombineFileInputFormat (1) <http://www.idryman.org/blog/2013/09/22/process-small-files-on-hadoop-using-combinefileinputformat-1/>`_


Map/Reduce Job with Side-Effects and Speculative Execution
==========================================================

I have a map/reduce job, and both the map and reduce have side-effects (i.e., component tasks need to create and/or write to side-files, which differ from the actual job-output files). I also want to set speculative execution on for my job.

**Use case**: I write out debug, performance, and exception files during the map phase and the reduce phase of the job. I call 3rd party library code in the map/reduce, and hence these stats are very useful. I am trying to create special output files (side-effect files) on HDFS for both the maps and reduces.

**Problem**: There could be issues with two instances of the same Mapper or Reducer running simultaneously (for example, speculative tasks) trying to open and/or write to the same file (path) on the `FileSystem`. 

**Ans**:

To avoid these issues the MapReduce framework, when the ``OutputCommitter`` is ``FileOutputCommitter``, maintains a special ``${mapreduce.output.fileoutputformat.outputdir}/_temporary/_${taskid}`` sub-directory accessible via ``${mapreduce.task.output.dir}`` for each task-attempt on the ``FileSystem`` where the output of the task-attempt is stored. On successful completion of the task-attempt, the files in the ``${mapreduce.output.fileoutputformat.outputdir}/_temporary/_${taskid}`` (only) are promoted to ``${mapreduce.output.fileoutputformat.outputdir}``. Of course, the framework discards the sub-directory of unsuccessful task-attempts. This process is completely transparent to the application.

The application-writer can take advantage of this feature by creating any side-files required in ``${mapreduce.task.output.dir}`` during execution of a task via :hadoop_rel_doc:`FileOutputFormat.getWorkOutputPath(Conext) <api/org/apache/hadoop/mapreduce/lib/output/FileOutputFormat.html>`, and the framework will promote them similarly for succesful task-attempts, thus eliminating the need to pick unique paths per task-attempt.


.. note:: The value of ``${mapreduce.task.output.dir}`` during execution of a particular task-attempt is actually ``${mapreduce.output.fileoutputformat.outputdir}/_temporary/_{$taskid}``, and this value is set by the MapReduce framework. So, just create any side-files in the path returned by ``FileOutputFormat.getWorkOutputPath(Conext)`` from MapReduce task to take advantage of this feature.
          The entire discussion holds true for maps of jobs with reducer=NONE (i.e. 0 reduces) since output of the map, in that case, goes directly to HDFS.


Some tasks fail but job succeeds?
=================================

If a job has its status set to `SUCCEEDED`, but some of the map tasks are listed as `FAILED`, does that mean that the `FAILED` map tasks were successfully re-executed?

**Ans**: YES

How to Keep Jobs Running Even Though Some Tasks Fail
====================================================

If you want your job to continue running even though some tasks fail (e.g. invalid input records), you can set maximum failures percent in ``jobconf.xml`` to a low value.

+--------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|             Configuration            |                                                                                                                                Description                                                                                                                               |
+======================================+==========================================================================================================================================================================================================================================================================+
| ``.job.maxtaskfailures.per.tracker`` | The number of task-failures on a node manager of a given job after which new tasks of that job aren't assigned to it. It MUST be less than mapreduce.map.maxattempts and mapreduce.reduce.maxattempts otherwise the failed task will never be tried on a different node. |
+--------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``.map.failures.maxpercent``         | the maximum percentage of map tasks that can fail without the job being aborted                                                                                                                                                                                          |
+--------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``.reduce.failures.maxpercent``      | the maximum percentage of reduce tasks that can fail without the job being aborted                                                                                                                                                                                       |
+--------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

When does the reducer phase start?
==================================

The documentation states that when all mappers are done the reducers start. However, when I run the program, the status on the console shows a few mappers then reducer then some lines for mappers.

**Ans**: Reducers begin copying the data as soon as maps dump it to disk. A map may dump partial results before it completes, and some maps finish before others. So in that sense reducers begin before the map phase completes. But since reducers first do a merge on all the data, they cannot truly start processing (that is, your reduce function is not envoked) until all map processes have finished and their data has been sorted and copied to the reducer.

To configure the reduce tasks to start after a percentage of map tasks are complete using the command line, add the following option `slowStart` option to your job submission command. For example to start reducers after 50% of the map jobs are completed:

  .. code-block:: bash

     # A value of 1.0 will wait for all the mappers to finish.
     mapreduce.job.reduce.slowstart.completedmaps=0.5
    


How to Handle Very Long Lines of Text
=====================================

If you are using text imput formats, you can set a config knob for the ``TextInputFormat`` that allows you to limit the length of lines returned.
This is recommended as a safeguard against corrupted files. Corruption in a file can manifest itself as a very long line, which can cause out-ofmemory errors and then task failure. By setting ``mapreduce.input.linerecordreader.line.maxlength`` to a value in bytes that fits in memory (and is comfortably greater than the length of lines in your input data), you ensure that the record reader will skip the (long) corrupt lines without the task failing. This can help protect you from an occasional missing newline without the complexities of bad record.

See :hadoop_rel_doc:`quick instructions to skip bad records <hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Reducer>`.


Performance tuning guidelines for Map/Reduce jobs
==================================================

.. todo:: find page MapRedPerfTuningReferenceDocument

Document for performance analysis of Map/Reduce job : `MapRedPerfTuningReferenceDocument <https://twiki.corp.yahoo.com/view/Grid/MapRedPerfTuningReferenceDocument>`_. Another `document available from AMD <https://developer.amd.com/wordpress/media/2012/10/Hadoop_Tuning_Guide-Version5.pdf>`_.

Data Join Using Map/Reduce
==========================

.. todo:: find page DataJoinUsingMapReduce

Is the join program described in `DataJoinUsingMapReduce <https://twiki.corp.yahoo.com/view/Grid/DataJoinUsingMapReduce>`_ generic for joining any two text files, or is it ULT specific? If it's generic, could the description of it be made generic?

See hadoop datajoin utility. ``$HADOOP_HOME/src/contrib/data_join``