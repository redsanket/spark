
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

See hadoop datajoin utility. ``$HADOOP_HOME/src/contrib/data_join``