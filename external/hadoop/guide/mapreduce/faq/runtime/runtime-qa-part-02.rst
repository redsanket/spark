..  _mapreduce_faq_runtime_increase_job_memory:

How to give your tasks more memory
==================================

:ref:`yarn_memory` is a good source to learn about memory configurations. See :ref:`yarn_memory_important_configurations_tasks` to increase memory of the running program.


.. sidebar:: Related Readings...

    * :ref:`yarn_memory`
    * :ref:`yarn_memory_important_configurations_tasks`
    * :ref:`YARN memory Troubleshooting <yarn_troubleshooting_memory>`


In summary, For MapReduce running on YARN there are actually two memory settings you have to configure at the same time:

* The physical memory for your YARN map and reduce processes
* The JVM heap size for your map and reduce processes


Configure ``mapreduce.map.memory.mb`` and ``mapreduce.reduce.memory.mb`` to set the YARN container physical memory limits for your map and reduce processes respectively. For example, if you want to limit your map process to `2GB` and your reduce process to `4GB`, and you wanted that to be the default in your cluster, then you’d set the following in `mapred-site.xml`:

  .. code-block:: xml

      <property>
        <name>mapreduce.map.memory.mb</name>
        <value>2048</value>
      </property>
      <property>
        <name>mapreduce.reduce.memory.mb</name>
        <value>4096</value>
      </property>

The physical memory configured for your job must fall within the minimum and maximum memory allowed for containers in your cluster (check the ``yarn.scheduler.maximum-allocation-mb`` and ``yarn.scheduler.minimum-allocation-mb`` properties respectively).

Next you need to configure the JVM heap size for your map and reduce processes. These sizes need to be less than the physical memory you configured in the previous section. Configure ``mapreduce.map.java.opts`` and ``mapreduce.reduce.java.opts`` to set the map and reduce heap sizes respectively. 

  .. code-block:: xml

      <property>
        <name>mapreduce.map.java.opts</name>
        <value>-Xmx1638m</value>
      </property>
      <property>
        <name>mapreduce.reduce.java.opts</name>
        <value>-Xmx3278m</value>
      </property>

The above values are 80% of the physical memory 2GB and 4GB respectively.

Finally, to configure those properties for your job, pass the configurations to your command line launching the job.

  .. include:: /common/yarn/memory/yarn-memory-appmaster-conf.rst

How are the binary files split by Hadoop programmatically?
==========================================================

**Q**: For a binary file, what kinds of metadata is stored to manage the sequence of the file blocks? Also, how is the split different from text files?

**Ans**: HDFS stores files in blocks, like any other file system. It has no notion of types of files. How data is provided to the maps is done using Hadoop's ``InputSplit``. Often these are written to give a single block to a single map, but this is not required. Handling the splitting of records across block boundaries is the responsibility of the InputSplit.

**Q**: The documentation says that a special sort of marker is used to define the boundary of split. However it does not say more about that marker. Do you have any idea about it?

**Ans**: As I understand it, what they do in ``SequenceFile`` is every so many records they write a sync marker. That way, when an ``InputSplit`` starts in the middle of a record (which in general it will) they can skip to the sync marker and then start reading records. When an ``InputSplit`` comes to the end of its split, it keeping reading past the end until it hits a sync marker.
This is exactly what ``TextRecordReader`` does, except it uses ``\n`` as a sync marker.
Since you can't use any single byte as a sync marker in binary data, it uses a longer string of many bytes that hopefully would not be in the data itself.

.. _runtime-qa-part-02-number-of-mappers:

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