..  _yarn_troubleshooting_memory:

Memory Running on Yarn
======================

.. admonition:: Related...
   :class: readingbox

    Make sure you check :ref:`yarn_memory_important_configurations_tasks`

There are times when a job exceeds its memory limits. You can see the following error:

  .. parsed-literal::

    Application application_1409135750325_48141 failed 2 times due to AM Container for
    appattempt_1409135750325_48141_000002 exited with exitCode: 143 due to: Container
    [pid=4733,containerID=container_1409135750325_48141_02_000001] is running beyond physical memory limits.
    Current usage: 2.0 GB of 2 GB physical memory used; 6.0 GB of 4.2 GB virtual memory used. Killing container

This means that your job is using more memory than YARN has allocated for it.
YARN will kill containers that exceed the amount of memory specified for the
container by ``yarn.nodemanager.resource.memory-mb`` values. |br|
Note that this value is different than than the amount of memory given to the
JVM inside of a container (e.g. the -Xmx value in
``mapreduce.{map,reduce}.java.opts``)

.. rubric:: Before Blindly Increasing Memory, Make Sure Your Job Has to Cache Data

It is possible that your task running out of memory means that data is being
cached in your map or reduce tasks.

Data can be cached for a number of reasons:

* Your job is writing out Parquet data, and Parquet buffers data in memory prior
  to writing it out to disk.
* Your code (including library you’re using) is caching data. An example here is
  joining two datasets together where one dataset is being cached prior to
  joining it with the other.


If your job does not really need to cache data, then it is possible to reduce
memory utilization by avoiding cached data.

.. rubric:: GC Overhead Limit Exceeded

Another possible memory issue is the JVM inside of the container running out of
memory. These errors will give a GC Overhead Limit Exceeded error.

  .. parsed-literal::

    2017-11-12 03:02:37,340 [PigTezLauncher-0] INFO org.apache.pig.backend.hadoop.executionengine.tez.TezJob - DAG Status:       status=FAILED, progress=TotalTasks: 14439 Succeeded: 477 Runn
    ing: 0 Failed: 1 Killed: 13961 FailedTaskAttempts: 781 KilledTaskAttempts: 35, diagnostics=Vertex failed, vertexName=scope-493, vertexId=vertex_1509095573435_2608817_1_06, diagnosti
    cs=[Task failed, taskId=task_1509095573435_2608817_1_06_000921, diagnostics=[TaskAttempt 0 failed, info=[Error: Encountered an Error while executing task: attempt_1509095573435_2608
    817_1_06_000921_0:java.lang.OutOfMemoryError: GC overhead limit exceeded
    at java.lang.StringCoding.decode(StringCoding.java:187)
    at java.lang.String.<init>(String.java:426)
    at java.lang.String.<init>(String.java:491)
    at org.apache.pig.data.utils.SedesHelper.readChararray(SedesHelper.java:85)
    at org.apache.pig.data.BinInterSedes.readDatum(BinInterSedes.java:419)
    at org.apache.pig.data.BinInterSedes.readDatum(BinInterSedes.java:318)

This means that your job is using more heap memory in the JVM than it was
allocated. The heap value is determined by the -Xmx value of the JVM, which
usually comes from the ``mapreduce.{map,reduce}.java.opts`` job config
(or a similarly mapped config).



Increasing the Memory Available to Your MapReduce Job
-----------------------------------------------------

.. admonition:: Reading...
   :class: readingbox

    * Check :ref:`yarn_memory`
    * Increasing Memory for Job (:numref:`mapreduce_faq_runtime_increase_job_memory`)