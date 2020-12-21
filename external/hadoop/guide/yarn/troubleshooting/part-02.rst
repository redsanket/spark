..  _yarn_troubleshooting_memory:

Memory Running on Yarn
======================

.. admonition:: Related...
   :class: readingbox

    Make sure you check :ref:`yarn_memory_important_configurations_tasks`

Memory Settings on YARN
--------------------------------------

There are two memory properties that you need to be concerned with on YARN containers. 

.. rubric:: 1) JVM Heap Size (mapreduce.{map,reduce}.java.opts)

This value controls how much memory the JVM will be given inside of the container.
The default value is 1 GB and the max value is cluster-dependent.
The JVM will never exceed this amount of memory.
If the JVM runs out of memory, it will die. 

.. rubric:: 2) YARN Container Size (yarn.nodemanager.resource.memory-mb)

This value controls how much memory YARN will allocate for the container. |br|
This value should be at least 0.5 GB larger than the JVM Heap Size to allow for
native memory to be allocated. The default vlaue is 1.5 GB. If a container
exceeds this memory allocation, it will be immediately killed by YARN.

Container Killed by YARN
--------------------------------------

There are times when a JVM exceeds its memory limits. You can see the following error:

  .. parsed-literal::

    Application application_1409135750325_48141 failed 2 times due to AM Container for
    appattempt_1409135750325_48141_000002 exited with exitCode: 143 due to: Container
    [pid=4733,containerID=container_1409135750325_48141_02_000001] is running beyond physical memory limits.
    Current usage: 2.0 GB of 2 GB physical memory used; 6.0 GB of 4.2 GB virtual memory used. Killing container

This means that your container is using more memory than YARN has allocated for it.
YARN will kill containers that exceed the amount of memory specified for the
container by ``yarn.nodemanager.resource.memory-mb`` values. |br|
Note that this value is different than than the amount of memory given to the
JVM inside of a container (e.g. the -Xmx value in
``mapreduce.{map,reduce}.java.opts``). Note also that this error does not mean that your JVM ran out of heap space, so increasing the JVM Heap Size will not help here.

.. rubric:: Data Caching

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

.. rubric:: Non-JVM Heap Memory

Your container may be using more native memory than YARN has allocated for your
container. This could be due to invoking a python ML library, creating a ton of
threads in the JVM, or something else.

In both cases, the amount of non-JVM heap memory that your container can use is
equal to the difference between your heap value and the size of the YARN
allocation. |br|
This boils down to: non-JVM memory space = YARN Container Size - JVM Heap Size. |br|
By default, the difference is 0.5 GB.

If your container uses a lot of non-JVM heap memory, you may need to increase the
YARN Container Size without increasing the JVM Heap Size to increase the
difference between these two values.

JVM Memory Exception (GC Overhead Limit Exceeded)
-------------------------------------------------

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

This means that your container is using more heap memory in the JVM than it was
allocated. The heap value is determined by the -Xmx value of the JVM, which
usually comes from the ``mapreduce.{map,reduce}.java.opts`` job config
(or a similarly mapped config). When increasing your JVM Heap Size, you will want
to also increase your YARN Container Size by the same amount.


Increasing the Memory Available to Your MapReduce Job
-----------------------------------------------------

.. admonition:: Reading...
   :class: readingbox

    * Check :ref:`yarn_memory`
    * Increasing Memory for Job (:numref:`mapreduce_faq_runtime_increase_job_memory`)