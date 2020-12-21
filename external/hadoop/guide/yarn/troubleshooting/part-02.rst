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

Monitoring the Memory of Your Container
---------------------------------------

:term:`NodeManager` periodically (every 3 seconds by default, which can be changed via ``yarn.nodemanager.container-monitor.interval-ms``) cycles through all the currently running containers, calculates the process tree (all child processes for each container), and for each process examines the ``/proc/<PID>/stat`` file (where `PID` is the process ID of the container) and extracts the physical memory (aka `RSS`) and the virtual memory (aka `VSZ` or `VSIZE`).

If virtual memory checking is enabled (configured via ``yarn.nodemanager.vmem-check-enabled``), then YARN compares the summed `VSIZE` extracted from the container process (and all child processes) with the maximum allowed virtual memory for the container. The maximum allowed virtual memory is basically the configured maximum physical memory for the container multiplied by ``yarn.nodemanager.vmem-pmem-ratio``. For example if `vmem-pmem-ratio` is set to `2.1` and your YARN container is configured to have a maximum of 2 GB of physical memory, then this number is multiplied by `2.1` which means you are allowed to use `4.2GB` of virtual memory.

If physical memory checking is enabled (`true` by default, overridden via ``yarn.nodemanager.pmem-check-enabled``), then YARN compares the summed RSS extracted from the container process (and all child processes) with the maximum allowed physical memory for the container.

If either the virtual or physical utilization is higher than the maximum permitted, YARN will kill the container, as shown in the error :ref:`at the top of this article <yarn_troubleshooting_memory>`.


  .. table:: `YARN NodeManager Configurations to Monitor Container`
    :widths: auto

    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |                           Property                           |                                                                                                                Description                                                                                                               |
    +==============================================================+==========================================================================================================================================================================================================================================+
    | ``yarn.nodemanager`` |br| ``.container-monitor.enabled``     | Enable container monitor                                                                                                                                                                                                                 |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``yarn.nodemanager`` |br| ``.container-monitor.interval-ms`` | How often to monitor containers. If not set, the value for `yarn.nodemanager.resource-monitor.interval-ms` will be used. If 0 or negative, container monitoring is disabled.                                                             |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``yarn.nodemanager`` |br| ``.vmem-check-enabled``            | Whether virtual memory limits will be enforced for containers.                                                                                                                                                                           |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``yarn.nodemanager`` |br| ``.vmem-pmem-ratio``               | Ratio between virtual memory to physical memory when setting memory limits for containers. Container allocations are expressed in terms of physical memory, and virtual memory usage is allowed to exceed this allocation by this ratio. |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``yarn.nodemanager`` |br| ``.pmem-check-enabled``            | Whether physical memory limits will be enforced for containers.                                                                                                                                                                          |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Increasing the Memory Available to Your MapReduce Job
-----------------------------------------------------

.. admonition:: Reading...
   :class: readingbox

    * Check :ref:`yarn_memory`
    * Increasing Memory for Job (:numref:`mapreduce_faq_runtime_increase_job_memory`)