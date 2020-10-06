.. _yarn_memory:

***********************************************
Hadoop Memory Optimization |nbsp| |green-badge|
***********************************************

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

Computing Hadoop Job Cost
=========================

.. include:: /common/yarn/yarn-compute-job-cost.rst


Am I wasting memory?
=====================

Probably.  The most reliable way to setup a test environment and try a smaller setting.  If your job runs well with the new setting, consider making the change in production.  Here are a few tools to help you.

Doppler Downsizer
-----------------

`yo/downsizer <yo/downsizer>`_


"This app helps identify wasted grid resources by showing the difference between the container size used by individual Map/Reduce apps and how big a container was actually needed to run those apps. It also estimates cost savings from "*right-sizing*" the container size of your apps, based on the Hadoop :abbr:`TCO (Total Cost of Ownership)` cost model."

How it works
------------

Downsizer will review specific job counters to see how large the JVM heap and off heap allocations were during production runs.  Unfortunately, Java will use more heap memory then it actually needs, inflating the perceived required memory.  Bottom line, changes suggested here are conservative and safe to apply, but may not not reflect the minimum memory your job needs. 


.. _yarn_memory_important_configurations_tasks:

Important Configurations (Tasks)
================================

There are a number of mapreduce options that control how much memory your tasks can access. Users/admins can also specify the maximum virtual memory of the launched child-task, and any sub-process it launches recursively.
The value ``mapreduce.{map|reduce}.memory.mb`` should be specified in mega bytes (MB). Default is `1536MB`. The configuration parameter controls how much memory space is allocated to your map tasks (Physical memory).
The physical memory configured for your job must fall within the minimum and maximum memory allowed for containers in your cluster. The maximum value is set as YARN confoguration parameter in the `mapred-site.xml`, dubbed ``yarn.scheduler.maximum-allocation-mb`` , which is currently set to `20480MB` (`131072` for AxoniteBlue) and not over-writable. See also ``yarn.scheduler.minimum-allocation-mb``.


Note the following:

* This is a *per-process* limit
* The value must be greater than or equal to the ``-Xmx`` passed to JavaVM, else the VM might not start.
* The container represents the capped memory for the running Java task

JVM Options - This controls the JVM heap size and other command line goodies you want to pass. Those configurations are used only for configuring the launched child tasks from ``MRAppMaster``.  Configuring the memory options for daemons is documented in :hadoop_rel_doc:`Configuring the Environment of the Hadoop Daemons<hadoop-project-dist/hadoop-common/ClusterSetup.html#Configuring_Environment_of_Hadoop_Daemons>`.

* mapred.child.java.opts (Old setting, used by both map and reduce)
* ``mapreduce.map.java.opts`` - Replaces `mapred.child.java.opts` for mappers, not set by default.
* ``mapreduce.reduce.java.opts`` - Replaces `mapred.child.java.opts` for reducers, not set by default.

By default only ``mapred.child.java.opts`` is set to "`-server -Xmx1024m -Djava.net.preferIPv4Stack=true`", and sets a `1024MB` heap size.


If you need more memory, then you configure the JVM heap size for your map and reduce processes. These sizes need to be less than the physical memory you configured 
ou can do this for both mapper and reducer with  ``mapreduce.{map|reduce}.java.opts``.  For example, to set the mapper heap size to `2GB`,
use ``-Dmapreduce.map.java.opts=-Xmx2048m``. Other ``java -X`` options let you the initial heap size & stack size.

By default `512MB` is left for native memory. We recommend users maintain the same ratio while increasing memory unless more native memory is required with JNI. So in general, ``mapreduce.{map|reduce}.memory.mb`` should be equal to sum of `-Xmx` specified in ``mapreduce.{map|reduce}.java.opts`` and `512 MB`. If you set ``mapreduce.{map|reduce}.memory.mb`` to 4096 but only have `-Xmx1536M` in your ``mapreduce.{map|reduce}.java.opts``, then you are wasting `2G` of memory.
Always ensure that you increase both ``memory.mb`` and ``java.opts`` together and the difference between them is  `512MB`.

.. note:: Reducers usually require more memory and increasing for both map and reducers will waste memory. So, you may need to set ``mapreduce.reduce.java.opts`` slightly higher than ``mapreduce.map.java.opts`` in order to save memory.


Quick Memory Checks
-------------------

.. rubric:: TEZ

For each vertex, you can check the max memory used by any task.  Click the gear and check the `PHYSICAL_MEMORY_BYTES` and `COMMITTED_HEAP_BYTES` counters.  These should appear in the table and you can click the heading to sort and find the max value.  `PHYSICAL` should always be bigger than `COMMITTTED_HEAP` so sort by that. The largest value is the most memory used by any task in that vertex.  This is the number that shows up in the Downsizer tool.

The difference between the two counters is how much off-heap memory your task is using.  This needs to be accounted for in the gap between the container and heap settings.  For instance, if you use `200MB` of off heap memory, the container should be (:math:`512+200\,\textit{MB}`) larger than the heap memory setting. 

.. image:: /images/yarn/memory/memory-counters.png
  :alt:
  :align: center

You can tell if a Tez vertex is a Map or Reduce task by seeing if it has a value for the `SHUFFLE_BYTES` counter.  Reducers shuffle, mappers don't have this counter. 

You can confirm the memory settings for the task by looking in the container GC log in the `-XX:MaxHeapSize` setting. . The top of the log should look like this:

  .. code-block:: bash

			Java HotSpot(TM) 64-Bit Server VM (25.162-b12) for linux-amd64 JRE (1.8.0_162-b12), built on Dec 19 2017 21:15:48 by "java_re" with gcc 4.3.0 20080428 (Red Hat 4.3.0-8)

			Memory: 4k page, physical 65403484k(25045080k free), swap 23984628k(23843916k free)

			#CommandLine flags:
			-XX:ErrorFile=/grid/5/tmp/yarn-logs/{application_ID}/{container_ID}/hs_err_pid%p.log \
			-XX:GCTimeLimit=50 -XX:InitialHeapSize=1046455744 -XX:MaxHeapSize=1073741824 \
			-XX:NewRatio=8 -XX:ParallelGCThreads=4 -XX:+PrintGC -XX:+PrintGCDateStamps \
			-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseCompressedClassPointers \
			-XX:+UseCompressedOops -XX:+UseParallelGC



.. rubric:: Yarn

You can check the actual Physical memory usage of the tasks in the Counters page of the job in the UI (:menuselection:`cluster/scheduler --> Tracking UI --> select Application[ID] --> Job --> Counters`) and tune (increase or reduce) the memory further based on actual usage. If the Counters page shows that there are lot of spill (:menuselection:`Map-Reduce Framework --> Spilled Records`), then increase ``mapreduce.task.io.sort.mb`` to 512 or 768. Default is 256. Reducing spill will also speed up the job.

List of other memory knobs
--------------------------

The following are the configuration options for mapreduce


.. table:: `YARN Map memory Configurations prefixed by mapreduce.`
  :widths: auto

  +--------------------------------------+-------+--------------------------------------------------------------------------------------------------------------------------------+
  |                 Name                 |  Type |                                                           Description                                                          |
  +======================================+=======+================================================================================================================================+
  | ``task.io.sort.mb``                  | int   | The cumulative size of the serialization and accounting buffers storing records emitted from the map, in megabytes.            |
  +--------------------------------------+-------+--------------------------------------------------------------------------------------------------------------------------------+
  | ``map.sort.spill.percent``           | float | The soft limit in the serialization buffer. Once reached, a thread will begin to spill the contents to disk in the background. |
  +--------------------------------------+-------+--------------------------------------------------------------------------------------------------------------------------------+


.. table:: `YARN Shuffle/Reduce memory Configurations prefixed by mapreduce.`
  :widths: auto

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

Important Configurations (AM)
=============================

Sometimes you'll see failures in the AM, not the tasks. `Port numbers in the 50520-51000 range are designated for AMs.` (JEagles) Errors with those ports likely means AM trouble.

.. include:: /common/yarn/memory/yarn-memory-appmaster-conf.rst


GC Logs
=======

Container GC logs have detailed information about container JVM memory usage. The most interesting is the maximum heap after a full garbage collection which reflects the static footprint of the job.  Often, this is much lower than the maximum size of the heap.  

You can find the GC log by clicking through to a task:

.. image:: /images/yarn/memory/gc-log-tez.png
  :alt:
  :align: center

.. figure:: /images/yarn/memory/gc-log-tez-full.png
  :alt:
  :align: center

  Then select gc.log

A GC Log viewer can be used to see how much memory your task is using.

`GCViewer <https://github.com/chewiebug/GCViewer>`_ - Garbage Collection and Heap Size Analysis

.. image:: /images/yarn/memory/gcviewer.png
  :alt:
  :align: center

The blue line shows the used heap at any given time.  The gray lines in the yellow band represent the new gen heap.  Ideally, all tuple allocations would stay in this region for quick GC cleanup. 

The larger drops in the blue line show CMS cleanups, operating on the old gen region. This happens in the background, for the most part, but indicates objects are being promoted to old gen and a larger new gen space may help. 

The black bars indicate a full GC pause.  This will happen if free space wasn't available via other collections.  The hight with time scale on the left shows the duration.  If GC is taking too much of the CPU time, the JVM will exit.
Throughput, (:math:`1 - \dfrac{\textit{GC}}{\textit{totalTime}}`), represents the time the CPU is doing useful work.  Below ``~90%`` indicates significant time spent doing collections, and you may want to try a larger heap. 


.. rubric:: Heap Dumps

.. caution:: Heap dumps contain secrets. Use with care and security in mind.  Could create lots of data, too.


  .. code-block:: bash

    mapreduce.map.java.opts=-Xmx6000m -XX:+HeapDumpOnOutOfMemoryError \
       -XX:HeapDumpPath=./myheapdump${PWD//\\//_}.hprof \
       -XX:OnOutOfMemoryError=\\"hadoop dfs -put *.hprof /user/$USER/heaps\\"
    ## for a running JVM
    /home/gs/java/current/bin/jmap -dump:format=b,file=/grid/0/tmp/amdump.hprof PID



Do I have a quota problem?
==========================

Doppler's project grid pages have information about your queue usage and computed total cost of ownership (:abbr:`TCO (Total Cost of Ownership)`). If you're under your quota limits, you may not need to optimize your jobs. Not wasting resources is always desirable, though.

If other teams/projects share this queue, you should discuss if your usage is starving other jobs in that queue.

.. figure:: /images/yarn/memory/project-quota-usage.png
  :alt:
  :align: center

  Project Quota and Usage

Resources
=========

.. include:: /common/yarn/memory/yarn-memory-resources.rst