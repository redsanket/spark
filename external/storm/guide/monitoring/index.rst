========================
YAMAS Metrics/Monitoring
========================

.. Status: Updated on 11/12/18.

The HTTP spout from ``ystorm_contrib`` exports metrics through the Storm 
metrics system. The ``ystorm_contrib`` package also includes with it a metrics 
collector that you can include with your topology to place these metrics 
in YAMAS.

Overview of YAMAS in Storm
==========================

You can use YAMAS to monitor Storm at the container and 
application level as shown in the figure below. At the container
level, YAMAS is communicating with Nimbus through a connector, allowing
you to monitor the performance and find issues in the Storm cluster.
At the application level, YAMAS is communicating with the Supervisor, so
you can monitor the performance of your topology.

.. image:: images/yamas_storm_metrics.jpg
   :height: 461 px
   :width: 850 px
   :scale: 90 %
   :alt: Storm With YAMAS 
   :align: left 


Yamas Metrics Consumer Basics
=============================

Before you collect YAMAS metrics in your code, you should review the following
sections to learn how to include dependencies, import the ``yamas`` package,
and understand what metrics are supported.

Adding Dependencies
-------------------

Add the following XML to your ``pom.xml`` to include the ``yamas_metrics_consumer`` package:

.. code-block:: xml

    <dependency>
       <groupId>yahoo.yinst.ystorm_contrib</groupId>
       <artifactId>yamas_metrics_consumer</artifactId>
       <version>0.4.0.130</version>
    </dependency>

Configuring Storm to Use YAMAS
------------------------------

In your driver file,  add the following when submitting your topology:

.. code-block:: java

   import com.yahoo.storm.metrics.yamas.HttpYamasMetricsConsumerV2; 
   ...
   conf.registerMetricsConsumer(HttpYamasMetricsConsumerV2.class, "yamas-app", 1);

Supported Metrics
-----------------

In addition to the metrics below, YAMAS also supports user-defined metrics from your topology. 
The name of the metric in your topology will be preserved when sent to YAMAS. 
Any named metric that is a ``com.yahoo.storm.metrics.yamas.AbsoluteNumber`` will be set. 
Any named metric that is any other ``Number`` will be incremented by the value. 
More complex objects are ignored.


.. csv-table:: Supported Metrics for Storm
   :header: "Name/Pattern", "Description"
   :widths: 15, 45

   "``uptimeSecs``", "The uptime in seconds for the worker."
   "``startTimeSecs``", "The UNIX time when the worker came up."
   "``GC/[ConcurrentMarkSweep|ParNew|Copy|MarkSweepCompact|G1OldGeneration|G1YoungGeneration]_count``", The number of garbage collections that have happened."
   "``GC/[ConcurrentMarkSweep|ParNew|Copy|MarkSweepCompact|G1OldGeneration|G1YoungGeneration]_timeMs``", "The time spent doing those garbage collections."	
   "``memory/[heap|nonHeap]_initBytes``", "The initial bytes for this memory type."
   "``memory/[heap|nonHeap]_committedBytes``", "The bytes committed to be used."
   "``memory/[heap|nonHeap]_usedBytes``", "The bytes currently used."
   "``memory/[heap|nonHeap]_maxBytes``", "The maximum bytes for this memory type."
   "``memory/[heap|nonHeap]_unusedBytes``", "It equals to (committedBytes - usedBytes)."
   "``memory/[heap|nonHeap]_virtualFreeBytes``", "It equals to (maxBytes - usedBytes)."
   "``CPU_user-ms``", "cpu user time reported by Sigar."
   "``CPU_sys-ms``", "cpu sys time reported by Sigar."
   "``CGroupCpu_user-ms``", "cpu user time reported by cgroup."	
   "``CGroupCpu_sys-ms``", "cpu sys time reported by cgroup."
   "``[__sendqueue|__receive]_write_pos``", "How many writes have happened so far."
   "``[__sendqueue|__receive]_read_pos``", "How many reads have happened so far."
   "``[__sendqueue|__receive]_capacity``", "The capacity of the queue."
   "``[__sendqueue|__receive]_population``", "The population in the queue."
   "``[__sendqueue|__receive]_overflow``", "The population in the overflow location."
   "``__ack-count``", "The number of tuples acked on non-system streams."
   "``__ack-count_system``", "The number of tuples acked on system streams."
   "``__fail-count``", "The number of failed tuples on non-system streams."
   "``__fail-count_system``", "The number of failed tuples on system streams."
   "``__emit-count``", "The number of tuples emitted on non-system streams."
   "``__emit-count_system``", "The number of tuples emitted on system streams."
   "``__execute-count``", "The number of tuples executed on non-system streams."
   "``__execute-count_system``", "The number of tuples executed on system streams."


Each of these metrics shown below also have several dimensions with them, 
so that you can get more details of what is happening.

- ``worker-host``
- ``host``
- ``worker-port``
- ``component-id`` (``__system`` for worker wide metrics)
- ``task-id`` (``-1`` for worker wide metrics)
- ``topology-submitter``


Customizing Metrics
-------------------

Storm metrics do not distinguish between an increment and a set once they get to 
the Collector. They also are very generic and could return complex objects like 
``Maps``, ``Lists``, etc. To handle set versus increment cases, we create an ``AbsoluteNumber`` 
class, that when a metric returns this YAMAS will call ``set()`` instead of ``increment()``.

For more complex metrics you can subclass the collector and override::

    public boolean handleDataPoint(DataPoint dp) throws StormMetricsException;

If the data point is something that you have handled yourself, then return ``true``.
If it is something you want default behavior for, then return ``false``. Do 
not send the metrics in your method, and do not change the dimensions: They are 
handled already and may cause problems.

System Metrics YAMAS Collector Already Handles
==============================================

Worker-Level Metrics
--------------------

.. csv-table:: Supported Metrics for Storm
   :header: "Purpose", "Metrics"
   :widths: 20, 45

   "Time", "
            - ``uptimeSecs``
            - ``startTimeSecs``"
   "Garbage Collection", "
                          - ``GC/[ConcurrentMarkSweep|ParNew|Copy|MarkSweepCompact|G1OldGeneration|G1YoungGeneration]_count``
                          - ``GC/[ConcurrentMarkSweep|ParNew|Copy|MarkSweepCompact|G1OldGeneration|G1YoungGeneration]_timeMs``"
   "Memory Usage", "
                    - ``memory/[heap|nonHeap]_initBytes``
                    - ``memory/[heap|nonHeap]_committedBytes``
                    - ``memory/[heap|nonHeap]_usedBytes``
                    - ``memory/[heap|nonHeap]_maxBytes``
                    - ``memory/[heap|nonHeap]_unusedBytes``
                    - ``memory/[heap|nonHeap]_virtualFreeBytes``"
   "CPU Usage", "
                - ``CPU_user-ms``
                - ``CPU_sys-ms``
                - ``CGroupCpu_user-ms``
                - ``CGroupCpu_sys-ms``"
                
   "Queue", "
            - ``[__sendqueue|__receive]_write_pos``
            - ``[__sendqueue|__receive]_read_pos``
            - ``[__sendqueue|__receive]_capacity``
            - ``[__sendqueue|__receive]_population``
            - ``[__sendqueue|__receive]_overflow``"
  
   
                  
Tuple-Level Metrics
-------------------

.. csv-table:: Supported Metrics for Storm
   :header: "Purpose", "Metrics"
   :widths: 20, 45

   "Acknowledge", "
                   - ``__ack-count``
                   - ``__ack-count_system``"
   "Failure", "
               - ``__fail-count``
               - ``__fail-count_system``"
   "Emit Throughput", "
                       - ``__emit-count``
                       - ``__emit-count_system``"
   "Execute Throughput", "
                          - ``__execute-count``
                          - ``__execute-count_system``"
   "Generic", "
               - Any named metric that is an ``AbsoluteNumber`` will be set.
               - Any named metric that is any other Number will be incremented by the value."

Dimensions
==========

YAMAS metrics are collected with the following dimensions:

- ``worker-host``
- ``host``
- ``worker-port``
- ``component-id`` (``__system`` for worker wide metrics)
- ``task-id`` (``-1`` for worker wide metrics)
- ``topology-submitter``

Steps for Collecting Metrics
============================

1. Register Metrics
-------------------

.. code-block:: java

   transient CountMetric _countMetric;
   transient ReducedMetric _wordLengthMeanMetric;

   @Override
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
       _collector = collector;
       _countMetric = new CountMetric();
       _wordLengthMeanMetric = new ReducedMetric(new MeanReducer());
    
       context.registerMetric("execute_count", _countMetric, 5);
       context.registerMetric("word_length", _wordLengthMeanMetric, 60);
   }

2. Register a Metrics Consumer Before Launching a Topology
----------------------------------------------------------

.. code-block:: java

   conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

3. Update the Metrics When Something Happens
--------------------------------------------

.. code-block:: java

   @Override
   public void execute(Tuple tuple) { 
       String word = tuple.getString(0);
       _collector.emit(tuple, new Values(word + "!!!"));
       _collector.ack(tuple); 
       _countMetric.incr();
       _wordLengthMeanMetric.update(word.length());
   } 


YAMAS Metrics Consumer
======================


Make sure to include ``yamas_metrics_consumer`` in your dependency tree.

.. code-block:: xml

   <dependency>
       <groupId>yahoo.yinst.ystorm_contrib</groupId>
       <artifactId>yamas_metrics_consumer</artifactId>
       <version>0.4.0.130</version>
   </dependency>

Please use YAMAS instead of Logging for metrics.

.. code-block:: java

   import com.yahoo.storm.metrics.yamas.YamasMetricsConsumer; 
   conf.registerMetricsConsumer(HttpYamasMetricsConsumerV2.class, "yamas-app", 1);
   
