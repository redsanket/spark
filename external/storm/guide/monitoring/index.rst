============================
YAMAS Monitoring and Logging
============================

Using YAMAS to Collect Storm Metrics
====================================

System Metrics YAMAS Collector Already Handles
==============================================

Worker-Level Metrics
--------------------


Tuple-Level Metrics
-------------------


Dimensions
==========

YAMAS metrics are collected with the following dimensions:
worker-host
worker-port
component-id (“__system” for worker wide metrics)
task-id (“-1” for worker wide metrics)

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

Please use YAMAS instead of Logging for metrics.

.. code-block:: java

   import com.yahoo.storm.metrics.yamas.YamasMetricsConsumer; 
   conf.registerMetricsConsumer(YamasMetricsConsumer.class, “yamas-app”, 1);
   
   <dependency>
       <groupId>yahoo.yinst.ystorm_contrib</groupId>
       <artifactId>yamas_metrics_consumer</artifactId>
       <version>0.1.0</version>
   </dependency>

Customization of YAMAS Logging
==============================

Storm metrics do not distinguish between an increment and a set once they get to the Collector.

They also are very generic and could return complex objects like Maps, Lists, etc.

To handle set vs. increment cases we created an AbsoluteNumber class, that when a metric returns this YAMAS will call set() instead of increment().

For more complex metrics you can subclass the collector and override

public boolean handleDataPoint(DataPoint dp, MonMetrics yamas) throws MonMetricsException;




