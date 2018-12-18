.. _dbg:

Spark Debugging
================
This section talks about debugging spark applications. 

.. _dbg_config:

Spark configs/references
------------------------

.. _dbg_logging:

Debug Logging
~~~~~~~~~~~~~

To turn on debug logging for your spark application
+++++++++++++++++++++++++++++++++++++++++++++++++++

Go to http://spark.apache.org/docs/latest/running-on-yarn.html and see the "Debugging your Application" section and look for log4j. There are a couple of options.

- cp $SPARK_CONF_DIR/log4j.properties ~/log4j.properties
- edit log4j.properties and change ``log4j.rootCategory=INFO``, console to ``log4j.rootCategory=DEBUG``, console or if you only want Spark to have debug change it to include the line: ``log4j.logger.org.apache.spark=DEBUG``
- add ``--files ~/log4j.properties``
- for spark-shell in client mode if you want to see message on the driver you can set with ``sc.setLogLevel("DEBUG")``

.. _dbg_tuning:

Tuning and Monitoring
---------------------

- http://spark.apache.org/docs/latest/tuning.html
- http://spark.apache.org/docs/latest/monitoring.html
- http://spark.apache.org/docs/latest/configuration.html

.. _dbg_heap_dumps:

Getting heap dump
-----------------

You can follow instructions very like mapreduce/tez to get a spark heap dump: https://twiki.corp.yahoo.com/view/Grid/PigTroubleshooting#Getting_Heapdump

The only difference is you specify the -XX options using configs ``spark.executor.extraJavaOptions`` and ``spark.driver.extraJavaOptions``

Here is an example:
- create a dump.sh script with contents, replacing youruser with your actual userid:

.. code-block:: console

  #!/bin/sh
  hadoop fs -put myheapdump.hprof /tmp/myheapdump_youruser/${PWD//\//_}.hprof

- Create the directory where hprof files will be dumped

  - hadoop fs -mkdir /tmp/myheapdump_youruser

- Launch Spark configuring HeapDumpOnOutOfMemoryError? in java.opts for getting heap dump

  - --conf spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./myheapdump.hprof -XX:OnOutOfMemoryError=./dump.sh"
  - --conf spark.driver.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./myheapdump.hprof -XX:OnOutOfMemoryError=./dump.sh"
  - --files /homes/tgraves/locguser/dump.sh

Full example spark-submit command:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./myheapdump.hprof -XX:OnOutOfMemoryError=./dump.sh" \
  --conf spark.driver.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=./myheapdump.hprof -XX:OnOutOfMemoryError=./dump.sh" \
  --executor-memory 2G \
  --files $SPARK_CONF_DIR/hive-site.xml,/homes/tgraves/dump.sh \
  --class com.oath.TestApp \
  --queue default \
  ./test.jar

.. _dbg_running_app:

Debugging Running Application
-----------------------------
The Spark Web UI has a lot of information about the running application. It has the executors it using, tasks, stages and details about each stage (gc, times, shuffle times an sizes, etc. This is the first place to go to find more details about what is happening with your application. After that you can look at the log files. The application master logs container the logs from the Spark Context which is the main driver for your application so look there next.

.. _dbg_spark_ui:

Finding Spark Web UI
~~~~~~~~~~~~~~~~~~~~

- Go the Hadoop ResourceManager web UI: 

  - ``http://[ResourceManager node for your cluster].ygrid.yahoo.com:8088/cluster/apps``

- Find your application id. The application id is printed on the client side (ie application identifier: application_1389725918559_11438) or search for your user name in the list.
- Click no the "Tracking UI" link and this will take you to the Spark web UI. This is Bouncer authenticated.

.. _dbg_view_acls:

View Acls
~~~~~~~~~

To enable other users to view my applications Web UI:
- place the list of users you want to have view permissions in the spark config: ``spark.ui.view.acls``
- export ``SPARK_JAVA_OPTS="$SPARK_JAVA_OPTS -Dspark.ui.view.acls=user1,user2,user3"``

.. _dbg_applogs:

Spark Logs
~~~~~~~~~~

.. _dbg_appmaster_logs:

Application Master Logs
+++++++++++++++++++++++

To find the Application Master Log (Spark Context info in yarn-standalone mode)

- Go the Hadoop ResourceManager web UI:

  - ``http://[ResourceManager node for your cluster].ygrid.yahoo.com:8088/cluster/apps``

- Find your application id. The application id is printed on the client side (ie application identifier: application_1389725918559_11438) or search for your user name in the list.
- Click on the application id to go to a page like:

  - ``http://[ResourceManager node for your cluster].ygrid.yahoo.com:8088/cluster/app/application_1389725918559_11438``

- In the ApplicationMaster table at the bottom click on the logs link on the right hand side

.. _dbg_executor_logs:

Executor Logs
+++++++++++++

To find the Executor logs

- Go to the Spark Web UI via the "Tracking UI" link on RM
- Click on the "Executors" tab and each one will have a link to the logs

.. _dbg_finished_app:

Debug Finished Application
--------------------------

Go to the Spark history server. This is the same URI as the Resourcemanager with the port changed to 50509.
``https://[ResourceManager node for your cluster].ygrid.yahoo.com:50509``

.. _dbg_finished_app_logs:

Logs
++++

You can use yarn logs to get all the logs for your application. Yarn logs can also get specific container logs but you have to know the container id and the host name. You should be able to get those by first looking at the application master log.
  - ``yarn logs -applicationId < your application id> | less``

You can also still go to the ResourceManager web UI to view the logs as described above in the Application is Running section.

You can also see just the application master logs by doing something like:
  - ``yarn logs -applicationId < your app id> -appOwner < app Owner> -am 1 -logFiles <stderr/stdout>``

.. _dbg_faq_hints:

Yamas Metrics
-------------

The Oath version of Spark (`yspark`) contains an additional metrics sink (`YamasSink`) to send Spark metrics (https://spark.apache.org/docs/latest/monitoring.html#metrics) to Yamas (http://http://yo/yamas-guide).

Configuration
~~~~~~~~~~~~~

To configure a `YamasSink`, please add a custom `metrics.properties` file when submitting your Spark application.

::
  
  $SPARK_HOME/spark-submit --conf spark.metrics.conf=./metrics.properties
  
The `metrics.properties` file should contain at least the following parameters:

::

  [instance].sink.yamas.class=org.apache.spark.metrics.sink.YamasSink
  [instance].sink.yamas.namespace=[your yamas namespace]

For example, `"[instance]"` (as defined here https://spark.apache.org/docs/latest/monitoring.html#metrics) could be: "*" for all instances, "driver", "executor", etc., and `"[your yamas namespace]"` is the target Yamas namespace. 

In order to allocate a Yamas namespace for your project, please onboard your namespace with the Yamas team (http://yo/yamas-namespace-onboarding). If you haven't onboarded your namespace with Yamas, you can still send the metrics to any namespace (e.g. your username) for testing purposes. Note that once you onboard your namespace to Yamas, you will be allowed to set alerts and pre-aggregations via Git. 

::

  *.sink.yamas.class=org.apache.spark.metrics.sink.YamasSink
  *.sink.yamas.namespace=My-Yamas-Namespace

The configuration above is the minimum required, but there are other important configs you should pay attention to:

spark.metrics.namespace
+++++++++++++++++++++++

::

  spark.metrics.namespace="${spark.app.id}" 

The setting `spark.metrics.namespace` is a regular Spark conf, and not included in `metrics.properties`. It is set using ``--conf spark.metrics.namespace="your-namespace"`` when submitting your Spark application. By default, the value of `spark.metrics.namespace` is your Yarn application id (e.g. application_123456789_12345). While it is useful to track metrics per application id, you may consider changing this to be the application name instead ``${spark.app.name}``:

::

  spark.metrics.namespace="${spark.app.name}"
  
This is recommended as it will reduce the cardinality of the metrics that Yamas needs to keep track of.

[instance].sink.yamas.application
+++++++++++++++++++++++++++++++++

::

  [instance].sink.yamas.application=[yamas-app-name]

In Yamas, each metric corresponds to an `application`. This does not map to any concept in Spark or Yarn, but instead can be thought of as a grouping of metrics. One consideration is to set `application` to the type of Spark app you are running (say "ML"), and setting `spark.metrics.namespace` to a more specific name for your Spark app (say "NLP-pipeline"). 

Having a Yamas `application` name that is of coarser granularity than a specific Spark application can be beneficial, for example, to supress Yamas alerts for all of the Spark applications under a Yamas application.

Configuration Setting Index
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The configurations in the table below are set in the `metrics.properties` file, and are all prefixed with ``[instance].sink.yamas.``:

.. table:: Metrics.properties Configuration Index

    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | Setting                              | Default Value           | Description                                                                               |
    +======================================+=========================+===========================================================================================+
    | class                                | None                    | Set to `org.apache.spark.metrics.sink.YamasSink` to activate `YamasSink`                  |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | namespace                            | None                    | Set to a Yamas namespace                                                                  |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | application                          | Spark                   | Set this to a string that can group a set of apps together                                |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | sendMessageUrl                       | Default Yamas Collector | Override only if the yamas collector changes                                              |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | reportingPeriodMinutes               | 1                       | Period in minutes (integer) when the sink reports to Yamas                                |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | metricRegexFilter                    | MetricFilter.ALL        | A regex that includes metrics that match it                                               |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | rateUnit                             | SECONDS                 | The unit to use to report rate of change                                                  |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | durationUnit                         | MILLISECONDS            | The unit to use to report time                                                            |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | invalidCharacterRegex                | ([^a-zA-Z0-9.\\-_])     | Regex used to replace invalid characters (in Yamas) with a default value                  |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | invalidCharacterReplacementValue     | __                      | Value to use to replace invalid characters (in Yamas)                                     |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | reportExpandedStats                  | false                   | If true, report if available  min, max, median, stddev, and percentiles                   | 
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | requestRetries                       | 3                       | Times to retry requests to Yamas. After reaching the last retry, the metrics are dropped. |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | httpSocketTimeoutMs                  | 5000                    | Millisecond socket timeout (waiting for data)                                             |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+
    | httpConnectTimeoutMs                 | 5000                    | Millisecond connection timeout (waiting for a connection)                                 |
    +--------------------------------------+-------------------------+-------------------------------------------------------------------------------------------+

The configuration below is set as a regular spark conf (`-\\-conf`):

.. table:: Spark Configurations (metrics-related)

    +--------------------------------------+-------------------------+---------------------------------------------------------------------------------+
    | Setting                              | Default Value           | Description                                                                     |
    +======================================+=========================+=================================================================================+
    | spark.metrics.namespace              | ${spark.app.id}         | Set to ${spark.app.name} to reduce cardinality                                  |
    +--------------------------------------+-------------------------+---------------------------------------------------------------------------------+

AccumulatorSource
~~~~~~~~~~~~~~~~~

A new metrics source was added to yspark for accumulators. Use this in the cases where you'd like a global counter of `LongAccumulator` or `DoubleAccumulator` type.

To add your accumulators to the metric system:

::

  import org.apache.spark.metrics.source.{LongAccumulatorSource, DoubleAccumulatorSource}

  val myLongAccumulator = sparkContext.longAccumulator("my-long-accumulator")
  LongAccumulatorSource.register(sparkContext, List(("my-long-accumulator" -> myLongAccumulator)).toMap)

  val myDoubleAccumulator = sparkContext.doubleAccumulator("my-double-accumulator")
  DoubleAccumulatorSource.register(sparkContext, List(("my-double-accumulator" -> myDoubleAccumulator)).toMap)

The accumulators will be reported as a single series for your application from the driver instance.

Yamas Pre-Aggregated Data
~~~~~~~~~~~~~~~~~~~~~~~~~

On the Yamas UI query side, the cardinality of metrics from a Spark app may be overwhelming, and you may notice a slow response time when querying. Consider using Yamas Pre-Aggregation (http://yo/yamas-preaggregation) to generate summary metrics that are more easily queried. Note, this requires an onboarded Yamas namespace, and a Git repo with .yo rule files (see: https://git.ouroath.com/pages/monitoring/yamas_userguide_2.0/DSL/DSL_user_guide/).

FAQs/Hints
----------

Many of the issues seen are simply sizing things properly. Look at the memory size of the workers and application master. Check the # of tasks at each phase.

Also look at the tuning guide: `Tuning <https://spark.apache.org/docs/latest/tuning.html>`_
