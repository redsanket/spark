.. _metrics_simon:

*****
Simon
*****


This Developer's Guide describes how to instrument an application so that its
metrics can be captured and archived by Simon.

In the Simon architecture,
applications send UDP messages, called `blurb messages`, to the Aggregator
process. There will generally be many application instances on many machines
sending blurb messages to the same Aggregator instance. These application
instances will be referred to as *monitoring clients* of the Aggregator. |br|
The Aggregator will also generally have one or more reporting clients which are the
consumers of the aggregated metric data. SimonWeb is a reporting client which
receives reports from the Aggregator and stores the data from those reports in
rrdtool. It also makes the stored data available to its clients as graphs.

To start using Simon to get metrics from your application you need to:

#. Prepare an XML configuration file.
   This defines both the content of the blurb messages that are sent by the
   application to the Aggregator, and the content of the reports that are published
   by the Aggregator and received by SimonWeb.
#. Run `simontool` on your configuration file, both to validate it and also to
   generate an application-specific API that you will use for reporting metric
   data.
#. Modify your application to have it report the metrics that you have defined.
#. Assuming that you have access to an installed Aggregator and SimonWeb server
   (see Simon `Administrator's Guide <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Yst/SimonAdminGuide.html>`_),
   you can now start viewing your metric data as described in Viewing the Metric Data.

This document assumes
that your application is written in C or C++. You can also instrument
applications written in Java, Perl or Python using SWIG. |br|
If that is your goal, you should read this document first, since the
configuration file will be the same, and the API that you will use will have a
direct correspondence to the C++ API described below. |br|
Then look at the sample code provided in
``$SIMON_HOME/client/samples.tgz``
(or ``/usr/releng/internal/simon/<release number>/client/samples.tgz``, if available).




Configuration File
==================


The starting point is to create an XML configuration file which describes:

#. The `blurbs` specifiying the names and types of metrics that your application
   will report.
#. The `reportItems` specifying the names and types of aggregated metrics that
   will be saved in the Simon archive, as well as how those metrics are to be
   computed from the blurb data.

Here is an example configuration file:

.. code-block:: xml

    <?xml version="1.0"?>

    <simonConfig name="exampleApp" >
       
      <!-- BLURBS (AGGREGATOR INPUT) -->
      <blurbMessage name="exampleBlurbMessage" version="0.1" period="2" >
      
        <blurb name="exampleBlurb">
          <tag     name="exampleTag"       type="string"            />
          <gauge   name="exampleGauge"     type="float"   cf="last" />
          <counter name="exampleCounter"   type="long"              />
        </blurb>
         
      </blurbMessage>
      
      <!-- REPORT PERIODS -->
      <reportPeriod name="fast" seconds="10"/>
      <reportPeriod name="slow" seconds="60"/>

      <!-- REPORTS (AGGREGATOR OUTPUT) -->
      <report name="exampleReport" version="0.1" periods="slow,fast">
      
        <reportItem name="byTag" blurb="exampleBlurb">
          <key   name="tag" tag="exampleTag" />
          <value name="gauge">         exampleGauge          </value>
          <value name="counter">       exampleCounter        </value>
          <value name="counterPerSec"> exampleCounter/period </value>
        </reportItem>

        <reportItem name="byNode" blurb="exampleBlurb">
          <key name="node" tag="nodeId" />
          <value name="gauge">         exampleGauge          </value>
          <value name="counter">       exampleCounter        </value>
          <value name="counterPerSec"> exampleCounter/period </value> 
        </reportItem>

        <reportItem name="aggregated" blurb="exampleBlurb">
          <value name="gauge">         exampleGauge          </value>
          <value name="counter">       exampleCounter        </value>
          <value name="counterPerSec"> exampleCounter/period </value> 
        </reportItem>
          
      </report>
        
    </simonConfig>


.. rubric:: The `simonConfig` Element

This is the root element. The `name` attribute is required and identifies the
application being monitored.


.. rubric:: The `blurbMessage` Element

There must be exactly one blurbMessage element. Its attributes are:

* `name`: This is currently required although not really useful. It may be made optional in a future release.
* `version`: Required. The format is up to four dot-separated integers, each in the range 0-255.
* `period`: Required. The period in seconds at which blurb messages are emitted. If zero, blurb messages are sent whenever a metric is updated, rather than being buffered and sent at the end of the period.
* `staleAccumulatorPolicy`: This determines what happens when there is no data for a particular accumulator in a report period, where an accumulator is the combination of a blurb type and a unique set of tags. There is always at least one tag for a blurb, the implicit `nodeId` tag, so in the simple case this policy determines what to do if there is no data for a particular blurb from a particular ``nodeId``. The possible values for the attribute are:
   
   * "`ignore`": this is the default, and it means that the blurb is ignored. No data from this blurb is used in the generation of the Aggregator's report.
   * "`ok`": this means the blurb is used just as if it had been updated. All counter values will be zero, and gauge values will be ``NaN``.
   * "`free`": this is like "ignore" but in addition, the accumulator is freed. This means that the next time a blurb of this type with this particular tag set is encountered, the accumulator will have to be re-created. When this happens, counters have to be reinitialized, so the first blurb after a gap is essentially lost.

In general, staleAccumulatorPolicy="free" should be used when there are transient metrics - metrics which only exist for a period of time. If it isn't used in this case, the Aggregator will use an ever increasing amount of RAM until it grinds to a halt.
|br| "`ignore`" is preferable when you have intermittent metrics, such as counters that are only very occasionally updated. |br|
"`ok`" is currently rather expensive in CPU time, so is probably best avoided 

.. rubric:: The `blurb` Element

There must be one or more `blurb` elements. The name attribute is required, and is used in `reportItems` as described below.
There is also an optional `staleAccumulatorPolicy` attribute, with the same meaning as for the `blurbMessage` element. This enables you to control the policy at the blurb level, overriding whatever policy applies at the blurbMessage level. This can be useful if you have both transient and intermittent metrics.


.. rubric:: The `gauge` and `counter` Element

There must be at least one gauge or counter element. The gauge and counter elements can be interspersed.

* Use `gauge` for metrics where you want to track the `value` of the metric at each point in time: e.g. percentage of disk utilization.
* Use `counter` for metrics where you want to track the `rate of change` in the metric per unit of time. |br|
  For example, you could use a counter to report the total number of queries that a server has handled since it was started, in which case the data to be recorded would be queries per second (or whatever time unit you choose).

**Note**: you can now increment a counter by a negative amount.

Both the `gauge` and `counter` elements have required attributes name and `type`, where `type` can be byte, short, long or float.

The `gauge` element has an additional `cf` attribute which is optional.
It defines a consolidation function, or how the metric values that are reported
each blurb period are to be consolidated over a report period. |br|
The possible values are: `average`, `last`, `max` and `min`. The default is `average`.

Generally, counter wrap-around is handled automatically, but be aware that both counters, and gauges with ``cf==average``, can overflow, giving incorrect results.
|br| The rules for avoiding overflow are:

* With counters, the value of the counter at the end of a report period must never differ from its value at the beginning of the period by more than half the range of the counter type. For example, with a byte counter, the total change in the value of the counter over the report period must be in the range [-128..127]. |br|
  So, for example, if your report period is 10 times your blurb period, the change in the byte counter in a single blurb period should be guaranteed to be in the range [-12..12].
* With averaging gauges, the sum of all the values over the report period must not exceed half the range of the gauge type. So again, if your report period is 10 times your blurb period, the value of a byte gauge should be guaranteed to be in the range [-12..12].

**Note**: if you have a counter that is monotonically increasing, or a gauge that is always positive, a sure sign of overflow is if you start seeing negative values for them.



.. rubric:: The `reportPeriod` Element

There must be at least one `reportPeriod` element, and both its attributes are required:

* `name`: This is used in the report element's periods attribute (see below) to define a period on which the report is to be published.
* `seconds`: Defines the period in seconds.

.. tip:: Tip It is recommended that the report periods be a multiple of the blurb period. Since blurb messages are sent via UDP, they cannot be relied upon to arrive exactly on time (and they may even get lost), so it is good to have the report period be a factor of a few times the blurb period. This way the data in the report will be relatively smooth.



.. rubric:: The `report` Element

There must be at least one report element, and all its attributes are required:

* `name`: When there are multiple reports, this can be used by reporting clients of the Aggregator to select a specific report.
* `version`: Same format as for blurbMessage version.
* `periods`: The name of the reportPeriod(s) at which the Aggregator is to publish this report. If there are multiple reportPeriods, their names are comma-separated.



.. rubric:: The `reportItem` Element

There must be one or more `reportItem` elements in a report. It has two required attributes:

* `name`: This will be used to identify the data in the Aggregator report.
* `blurb`: The name of the blurb on which this `reportItem` is based. If the `reportItem` is based on more than one blurb, the blurb names are comma separated.


.. rubric:: The `key` Element

There may be zero or more key elements in a `reportItem`, and they precede the value elements. There are two required attributes:

* `name`: To appear in the Aggregator report. It is like a column name in database terminology.
* `tag`: The name of a tag element in the blurb. This can also be the special tag name `nodeId`, which is essentially an implicit tag in every blurb. The form of the `nodeId` which will appear in reports is `hostname.pid`, indicating the machine and the process id of the monitoring client.

There will be a separate rrd file for every combination of keys/tags.


.. rubric:: The `value` Element

There must be one or more value elements in a `reportItem`. Again name is a
required attribute, and it is like a column name. There is also an optional
attribute decimals which should be a non-negative integer indicating the number
of decimal places to be used when the value is output in a text format. |br|
If omitted, it defaults to 2 if the expression evaluates to a floating point
number, or 0 if it evaluates to an integer value.

The contents of the value Element are parsed as an arithmetic expression, with
the variables being the names of the counters and gauges in the blurb(s). |br|
The usual operators are supported: ``+, -, * and /``. The arithmetic is done in
floating point.

The value of a named variable is in general an aggregation, as explained below.

The arithmetic expressions can also contain:

* the special variable `period` which provides the value of the report period.
* the special variable `blurbCount` which provides the number of blurb instances
  which were received in the report period. This can be useful for tracking how
  many UDP packets are being lost.
* the special variable `newCount` which provides the number of blurb instances
  received in the report period which contained a new tag combination.
  (See :numref:`metrics_aggregation_new-count`) 
* ``COUNT(tagName)`` where tagName is the name of a tag in the blurb for which
  there is not a corresponding key in the reportItem. This returns a count of
  how many different values of that tag were aggregated over.
* ``AVERAGE(metricName)`` where metricName is the name of a gauge or counter.
  In the case where the metric is being aggregated over some tag(s), this
  returns the average value rather than the total which is the default.


.. note:: 
  * **Names** |br|
    In the `blurbMessage` element and all of its sub-elements, name attributes
    are required to begin with a letter and may contain letters, digits and
    underscores. However, in the report element and its sub-elements, name
    attributes can be arbitrary text.
  * **Versions** |br|
    Simon can handle the case where different application instances are using
    different versions of the `blurbMessage`. In the case where there are
    different versions of a report, Simon uses the most recent version only. |br|
    It is good practice, but not required, to increment the version numbers on
    blurbMessages and reports when you change them. Simon uses a 32-bit hash,
    in addition to the version number, to identify blurb messages and reports,
    so if you don't change the version number there is a small chance,
    about 1 in 4 billion, that the Aggregator will fail to notice that the
    configuration has changed and it will get confused.
 

.. _metrics_aggregation:

How Aggregation Works
=====================

Suppose that we have this `blurb`:

  .. code-block:: xml

    <blurb name="myBlurb">
      <tag name="t1" type="string"/>
      <tag name="t2" type="string"/>
      <gauge name="myMetric" type="float"/>
    </blurb>

and this `reportItem`:

  .. code-block:: xml

    <reportItem name="myReportItem" blurb="myBlurb">
      <key name="T1" tag="t1"/>
      <value name="MyMetric"> myMetric </value>
    </reportItem>


During the report period, the Aggregator will receive multiple blurbs and store
them in a table which might look something like this:

+----+----+----------+
| t1 | t2 | myMetric |
+====+====+==========+
| x  | p  | 0.4      |
+----+----+----------+
| x  | p  | 0.3      |
+----+----+----------+
| x  | q  | 0.6      |
+----+----+----------+
| x  | q  | 0.9      |
+----+----+----------+
| y  | p  | 0.5      |
+----+----+----------+
| y  | p  | 0.5      |
+----+----+----------+
| y  | q  | 0.1      |
+----+----+----------+
| y  | q  | 0.3      |
+----+----+----------+

When it is time to publish a report, the first thing to be done is to
consolidate the blurbs that have arrived over the time period. This means
collecting together all the blurb records which have the same set of tag values,
and, in the case of gauge metrics, applying the consolidation functions to the
corresponding metrics. So, assuming that the consolidation function is
`average`, we get a table like this:

+----+----+----------+
| t1 | t2 | myMetric |
+====+====+==========+
| x  | p  | 0.35     |
+----+----+----------+
| x  | q  | 0.75     |
+----+----+----------+
| y  | p  | 0.5      |
+----+----+----------+
| y  | q  | 0.2      |
+----+----+----------+

The next thing to be done is to aggregate the data. In this case, the tag `t1`
appears in the reportItem, but `t2` does not, so we need to aggregate over `t2`.
This means grouping the consolidated records which have the same value of `t1`
and summing the corresponding metric value. So we get:

+----+----------+
| t1 | myMetric |
+====+==========+
| x  | 1.10     |
+----+----------+
| y  | 0.7      |
+----+----------+

The rows in this table correspond to `reportItems` to be emitted in the report.
The final step is to evaluate the expression in the value element, which in this
case is just the variable `myMetric`. |br|
So, the Aggregator report would contain an
XML representation of these two rows of data. If the value expression above had
been `AVERAGE(myMetric)`, then the values would have been averaged across the
different values of `t2`, so the output would have been an XML representation of
this:

+----+----------+
| t1 | myMetric |
+====+==========+
| x  | 0.55     |
+----+----------+
| y  | 0.35     |
+----+----------+

This example has used a gauge metric. A counter would be treated the same,
except that the consolidation phase for a counter means subtracting the value it
had at the beginning of the report period from its most recent value. |br|
It is often useful to divide the value of a counter variable by `period` to get
`documents/queries/whatever` per second.

**Note:** that in this example I have ignored the implicit tag `nodeId` which
exists for every blurb. To make the example accurate, either `t1` or `t2` would
have to actually be `nodeId`.



.. _metrics_aggregation_new-count:

Special Variable `newCount`
---------------------------

The motivation for introducing this variable is to support the use case
described here, although there may be other uses.

In situations where there is expected to be a single application instance running
on a machine continuously, it may be useful to track the number of times that
the application restarts in a report period, both on a `per-node` basis and
`totalled` across the cluster.

Here is an example of how this can be done.

  .. code-block:: xml

    <blurb name="dummy">
      <tag name="nodeName"  type="string" />
    </blurb>

    <!-- rest of blurbs -->

    <reportItem name="perNode" blurb="dummy">
      <key name="nodeName" tag="nodeName" />
      <value name="restarts"> newCount </value>
    </reportItem>

    <reportItem name="totals" blurb="dummy">
      <value name="restarts"> newCount </value>
    </reportItem>


This "`dummy`" blurb will need to be updated at least once every blurb period,
even though it contains no metrics. |br|
As an alternative to having a dummy blurb like this, you could use any existing
blurb which has only the single tag nodeName. Similarly, you can put the
"`restarts`" values in existing reportItems, instead of introducing new ones
like this, if you have one with the key nodeName and one with no key.


Example Hadoop DFS metrics in
`hadoop-dfs-simon-metrics.xml#L182 - namenode operations <https://git.vzbuilders.com/hadoop/hadoop_configs/blob/y-branch-2.10/confSupport/templates/hadoop-dfs-simon-metrics.xml#L182>`_:

  .. code-block:: xml

      <blurb name="namenode">
        <tag     name="Hostname"          type="string" />
        <tag     name="SessionId"         type="string" />
        <!-- .... -->
      </blurb>

      <blurb name="FSDirectory">
        <tag     name="Hostname"            type="string"  />
        <tag     name="SessionId"           type="string"  />
      </blurb>

      <report name="DFS" version="2.8.5.35" periods="perMinute">

        <reportItem name="namenode operations" blurb="namenode,FSDirectory">
          <key   name="node" tag="Hostname" />
          <value name="#pids">            count(nodeId) </value>
          <value name="#restarts">        newCount      </value>
          <!-- .... -->
        </reportItem>
        <!-- .... -->

      </report>



Simon on the YGrid
==================

Current Instances
-----------------

Red
~~~

============== ================================================================== ===============================================================================
*Cluster*      *hostname*                                                         *reports*
============== ================================================================== ===============================================================================
Dilithium Red  `smrrd432.red <http://smrrd432.red.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,yarn <http://smrrd432.red.ygrid.yahoo.com:9999/data>`__
^              `smrrd433.red <http://smrrd433.red.ygrid.yahoo.com:9999/status>`__ `system <http://smrrd433.red.ygrid.yahoo.com:9999/data>`__
^              `smrrd431.red <http://smrrd431.red.ygrid.yahoo.com:9999/status>`__ `disks <http://smrrd431.red.ygrid.yahoo.com:9999/data>`__
Phazon Red     `smrrd426.red <http://smrrd426.red.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,yarn <http://smrrd426.red.ygrid.yahoo.com:9999/data>`__
^              `smrrd427.red <http://smrrd427.red.ygrid.yahoo.com:9999/status>`__ `system <http://smrrd427.red.ygrid.yahoo.com:9999/data>`__
^              `smrrd428.red <http://smrrd428.red.ygrid.yahoo.com:9999/status>`__ `disks <http://smrrd428.red.ygrid.yahoo.com:9999/data>`__
Bassnium Red   `smrrd421.red <http://smrrd421.red.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,yarn <http://smrrd421.red.ygrid.yahoo.com:9999/data>`__
^              `smrrd420.red <http://smrrd420.red.ygrid.yahoo.com:9999/status>`__ `system <http://smrrd420.red.ygrid.yahoo.com:9999/data>`__
^              `smrrd422.red <http://smrrd422.red.ygrid.yahoo.com:9999/status>`__ `disks <http://smrrd422.red.ygrid.yahoo.com:9999/data>`__
Mithril Red    `smrrd424.red <http://smrrd424.red.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,yarn <http://smrrd424.red.ygrid.yahoo.com:9999/data>`__
^              `smrrd423.red <http://smrrd423.red.ygrid.yahoo.com:9999/status>`__ `system,disks <http://smrrd423.red.ygrid.yahoo.com:9999/data>`__
Kryptonite Red `smrrd429.red <http://smrrd429.red.ygrid.yahoo.com:9999/status>`__ `all <http://smrrd429.red.ygrid.yahoo.com:9999/data>`__
Axonite Red    `smrrd430.red <http://smrrd430.red.ygrid.yahoo.com:9999/status>`__ `All <http://smrrd430.red.ygrid.yahoo.com:9999/data>`__
Relux Red      `smrrd410.red <http://smrrd410.red.ygrid.yahoo.com:9999/status>`__ `system,disks,jvm,hdfs,hbase <http://smrrd410.red.ygrid.yahoo.com:9999/data>`__
Lux Red        `smrrd434.red <http://smrrd434.red.ygrid.yahoo.com:9999/status>`__ `system,disks,jvm,hdfs,hbase <http://smrrd434.red.ygrid.yahoo.com:9999/data>`__
Ebony Red      TBD                                                               
Fubarite Red   TBD                                                               
Strontium Red  TBD                                                               
============== ================================================================== ===============================================================================

Tan
~~~

============ ================================================================== =============================================================================== ================================================================
*Cluster*    *hostname*                                                         *reports*                                                                       *cname*
============ ================================================================== =============================================================================== ================================================================
Uranium Tan  `smrrd321.tan <http://smrrd321.tan.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,hbase <http://smrrd321.tan.ygrid.yahoo.com:9999/data>`__              `sm0 <http://uraniumtan-sm0.tan.ygrid.yahoo.com:9999/status>`__
^            `smrrd320.tan <http://smrrd320.tan.ygrid.yahoo.com:9999/status>`__ `system,disks <http://smrrd320.tan.ygrid.yahoo.com:9999/data>`__                `sm1 <http://uraniumtan-sm1.tan.ygrid.yahoo.com:9999/status>`__
Zanium Tan   `smrrd323.tan <http://smrrd323.tan.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,hbase <http://smrrd323.tan.ygrid.yahoo.com:9999/data>`__              `sm0 <http://zaniumtan-sm0.tan.ygrid.yahoo.com:9999/status>`__
^            `smrrd322.tan <http://smrrd322.tan.ygrid.yahoo.com:9999/status>`__ `system,disks <http://smrrd322.tan.ygrid.yahoo.com:9999/data>`__                `sm1 <http://zaniumtan-sm1.tan.ygrid.yahoo.com:9999/status>`__
Bassnium Tan `smrrd325.tan <http://smrrd325.tan.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,hbase <http://smrrd325.tan.ygrid.yahoo.com:9999/data>`__              `sm0 <http://bassniumtan-sm0.tan.ygrid.yahoo.com:9999/status>`__
^            `smrrd324.tan <http://smrrd324.tan.ygrid.yahoo.com:9999/status>`__ `system,disks <http://smrrd324.tan.ygrid.yahoo.com:9999/data>`__                `sm1 <http://bassniumtan-sm1.tan.ygrid.yahoo.com:9999/status>`__
Phazon Tan   `smrrd327.tan <http://smrrd327.tan.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,hbase <http://smrrd327.tan.ygrid.yahoo.com:9999/data>`__              `sm0 <http://phazontan-sm0.tan.ygrid.yahoo.com:9999/status>`__
^            `smrrd326.tan <http://smrrd326.tan.ygrid.yahoo.com:9999/status>`__ `system,disks <http://smrrd326.tan.ygrid.yahoo.com:9999/data>`__                `sm1 <http://phazontan-sm1.tan.ygrid.yahoo.com:9999/status>`__
Tiberium Tan `smrrd329.tan <http://smrrd329.tan.ygrid.yahoo.com:9999/status>`__ `jvm,hdfs,yarn <http://smrrd329.tan.ygrid.yahoo.com:9999/data>`__              
^            `smrrd328.tan <http://smrrd328.tan.ygrid.yahoo.com:9999/status>`__ `system,disks <http://smrrd328.tan.ygrid.yahoo.com:9999/data>`__               
Lux Tan      `smrrd330.tan <http://smrrd330.tan.ygrid.yahoo.com:9999/status>`__ `system,disks,jvm,hdfs,hbase <http://smrrd330.tan.ygrid.yahoo.com:9999/data>`__
Relux Tan    `smrrd331.tan <http://smrrd331.tan.ygrid.yahoo.com:9999/status>`__ `system,disks,jvm,hdfs,hbase <http://smrrd331.tan.ygrid.yahoo.com:9999/data>`__
============ ================================================================== =============================================================================== ================================================================

Blue
~~~~

+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Cluster        | hostname                                                                         | reports                                                                                                                          | status                                                                |
+================+==================================================================================+==================================================================================================================================+=======================================================================+
| Axonite Blue   |  `smrrd122.blue <http://smrrd122.blue.ygrid.yahoo.com:9999/status>`__            |  `system,disks,jvm,hdfs,yarn <http://smrrd122.blue.ygrid.yahoo.com:9999/data>`__                                                 |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Jet Blue       |  `smrrd124.blue <http://smrrd124.blue.ygrid.yahoo.com:9999/status>`__            |  `system,disks <http://smrrd124.blue.ygrid.yahoo.com:9999/data>`__                                                               |  `sm1 <http://jetblue-sm1.blue.ygrid.yahoo.com:9999/status>`__        |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| ^              |  `smrrd125.blue <http://smrrd125.blue.ygrid.yahoo.com:9999/status>`__            |  `jvm,hdfs,yarn <http://smrrd125.blue.ygrid.yahoo.com:9999/data>`__                                                              |  `sm0 <http://jetblue-sm0.blue.ygrid.yahoo.com:9999/status>`__        |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Dilithium Blue |  `smrrd121.blue <http://smrrd121.blue.ygrid.yahoo.com:9999/status>`__            |  `jvm,hdfs,yarn <http://smrrd121.blue.ygrid.yahoo.com:9999/data>`__ |br| suspended metrics collection UI available for old data  |  `sm0 <http://dilithiumblue-sm0.blue.ygrid.yahoo.com:9999/status>`__  |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| ^              |  `smrrd139.blue <https://dilithiumblue-sm3.blue.ygrid.yahoo.com:9999/status>`__  |  `yarn <https://dilithiumblue-sm3.blue.ygrid.yahoo.com:9999/data>`__                                                             |  `sm3 <http://dilithiumblue-sm3.blue.ygrid.yahoo.com:9999/status>`__  |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| ^              |  `smrrd144.blue <https://dilithiumblue-sm4.blue.ygrid.yahoo.com:9999/status>`__  |  `hdfs,rpc,jvm <https://dilithiumblue-sm4.blue.ygrid.yahoo.com:9999/data>`__                                                     |  `sm4 <http://dilithiumblue-sm4.blue.ygrid.yahoo.com:9999/status>`__  |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| ^              |  `smrrd120.blue <http://smrrd120.blue.ygrid.yahoo.com:9999/status>`__            |  `system,disks <http://smrrd120.blue.ygrid.yahoo.com:9999/data>`__                                                               |  `sm1 <http://dilithiumblue-sm1.blue.ygrid.yahoo.com:9999/status>`__  |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| ^              |  `smrrd128.blue <http://smrrd128.blue.ygrid.yahoo.com:9999/status>`__            |  `disks <http://smrrd128.blue.ygrid.yahoo.com:9999/data>`__                                                                      |  `sm2 <http://dilithiumblue-sm2.blue.ygrid.yahoo.com:9999/status>`__  |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Bassnium Blue  |  `smrrd136.blue <http://smrrd136.blue.ygrid.yahoo.com:9999/status>`__            |  `jvm,hdfs,yarn <http://smrrd136.blue.ygrid.yahoo.com:9999/data>`__                                                              |  `sm0 <http://bassniumblue-sm0.blue.ygrid.yahoo.com:9999/status>`__   |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| ^              |  `smrrd137.blue <http://smrrd137.blue.ygrid.yahoo.com:9999/status>`__            |  `system,disks <http://smrrd137.blue.ygrid.yahoo.com:9999/data>`__                                                               |  `sm1 <http://bassniumblue-sm1.blue.ygrid.yahoo.com:9999/status>`__   |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| ^              |  `smrrd138.blue <http://smrrd138.blue.ygrid.yahoo.com:9999/status>`__            |  `disks <http://smrrd138.blue.ygrid.yahoo.com:9999/data>`__                                                                      |  `sm2 <http://bassniumblue-sm2.blue.ygrid.yahoo.com:9999/status>`__   |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Oxium Blue     |  `smrrd123.blue <http://smrrd123.blue.ygrid.yahoo.com:9999/status>`__            |  `system,disks,jvm,hdfs,yarn <http://smrrd123.blue.ygrid.yahoo.com:9999/data>`__                                                 |  `sm0 <http://oxiumblue-sm0.blue.ygrid.yahoo.com:9999/status>`__      |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Uranium Blue   |  `smrrd102.blue <http://smrrd102.blue.ygrid.yahoo.com:9999/status>`__            |  `system,disks,jvm,hdfs,yarn <http://smrrd102.blue.ygrid.yahoo.com:9999/data>`__                                                 |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Relux Blue     |  `smrrd103.blue <http://smrrd103.blue.ygrid.yahoo.com:9999/status>`__            |  `system,disks,jvm,hdfs,hbase <http://smrrd103.blue.ygrid.yahoo.com:9999/data>`__                                                |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Lux Blue       |  `smrrd101.blue <http://smrrd101.blue.ygrid.yahoo.com:9999/status>`__            |  `system,disks,jvm,hdfs,hbase <http://smrrd101.blue.ygrid.yahoo.com:9999/data>`__                                                |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| ^              |  `smrrd126.blue <http://smrrd126.blue.ygrid.yahoo.com:9999/status>`__            |  `All <http://smrrd126.blue.ygrid.yahoo.com:9999/data>`__                                                                        |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Relux Blue     |  `smrrd127.blue <http://smrrd127.blue.ygrid.yahoo.com:9999/status>`__            |  `All <http://smrrd127.blue.ygrid.yahoo.com:9999/data>`__                                                                        |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Vizorium Blue  |  `smrrd127.blue <http://smrrd127.blue.ygrid.yahoo.com:9999/status>`__            |  `All <http://smrrd127.blue.ygrid.yahoo.com:9999/data>`__                                                                        |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Perf Blue      |  `smrrd127.blue <http://smrrd127.blue.ygrid.yahoo.com:9999/status>`__            |  `All <http://smrrd127.blue.ygrid.yahoo.com:9999/data>`__                                                                        |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Iridium Blue   | TBD                                                                              |                                                                                                                                  |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+
| Ebony Blue     | TBD                                                                              |                                                                                                                                  |                                                                       |
+----------------+----------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------+

Configuration Details
---------------------

Components:
~~~~~~~~~~~

Aggregator
   One instance per cluster, vip, `-agg0` should point to
   this host. All clients (`simond`, `hadoop-metrics.properties`) should use
   the vip as the aggregator

SimonWeb
   Contacts the aggregator and writes the metrics to rrds
   to local disks. For scalability, depending on the amount of metric
   data collected, there may be multiple instances of SimonWeb
   configured for a cluster. |br|
   Each instance uses a different url, eg: for `mithrilgold`, `gsadm2000` is
   configured to collect mapred and dfs metrics. |br|
   In ``simonweb.properties`` aggregator url is set to
   ``http://mithrilgold-agg0:4080/Aggregator/data?report=(dfs|mapreduce).*``.
   |br|
   The data directory is NFS exported to SimonWeb Fronted.

SimonWeb Frontend
   Not configured to use aggregator. External SimonWeb data directories are
   mounted via NFS. Used as the main interface to access all the metric data
   and generate graphs.


.. _metrics_interumentations:


QE environment
==============


Installation
------------

Installation of simon services on a QE cluster involves:

* install `simon` package for simon-web.
* install `simon_aggregator` for aggregator
* configuring `simon` and `simon_aggregator`.
* configuring simon plugin.
  
Two aggregators are run by default. JVM stat, Host stat (from native `simond`,
not the plugin), HDFS stat and YARN stats are divided among the two aggregators.

  .. code-block:: bash

    -bash-4.2$ yinst install simon simon_aggregator
    Touch YubiKey:
    yinst: Identifying packages for installation ...
    yinst: Found: simon-1.4.5.2
    yinst: Found: simon_aggregator-1.4.5.2
    yinst: Adding simon_aggregator-1.4.5.2 prerequisite: daemontools_y-0.78.47.7
    yinst: Checking prerequisites ... [OK]


Configurations
--------------



Configure simon servers
~~~~~~~~~~~~~~~~~~~~~~~

Let’s do aggregator first. Without this the aggregators will only listen
to the local interface.

.. code-block:: bash

   -bash-4.2$ yinst set simon_aggregator.bind_host=0.0.0.0

Then restart the service.

.. code-block:: bash

   -bash-4.2$ yinst restart simon_aggregator
   yinst: simon_aggregator-1.4.5.2: restarting ...


Now let’s move on to the simon web server. This is how data is
processed, we are copying how production simon servers are configured.

.. code-block:: bash

   -bash-4.2$ yinst set simon.archives="RRA:AVERAGE:0.5:1:129600 RRA:AVERAGE:0.5:5:103680"

The simon server needs to be told where to get the data.
``simon.agg_uri`` is a list of aggregator addresses that will provide
reports. If a base URL is give, it will pull all data that is available.
Optionally, it can only subscribe to a subset of data by specifying
“application”. For QE purposes, we would want all, so specifying the
base URL is sufficient.

.. code-block:: bash

   -bash-4.2$ yinst set simon.agg_uri="http://openqe55blue-n4.blue.ygrid.yahoo.com:9990/Aggregator/data \
    http://openqe55blue-n4.blue.ygrid.yahoo.com:9991/Aggregator/data"
    yinst: The following config files were changed:
    yinst:   M /home/y/conf/simon/simonweb.properties


Restart the service. The simon server was probably not started
automatically after installation, so the `"stop"` phase will fail, but it
is okay to ignore.

.. code-block:: bash

   -bash-4.2$ yinst restart simon
   yinst: simon-1.4.5.2: restarting ... 


Configure Hadoop services
~~~~~~~~~~~~~~~~~~~~~~~~~~

On QE clusters, the aggregator address can be set using `yinst` with the
proper yinst installation root specified. The following tells you the
config package you need to change settings for.

.. code-block:: bash

   -bash-4.2$ yinst ls -root /home/gs/gridre/yroot.openqe55blue |grep -i HadoopConfig
   HadoopConfigopenstacklargedisk-2.8.5.24.2001301723

Now, set the aggregator hostname.

.. code-block:: bash

   -bash-4.2$ yinst set \
    HadoopConfigopenstacklargedisk.TODO_SIMON_AGGREGATOR_HOST=openqe55blue-n4.blue.ygrid.yahoo.com \
    -root /home/gs/gridre/yroot.openqe55blue

This will set the hostname in ``hadoop-metrics2.properties`` and
``simond.properties``. ``simond`` is like hadoop simon plugin, but is a
standalone native program that reports system stats. It is installed on
the system in the form of a RPM package. The RHEL7 version comes with a
systemd profile for auto start.

You will need to set the aggregator host on all cluster nodes and
restart all hadoop services.

Limiting reports to simon web server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When setting the aggregaor URL, you can limit the type of reports to
pull. For example,

.. code-block:: bash

   aggregators=http://my-agg0.blue.ygrid.yahoo.com:9990/Aggregator/data?
   application=hadoop.dfs&reportItem=FSNamesystem%20status&reportItem=namenode%20operations&
   reportItem=hdfs%20throughput&application=simond&reportItem=percluster&application=jvm&
   reportItem=by%20process%20name&application=hadoop.rpc&reportItem=rpc%20overview%20by%20port&
   reportItem=by%20node%20name&by%20node%20name.ProcessName=namenode&by%20node%20name.ProcessName=resourcemanager


Checking service status
------------------------

Each simon service has a status page. For example,

.. code-block:: bash

   http://openqe55blue-n4.blue.ygrid.yahoo.com:9999/status/
   http://openqe55blue-n4.blue.ygrid.yahoo.com:9990/Aggregator/status
   http://openqe55blue-n4.blue.ygrid.yahoo.com:9991/Aggregator/status

Port 9999 is open so, you can easily access it, but 9990 and 9991 are
blocked. You will need to run ssh tunnel to get to these pages.

.. code-block:: bash

   $ ssh -L 8080:openqe55blue-n4.blue.ygrid.yahoo.com:9990 openqe55blue-n4.blue.ygrid.yahoo.com

and point the browser to ``http://localhost:8080/Aggregator/status``

There are other "creative" solutions like running a proxy server on the
gw machine.
   