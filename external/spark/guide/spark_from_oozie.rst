.. _sfo:

Spark From Oozie
================

This guide will walk you through launching spark jobs using oozie.

.. _sfo_spark_action:

Using Spark Action
------------------

The Yahoo version of oozie recently started supporting the Spark action in oozie.

See instructions at for various workflow.xml setups: https://git.ouroath.com/pages/hadoop/docs/oozie/workflows.html

- create an hdfs directory for your oozie application, for example spark_oozie/apps.
- upload the application jar to be run to hdfs (for example put it in spark_oozie/apps/lib/yourapplication.jar), your application jar should not include hadoop or spark.
- update workflow.xml (see examples at https://git.ouroath.com/pages/hadoop/docs/oozie/workflows.html) and upload into hdfs: spark_oozie/apps/spark/workflow.xml
  - Note put your jobtracker and namenode in a global section in your workflow.xml

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='SparkTest'>
    <global>
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
    </global>
    <start to="spark-node"/>
    <action name='spark-node'/>
    <spark xmlns="uri:oozie:spark-action:0.2"/>
  </workflow-app>

- job.properties file - make sure to update for your cluster

::

  nameNode=hdfs://your_namenode:8020
  jobTracker=your_resourcemanager:8032
  queueName=default
  wfRoot=spark_oozie
  oozie.libpath=/user/${user.name}/${wfRoot}/apps/lib
  oozie.wf.application.path=${nameNode}/user/${user.name}/${wfRoot}/apps/spark

- export oozie url according to the cluster you are on: ``export OOZIE_URL=http://your_cluster-oozie.colo.ygrid.yahoo.com:4080/oozie``
- example ``export OOZIE_URL=https://axonitered-oozie.red.ygrid.yahoo.com:4443/oozie/``
- run it: ``oozie job -run -config job.properties -auth KERBEROS``

Full example workflow.xml for reference

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='SparkTest'>
    <global>
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
    </global>
    <start to="spark-node"/>
    <action name='spark-node'>
      <spark xmlns="uri:oozie:spark-action:0.2">
        <configuration>
          <property>
            <name>oozie.action.sharelib.for.spark</name>
            <value>spark_current</value>
          </property>
        </configuration>
        <master>yarn</master>
        <mode>cluster</mode>
        <name>Spark-oozietest</name>
        <class>yahoo.spark.SparkWordCount</class>
        <jar>yahoo-spark_2.10-1.0-jar-with-dependencies.jar</jar>
        <spark-opts>--queue default</spark-opts>
        <arg>README.md</arg>
        <arg>sparkwordout22</arg>
      </spark>
      <ok to="end" />
      <error to="fail" />
    </action>
    <kill name="fail">
      <message>Script failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name='end' />
  </workflow-app>

.. _sfo_debug_logging:

Enable Debug Logging
--------------------
If you just want to change the Spark core log level you can specify it in the configuration section of your workflow.xml:

.. code-block:: xml

  <configuration>
      <property>
          <name>oozie.spark.log.level</name>
          <value>DEBUG</value>
      </property>
   </configuration>

If you want to specify a custom log4j.properties file you can upload a log4j.properties file to your oozie lib directory pass it in the spark-opts:
::

  --files ./log4j.properties --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j.properties" --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j.properties"


.. _sfo_other_namenode:

Accessing other namenodes
-------------------------

This section describes how to access other namenodes through oozie. In your workflow.xml configuration add the servers with oozie.launcher.mapreduce.job.hdfs-servers, similar to below:

.. code-block:: xml

  <property>
    <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
    <value>hdfs://jetblue-nn1.blue.ygrid.yahoo.com,hdfs://phazonblue-nn1.blue.ygrid.yahoo.com</value>
  </property>

This allows you to use the spark --conf spark.yarn.access.hadoopFileSystems=hdfs://phazonblue-nn1.blue.ygrid.yahoo.com option

.. _sfo_oozie_pyspark:

PySpark with Python 3.6
-----------------------
This section describes how to run Spark 2.x on Yarn through Oozie using PySpark with Python 3.6 Example.

.. _sfo_pyspark_default_python:

PySpark & Default Grid Python Installation
------------------------------------------

This section describes how to run Spark on Yarn through Oozie using PySpark with default grid installed version of Python.

You can use the default python installed on the grid :ref:`swp` with oozie. This happens by default without needing to specify extra parameters.

workflow.xml

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='SparkTest'>
  <start to='spark-node' />
  <action name='spark-node'>
    <spark xmlns="uri:oozie:spark-action:0.2">
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
      <configuration>
        <property>
          <name>oozie.action.sharelib.for.spark</name>
          <value>spark_current</value>
        </property>
      </configuration>
      <master>${master}</master>
      <mode>${mode}</mode>
      <name>spark-pyspark</name>
      <jar>${nameNode}/user/tgraves/oozie-pyspark2/lib/pi.py</jar>
      <spark-opts>--queue grideng --num-executors 5 --executor-memory 7g --driver-memory 7g </spark-opts>
    </spark>
    <ok to="end" />
    <error to="fail" />
  </action>
  <kill name="fail">
    <message>Workflow failed, error
      message[${wf:errorMessage(wf:lastErrorNode())}]
    </message>
  </kill>
  <end name='end' />
  </workflow-app>

job.properties

::

  nameNode=hdfs://axonitered-nn1.red.ygrid.yahoo.com:8020
  jobTracker=axonitered-jt1.red.ygrid.yahoo.com:8032
  master=yarn
  mode=cluster
  queueName=default
  wfRoot=oozie-pyspark2
  oozie.libpath=/user/${user.name}/${wfRoot}/apps/lib
  oozie.wf.application.path=${nameNode}/user/${user.name}/${wfRoot}/apps/spark

.. _sfo_oozie_sparkr:

Using Spark R
------------------------------
This section describes how to run SparkR through Oozie. Since sparkr.zip is missing from sharelib, you need to follow the instructions below to upload sparkr.zip to HDFS and specify spark.rpackage.home.

- Manually copy ``$SPARK_HOME/R/lib/sparkr.zip`` on gateway to ``<oozie.libpath>/R/lib/sparkr.zip`` on HDFS.
- In <spark-opts> of workflow.xml, add ``--conf "spark.rpackage.home=./"``.

workflow.xml example

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='SparkRTest'>
    <global>
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
    </global>
    <start to="spark-node"/>
    <action name='spark-node'>
      <spark xmlns="uri:oozie:spark-action:0.2">
        <configuration>
          <property>
            <name>oozie.action.sharelib.for.spark</name>
            <value>spark_latest</value>
          </property>
        </configuration>
        <master>yarn</master>
        <mode>cluster</mode>
        <name>spark_R_test</name>
        <jar>dataframe.R</jar>
        <spark-opts>--queue default --conf "spark.rpackage.home=./"</spark-opts>
        <arg>people.json</arg>
      </spark>
      <ok to="end" />
      <error to="fail" />
    </action>
    <kill name="fail">
      <message>Script failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name='end' />
  </workflow-app>

job.properties example

::

  nameNode=hdfs://axonitered-nn1.red.ygrid.yahoo.com:8020
  jobTracker=axonitered-jt1.red.ygrid.yahoo.com:8032
  wfRoot=spark_R_test
  oozie.libpath=/user/${user.name}/${wfRoot}/apps/lib
  oozie.wf.application.path=${nameNode}/user/${user.name}/${wfRoot}/apps/spark

.. _sfo_custom_version:

Running a Different Spark Version
---------------------------------
To use a different or an older version of spark from oozie you need to do the following apart from following the instructions listed above:
- Remove spark sharelib from workflow.xml, i.e. remove the property below:

.. code-block:: xml

  <configuration>
    <property>
      <name>oozie.action.sharelib.for.spark</name>
      <value>spark_current</value>
    </property>
  </configuration>

- Install the required version of spark on the gateway. Refer to the instructions to perform self installation of yspark_yarn package in :ref:`soy_selfinstall`
- Upload the yspark-jars-version.tgz file present in share/spark (e.g. yspark-jars-2.4.5.x.tgz) to an hdfs directory (example: hdfs:///user/YOUR_USERNAME/spark_jars_tgz/yspark-jars-2.4.5.x.tgz). You must also upload all the jars present in share/spark/lib to a separate directory (hdfs:///user/YOUR_USERNAME/spark_lib/spark_jars) in hdfs.
- Upload the corresponding version of conf, i.e. spark-defaults.conf from the cluster configs in $SPARK_CONF_DIR (see below for example) into hdfs: hdfs:///user/YOUR_USERNAME/spark_lib/spark_jars/spark-defaults.conf.
- If you are using hive you also need hive-site.xml and the datanucleus jars
- In the job.properties, you have to specify two paths to the oozie.libpath property like below, the example below assumes your normal oozie workflow lib dir is /user/${user.name}/spark_oozie/apps/lib, so essentially you are just adding in the libpath for where you put spark in the steps above: oozie.libpath=/user/${user.name}/spark_oozie/apps/lib,/user/${user.name}/spark_lib/spark_jars
- Set the config --conf spark.yarn.archive=hdfs:///user/YOUR_USERNAME/spark_jars_tgz/yspark-jars-2.4.5.x.tgz in <spark-opts> in your workflow.xml file.
- If you are accessing hive, you cannot provide your own hive-site.xml as oozie doesn't support this. You need to set the sharelib entry to fetch hive-site.xml for you. You can do this by adding a configuration as below:

.. code:: xml

  <configuration>
    <property>
      <name>oozie.action.sharelib.for.spark</name>
      <value>hcat_current</value>
    </property>
  </configuration>

.. note:: Using the sharelib entry `hcat_current` for pulling hive-site.xml can cause your spark job to fail because of conflicting or unwanted jars being pulled into the classpath. Try to avoid this as much as you can.

.. _sfo_change_configs:

Changing/Adding configs
-----------------------
Change or add any configs you need by using the --conf option to spark-submit

.. _sfo_workflow_sparkpi:

Access Hive from Spark via Oozie
--------------------------------

These are instructions in addition to the ones above in Running Spark on Yarn through Oozie section. Note that if you are using the spark sharelib, it automatically pulls in hive-site.xml and any other jars you need.
Oozie automatically picks the required hive credentials by specifying the credential section below.
Also Note you should only be accessing the hive instance on the cluster you are running on. If you need data from a hive not on the current cluster you should copy it or setup metadata replication. Talk to the hive team about this.

Example

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='SparkHive'>
    <credentials>
      <credential name='hive_credentials' type='hcat'>
      </credential>
    </credentials>
    <start to='spark-node' />
    <action name="spark-node" cred='hive_credentials'>
      <spark xmlns="uri:oozie:spark-action:0.2">
        <job-tracker>${jobTracker}</job-tracker>
        <name-node>${nameNode}</name-node>
        <configuration>
          <property>
            <name>oozie.action.sharelib.for.spark</name>
            <value>spark_latest</value>
          </property>
        </configuration>
        <master>${master}</master>
        <mode>${mode}</mode>
        <name>Spark-Hive</name>
        <class>yahoo.spark.SparkSqlHive</class>
        <jar>yahoo-spark_2.11-1.0-jar-with-dependencies.jar</jar>
        <spark-opts>--conf spark.yarn.security.tokens.hive.enabled=false</spark-opts>
      </spark>
      <ok to="end" />
      <error to="fail" />
    </action>
    <kill name="fail">
      <message>Workflow failed, error
        message[${wf:errorMessage(wf:lastErrorNode())}]
      </message>
    </kill>
    <end name='end' />
  </workflow-app>


.. _sfo_hbase:

Access HBase from Spark via Oozie
---------------------------------

These are instructions in addition to the ones above in Running Spark on Yarn through Ooozie section.
Oozie automatically picks the required hbase credentials by specifying the credential section for below.

Example

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='SparkHBaseViaOozieTest'>
    <global>
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
    </global>
    <credentials>
      <credential name="hbase.cert" type="hbase"></credential>
    </credentials>
    <start to="spark-node"/>
    <action name='spark-node' cred="hbase.cert">
      <spark xmlns="uri:oozie:spark-action:0.2">
        <configuration>
          <property>
            <name>oozie.action.sharelib.for.spark</name>
            <value>spark_latest,hbase_current,hbase_conf_reluxred</value>
          </property>
        </configuration>
        <master>yarn</master>
        <mode>cluster</mode>
        <name>SparkHBaseViaOozieTest</name>
        <class>com.yahoo.spark.starter.SparkClusterHBase</class>
        <jar>spark-starter-2.0-SNAPSHOT-jar-with-dependencies.jar</jar>
        <spark-opts>--queue default --conf "spark.yarn.security.tokens.hbase.enabled=false" --conf "spark.yarn.security.tokens.hive.enabled=false"</spark-opts>
        <arg>${tableName}</arg>
      </spark>
      <ok to="end" />
      <error to="fail" />
    </action>
    <kill name="fail">
      <message>Script failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name='end' />
  </workflow-app>

An example of this is bundled with the spark-starter to try out. You can get the Spark HBase example here : https://git.ouroath.com/hadoop/spark-starter/blob/branch-2.0/src/main/scala/com/yahoo/spark/starter/SparkClusterHBase.scala You can get the Spark HBase Oozie files here : https://git.ouroath.com/hadoop/spark-starter/tree/branch-2.0/src/main/resources/oozie/hbase

- export oozie url according to the cluster you are on: ``export OOZIE_URL=http://your_cluster-oozie.colo.ygrid.yahoo.com:4443/oozie``
- run it: ``oozie job -run -config job.properties -auth KERBEROS``

