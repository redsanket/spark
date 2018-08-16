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

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='TomSparkTest'>
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

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='TomSparkTest'>
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

This allows you to use the spark --conf spark.yarn.access.namenodes=hdfs://phazonblue-nn1.blue.ygrid.yahoo.com option

.. _sfo_oozie_pyspark:

PySpark with Python 3.6
-----------------------
This section describes how to run Spark 2.2 on Yarn through Oozie using PySpark with Python 3.6 Example.

Spark 2.2 automatically picks up python 3.6 for you so as long as you are using sharelib you should automatically get python 3.6. You can get python 2.7 by overriding the configs talked about `here <https://twiki.corp.yahoo.com/view/Grid/PySparkIPython#Using_Python_2.7_473.6_installed_in_Grid_with_Pyspark_91_BEING_DEPLOYED_not_on_all_GRIDS_YET_93>`_

.. _sfo_pyspark_default_python:

PySpark & default grid Python installation 
------------------------------------------

This section describes how to run Spark on Yarn through Oozie using PySpark with default grid installed version of Python.

You can use the default python installed on the grid (reference https://twiki.corp.yahoo.com/view/Grid/PySparkIPython) with oozie. This happens by default without needing to specify extra parameters.

workflow.xml

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='TomSparkTest'>
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

.. _sfo_pyspark_grid_python-2.7:

PySpark & grid installed Python 2.7 Example (Spark 2.x)
-------------------------------------------------------

You can use the python 2.7 or 3.6 installed on the grid (reference https://twiki.corp.yahoo.com/view/Grid/PySparkIPython) with oozie.

workflow.xml

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='TomSparkTest'>
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
      <spark-opts>--queue grideng --num-executors 5 --executor-memory 7g --driver-memory 7g --conf spark.pyspark.python=./python27/bin/python2.7 --conf spark.executorEnv.LD_LIBRARY_PATH=./python27/lib --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./python27/lib</spark-opts>
      <archive>hdfs:///sharelib/v1/python27/python27.tgz#python27</archive>
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

job.properties:

::

  nameNode=hdfs://axonitered-nn1.red.ygrid.yahoo.com:8020
  jobTracker=axonitered-jt1.red.ygrid.yahoo.com:8032
  master=yarn
  mode=cluster
  queueName=default
  wfRoot=oozie-pyspark2
  oozie.libpath=/user/${user.name}/${wfRoot}/apps/lib
  oozie.wf.application.path=${nameNode}/user/${user.name}/${wfRoot}/apps/spark

.. _sfo_pyspark_custom_python-2.7:

PySpark & own version of Python 2.7 Example (Spark 2.x)
-------------------------------------------------------

To run Python 2.7 you need to first follow the instructions to get Python 2.7 here: https://twiki.corp.yahoo.com/view/Grid/PySparkIPython Those instructions put Python2.7 into HDFS in a directory like /user/tgraves. Once you have that you just need to specify the configs mentioned on that page as well.

workflow.xml

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='TomSparkTest'>
  <start to='spark-node' />
  <action name='spark-node'>
    <spark xmlns="uri:oozie:spark-action:0.2">
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
      <prepare>
        <delete path="${nameNode}/user/tgraves/${wfRoot}/output-data/spark"/>
      </prepare>
      <configuration>
        <property>
          <name>oozie.action.sharelib.for.spark</name>
          <value>spark_latest</value>
        </property>
      </configuration>
      <master>${master}</master>
      <mode>${mode}</mode>
      <name>spark-pyspark</name>
      <jar>${nameNode}/user/tgraves/oozie-pyspark2/lib/pi.py</jar>
      <spark-opts>--num-executors 5 --executor-memory 7g --driver-memory 7g --conf spark.executorEnv.PYSPARK_PYTHON=./Python2.7.10/bin/python --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./Python2.7.10/bin/python</spark-opts>
      <archive>/user/tgraves/PythonOozie.zip#Python2.7.10</archive>
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

.. _sfo_sparkr-2.2:

Using Spark R (Spark 2.2 only)
------------------------------
With spark 2.2 we automatically include R so there should be no extra steps to use it.

.. _sfo_sparkr-2.1:

Using Spark R (Spark 2.0 & 2.1)
-------------------------------

Assuming R installation(https://twiki.corp.yahoo.com/view/Grid/SparkRInstall) is done.
- Create a hdfs directory for your oozie application, (oozieR/apps).
- Upload the application file to be run to hdfs (oozieR/apps/lib/myscript.R)
- Upload sparkr.zip into the oozie lib dir which is where spark.rpackage.home will point (oozieR/lib/R/lib/sparkr.zip).
- Add hive credential section to workflow.xml ( Refer to **Access Hive from Spark via Oozie** Section).
- Update spark action section of workflow.xml according to the following format and upload into hdfs: oozieR/apps/spark/workflow.xml

.. code-block:: xml

  <action name='spark-node' cred='hive_credentials'>
    <spark xmlns="uri:oozie:spark-action:0.2">
      ....
      <jar>myscript.R</jar>
      <spark-opts>--conf spark.sparkr.r.command=./R_installation/bin/Rscript --conf spark.yarn.security.tokens.hive.enabled=false --conf spark.rpackage.home=./</spark-opts>
      <archive>/user/${user.name}/R_install.tgz#R_installation</archive>
      ....
    </spark>
  </action>

- job.properties file - make sure to update for your cluster.

::

  nameNode=hdfs://your_namenode:8020
  jobTracker=your_resourcemanager:8032
  queueName=default
  wfRoot=oozieR
  oozie.libpath=/user/${user.name}/${wfRoot}/apps/lib
  oozie.wf.application.path=${nameNode}/user/${user.name}/${wfRoot}/apps/spark

- export oozie url according to the cluster you are on: ``export OOZIE_URL=http://your_cluster-oozie.colo.ygrid.yahoo.com:4080/oozie``
- example ``export OOZIE_URL=http://axonitered-oozie.red.ygrid.yahoo.com:4080/oozie/``
- run it: ``oozie job -run -config job.properties -auth KERBEROS``

*Full example of workflow.xml and job.propertices for reference:*
- example-code : https://git.ouroath.com/hadoop/spark/blob/yspark_2_1_0/examples/src/main/r/data-manipulation.R
- data-set: http://s3-us-west-2.amazonaws.com/sparkr-data/flights.csv (upload it to oozieR/apps/lib/ in hdfs)
- workflow.xml

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='JohnSparkTest'>
    <global>
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
    </global>
    <credentials>
      <credential name='hive_credentials' type='hcat'>
        <property>
          <name>hcat.metastore.uri</name>
          <value>thrift://axonitered-hcat.ygrid.vip.bf1.yahoo.com:50513</value>
        </property>
        <property>
          <name>hcat.metastore.principal</name>
          <value>hcat/axonitered-hcat.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM</value>
        </property>
      </credential>
    </credentials>
    <start to="spark-node"/>
    <action name='spark-node' cred='hive_credentials'>
      <spark xmlns="uri:oozie:spark-action:0.2">
        <configuration>
          <property>
            <name>oozie.action.sharelib.for.spark</name>
            <value>spark_latest</value>
          </property>
        </configuration>
        <master>yarn</master>
        <mode>cluster</mode>
        <name>SparkR_oozietest_dm</name>
        <jar>data-manipulation.R</jar>
        <spark-opts>--files flights.csv  --conf spark.sparkr.r.command=./R_installation/bin/Rscript --conf spark.yarn.security.tokens.hive.enabled=false --conf spark.rpackage.home=./</spark-opts>
        <arg>flights.csv</arg>
        <archive>/user/jlee2/__yspark_R.tgz#R_installation</archive>
      </spark>
      <ok to="end" />
      <error to ="fail" />
    </action>
    <message>Script failed. error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
  </kill>
  <end name='end' />

- job.properties

::

  nameNode=hdfs://axonitered-nn1.red.ygrid.yahoo.com:8020
  jobTracker=http://axonitered-jt1.red.ygrid.yahoo.com:8032
  queueName=default
  wfRoot=oozieR
  oozie.libpath=/user/${user.name}/${wfRoot}/apps/lib
  oozie.wf.application.path=${nameNode}/user/${user.name}/${wfRoot}/apps/spark

.. _sfo_custom_version:

Running a different Spark version
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

- Install the required version of spark on the gateway. Refer to the instructions to perform self installation of yspark_yarn package in https://twiki.corp.yahoo.com/view/Grid/SparkOnYarnProduct#Self_Installation_40if_you_need_a_version_not_on_the_gateway_41.
- Upload the ysarpk-jars-version.tgz file present in share/spark (e.g. yspark-jars-2.2.0.29.tgz) to an hdfs directory (example: hdfs:///user/YOUR_USERNAME/spark_jars_tgz/yspark-jars-2.2.0.29.tgz). You must also upload all the jars present in share/spark/lib to a separate directory (hdfs:///user/YOUR_USERNAME/spark_lib/spark_jars) in hdfs.
- Upload the corresponding version of conf, i.e. spark-defaults.conf from the cluster configs in $SPARK_CONF_DIR (see below for example) into hdfs: hdfs:///user/YOUR_USERNAME/spark_lib/spark_jars/spark-defaults.conf.
- If you are using hive you also need hive-site.xml and the datanucleus jars
- In the job.properties, you have to specify two paths to the oozie.libpath property like below, the example below assumes your normal oozie workflow lib dir is /user/${user.name}/spark_oozie/apps/lib, so essentially you are just adding in the libpath for where you put spark in the steps above: oozie.libpath=/user/${user.name}/spark_oozie/apps/lib,/user/${user.name}/spark_lib/spark_jars
- Set the config --conf spark.yarn.archive=hdfs:///user/YOUR_USERNAME/spark_jars_tgz/yspark-jars-2.2.0.29.tgz in <spark-opts> in your workflow.xml file.

.. _sfo_java_action:

Using Java Action
-----------------

.. note:: Using Java Action is NOT RECOMMENDED

IMPORTANT: if you are using the java action you will need to make sure to upload the spark-defaults.conf file we provide and make sure you upload it everytime we do new spark release. Otherwise the confs will not match the oozie sharelib current/latest labels.
I have successfully launch Spark on Yarn through oozie using the java action on a secure Hadoop QE cluster. Here is the workflow and job.properties file I used.

- upload the jar to be run to hdfs (here I use the example jar): spark_oozie/apps/lib/spark-examples-1.0.0.0-hadoop0.23.jar
- update workflow.xml (see below) and upload into hdfs: spark_oozie/apps/spark/workflow.xml
- Upload spark-defaults.conf from the cluster configs in $SPARK_CONF_DIR (see below for example) into hdfs: spark_oozie/apps/lib/spark-defaults.conf.
- Use the spark assembly jar in the hdfs share lib dir (not on all Grids yet, only on AR and KR). If you need to have your own version of the spark-assembly upload the spark assembly jar to the hdfs app lib dir: spark_oozie/apps/lib/spark-assembly-1.0.0.0-hadoop0.23.jar

To pick up the spark assembly jar from the hdfs sharelib use:

.. code-block:: xml

  <property>
    <name>oozie.action.sharelib.for.java</name>
    <value>spark_current</value>
  </property> 

job.properties file - make sure to update for your cluster:

::

  nameNode=hdfs://your_namenode:8020
  jobTracker=your_resourcemanager:8032
  queueName=default
  wfRoot=spark_oozie
  oozie.libpath=/user/${user.name}/${wfRoot}/apps/lib
  oozie.wf.application.path=${nameNode}/user/${user.name}/${wfRoot}/apps/spark

- export oozie url according to the cluster you are on: ``export OOZIE_URL=http://your_cluster-oozie.colo.ygrid.yahoo.com:4080/oozie``
- run it: ``oozie job -run -config job.properties -auth KERBEROS``

.. _sfo_known_issues:

Known Issues
------------

**IMPORTANT**: with spark 1.6.2 rollout if using the java action and sharelib, please also include add ``conf: spark.yarn.security.tokens.hive.enabled=false``

If ``spark.yarn.security.tokens.hive.enabled`` is not set to false and you don't have the other hive jars needed it will throw an exception like:
``java.lang.RuntimeException: Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient``


.. _sfo_change_configs:

Changing/Adding configs
-----------------------
Change or add any configs you need by using the --conf option to spark-submit

.. _sfo_workflow_sparkpi:

workflow.xml for SparkPi via SparkSubmit
----------------------------------------

.. code-block:: xml

  <workflow-app xmlns="uri:oozie:workflow:0.2" name="spark_oozie_wf">
    <start to="spark-node"/>
    <action name="spark-node">
      <java>
        <job-tracker>${jobTracker}</job-tracker>
        <name-node>${nameNode}</name-node>
        <prepare>
          <delete path="${nameNode}/user/${wf:user()}/${wfRoot}/output-data/pig"/>
        </prepare>
        <configuration>
          <property>
            <name>mapred.job.queue.name</name>
            <value>${queueName}</value>
          </property>
          <property>
            <name>mapred.compress.map.output</name>
            <value>true</value>
          </property>
          <!-- To use hdfs sharelib for current spark version. 
               Please use if you do not want to upload spark jar 
               to hdfs: spark_oozie/apps/lib/spark-assembly-1.0.0.0-hadoop0.23.jar-->
          <property>
            <name>oozie.action.sharelib.for.java</name>
            <value>spark_current</value>
          </property>
        </configuration>
        <main-class>org.apache.spark.deploy.SparkSubmit</main-class>
        <arg>--master</arg>
        <arg>yarn</arg>
        <arg>--deploy-mode</arg>
        <arg>cluster</arg>
        <arg>--class</arg>
        <arg>org.apache.spark.examples.SparkPi</arg>
        <arg>--properties-file</arg>
        <arg>spark-defaults.conf</arg>
        <arg>--num-executors</arg>
        <arg>3</arg>
        <arg>--executor-memory</arg>
        <arg>5g</arg>
        <arg>--driver-memory</arg>
        <arg>5g</arg>
        <arg>--queue</arg>
        <arg>default</arg>
        <arg>spark-examples-1.0.0.0-hadoop0.23.jar</arg>
        <capture-output/>
      </java>
      <ok to="end"/>
      <error to="fail"/>
    </action>
    <kill name="fail">
      <message>Script failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>    <end name='end' />
  </workflow-app>

.. sfo_hive:

Access Hive from Spark via Oozie
--------------------------------

These are instructions in addition to the ones above in Running Spark on Yarn through Oozie section. Note that if you are using the spark sharelib, it automatically pulls in hive-site.xml and any other jars you need.

Please check the known issues section: http://twiki.corp.yahoo.com/view/Grid/SparkEngineering#Known_Issues

You need a build of spark which allows you to turn off getting hive credentials on the client.
- add ``conf: spark.yarn.security.tokens.hive.enabled=false``

Then you need to tell oozie to get the hive credentials in your workflow.xml. This consists of multiple parts
- specify the credentials like in example below (change server to be the grid you are on, you can find the settings in hive-site.xml)
- change your action to include creds

Also Note you should only be accessing the hive instance on the cluster you are running on. If you need data from a hive not on the current cluster you should copy it or setup metadata replication. Talk to the hive team about this.

Example

.. code-block:: xml

  <workflow-app xmlns='uri:oozie:workflow:0.5' name='SparkHive'>
    <credentials>
      <credential name='hive_credentials' type='hcat'>
        <property>
          <name>hcat.metastore.uri</name>
          <value>thrift://kryptonitered-hcat.ygrid.vip.bf1.yahoo.com:50513</value>
        </property>
        <property>
          <name>hcat.metastore.principal</name>
          <value>hcat/kryptonitered-hcat.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM</value>
        </property>
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

Please check the known issues section: http://twiki.corp.yahoo.com/view/Grid/SparkEngineering#Known_Issues

You need a build of spark which allows you to turn off getting hbase credentials on the client. So you need yspark_yarn-1.5.2.1_2.6.0.16.1506060127_1512101638 or greater.
- add conf: ``spark.yarn.security.tokens.hbase.enabled=false``

Then you need to tell oozie to get the hbase credentials in your workflow.xml. This consists of multiple parts
- Make sure workflow version 0.5
- specify the credentials like in example below
- change your action to include creds

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

.. _sfo_pyspark_cluster_mode:

PySpark in cluster mode
-----------------------

These are instructions in addition to the ones above in Running Spark on Yarn through Oozie section.

Please check the known issues section: http://twiki.corp.yahoo.com/view/Grid/SparkEngineering#Known_Issues

Pyspark requires a couple of extra files and the SPARK_HOME env variable set.
  - Upload the python zip files (py4j-0.9-src.zip and pyspark.zip) to oozie.libpath under python/lib from $SPARK_HOME/python/lib.
    - For example: ``hadoop fs -put /home/gs/spark/latest/python/ spark_oozie/apps/lib/`` where spark_oozie/apps/lib is your oozie libpath.
  - Modify your workflow.xml to set SPARK_HOME env variable, see example below.

The below examples picks up spark assembly from sharelib and it has the conf setting to turn off getting hive tokens.

You can get an example python script pi.py from: https://github.com/apache/spark/blob/branch-1.6/examples/src/main/python/pi.py

Upload it into your oozie.libpath: ``hadoop fs -put pi.py spark_oozie/apps/lib/``

.. code-block:: xml

  <workflow-app xmlns="uri:oozie:workflow:0.2" name="spark_oozie_wf">
    <start to="spark-node"/>
    <action name="spark-node">
      <java>
        <job-tracker>${jobTracker}</job-tracker>
        <name-node>${nameNode}</name-node>
        <configuration>
          <property>
            <name>oozie.launcher.mapred.child.env</name>
            <value>SPARK_HOME=./</value>
          </property>
          <property>
            <name>mapred.job.queue.name</name>
            <value>${queueName}</value>
          </property>
          <property>
            <name>oozie.action.sharelib.for.java</name>
            <value>spark_current</value>
          </property>
        </configuration>
        <main-class>org.apache.spark.deploy.SparkSubmit</main-class>
        <arg>--master</arg>
        <arg>yarn</arg>
        <arg>--deploy-mode</arg>
        <arg>cluster</arg>
        <arg>--num-executors</arg>
        <arg>3</arg>
        <arg>--executor-memory</arg>
        <arg>3g</arg>
        <arg>--driver-memory</arg>
        <arg>3g</arg>
        <arg>--conf</arg>
        <arg>spark.yarn.security.tokens.hive.enabled=false</arg>
        <arg>--queue</arg>
        <arg>default</arg>
        <arg>pi.py</arg>
        <capture-output/>
      </java>
      <ok to="end"/>
      <error to="fail"/>
    </action>
    <kill name="fail">
      <message>Script failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>    <end name='end' />
  </workflow-app>