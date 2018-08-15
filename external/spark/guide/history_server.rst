.. _shs:

Spark History Server
====================
Spark 1.0 introduced a history server. We want to run this on our grids similar to the MapReduce job history server. We can run it on the same machine as the MR history server assuming it has enough resource.
- spark history server does not have logic to remove old files. I don’t think this is a problem right now as not to many people are running Spark.
- Spark history server only has a UI. No one connects to it via RPC.
- It reads a keytab file and should run as super user
- Spark history directory should be created manually and have permissions set same as MR done_intermediate: mapred:hadoop drwxrwxrwxt
- It supports bouncer and acls to restrict users viewing, just like the MR history server

.. _shs_configure:

Configuration
-------------
- Manually create the spark history directory. Perhaps ``/mapred/sparkhistory/``
- Make sure permissions are set appropriately: ``mapred:hadoop drwxrwxrwxt (chmod 1777)``
- Make sure keytab present on the machine


.. _shs_start_yinst:

Start using yinst
-----------------

- yinst install the latest yspark_yarn_history_server package
- yinst start yspark_yarn_history_server
- Go to UI to verify its up: ``host: 18080``
- Run a job to verify it read history server, make sure the following configs are set in conf/spark-defaults.conf

  - ``spark.eventLog.enabled           true``
  - ``spark.eventLog.dir               hdfs:///mapred/sparkhistory``

.. _shs_start_manual:

Manual start
------------

**Spark 2.1.x and older**

- Make sure HADOOP_CONF_DIR and HADOOP_PREFIX are set in your environment

.. code-block:: console

  export SPARK_CLASSPATH="$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/yjava_servlet.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/yjava_filter_logic*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/yjava_byauth*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/bouncer_auth_java*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/BouncerFilterAuth*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/yjava_servlet_filters*.jar)"

- set configs

.. code-block:: console

  export SPARK_DAEMON_JAVA_OPTS="-Dproc_sparkhistoryserver -Dspark.ui.filters=yjava.servlet.filter.BouncerFilter -Dspark.history.kerberos.enabled=true -Dspark.history.kerberos.principal=mapred/HOSTNAME@Y -Dspark.history.kerberos.keytab=/etc/grid-keytabs/HOST.keytab -Dspark.history.ui.acls.enable=true -Dspark.authenticate=false -Dspark.ui.acls.enable=false -Dspark.history.retainedApplications=5000 -Dspark.yjava.servlet.filter.BouncerFilter.params='bouncer.public.key.file=/usr/local/conf/bouncer'"

- set log dir: ``export SPARK_LOG_DIR=/home/gs/var/log/mapred``
- setup memory: ``export SPARK_DAEMON_MEMORY=8g`` (make sure using java 64 bit).
- ``${SPARK_HOME}/sbin/start-history-server.sh hdfs:///mapred/sparkhistory``
- Starts the history server on port 18080 by default. This can be controlled by setting ``-Dspark.history.ui.port=otherport``

**Spark 2.2.x**

- Make sure HADOOP_CONF_DIR and HADOOP_PREFIX are set in your environment

.. code-block:: console

  export SPARK_DAEMON_CLASSPATH="$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/yjava_servlet.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/yjava_filter_logic*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/yjava_byauth*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/bouncer_auth_java*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/BouncerFilterAuth*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/yjava_servlet_filters*.jar)"

- set configs

.. code-block:: console

  export SPARK_DAEMON_JAVA_OPTS="-Dproc_sparkhistoryserver -Dspark.ui.filters=yjava.servlet.filter.BouncerFilter -Dspark.history.kerberos.enabled=true -Dspark.history.kerberos.principal=mapred/HOSTNAME@Y -Dspark.history.kerberos.keytab=/etc/grid-keytabs/HOST.keytab -Dspark.history.ui.acls.enable=true -Dspark.authenticate=false -Dspark.ui.acls.enable=false -Dspark.history.retainedApplications=5000 -Dspark.yjava.servlet.filter.BouncerFilter.params='bouncer.public.key.file=/usr/local/conf/bouncer'"

- set log dir: ``export SPARK_LOG_DIR=/home/gs/var/log/mapred``
- setup memory: ``export SPARK_DAEMON_MEMORY=8g`` (make sure using java 64 bit).
- ``${SPARK_HOME}/sbin/start-history-server.sh hdfs:///mapred/sparkhistory``
- Starts the history server on port 18080 by default. This can be controlled by setting ``-Dspark.history.ui.port=otherport``

.. note:: If kerberos is disabled, you need to generate kerberos tokens by kinit before accessing a secure HDFS cluster.

.. _shs_stop:

Stopping
--------

To stop the spark history server: ``${SPARK_HOME}/sbin/stop-history-server.sh``