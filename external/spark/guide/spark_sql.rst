.. _sql:

Spark SQL Cli
================

This guide will walk you through using the Spark SQL Cli.

.. _sql_shell:

Spark-SQL Shell (spark-sql)
---------------------------
spark-sql is a utility that allows interactive execution of Spark SQL statements, without having to wrap them in Scala or other code. It uses HCat and Hive so Oath Grid Hive databases and tables are all available. It can be accessed in Oath grids via the spark-sql command. Note that the command supports client mode only (interactive).

.. _sql_capabilities:

Capabilities
------------
- Execute Spark SQL statements, and analyze results interactively.
- Pass an initialization SQL file (-i option) to set common values or perform other initialization steps.
- Pass a SQL script as a file (-f option).
- Execute Spark SQL code inline (-e option).
- The shell supports creation/usage of Hive UDFs, and other built-in functions added by Spark.

.. _sql_shell_start:

Starting the Shell
------------------

.. code-block:: console

  -bash-4.1$ $SPARK_HOME/bin/spark-sql
  18/05/25 21:52:19 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
  Setting default log level to "WARN".
  To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
  18/05/25 21:52:21 WARN DomainSocketFactory: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.
  18/05/25 21:52:24 WARN Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
  mapred.job.queue.name   default
  mapred.min.split.size   134217728
  mapred.max.split.size   1073741824
  18/05/25 21:52:43 WARN SetCommand: 'SET hive.exec.compress.output=false' might not work, since Spark doesn't support changing the Hive config dynamically. Please pass the Hive-specific config by adding the prefix spark.hadoop (e.g. spark.hadoop.hive.exec.compress.output) when starting a Spark application. For details, see the link: https://spark.apache.org/docs/latest/configuration.html#dynamically-loading-spark-properties.
  hive.exec.compress.output   false
  mapred.child.java.opts   -server -Xmx2600m -Djava.net.preferIPv4Stack=true
  mapred.job.map.memory.mb   3072
  mapred.job.reduce.memory.mb   3072
  tez.queue.name   default
  spark-sql>

.. _sql_command_examples:

Command Examples
----------------

.. code-block:: console

  spark-sql> show databases like '%starling%';
  starling
  starling_msys
  Time taken: 0.021 seconds, Fetched 2 row(s)
  spark-sql> use starling;
  Time taken: 0.016 seconds
  spark-sql> show tables like '%spark%';
  starling   starling_spark_executor_info   false
  starling   starling_spark_job_am_conf   false
  starling   starling_spark_jobs   false
  starling   starling_spark_stage_task_attempt_counters   false
  starling   starling_spark_stage_task_attempts   false
  starling   starling_spark_stage_tasks   false
  starling   starling_spark_stages   false
  starling   starling_spark_stages_counters   false
  Time taken: 0.039 seconds, Fetched 8 row(s)
  spark-sql>
  spark-sql> select count(*) from starling_spark_jobs where dt="2018_05_24";
  6199
  Time taken: 18.275 seconds, Fetched 1 row(s)

.. _sql_migration_guide:

SQL Migration Guide
------------------
- You can follow the sql upgrading guide here: https://spark.apache.org/docs/latest/sql-migration-guide-upgrade.html  which enlists the changes required to upgrade
  from one version of spark to another(example: upgrading from Spark SQL 2.3 to 2.4).
