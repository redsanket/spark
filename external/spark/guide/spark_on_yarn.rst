.. _soy:

Spark on YARN Product
============================

Spark on YARN is running Spark on top of Hadoop YARN.

.. _soy_releases:

Releases
--------

Release Information:

Note that spark 2.3 is latest and spark 2.2 is the current version.  Check your grid to see if it has rolled out ``ls /home/gs/spark`` on a grid gateway.

.. _soy_start:


Getting Started
---------------
Spark can be run on any of the Yahoo Grids. You must have onboarded to Hadoop and have capacity and access to a queue on the grid you want to run.

Please subscribe to yspark-users ilist (yspark-users@oath.com) to get announcements about Spark.


.. _soy_installation:

Installation on the Grids
~~~~~~~~~~~~~~~~~~~~~~~~~

The grid gateways will have Spark installed on them in ``/home/gs/spark``. There is a ``latest`` symlink and then a ``current`` symlink. There are also some default spark configs install in ``/home/gs/conf/spark`` with both current and latest symlinks. These will automatically be set for you when you login to a grid gateway via the environment variables below. They default to the spark current version. You can change this from current to latest to change the spark version.

To change to use latest you just have to export the following variables:

.. code-block:: console

    export SPARK_CONF_DIR=/home/gs/conf/spark/latest
    export SPARK_HOME=/home/gs/spark/latest

To switch back to current:

.. code-block:: console

    export SPARK_CONF_DIR=/home/gs/conf/spark/current
    export SPARK_HOME=/home/gs/spark/current

If you want to get the scripts in your path just set:

.. code-block:: console

    export PATH=$SPARK_HOME/bin:$PATH


.. _soy_selfinstall:

Self Installation (if you need a version not on the gateway)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The spark on yarn package is here: `yspark_yarn <http://dist.corp.yahoo.com/by-package/yspark_yarn/>`_

If you are running on a gateway you can install this package in the root somewhere. Please always check dist to make sure these are the latest versions. 
Also note that the newer versions of Spark conf point to a jar in hdfs. If you do self install a different version you will want to override that or install yspark_yarn_conf package as well.
To point to your self installed version: 

For spark 2.x: ``--conf spark.yarn.archive=file:///homes/$USERNAME$/testroot/share/spark/yspark-jars-2.0.0.24.tgz``

You can also install yspark_yarn_conf package to get the default set of confs. If you do this you will also need to change SPARK_CONF_DIR and still set spark.yarn.jar or spark.yarn.archive:

::

    export SPARK_CONF_DIR=/homes/$USERNAME$/testrootconfs/conf/spark

Once you install it locally you need to change ``SPARK_HOME`` to point the install location, for example: ``export SPARK_HOME=/homes/dashar/testroot/share/spark``.

- Spark 2.3.x

::

    yinst i -nosudo -r /homes/dashar/testroot yspark_yarn-2.3.0.29
    yinst i -nosudo -r /homes/dashar/testrootconfs yspark_yarn_conf-2.3.0.29

- Spark 2.2.x

::

    yinst i -nosudo -r /homes/dashar/testroot yspark_yarn-2.2.0.27
    yinst i -nosudo -r /homes/dashar/testrootconfs yspark_yarn_conf-2.2.0.27

Make sure to use the full path to the root directory in the -r option. If you put the relative path you might have problems.

Alternatively if you have your own private launcher you can just yinst install it and use ``/home/y`` instead of ``/homes/%USERNAME%/testroot``.

.. _soy-launcherbox:

Launcher Box Setup
~~~~~~~~~~~~~~~~~~

Note that by default you cannot run in yarn client mode from launchers, this means spark-shell, pyspark, anything else client mode won't run from a launcher box. This is due to acls not being open to connect back to the launcher. If you have a use case for this you can talk to the paranoids to see if you can get an exception.

Setup like normal Grid launcher box for acls: https://yahoo.jiveon.com/docs/DOC-26932. This dist_tag only shows current stable version, if you are looking for the spark latest you will need to go to a gateway and run yinst set yspark_yarn_install.

There are 2 ways to setup launcher boxes. Normal yinst install and then setup like the grid gateways.

.. _soy_launcherbox_yinst:

Normal yinst
++++++++++++

Install yspark_yarn and yspark_conf. Get the latest versions by looking at the gateway boxes for your grid. Look at /home/gs/spark. Then take that version and install it

::
    
    yinst i yspark_yarn-{VERSION} yspark_yarn_conf-{VERSION} -br current

.. note:: The version here should match the gateway on your grid, if you pull from current you may get a version not yet deployed!!!

Then you need to export SPARK_HOME and SPARK_CONF_DIR to pick them up

::
    
    export SPARK_HOME=/home/y/share/spark
    export SPARK_CONF_DIR=/home/y/conf/spark

If you are using hive then you should install the ``hive_conf_(your grid)`` package. For instance on axonite red its: ``hive_conf_axonitered``. Go to a gateway and see the version installed there and install the same on your launcher box.  Spark 2.2 also adds in a ``hbase-site.xml`` link in the $SPARK_CONF_DIR so if you need hbase you should install the hbase confs. Note that the ``hbase-site.xml`` link in the $SPARK_CONF_DIR expects that file to be in the launcher box location ``/home/gs/conf/hbase/hbase-site.xml``, if your launcher box install is a different location you will need to update that.

.. _soy_gridgateway:

Like Grid Gateways
++++++++++++++++++

On the grid gateways multiple versions are installed and it has latest/current symlinks. There is a ``yspark_yarn_install`` package that can be used to install yspark and the yspark confs and multiple versions with latest and current symlinks.

You should find the latest versions for your grid and then install it like

::

    yinst i yspark_yarn_install -br current \
    -set yspark_yarn_install.CURRENT=yspark_yarn-2.2.1.45 \
    -set yspark_yarn_install.LATEST=yspark_yarn-2.3.0.60

This will create ``/home/gs/spark`` and ``/home/gs/conf/spark`` symlinks.

Then you need to export SPARK_HOME and SPARK_CONF_DIR to pick them up

.. code-block:: console

    export SPARK_HOME=/home/gs/spark/current
    export SPARK_CONF _DIR=/home/gs/conf/spark/current

If you are using hive then you should install the ``hive_conf_(your grid)`` package. For instance on axonite red its: ``hive_conf_axonitered``. Go to a gateway and see the version installed there and install the same on your launcher box.
Spark 2.2 and greater adds in a ``hbase-site.xml`` link in the $SPARK_CONF_DIR so if you need hbase you should install the hbase confs. Note that the ``hbase-site.xml`` link in the $SPARK_CONF_DIR expects that file to be in the launcher box location ``/home/gs/conf/hbase/hbase-site.xml``, if your launcher box install is a different location you will need to update that.

.. _soy_configs:

Spark Configs
-------------
If you want to add configs to your run its recommended that when you call spark-submit using the ``--conf`` option to add or override the defaults. This is the preferred method so that you pick up the default configs we have set.

Note the normal spark configs should work on YARN. Please see the Spark documentation for details on those - find the release notes for the version you are running on and it will have a link to the matching configs. The latest open source docs are here: http://spark.apache.org/docs/latest/configuration.html

.. _soy_configs_hadoop:

Specify hadoop configs in spark job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can specify hadoop configs via spark confs by prefixing it with ``spark.hadoop.``
For example to turn on success file for the mapred file output committer, specify

::

    --conf spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=true

.. _soy_remotegridaccess:

Access data on remote grids
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are accessing data note on the grid you are running on you have to specify the remote grids in ``spark.yarn.access.namenodes`` for Spark 2.1, for Spark 2.2 and 2.3 use ``spark.yarn.access.hadoopFileSystems``. This includes accessing both through hdfs and webhdfs.

For example if I'm running on AR and want to access data on KR

::

    $SPARK_HOME/bin/spark-shell  --executor-cores 1  --master yarn --deploy-mode client 
    --executor-memory 2g --queue default --num-executors 6  
    --conf spark.yarn.access.namenodes=hdfs://kryptonitered-nn1.red.ygrid.yahoo.com:8020

Conf for accessing webhdfs on JB

::

    --conf spark.yarn.access.namenodes=webhdfs://jetblue-nn1.blue.ygrid.yahoo.com:50070

For full usage information see the spark docs for your version of Spark, the latest are here: https://spark.apache.org/docs/latest/running-on-yarn.html

.. _soy_modes:

Spark Run Modes
---------------

.. _soy_modes_yarn_cluster:

YARN cluster mode
~~~~~~~~~~~~~~~~~
The yarn cluster mode is a batch mode where the entire application runs on the grid. The SparkContext runs in the ApplicationMaster. The client is just a thin client that polls the RM for status on the application. The client can go away and the application still runs.

::

    spark-submit --master yarn --deploy-mode cluster --class <your_main_class_to_run> \
    --queue <queue_name> <name and path to your application jar> <arguments your program takes>

Please run ``spark-submit --help`` to see the command line options.

.. _soy_modes_yarn_client:

YARN client mode (spark-shell, pyspark, sparkR and spark-sql support)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In the yarn client mode the client is a fat client. Instead of the SparkContext running on the grid in the application master, the SparkContext runs on your gateway or launcher box. Note that this can cause more load on your gateway and if you gateway goes down your application dies. This mode allows you to run the spark-shell, and other repls.

.. note:: IMPORTANT: This mode should only be used for ad-hoc queries or development.

- SPARK Shell for interactive queries in scala

::

    spark-shell --master yarn --deploy-mode client

- PYSpark for interactive queries in python

::

    pyspark --master yarn --deploy-mode client

- sparkR for interactive queries in R: :ref:`soy_sparkr`

- spark-sql for interactive queries in SQL: :ref:`sql`

- Batch mode

::

    spark-submit --master yarn --deploy-mode client --class <your_main_class_to_run> \
    --queue <queue_name> <name and path to your application jar> <arguments your program takes>


.. _soy_oozie:

Spark via Oozie
---------------
:ref:`sfo`

.. _soy_addon_svc:

Accessing Services (Hive/HBASE/etc)
-----------------------------------

.. _soy_addon_svc_hive:

Spark Sql accessing Hive (spark 1.3.1 and greater)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Spark Sql can now access our Hive installations in either client or cluster mode. You can use most regular hive command for both reading and creating tables. See the Apache Spark docs for specifics on what might not be supported: http://spark.apache.org/docs/latest/sql-programming-guide.html#supported-hive-features

Note if you are using subdirectories in your hive partitions then you will have to enable recursive directory traversing when reading

::

    --conf spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=true

.. _soy_addon_svc_hive_client_mode:

For client mode
++++++++++++++++

For example to run via spark shell:

.. code-block:: scala

    $SPARK_HOME/bin/spark-shell --master yarn --deploy-mode client 

    scala> spark.sql("show databases").collect()
    scala> spark.sql("select * from tgraves.doctors").collect().foreach(println)

.. _soy_addon_svc_hive_cluster_mode:

For cluster mode
++++++++++++++++

Spark 2.x

- Make sure your application jar does not include Spark in it (you should pick it up from the spark-assembly provided with yspark)
- ship hive-site.xml with your job 
  - ``--files $SPARK_CONF_DIR/hive-site.xml``

For Spark 2.x if you are running cluster mode with SparkSession you need to enable Hive support:

.. code-block:: scala

    SparkSession spark = SparkSession
        .builder()
        .appName("test2.0")
        .enableHiveSupport()
        .getOrCreate();

Example run command calling a python sql script

.. code-block:: console

    $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --executor-cores 1 \
    --executor-memory 3g  --queue default --files $SPARK_CONF_DIR/hive-site.xml ~/sqlspark2.py


Example python sql script accessing hive:

.. code-block:: python
    
    from __future__ import print_function

    import sys
    from random import random
    from operator import add

    from pyspark.sql import SparkSession

    if __name__ == "__main__":
        """
            Usage: sqlspark2
        """
        spark = SparkSession\
            .builder\
            .enableHiveSupport()\
            .appName("PythonHiveExample")\
            .getOrCreate()

        for db in spark.sql("show databases").collect():
          print(db)

        for r in spark.sql("select * from tgraves.doctors").collect():
          print(r)

        spark.stop()

.. _soy_addon_svc_known_issues:

Known Issues with Spark Sql accessing hive
++++++++++++++++++++++++++++++++++++++++++

- Before spark 2.2 dataframe creates of tables can be a problem.  Meaning there are sometimes issues reading it from hive.  If you are planning on reading/writing from both Spark and Hive you should use the sql interface to create and alter tables.  Spark 2.2 supports integration with the dataframe api.  See https://issues.apache.org/jira/browse/SPARK-19150.

- Alter table only supported starting in spark 2.2, see: https://issues.apache.org/jira/browse/SPARK-19261

- cache the metadata ``sqlContext.table("tableName").registerTempTable(...)`` which caches the list of partitions in memory on the driver. The initial pull is expensive but it is much faster after that.

.. _soy_addon_svc_hive_hcatalog:

Accessing hive through HCatalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _soy_addon_svc_hive_hcatalog_2.2+:

From spark 2.2.x and greater
++++++++++++++++++++++++++++

Here we give an example to access hive from spark-shell using hcatalog for yspark_yarn version 2.2.x and greater

.. code-block:: console

    /homes/%USERNAME%/testroot/share/spark/bin/spark-shell --master yarn --deploy-mode client --conf spark.ui.port=4044--conf spark.driver.extraClassPath="/home/y/libexec/hive/lib/hcatalog-support.jar:/home/y/libexec/hive/lib/hive-hcatalog-core.jar:$(ls /home/y/libexec/hive/lib/guava-*.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/common/hadoop-gpl-compression.jar):$(ls ${HADOOP_PREFIX}/share/hadoop/hdfs/lib/YahooDNSToSwitchMapping-*.jar)" --jars /home/y/libexec/hive/lib/hcatalog-support.jar,/home/y/libexec/hive/lib/hive-hcatalog-core.jar,$(ls /home/y/libexec/hive/lib/guava-*.jar)

.. _soy_addon_svc_hive_example:

Example
+++++++

.. code-block:: scala

    import org.apache.hive.hcatalog.mapreduce.HCatInputFormat
    import org.apache.hadoop.mapreduce.InputFormat
    import org.apache.hadoop.io.WritableComparable
    import org.apache.hive.hcatalog.data.HCatRecord
    val hconf = new org.apache.hadoop.conf.Configuration()
    org.apache.hive.hcatalog.mapreduce.HCatInputFormat.setInput(hconf, "db_name", "table_name")
    val inputFormat = (new HCatInputFormat).asInstanceOf[InputFormat[WritableComparable[_],HCatRecord]].getClass
    val key = classOf[WritableComparable[_]]
    val value = classOf[HCatRecord]
    val rdd = sc.newAPIHadoopRDD(hconf,inputFormat,key,value)
    rdd.count()

.. _soy_addon_svc_hbase:


Spark accessing HBase table
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Firstly, make sure you have permissions to certain HBase clusters. If not, you can go to https://supportshop.cloud.corp.yahoo.com:4443/doppler/hbase/ to request for the permission. For example, now you have permission to the "spark_test" namespace of the HBase on relux-red cluster. 

.. _soy_addon_svc_hbase_spark_2.1:

HBase access from Spark 1.4 to 2.1
++++++++++++++++++++++++++++++++++

In order to access hbase you currently have to setup the classpath on the gateway to pick up the jars and hbase-site.xml and then you also need to ship those with your application.

Then, prepare the package and classpath (make sure that your hbase-core and guava have correct version number). Use the hbase client that matches the hbase cluster you are accessing. You are going to add the classpath to sparks class path below

.. code-block:: console

    $SPARK_CONF_DIR:/home/gs/hbase/current/lib/hbase-protocol.jar:/home/gs/hbase/current/lib/hbase-common.jar:/home/gs/hbase/current/lib/hbase-client.jar:/home/gs/hbase/current/lib/htrace-core-2.04.jar:/home/gs/hbase/current/lib/hbase-server.jar:/home/gs/hbase/current/lib/guava-12.0.1.jar:/home/gs/conf/hbase/

Make a copy of the Spark confs and add classpath. Make sure to have a log4j.properties file in the spark conf dir otherwise the hbase one will be loaded and errors will happen:

.. code-block:: console

    mkdir ~/sparkconf
    cp $SPARK_CONF_DIR/* ~/sparkconf/
    # Edit ~sparkconf/spark-env.sh and add the above path to the end of the SPARK_CLASSPATH
    export SPARK_CONF_DIR=~/sparkconf

Launch the spark shell, update the namenode to be the Hbase cluster namenode you are accessing

.. code-block:: console

    $SPARK_HOME/bin/spark-shell --master yarn --deploy-mode client --conf spark.ui.port=4044 \
    --jars /home/gs/hbase/current/lib/hbase-protocol.jar,/home/gs/hbase/current/lib/hbase-common.jar,/home/gs/hbase/current/lib/hbase-client.jar,/home/gs/hbase/current/lib/htrace-core-2.04.jar,/home/gs/hbase/current/lib/hbase-server.jar,/home/gs/hbase/current/lib/guava-12.0.1.jar,/home/gs/conf/hbase/hbase-site.xml

.. _soy_addon_svc_hbase_spark_2.2+:

HBase access from Spark 2.2 and greater
+++++++++++++++++++++++++++++++++++++++

The gateways generally have hbase installed on them.  See ``/home/gs/conf/hbase/`` and ``/home/gs/hbase/current``

Spark 2.2 we added back in the hbase example converters for python: https://git.corp.yahoo.com/hadoop/spark/blob/yspark_2_2_0/examples/src/main/scala/org/apache/spark/examples/pythonconverters/HBaseConverters.scala

Spark has a symlink in $SPARK_CONF_DIR to automatically pull in hbase-site.xml.  You just have to send the hbase jars with your application.

Launch the spark shell, update the namenode to be the Hbase cluster namenode you are accessing

.. code-block:: console

    $SPARK_HOME/bin/spark-shell --master yarn --deploy-mode client \
    --jars /home/gs/hbase/current/lib/hbase-protocol.jar,/home/gs/hbase/current/lib/hbase-common.jar,/home/gs/hbase/current/lib/hbase-client.jar,/home/gs/hbase/current/lib/htrace-core-2.04.jar,/home/gs/hbase/current/lib/hbase-server.jar,/home/gs/hbase/current/lib/guava-12.0.1.jar

For cluster mode you also have to send the $SPARK_CONF_DIR/hbase-site.xml file

.. code-block:: console

    $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster  --jars /home/gs/hbase/current/lib/hbase-protocol.jar,/home/gs/hbase/current/lib/hbase-common.jar,/home/gs/hbase/current/lib/hbase-client.jar,/home/gs/hbase/current/lib/htrace-core-2.04.jar,/home/gs/hbase/current/lib/hbase-server.jar,/home/gs/hbase/current/lib/guava-12.0.1.jar --class yahoo.spark.SparkHbase --files $SPARK_CONF_DIR/hbase-site.xml ~/yahoo-spark_2.11-1.0-jar-with-dependencies.jar

.. _soy_addon_svc_hbase_example:

Spark examples accessing HBase
++++++++++++++++++++++++++++++

After that, you can try to access your HBase table from Spark shell.

.. code-block:: scala

    import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
    import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, HColumnDescriptor, TableName}
    import org.apache.hadoop.hbase.mapreduce.TableInputFormat
    import org.apache.spark._

    val hconf = HBaseConfiguration.create()
    val tableName = "spark_test:zliu1"
    hconf.set(TableInputFormat.INPUT_TABLE, tableName)
    val admin = new HBaseAdmin(hconf)

    // create the table if not existed
    if(!admin.isTableAvailable(tableName)) {
        val tableDesc = new HTableDescriptor(tableName)
        tableDesc.addFamily(new HColumnDescriptor("cf1".getBytes()));
        admin.createTable(tableDesc)
    }

    // put data into the table
    val myTable = new HTable(hconf, tableName);
    for (i <- 0 to 5) {
        val p = new Put(new String("row" + i).getBytes());
        p.add("cf1".getBytes(), "column-1".getBytes(), new String("value " + i).getBytes());
        myTable.put(p);
    }
    myTable.flushCommits();

    // access the table through RDD
    val hBaseRDD = sc.newAPIHadoopRDD(hconf, classOf[TableInputFormat], 
          classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
          classOf[org.apache.hadoop.hbase.client.Result])
    val count = hBaseRDD.count()
    print("HBase RDD count:"+count)

Example writing to HBASE.

.. code-block:: scala

    import org.apache.hadoop.mapred.JobConf
    import org.apache.hadoop.hbase.mapred.TableOutputFormat
    // set up Hadoop HBase configuration using TableOutputFormat
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig = new JobConf(conf, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
     
    //convert data to puts then write to OF
    rdd = <RDD data represented as hbase Puts>
    rdd.saveAsHadoopDataset(jobConfig)

You can also put the above codes into a Spark class by referring to this link. https://github.com/apache/spark/blob/branch-1.6/examples/src/main/scala/org/apache/spark/examples/HBaseTest.scala

An example of the above code (writing from the driver and reading from and RDD) is available for your reference built with Spark 2.0+ here: https://git.corp.yahoo.com/hadoop/spark-starter/blob/branch-2.0/src/main/scala/com/yahoo/spark/starter/SparkClusterHBase.scala

An example writing from a RDD to Hbase : https://git.corp.yahoo.com/tgraves/sparkScripts/blob/spark2/sparkbuild/src/main/scala/yahoo/spark/SparkHbase.scala

An example reading from HBASE via python: 
  - spark < 2.2: https://git.corp.yahoo.com/hadoop/spark-starter/blob/branch-2.0/src/main/python/hbaseread.py
  - spark 2.2: https://git.corp.yahoo.com/hadoop/spark-starter/blob/branch-2.0/src/main/python/hbaseread22.py

More examples and information on this in the hbase documentation at: http://hbase.apache.org/book.html#spark

.. _soy_readdata:

Reading data (ORC files, avro, etc)
-----------------------------------

.. _soy_avro:

Reading Avro data from Spark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Databricks has created a spark-avro library for easily reading avro data in Spark.

.. _soy_avro_till2.2:

Spark version >= 2.2
++++++++++++++++++++

Starting with Spark 2.2 we are including the spark-avro jar with the yspark distribution.  So all you have to do is reference it from the code.

Note: The configuration setting spark.sql.files.maxPartitionBytes is not always honoured by spark-avro. It only works when the size of the avro file to be read is large enough. For small files, you have to set the value of the setting spark.sql.files.openCostInBytes greater than spark.sql.files.maxPartitionBytes in order to make it work.
+++++
.. _soy_avro_example:

Example
+++++++

.. code-block:: scala

    // import needed for the .avro method to be added
    import com.databricks.spark.avro._
    import org.apache.spark.sql.SQLContext

    // The Avro records get converted to Spark types, filtered, and
    // then written back out as Avro records
    val df = spark.read.avro("src/test/resources/episodes.avro")
    df.filter("doctor > 5").write.avro("/tmp/output")

.. soy_hive_orc:

Spark Sql accessing Hive ORC file (spark 1.4+)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here we show a simple example on how to save/load Hive ORC files in Spark. You can also see more information by refering to (https://hortonworks.com/blog/bringing-orc-support-into-apache-spark/)

Start the spark-shell and load some sample data to HDFS (make sure hive-site.xml has been copied to ``/homes/%USERNAME%/testroot/share/spark/conf``).
  - put sample data into hdfs: ``hadoop fs -put $SPARK_HOME/examples/src/main/resources/people.txt``
  - Start spark-shell

::

    $SPARK_HOME/bin/spark-shell --master yarn --deploy-mode client --conf spark.ui.port=4044 --jars /home/y/libexec/hive/lib/hcatalog-support.jar

- Import necessary packages, obtain the HiveContect and load the sample data as a table DataFrame.

.. code-block:: scala
  
    import org.apache.spark.sql.hive.orc._
    import org.apache.spark.sql._
    import org.apache.spark.sql.types._
    val ctx = new org.apache.spark.sql.hive.HiveContext(sc)
    val people = sc.textFile("people.txt")
    val schemaString = "name age"
    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))
    val peopleSchemaRDD = ctx.applySchema(rowRDD, schema)
    peopleSchemaRDD.registerTempTable("people")
    val results = ctx.sql("SELECT name FROM people")
    results.map(t => "Name: " + t(0)).collect().foreach(println)

- Write a DataFrame to HDFS as ORC file format.

.. code-block:: scala

    peopleSchemaRDD.write.format("orc").mode("overwrite").save("people.orc")

- Load an ORC file as a DataFrame in memory and register it as a temp table

.. code-block:: scala

    val df = ctx.read.format("orc").load("people.orc")
    df.registerTempTable("orcTable")

- Do a sql query on the loaded table.

.. code-block:: scala

    val teenagers = ctx.sql("SELECT name FROM orcTable WHERE age >= 13 AND age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

- Conduct a direct filtering on the dataframe.

.. code-block:: scala

    df.filter(df("age")<20).select("name").show()

.. _soy_sparkr:

SparkR
------
SparkR requires yspark_yarn-1.5.1.1_2.6.0.16.1506060127_1510071630 or greater to use. 
  - :ref:`r` 

.. soy_sql:

Spark-sql
---------
  - spark-sql introduction :ref:`sql`

.. _soy_pyspark:

PySpark usage [PySpark+Anaconda,IPython,Hive,Python2.7 and packages]
--------------------------------------------------------------------
Please note that if you are using python with Spark, the python process uses off heap memory.  The way to configure
off heap memory on Spark is with the overhead configurations ``spark.driver.memoryOverhead`` and ``spark.executor.memoryOverhead``.  Please see the configuration docs on specifics about those.

  - `PYspark, Pyspark + Anaconda,IPython,Hive` :ref:`swp`

.. _soy_streaming:

Spark Streaming
-----------------------
Spark streaming can be run on Yarn but there are a few things to keep in mind since Yarn is a multi-tenant environment. 

- Spark Streaming is not good for sub-second latency requirements, we recommend you look at Storm for this
- Yarn does not provide network or disk isolation. This can affect Spark Streaming jobs since they are running on multi-tenant hosts and another application may start to use all the network or disk bandwidth which could slow your streaming job down or worst case cause that executor to fail.
- HDFS tokens have a max lifetime of 28 days so you must restart your application at least once every 28 days.
- Yarn does not handle aggregating the spark streaming job logs while its running, so be sure to use a log4j configuration that removes them so you don't cause the nodes to run out of disk space.

.. _soy_jupyter:

Spark on Jupyter hosted
-----------------------
Start at: yo/jupyter

.. _soy_python_jupyter:

Python packages with HUE/Jupyter
--------------------------------
  - `Hue - add python packages` :ref:`swp_packages`

.. _soy_hue:

Spark access from Hue
---------------------

Yahoo production grids currently are on spark 2.2.

Hue currently supports pyspark and scala. Go to notebooks and select new notebook. Then in the middle of the screen you can choose either Scala or PySpark. Note that access to Hive from Hue is currently not supported, we are working on it.

Once you create the notebook, the upper right corner has a "Context" button where you can set spark configs, send jars, archives, etc just like through the command line.

From there you can just type spark commands. To do tables you can use %table dataset and it will pull up a table and you can configure it. A simple example:

.. code-block:: scala

    val textFile = sc.textFile("README.md")
    %table textFile

If you are using pyspark you by default get python 2.7 with numpy and pandas. If you need to add your own python packages follow instructions here:
  - `Hue - add python packages` :ref:`swp_packages`


.. _soy_sparkconfs_hue:

Setting Spark configs in Hue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can click on the "Context" button on the upper right corner and select any standard property that you want to set out of the default ones. If you want to set a specific spark config, select "Spark Conf" from the drop down and then add the name of the config in Key and its corresponding value. You can add multiple of these.  once you are done hit the "Recreate" button to start a new session with the configs applied.

.. _soy_hue_files:

Sending files through Hue
~~~~~~~~~~~~~~~~~~~~~~~~~

You can pass files to be stored in the working directory of each executor. These files have to be stored in hdfs. Click on the "Context" button and select the type of file you want to send - i.e. Files/PyFiles/Jars/Archives and click on the file browser(...) to point to your file on hdfs. Once you are done hit the "Recreate" button to start a new session.

.. _soy_hue_hive:

Accessing hive through Hue
~~~~~~~~~~~~~~~~~~~~~~~~~~

For hue with Spark 2.x, everything should just work.
  - For example just run: ``spark.sql("show databases").collect()``

For hue with Spark 1.6 the following steps are required:
  - Upload hive-site.xml and datanucleus jars to hdfs from a gateway

    - hadoop fs -mkdir huehive (creates directory /user/yourid/huehive
    - hadoop fs -put $SPARK_HOME/lib/datanucleus-{api-jdo,core,rdbms}.jar huehive
    - Modify hive-site.xml file

      - cd; cp $SPARK_CONF_DIR/hive-site.xml
      - Change hive.querylog.location from ${user.name} to ${java.io.tmpdir}/hivelogs so that its in the container directory, its unique and will get cleaned up on exit

      .. code-block:: xml

          <property>
            <name>hive.querylog.location</name>
            <value>${java.io.tmpdir}/hivelogs</value>
            <description>Local Directory where structured hive query logs are created. One file per session is created in this directory. If this variable set to empty string structured log will not be created.</description>
          </property>

      - hadoop fs -put hive-site.xml huehive

  - Now Start your hue session and you will have to specify the datanucleus jars and hive-site.xml in the configuration settings

    - Start your spark hue notebook
    - Once its started to go upper right corner select "Context"
    - In drop down menu select "Jars" and you are going to add 3 jars
    - select the ".." to see hdfs and select the huehive directory and then one of the jars like (datanucleus-api-jdo.jar)
    - select the "+" to add another jar and repeat above step and select datanucleus-core.jar
    - seelct the "+" to add another jar and select datanucleus-rdbms.jar
    - Now from the dropdown menu select "Archives" and press "+" to add
    - Go to the Archives field and select ".." and select huehive/hive-site.xml
    - Now hit the "Recreate" button and you will have hive access 

.. _soy_hue_avro:

Accessing avro through Hue
~~~~~~~~~~~~~~~~~~~~~~~~~~

Spark version < 2.2:

If you are bundling the avro jar as a dependency with your application, then you don't need to supply any additional files. If not, you would have to first download the avro jar file and upload it to hdfs. Then you can select the avro jar by following the instructions above and recreate the hue session. The avro file should now be loaded and available to use.

You can find the required avro jar version and try out an example by refering the section :ref:`soy_avro` and download the avro jar from ``http://spark-packages.org/package/databricks/spark-avro``

Spark version >= 2.2: spark-avro jar is included with yspark so you can just use it.

.. _soy_monitoring:

Controlling & Monitoring 
------------------------

You can kill a spark application via:
  - ``yarn application -kill <application id>``
You can see the logs for your application by either going to the web ui or with:
  - ``yarn logs -applicationId <application id> -appOwner <app owner> | less``

You can see the log files for your individual workers by going to the Yarn NodeManager WebUI and clicking on the container.

See more information:
  - `SparkDebugging` :ref:`dbg`

.. _soy_sparkstarter:

Creating your own application jar/Spark starter repo
----------------------------------------------------

If you are starting out writing a spark application and don't yet have a build environment setup, there is an example starter repo here: https://git.corp.yahoo.com/hadoop/spark-starter/tree/branch-2.0

It contains a couple of the normal examples from Spark - SparkPi and JavaWordCount as well as one to access Hive. It also contains the basic pom file necessary to build.

Please look at the starter package pom file on how to properly include spark in your application pom file. You should not include Spark itself in your application jar. You can use the yspark versions in your pom just like you can the open source versions with org.apache.spark starting with 1.5.1.1_2.6.0.16.1506060127_1510272107.

.. _soy_examples:

Examples
--------

.. _soy_examples_sparkpi:

SparkPi Example
~~~~~~~~~~~~~~~

- kinit on the cluster ``kinit <userid>@Y.CORP.YAHOO.COM``
- run it
  - The usage of the ``SparkPi`` example is ``Usage: SparkPi [<slices>]``
  - run it on YARN (substitute user as appropriate)
  ::

    spark-submit  --master yarn --deploy-mode cluster \
      --class org.apache.spark.examples.SparkPi --num-executors 2 --executor-memory 2g --queue default \
      $SPARK_HOME/lib/spark-examples.jar

  - Some dummy text
- See the results by looking at the Application Master's logs via yarn logs: ``yarn logs -applicationId <your_app_id> | less``.

::

  LogType: stdout
  LogLength: 22
  Log Contents:
  Pi is roughly 3.13612

.. _soy_examples_hdfslr:

SparkHdfsLR Example using HDFS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- kinit on the cluster: ``kinit %USERNAME%@Y.CORP.YAHOO.COM``
- Download the ``lr_data.txt`` file: ``wget http://raw.githubusercontent.com/apache/spark/master/data/mllib/lr_data.txt --no-check-certificate``
- upload it into your hdfs directory: ``hadoop fs -put lr_data.txt``

  - run it

    - The usage of the SparkHdfsLR example is: ``Usage: SparkHdfsLR <file> <iters>``
    - Here is how you run it on YARN (substitute user, and your_userid as appropriate)

    ::

      spark-submit --master yarn --deploy-mode cluster \
        --class org.apache.spark.examples.SparkHdfsLR  --executor-memory 3G --executor-cores 2 \
        --queue default --num-executors 3 --driver-memory 3g  \
        $SPARK_HOME/jars/spark-examples.jar lr_data.txt 10

    - See the results by looking at the Application Master's logs via yarn logs: ``yarn logs -applicationId <your appId> | less``

.. _soy_examples_wordcount:

JavaWordCount example
~~~~~~~~~~~~~~~~~~~~~

- kinit on the cluster: ``kinit %USERNAME%@Y.CORP.YAHOO.COM``
- Download the ``README.md`` file: ``wget https://raw.github.com/mesos/spark/master/README.md --no-check-certificate``
- upload it into your hdfs directory: ``hadoop fs -put README.md``
- run it

  - The usage of the JavaWordCount example is

  ::

    Usage: JavaWordCount <file>

  - Here is how you run it on YARN (substitute user, and your_userid as appropriate)

  ::

    spark-submit --class org.apache.spark.examples.JavaWordCount \
       --master yarn --deploy-mode cluster  --executor-memory 3g  --queue default --num-executors 3 --driver-memory 3g \
       $SPARK_HOME/lib/spark-examples.jar README.md

  - See the results by looking at the Application Master's logs via yarn logs: ``yarn logs -applicationId < your appId > | less``

.. _soy_conf:

Custom configs
--------------

.. _soy_conf_setup:

Setup
~~~~~

If you aren't using the default configs provide you will need to set the classpath yourself:

.. _soy_conf_setup_java_ldlib:

JAVA_HOME and LD_LIBRARY _PATH
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Use 64 bit jdk:

::

  spark.executorEnv.JAVA_HOME /home/gs/java8/jdk64/
  spark.executorEnv.LD_LIBRARY_PATH /home/gs/hadoop/current/lib/native/Linux-amd64-64/
  spark.yarn.appMasterEnv.JAVA_HOME /home/gs/java8/jdk64/
  spark.yarn.appMasterEnv.LD_LIBRARY_PATH /home/gs/hadoop/current/lib/native/Linux-amd64-64/

.. _soy_conf_history_server:

Spark Configs for History Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Spark has a history server similar to the MapReduce one. You have to have the following configs on for it to save the history for your application. 
- ``spark.eventLog.enabled true``
- ``spark.eventLog.dir hdfs:///mapred/sparkhistory``

You can also set this config for it to properly link the RM to the Spark history server # modify this to link the RM history UI link to the spark history server properly on your grid (change grid and colo below) 
- ``spark.yarn.historyServer.address grid-jt1.colo.ygrid.yahoo.com:18080``
The spark history server URI is: ``ResourceManager:18080``. So AxoniteRed would be: ``axonitered-jt1.red.ygrid.yahoo.com:18080``

.. _soy_yahoozip:

Reading YahooZip Compressed Files
---------------------------------

See `YahooZip user guide <http://twiki.corp.yahoo.com/view/SDSMain/YahooZipUserGuide>`_ for background and `Spark section <http://twiki.corp.yahoo.com/view/SDSMain/YahooZipUserGuide#A_7_Spark>`_ for example usage.

.. _soy_debugging:

Debugging information
---------------------
- `SparkDebugging` :ref:`dbg`

.. _soy_faq:

FAQ
---

- My application Final app status: SUCCEEDED, exitCode: 0 but application failed and retried

  - call spark.stop() at the end of your program
- RDDs vs Datasets vs Dataframes:

  - RDDs are lower level constructs. Users can apply general lambda functions to RDD methods like .filter, .flatMap, .reduce, but they don't have the support of Spark SQL's catalyst and other optimizations.
  - Dataset is a new interface added in Spark 1.6 that provides the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL’s optimized execution engine. A Dataset can be constructed from JVM objects and then manipulated using functional transformations (map, flatMap, filter, etc.). The Dataset API is available in Scala and Java. Python does not have the support for the Dataset API. But due to Python’s dynamic nature, many of the benefits of the Dataset API are already available (i.e. you can access the field of a row by name naturally row.columnName). The case for R is similar.
  - A DataFrame is a Dataset organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs. The DataFrame API is available in Scala, Java, Python, and R. In Scala and Java, a DataFrame is represented by a Dataset of Rows. In the Scala API, DataFrame is simply a type alias of Dataset[Row]. While, in Java API, users need to use Dataset<Row> to represent a DataFrame.
  - See also: https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html, and https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes.


.. _soy_local_mode:

Running in Local Mode
---------------------

The Yahoo version of Spark has authentication on by default. On YARN the secret key is generated for the user automatically but when running in local mode the secret key must be set manually.
- add `` --conf spark.authenticate.secret=testingsecret`` to your spark-submit command 
- run it ``./bin/spark-shell``

  - alternatively you can specify a number of executors to use like ``./bin/spark-shell --master local[2] --conf spark.authenticate.secret=testingsecret``

- See the Spark documentation here: http://spark.apache.org/docs/latest/index.html for more details.

.. _soy_jira:

Spark Jira
----------
- https://jira.corp.yahoo.com/browse/YSPARK

.. _soy_mailing_list:

Spark Users mailing list
------------------------
``yspark-users@oath.com``

Spark Dev mailing list
----------------------
``spark-devel@oath.com``

Spark Users slack channel
-------------------------
``#spark-users``
