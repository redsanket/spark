.. _r:

How to use R on Spark
=====================

Using R on Spark requires spark to be compiled with R support and R to be available on the nodes.
sparkR requires yspark_yarn-1.5.1.1_2.6.0.16.1506060127_1510071630 or higher.

.. _r_grid:

R installed on the Grid gateways and HDFS for Spark > 2.2
---------------------------------------------------------
Starting with spark 2.2 we automatically handle shipping R for you. You simply start sparkR.

.. code-block:: console
  For example:
  $SPARK_HOME/bin/sparkR  --master yarn --deploy-mode client

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster ~/datamanipulation.R flights.csv
  You can also use the tgz installed in hdfs for older versions: /sharelib/v1/yspark_yarn_R32/yspark_yarn_R32.tgz and on the gateways /home/y/var/yspark_yarn_R32/

.. _r_manual_install:

Manually Install Base R package (if not using grid installed version)
---------------------------------------------------------------------
Assumes running from /homes/<user>
- mkdir R_installation
- wget http://cran.rstudio.com/src/base/R-3/R-3.2.1.tar.gz
- tar xvf R-3.2.1.tar.gz
- cd R-3.2.1
- export R_HOME?=/home/schintap/R_installation/R-3.2.1
- ./configure prefix=$R_HOME/R
- make && make install

.. _r_modules:

Add other R modules (if you need to)
------------------------------------
If you need to use other R modules in spark you can install those in a separate archive and send them with your application. You just need to load them from whatever location you specify.
For instance, I can package the astsa module and ship it with my spark application. In my R code when I load it I would load it like: library("astsa",lib.loc='./R_library/')

Create an archive with the R modules you want: (assumes you installed manually installed base R package below in /homes/schintap/R_installation. See section on Manually Install Base R package)
- cd R_installation
- mkdir /homes/schintap/R_library
- export PATH=/homes/schintap/R_installation/bin:$PATH
- ./bin/R to start R
- install.packages('packageName',lib='/homes/schintap/R_library')
- follow prompts to install
- Iterate if you need to install other packages
- once installed all then exit R
- cd /homes/<user>/R_library
- tar -zcvf R_library.tgz *
- Put it into HDFS if using with yarn cluster mode: hadoop fs -put R_library.tgz

You now have an archive you can ship with your spark application. Your code needs to load it via the path you specify for yarn cluster mode.
Examples below use "./R_library/". for yarn client mode it should load them from the gateway install location /homes/schintap/R_library.
See examples in sections below.

.. _r_client_mode:

Run Spark R in yarn client mode using Manual installed R
--------------------------------------------------------
- export PATH=/homes/schintap/R_installation/bin:$PATH
- export R_HOME=/homes/tgraves/R_installation/lib64/R
- cd /homes/schintap (or where ever your base install of R_installation is)

.. code-block:: console

  Interactive:
  $SPARK_HOME/bin/sparkR --master yarn --deploy-mode client --conf spark.sparkr.r.command=./R_installation/bin/Rscript --archives hdfs:///user/%USERNAME%/__yspark_R.tgz#R_installation

  Batch using script:
  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --conf spark.sparkr.r.command=./R_installation/bin/Rscript --archives hdfs:///user/%USERNAME%/__yspark_R.tgz#R_installation  myscript.R

  For accessing other R modules in client mode simply access them like: library("astsa",lib.loc='./R_library/') or library("astsa",lib.loc='/homes/%USERNAME%/R_library/')


.. _r_cluster_mode:

Run Spark R in yarn cluster mode using Manual installed R
---------------------------------------------------------

This assumes you have already manually installed R.
- Run specifying archives and path to R installation. Note the config spark.sparkr.r.command must be as specified (--conf spark.sparkr.r.command=./R_installation/bin/Rscript).
- For accessing other R modules you installed in R_library.tgz change your R code to access them via library("astsa",lib.loc='./R_library/.

.. code-block:: console
  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf spark.sparkr.r.command=./R_installation/bin/Rscript --archives hdfs:///user/%USERNAME%/__yspark_R.tgz#R_installation,hdfs:///user/%USERNAME%//R_library.tgz#R_library ~/test.R

.. _r_oozie:

Run Spark R with Oozie
----------------------
Information regarding running Spark R with Oozie is in the link below
https://git.ouroath.com/pages/hadoop/docs/spark/spark_from_oozie.rst

.. _r_hive:

Hive Access
-----------
For client mode it should just work, in cluster mode see the version specific instructions (https://git.ouroath.com/pages/hadoop/docs/spark/spark_on_yarn.rst).
In general you need to send along the hive-site.xml and possibly the datanucleus jars (for 1.x versions).
Make sure to initialize your spark session with hive enabled: sparkR.session(appName = "test", enableHiveSupport = TRUE)

.. code-block:: console

  Examples:
  Spark 1.6:
  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf spark.sparkr.r.command=./R_installation/bin/Rscript
  --archives hdfs:///user/tgraves/__yspark_R.tgz#R_installation,hdfs:///user/tgraves//R_library.tgz#R_library --files $SPARK_CONF_DIR/hive-site.xml
  --jars $SPARK_HOME/lib/datanucleus-core-3.0.9.jar,$SPARK_HOME/lib/datanucleus-rdbms-3.0.8.jar,$SPARK_HOME/lib/datanucleus-api-jdo-3.0.7.jar ~/test.R

  Spark 2.X:
  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf spark.sparkr.r.command=./R_installation/bin/Rscript
  --archives hdfs:///user/tgraves/__yspark_R.tgz#R_installation,hdfs:///user/tgraves//R_library.tgz#R_library
  --files $SPARK_CONF_DIR/hive-site.xml ~/test.R

.. _r_examples:

Examples
--------
See the spark R documentation. http://spark.apache.org/docs/latest/sparkr.html.
Note the faithful dataset they refer to doesn't seem to be present in this distribution of R.

.. _r_parquet:

Reading and writing parquet
---------------------------
Get https://github.com/apache/spark/blob/master/examples/src/main/resources/users.parquet and put into hdfs in /user/%USERNAME%/
library(SparkR)

.. code-block:: console
  sc <- sparkR.init()
  sqlContext <- sparkRSQL.init(sc)
  people <- read.df(sqlContext, "/user/%USERNAME%/users.parquet")
  head(people)
  write.df(people, path="people.parquet", source="parquet", mode="overwrite")
