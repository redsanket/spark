.. _r:

R on Spark
==========

Using R on Spark requires spark to be compiled with R support and R to be available on the nodes.

.. _r_grid:

VMG/VCG Grid Install
---------------------

For VMG/VCG we automatically handle shipping R for you for both current and latest (yspark_yarn-2.4.x).
Current R version supported is R-3.4.3 and the way to set the docker container is by specifying `--conf spark.oath.dockerImage=spark/rhel7_sparkr:current` during launch on your respective gateways.

Getting started on VMG/VCG and available gateways: https://git.vzbuilders.com/pages/developer/Bdml-guide/

VCG gateways:
  * Research/Sandbox: kessel-gw.gq.vcg.yahoo.com
  * Production: polaris-gw.gq.vcg.yahoo.com

Getting started on Oath Grid and know more about available clusters and gateways: https://yahoo.jiveon.com/community/science-technology/hadoop-and-big-data-platform


.. _r_examples:

Examples
--------
See the spark R documentation for examples. http://spark.apache.org/docs/latest/sparkr.html.
Note the faithful dataset they refer to doesn't seem to be present in this distribution of R.

.. _r_cluster_mode:

Spark R cluster mode
--------------------
The following script enables you to launch R on Spark in cluster mode and enables you to work with R-3.4.3.

R script is available here: https://github.com/apache/spark/blob/master/examples/src/main/r/data-manipulation.R

For example:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf spark.oath.dockerImage=spark/rhel7_sparkr:current ~/datamanipulation.R flights.csv


.. _r_client_mode:

Spark R client mode
-------------------
The R on spark client mode is an interative repl framework. On VCG/Oath gateway we use the following script to launch to work with R version R-3.4.3.

Interactive/Client Mode:

.. code-block:: console

  $SPARK_HOME/bin/sparkR  --master yarn --deploy-mode client  --conf spark.oath.dockerImage=spark/rhel7_sparkr:current

.. _r_hive:

Spark R with Hive
-----------------
R on spark can talk to hive and by default hive support is enabled. Make sure to initialize your spark session with hive enabled: `sparkR.session(appName = "test", enableHiveSupport = TRUE)`.

Examples:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf spark.oath.dockerImage=spark/rhel7_sparkr:current ~/test.R

.. _r_parquet:

Spark R with parquet
--------------------
Get https://github.com/apache/spark/blob/master/examples/src/main/resources/users.parquet and put into hdfs in /user/%USERNAME%/
library(SparkR)

.. code-block:: console

  sc <- sparkR.init()
  sqlContext <- sparkRSQL.init(sc)
  people <- read.df(sqlContext, "/user/%USERNAME%/users.parquet")
  head(people)
  write.df(people, path="people.parquet", source="parquet", mode="overwrite")

.. _r_jupyter:

How to use Sparkmagics R kernel on Jupyter
------------------------------------------
Jupyter has a R kernel available which enables users to have a shell based interaction framework similar to pyspark kernels. The demo_ gives us an introduction to the libraries available with R-3.4.3 and interact with R on spark via Jupyter.

.. _demo: https://kesselgq-jupyter.gq.vcg.yahoo.com:9999/nb/notebooks/projects/jupyter/demo/samples/sparkmagic/Jupyter_Demo_3.1_SparkR.ipynb


.. _r_custom_pkg:

Custom R packaging
------------------

If we want to use any other R package apart from the default packages available in the docker container please follow the steps here_

.. _here: https://git.vzbuilders.com/hadoop/sparktest/blob/spark_custom_R/README.md
