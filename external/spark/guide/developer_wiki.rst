.. _dev_wiki:

Developer Wiki
===============

Spark Performance Tests
-----------------------

spark-perf is a spark performance testing framework. We currently support running performance tests for spark core (RDD API), pyspark, and spark streaming in the grid.

Usage
~~~~~

- Install **yspark_yarn_perf** package from https://dist.corp.yahoo.com/by-package/yspark_yarn_perf/ on a grid gateway or a spark launcher box. (E.g., run ``yinst i yspark_yarn_perf -br quarantine -r ~/SPARK_PERF_DIR``)

- Copy **share/spark-perf/config/config.py.template** to **share/spark-perf/config/config.py** and edit that file to update the configs or test-specific parameters (E.g., tests to include, the scale factor, and the memory settings, etc.). You can also leave them as default. More information can be found on  https://git.vzbuilders.com/hadoop/spark-perf/blob/master/README.md.

- Execute ``share/spark-perf/bin/run`` to kick off the spark-perf.
