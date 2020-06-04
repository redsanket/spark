Instructions for running this oozie application:

- create a directory `spark_python/` in HDFS for the oozie application.

- upload `workflow.xml` to `spark_python/apps/spark/`.

- upload the .py file `spark-starter/src/main/python/python_word_count.py` to `spark_python/apps/lib/`.

- upload resource files `spark-starter/src/main/resources/data/README.md` to `spark_python/data/`.

- update `nameNode` and `jobTracker` in `job.properties` if you are running on the cluster other than AR.

- export OOZIE_URL, for example, `export OOZIE_URL=https://axonitered-oozie.red.ygrid.yahoo.com:4443/oozie/`.

- submit the oozie job using `oozie job -run -config job.properties -auth KERBEROS`