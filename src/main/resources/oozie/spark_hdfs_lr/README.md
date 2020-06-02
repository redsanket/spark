Instructions for running this oozie application:

- create a directory `spark_hdfs_lr/` in HDFS for the oozie application.

- upload `workflow.xml` to `spark_hdfs_lr/apps/spark/`.

- use `mvn clean package` to create the jar package of spark-starter if you haven't done so.

- upload the jar package `spark-starter/target/spark-starter-2.0-SNAPSHOT-jar-with-dependencies.jar` to `spark_hdfs_lr/apps/lib/`.

- upload resource files `spark-starter/src/main/resources/data/lr_data.txt` to `spark_hdfs_lr/data/`.

- update `nameNode` and `jobTracker` in `job.properties` if you are running on the cluster other than AR.

- export OOZIE_URL, for example, `export OOZIE_URL=https://axonitered-oozie.red.ygrid.yahoo.com:4443/oozie/`.

- submit the oozie job using `oozie job -run -config job.properties -auth KERBEROS`
