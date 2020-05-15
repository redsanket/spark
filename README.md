# Example Spark Jobs

spark-starter contains simple examples of using Spark. These simple examples should get you started with using Spark. The pom file included in this project should also help you get started with build your own Spark project. It includes stuff to be able to build Java or Scala files as well as the spark dependencies necessary for accessing Spark Core, Sql, and Hive apis.

#Running with Maven

To use hortonworks Spark-HBase Connector, you will need to specify a compatible shc-core jar in pom.xml. Since the latest shc-core does not exist in public repositories, you will need to install it locally.
```
<configuration>
    <groupId>com.hortonworks</groupId>
    <artifactId>shc-core</artifactId>
    <version>1.0</version>
    <packaging>jar</packaging>
    <file>${basedir}/lib/shc-core-1.1.2-<spark_version>-s_<scala_version>-SNAPSHOT.jar</file>
</configuration>
```
`<spark_version>` and `<scala_version>` should be substituted with the version you are using.

To install shc-core to local maven repository:

```
mvn initialize
```

To create a package:

```
mvn clean package
```

Jars are located in <project-root>/target.  Two jars will be created in <project-root>/target, one jar packaged with all dependencies:

```
spark-starter-2.0-SNAPSHOT-jar-with-dependencies.jar
```

and another packaged with just the example code:

```
spark-starter-2.0-SNAPSHOT.jar
```

Example of how to run on grid:

> Ensure that ```SPARK_HOME``` and ```SPARK_CONF_DIR``` are set on the gateway or launcher box.

* Running word count example in java. 
```
spark-submit --class com.yahoo.spark.starter.SparkPi \
--master yarn \
--deploy-mode cluster  \
--executor-memory 3g  \
--queue default \
--num-executors 3 \
--driver-memory 3g \
$SPARK_STARTER_HOME/target/spark-starter-2.0-SNAPSHOT-jar-with-dependencies.jar
```

* Running word count example in scala which writes output to provided uri.
```
spark-submit --class com.yahoo.spark.starter.ScalaWordCount \
--master yarn \
--deploy-mode cluster \
--queue default \
--driver-memory 5g \
$SPARK_STARTER_HOME/target/spark-starter-2.0-SNAPSHOT.jar  largeRandomText/chunk[1-3]/*  largeRandomText/output/
``` 

For more details please reference 

http://twiki.corp.yahoo.com/view/Grid/SparkOnYarnProduct
