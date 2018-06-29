#### Connecting to Apache HBase from Apache Spark using the SHC(Apache Spark - Apache HBase) Connector. 
For the original project you can look up the documentation at:
* https://github.com/hortonworks-spark/shc

For using the connector you need to use HBase Version 1.3 and above. Most of our grids (verify the status of HBase deployment) have the required version of HBase although we have 0.98 clients running on most of the gateways for backward compatibility.


***

#### Setup

* You would need hbase 1.3 jars to be supplied with your spark job.
```bash
# Since hbase-1.3 is not available on the gateways, you need to install hbase-1.3 locally to access the jars.
yinst i hbase-1.3.2.9.1804250211_h2 -br quarantine -r ~/hbase -nosudo

# Define an ENV variable - HBASE_JARS to point to the directory you installed hbase locally.
export HBASE_JARS=~/hbase/libexec/hbase/lib
```

#### Getting the connector
You can add the spark hbase connector jar in your project directory and update your pom file to package the dependencies with your application jar. Refer to the pom file in the spark-starter project for details.

1. Copy the spark-hbase-connector jar from the spark-starter project to your project under a directory - `lib`. It can be any appropriate location.
2. Update your pom file to ensure the dependency is installed when you build your project.
```xml
<!-- Add this to the plugins section -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-install-plugin</artifactId>
    <executions>
        <execution>
            <phase>initialize</phase>
            <goals>
                <goal>install-file</goal>
            </goals>
            <configuration>
                <groupId>com.hortonworks</groupId>
                <artifactId>shc-core</artifactId>
                <version>1.0</version>
                <packaging>jar</packaging>
                <file>${basedir}/lib/shc-core-1.1.2-2.2-s_2.11-SNAPSHOT.jar</file>
            </configuration>
        </execution>
    </executions>
</plugin>

<!-- specify the dependency under the dependencies section -->
<dependency>
    <groupId>com.hortonworks</groupId>
    <artifactId>shc-core</artifactId>
    <version>1.0</version>
</dependency>
```

* Launching in cluster mode
```bash
# launch the shell
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --conf "spark.hadoop.validateOutputSpecs=false" --jars $HBASE_JARS/hbase-protocol.jar,$HBASE_JARS/hbase-common.jar,$HBASE_JARS/hbase-client.jar,$HBASE_JARS/htrace-core-3.1.0-incubating.jar,$HBASE_JARS/hbase-server.jar,$HBASE_JARS/guava-12.0.1.jar,$HBASE_JARS/metrics-core-2.2.0.jar --files $SPARK_CONF_DIR/hbase-site.xml ...
```

* Launching in client mode
```bash
# launch the shell
$SPARK_HOME/bin/spark-submit  --master yarn --deploy-mode client --conf "spark.hadoop.validateOutputSpecs=false" --jars $HBASE_JARS/hbase-protocol.jar,$HBASE_JARS/hbase-common.jar,$HBASE_JARS/hbase-client.jar,$HBASE_JARS/htrace-core-3.1.0-incubating.jar,$HBASE_JARS/hbase-server.jar,$HBASE_JARS/guava-12.0.1.jar,$HBASE_JARS/metrics-core-2.2.0.jar .... 
```

* If you plan to write to HBase using the connector, set the required conf `"spark.hadoop.validateOutputSpecs=false"`. This is to workaround a bug in HBase. See YHBASE-2131 for more details.

### Using the connector
You need to define a catalog so that a DataFrame can be mapped to an HBase table or vice-versa. This is defined in a json format. Below is an example for your reference explaining how to define your own catalog.
```
def catalog = s"""{
        |"table":{"namespace":"default", "name":"table1"},
        |"rowkey":"key",
        |"columns":{
          |"df_col0":{"cf":"rowkey", "col":"key", "type":"string"},
          |"df_col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
          |"df_col2":{"cf":"cf2", "col":"col2", "type":"double"},
          |"df_col3":{"cf":"cf3", "col":"col3", "type":"float"},
          |"df_col4":{"cf":"cf4", "col":"col4", "type":"int"},
          |"df_col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
          |"df_col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
          |"df_col7":{"cf":"cf7", "col":"col7", "type":"string"},
          |"df_col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
        |}
      |}""".stripMargin
```
* `table` - you specify the namespace and the name of the table
* `rowkey` - you specify the column/s in the hbase table that form the rowkey. Composite keys are not currently supported.
* `columns` field, you specify the name of the column in the DataFrame and its corresponding column_family, column_name and column_type in hbase. You identify the column/s which are used to form the rowkey using the column_family as rowkey.

Note: Since a user can add any arbitrary columns to a column family, individual columns are not specified under the table schema in HBase table description. Therefore it is helpful to know the columns of interest available in hbase table while defining the catalog.

For more examples, you can check the examples in the original repo [here](https://github.com/hortonworks-spark/shc).

 


