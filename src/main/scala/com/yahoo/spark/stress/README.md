Spark stress testing jobs, adapted from spark-perf (https://github.com/databricks/spark-perf).

Available tests:
- aggregate-by-key
- aggregate-by-key-int
- aggregate-by-key-naive
- sort-by-key
- sort-by-key-int
- count
- count-with-filter

###  Usage

#### spark-submit:

Pass test name as the first argument, then pass test-specific options with --key=value format. The description of test-specific options can be found on the test files.

Example command for running a sort-by-key-int test:

```
$SPARK_HOME/bin/spark-submit \
--deploy-mode cluster \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=8g \
--conf spark.driver.memory=8g \
--class com.yahoo.spark.stress.SparkStressTest \
./spark-starter-2.0-SNAPSHOT-jar-with-dependencies.jar \
sort-by-key-int \
--num-trials=1 \
--inter-trial-wait=10 \
--num-partitions=1000 \
--reduce-tasks=1000 \
--num-records=20000000000 \
--unique-keys=200000 \
--key-length=100 \
--unique-values=10000000 \
--value-length=100 \
--random-seed=1 \
--persistent-type=hdfs \
--storage-location="hdfs:///tmp/stresstest/data" \
```

With the above options, the spark will generate around 130 GB shuffle intermediate data.