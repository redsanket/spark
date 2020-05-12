from __future__ import print_function

import sys

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python_word_count <input_file> <output_file>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()\
        .set('spark.executor.memory', '4g')\
        .set('spark.executor.cores', '2')\
        .set('spark.driver.memory', '8g')

    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .appName("Python Word Count")\
        .getOrCreate()

    # Add log messages to spark using the same log4j logger.
    # Note: This pattern works only for the driver.
    jvmLogger = spark.sparkContext._jvm.org.apache.log4j
    logger = jvmLogger.LogManager.getLogger('SparkStarter')
    logger.info('Input : ' + sys.argv[1])
    logger.info('Input : ' + sys.argv[2])

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile(sys.argv[2])

    spark.stop()
