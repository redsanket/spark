Specifying Configuration Variables with the ``-D`` Option
=========================================================

You can specify additional configuration variables by using ``-D <property>=<value>``.

  .. code-block:: bash

    # To change the local temp directory use:
    -D dfs.data.dir=/tmp

    # To specify additional local temp directories use:
    -D mapred.local.dir=/tmp/local
    -D mapred.system.dir=/tmp/system
    -D mapred.temp.dir=/tmp/temp


Often, you may want to process input data using a map function only. To do this, simply set ``mapreduce.job.reduces`` to zero. The Map/Reduce framework will not create any reducer tasks. Rather, the outputs of the mapper tasks will be the final output of the job. To be backward compatible, Hadoop Streaming also supports the ``-reducer NONE`` option, which is equivalent to ``-D mapreduce.job.reduces=0``.

  .. code-block:: bash

    # Specifying Map-Only Jobs
    -D mapreduce.job.reduces=0
    
    # Specifying the Number of Reducers, for example two, use:
    hadoop jar hadoop-streaming-2.10.0.jar \
      -D mapreduce.job.reduces=2 \
      -input myInputDirs \
      -output myOutputDir \
      -mapper /bin/cat \
      -reducer /usr/bin/wc

When the Map/Reduce framework reads a line from the stdout of the mapper, it splits the line into a key/value pair. By default, the prefix of the line up to the first tab character is the key and the rest of the line (excluding the tab character) is the value.
However, you can customize how Lines are Split into Key/Value Pairs. You can specify a field separator other than the tab character (the default), and you can specify the n :superscript:`th` (``n >= 1``) character rather than the first character in a line (the default) as the separator between the key and value. For example:

  .. code-block:: bash

    # Customizing How Lines are Split into Key/Value Pairs
    hadoop jar hadoop-streaming-2.10.0.jar \
      -D stream.map.output.field.separator=. \
      -D stream.num.map.output.key.fields=4 \
      -input myInputDirs \
      -output myOutputDir \
      -mapper /bin/cat \
      -reducer /bin/cat

In the above example, ``-D stream.map.output.field.separator=.`` specifies ``.`` as the field separator for the map outputs, and the prefix up to the fourth ``.`` in a line will be the key and the rest of the line (excluding the fourth ``.``) will be the value. If a line has less than four ``.`` s, then the whole line will be the key and the value will be an empty Text object (like the one created by new Text("")). |br|
Similarly, you can use ``-D stream.reduce.output.field.separator=SEP`` and ``-D stream.num.reduce.output.fields=NUM`` to specify the :superscript:`th` field separator in a line of the reduce outputs as the separator between the key and the value. |br|
Similarly, you can specify ``stream.map.input.field.separator`` and ``stream.reduce.input.field.separator`` as the input separator for Map/Reduce inputs. By default the separator is the tab character.


How to work with Mutiple Files, Large Files and Archives?
=========================================================

The ``-files`` and ``-archives`` options allow you to make files and archives available to the tasks. The argument is a URI to the file or archive that you have already uploaded to HDFS. These files and archives are cached across jobs. You can retrieve the host and `fs_port` values from the ``fs.default.name`` config variable.

The ``-files`` option creates a symlink in the current working directory of the tasks that points to the local copy of the file. In this example, Hadoop automatically creates a symlink named testfile.txt in the current working directory of the tasks. This symlink points to the local copy of testfile.txt.


  .. code-block:: bash

    -files hdfs://host:fs_port/user/testfile.txt

    # User can specify a different symlink name for -files using #.
    -files hdfs://host:fs_port/user/testfile.txt#testfile

    # Multiple entries can be specified like this:
    -files hdfs://host:fs_port/user/testfile1.txt,hdfs://host:fs_port/user/testfile2.txt


The ``-archives`` option allows you to copy jars locally to the current working directory of tasks and automatically unjar the files. In this example, Hadoop automatically creates a symlink named ``testfile.jar`` in the current working directory of tasks. This symlink points to the directory that stores the unjarred contents of the uploaded jar file.

  .. code-block:: bash

    -archives hdfs://host:fs_port/user/testfile.jar

    # User can specify a different symlink name for -archives using #.
    -archives hdfs://host:fs_port/user/testfile.tgz#tgzdir

In this example, the `input.txt` file has two lines specifying the names of the two files: `cachedir.jar/cache.txt` and `cachedir.jar/cache2.txt`. `cachedir.jar` is a symlink to the archived directory, which has the files `cache.txt` and `cache2.txt`.

.. literalinclude:: /resources/code/mapreduce/streaming-faq-archive-command-example.bash
   :language: bash
   :caption: Making Archives Available to Tasks
   :linenos:

How to partition the map outputs based on certain key fields?
=============================================================

Hadoop has a library class, :hadoop_rel_doc:`KeyFieldBasedPartitioner <api/org/apache/hadoop/mapred/lib/KeyFieldBasedPartitioner.html>`, that is useful for many applications. This class allows the Map/Reduce framework to partition the map outputs based on certain key fields, not the whole keys. For example:

  .. parsed-literal::

     hadoop jar hadoop-streaming-|HADOOP_RELEASE_VERSION|.jar
        -D stream.map.output.field.separator=.
        -D stream.num.map.output.key.fields=4
        -D map.output.key.field.separator=.
        -D mapreduce.partition.keypartitioner.options=-k1,2
        -D mapreduce.job.reduces=12
        -input myInputDirs
        -output myOutputDir
        -mapper /bin/cat
        -reducer /bin/cat
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

Here, ``-D stream.map.output.field.separator=.`` and ``-D stream.num.map.output.key.fields=4`` are as explained in previous example. The two variables are used by streaming to identify the key/value pair of mapper. |br|
The map output keys of the above Map/Reduce job normally have four fields separated by `.`. However, the Map/Reduce framework will partition the map outputs by the first two fields of the keys using the ``-D mapred.text.key.partitioner.options=-k1,2`` option. Here, ``-D map.output.key.field.separator=.`` specifies the separator for the partition. This guarantees that all the key/value pairs with the same first two fields in the keys will be partitioned into the same reducer.


How Can I specify Hadoop Comparator Class?
==========================================

Hadoop has a library class, :hadoop_rel_doc:`KeyFieldBasedComparator <api/org/apache/hadoop/mapreduce/lib/partition/KeyFieldBasedComparator.html>`, that is useful for many applications. This class provides a subset of features provided by the Unix/GNU Sort. For example:

  .. parsed-literal::

     hadoop jar hadoop-streaming-|HADOOP_RELEASE_VERSION|.jar
        -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator
        -D stream.map.output.field.separator=.
        -D stream.num.map.output.key.fields=4
        -D mapreduce.map.output.key.field.separator=.
        -D mapreduce.partition.keycomparator.options=-k2,2nr
        -D mapreduce.job.reduces=1
        -input myInputDirs
        -output myOutputDir
        -mapper /bin/cat
        -reducer /bin/cat


Does Streaming support Aggregators?
===================================

Hadoop has a library package called :hadoop_rel_doc:`Aggregate <api/org/apache/hadoop/mapred/lib/FieldSelectionMapReduce.html>`. Aggregate provides a special reducer class and a special combiner class, and a list of simple aggregators that perform aggregations such as `sum`, `max`, `min` and so on over a sequence of values. Aggregate allows you to define a mapper plugin class that is expected to generate `aggregatable items` for each input key/value pair of the mappers. The combiner/reducer will aggregate those aggregatable items by invoking the appropriate aggregators. |br|
To use Aggregate, simply specify ``-reducer aggregate``:


  .. parsed-literal::

     hadoop jar hadoop-streaming-|HADOOP_RELEASE_VERSION|.jar
        -input myInputDirs
        -output myOutputDir
        -mapper myAggregatorForKeyCount.py
        -reducer aggregate
        -file myAggregatorForKeyCount.py

The python program `myAggregatorForKeyCount.py` looks like:

  .. code-block:: python
     
      #!/usr/bin/python

      import sys;

      def generateLongCountToken(id):
          return "LongValueSum:" + id + "\t" + "1"

      def main(argv):
          line = sys.stdin.readline();
          try:
              while line:
                  line = line&#91;:-1];
                  fields = line.split("\t");
                  print generateLongCountToken(fields&#91;0]);
                  line = sys.stdin.readline();
          except "end of file":
              return None
      if __name__ == "__main__":
           main(sys.argv)

How To process text data like the unix `cut` utility?
=====================================================

Hadoop has a library class, :hadoop_rel_doc:`FieldSelectionMapReduce <api/org/apache/hadoop/mapred/lib/FieldSelectionMapReduce.html>, that effectively allows you to process text data like the unix `cut` utility. The map function defined in the class treats each input key/value pair as a list of fields. You can specify the field separator (the default is the tab character). You can select an arbitrary list of fields as the map output key, and an arbitrary list of fields as the map output value. Similarly, the reduce function defined in the class treats each input key/value pair as a list of fields. You can select an arbitrary list of fields as the reduce output key, and an arbitrary list of fields as the reduce output value. For example:

  .. parsed-literal::

     hadoop jar hadoop-streaming-|HADOOP_RELEASE_VERSION|.jar
        -D mapreduce.map.output.key.field.separator=.
        -D mapreduce.partition.keypartitioner.options=-k1,2
        -D mapreduce.fieldsel.data.field.separator=.
        -D mapreduce.fieldsel.map.output.key.value.fields.spec=6,5,1-3:0-
        -D mapreduce.fieldsel.reduce.output.key.value.fields.spec=0-2:5-
        -D mapreduce.map.output.key.class=org.apache.hadoop.io.Text
        -D mapreduce.job.reduces=12
        -input myInputDirs
        -output myOutputDir
        -mapper org.apache.hadoop.mapred.lib.FieldSelectionMapReduce
        -reducer org.apache.hadoop.mapred.lib.FieldSelectionMapReduce
        -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner

The option ``-D mapreduce.fieldsel.map.output.key.value.fields.spec`` specifies key/value selection for the map outputs. Key selection spec and value selection spec are separated by `:`. In this case, the map output key will consist of fields 6, 5, 1, 2, and 3. The map output value will consist of all fields (0- means field 0 and all the subsequent fields).

The option ``-D mapreduce.fieldsel.reduce.output.key.value.fields.spec`` specifies key/value selection for the reduce outputs. In this case, the reduce output key will consist of fields 0, 1, 2 (corresponding to the original fields 6, 5, 1). The reduce output value will consist of all fields starting from field 5 (corresponding to all the original fields).


How do I run an arbitrary set of (semi) independent tasks?
==========================================================

Often you do not need the full power of Map Reduce, but only need to run multiple instances of the same program - either on different parts of the data, or on the same data, but with different parameters. You can use Hadoop Streaming to do this.

How do I process files, one per map?
====================================

As an example, consider the problem of zipping (compressing) a set of files across the hadoop cluster. You can achieve this by using Hadoop Streaming and custom mapper script:

* Generate a file containing the full HDFS path of the input files. Each map task would get one file name as input.
* Create a mapper script which, given a filename, will get the file to local disk, gzip the file and put it back in the desired output directory.

How do I specify multiple input directories?
============================================

You can specify multiple input directories with multiple ``-input`` options:

  .. parsed-literal::

     hadoop jar hadoop-streaming-|HADOOP_RELEASE_VERSION|.jar
           -input '/user/foo/dir1' -input '/user/foo/dir2'
           (rest of the command)

How do I generate output files with gzip format?
================================================

Instead of plain text files, you can generate gzip files as your generated output.
Pass the following as option to your streaming job:

  .. parsed-literal::

     -D mapreduce.output.fileoutputformat.compress=true
     -D mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec