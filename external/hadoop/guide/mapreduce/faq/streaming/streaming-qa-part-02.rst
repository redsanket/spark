.. _guide_faq_compressing_output_of_streaming_job:

Compressing the output of a streaming job
=========================================


**Q:** Is there a way to set the output format in hadoop streaming to be gzip similar to the way input compression is specified:
``-jobconf stream.recordreader.compression=gzip``

**Ans:**

  .. code-block:: bash

    -jobconf mapred.output.compress=true \
    -jobconf mapred.output.compression.type=gzip \
    -jobconf mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec


What are the rules for hadoop streaming quotes?
===============================================

This works:

  .. code-block:: bash

    -mapper "perl -ane 'print join(\"\n\", @F), \"\n\"'


Sorting data to use off the grid
================================


.. todo:: find missing page `Lexical Partitioner`


Sometimes it is necessary to take data off the grid and occasionally you would like it to be sorted. The grid does a great job of partitioning data and sorting it efficiently within each partion, but to get the sorted data off, you need to merge the partitions.
``dfs -getmerge`` won't do it for you (it just concatenates the parts) and specifying that you want all output to go to a single reducer is just as bad as doing that final merge on a single machine.
The right way to do it is to use a `Lexical Partitioner <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Grid/PartitioningInLexiconOrde>`_ as suggested in the original :cite:`2004:OSDI-MR`. Unfortunately, the only partitioners supplied in the streaming jar are the default HashPartitioner and ``KeyFieldBasedPartitioner``.

An easy way to speed up sorts, provided your keys are reasonably well-behaved, is to prefix each record with just a part of the key that you want to sort by.
For example, to sort alphabetically, you could use as a mapper:

  .. code-block:: bash

    awk -F '\t' '{ print substr($1, 0, 2) \"\t\" $0 }'


You then specify that you want to use ``KeyFieldBasedPartitioner`` to partition on just the single prefix field while sorting within the partition on the full key:

  .. code-block:: bash

    -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
    -jobconf num.key.fields.for.partition=1 stream.num.map.output.key.fields=2. 

Your reducer can just be cat. Each reducer output file will be fully sorted and all the records with a given prefix will go to just a single file. Note that if you are sorting URLs, the ww partition may be rather large. If you are sorting UTF-8 strings with many non-ascii characters (e.g. Japanese), you may want to use a 3- or 6-byte prefix instead of 2 bytes as in the example above.

Now you need to put the files together off the grid. sort ``-m`` won't do because it doesn't utilize the knowledge you have about distinct keys being in different files, so it takes a very long time doing unnecessary comparisons.

You need to write a merge program of your own to merge the files off the grid. This program merges the files normally except that whenever it reads a new key from a file, it just slurps all the records with the same key from that file and sends them to the output. Experience sorting queries in a 6GB sample on 200 nodes is that each of 800 or so reducer files had just 1 to 4 partitions in it. Sorting on the grid took about 3 minutes instead of 30 minutes using a single reducer. The final merge was comparable in speed to using cat.

A sample merge loop in `C++` is:

  .. code-block:: c++

    void fast_merge(heap_of_files &heap , ostream *out)
    {
      source_t *top = heap.top();
      for ( top = heap.top(); ! top->eof(); top = heap.top() ) {
        char currkey[1024];
        strcpy(currkey, top->key);
        do {                    // slurp all the records with a given key
          (*out) << top->value << '\n';
          top->advance();
        } while (! top->eof() && strcmp(top->key, currkey) == 0);
        heap.topModified();    // force the next merge step
      }
    }

Similarly, to sort descending by frequency, where frequency is in the second field of a record, use a mapper such as:

  .. code-block:: bash

    awk -F '\t' '{ print 1000000000-$2 \"\t\" $0 }'

and change the sort criterion in the merge program to compare numerically.


How to process 10% webdata using streaming
==========================================

`tenPercentSample` webdata could be used with streaming by specifying input format as `SequenceFileAsTextInputFormat`. Since value class of the webdata is of Document class, we would also need to included the tenPercentSample jar via the ``-file`` option. So your streaming command would include 2 more options

  .. code-block:: bash

    –inputformat org.apache.hadoop.mapred.SequenceFileAsTextInputFormat \
    -file /usr/releng/tools/hadoop/kryptonite/examples/tenPercentSample/build/kryptonite-0.0.1-dev.jar

As an example, to cat the contents of `tenPercentSample`.

  .. code-block:: bash

    hod -m 50 -b 4 \
      -a 'stream -inputformat org.apache.hadoop.mapred.SequenceFileAsTextInputFormat \
      -input "/data/webdata/tenPercentSample/31Aug2007/part-00147" \
      -output /user/lohit/testOut -verbose \
      -mapper cat -reducer NONE \
      -file /usr/releng/tools/hadoop/kryptonite/examples/tenPercentSample/build/kryptonite-0.0.1-dev.jar'

How to process SDS data using streaming
=======================================

.. todo:: find missing page `How to process ULTRecords`

Please refer SDS processing using streaming for examples and usage.
Also refer to `How to process ULTRecords <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Grid/UsingStreaming>`_ (below) for more generic options.

How to process ULTRecords with streaming
========================================

How to turn speculative execution on and off?
=============================================

Supply the following parameters on the command line.

  .. code-block:: bash

    -jobconf mapred.map.tasks.speculative.execution=true \
    -jobconf mapred.reduce.tasks.speculative.execution=true


The ``stream.non.zero.exit.status.is.failure`` Parameter and Job Failures
=========================================================================

In 0.18.0+, configuration parameter ``stream.non.zero.exit.status.is.failure`` is set to true as a default. So, the ``egrep/grep`` command used in the mapper, if not finding any matching records, would return non-zero return code, thus would fail the map task and hence the job. You can set this configuration parameter to false, if encounter such problem.

Processing files, one per map
=============================

An example of this was the problem of zipping file. Given a set of files, the user wanted to zip it up and wanted to do it across the hadoop cluster.
This could be achieved in few ways.

#. Using Hadoop Streaming and custom mapper script. Generate a file containing the full DFS path of the input files. Each map task would get one file name as input. Create a mapper script which, given a filename will get the file to local disk, gzips the file and puts it back in the desired output directory

#. Use the existing Hadoop Framework to achieve this Add these to your main function:

   .. code-block:: java

     OutputFormatBase.setCompressOutput(conf, true);
     OutputFormatBase.setOutputCompressorClass(conf,
          org.apache.hadoop.io.compress.GzipCodec.class);
     conf.setOutputFormat(NonSplitableTextInputFormat.class);
     conf.setNumReduceTasks(0);

#. Write your map function as show below

   .. code-block:: java
     
      public void map(WritableComparable key, Writable value, 
                      OutputCollector output, 
                      Reporter reporter) throws IOException {
        output.collect((Text)value, null);
      }

However, the output filename will not be the same as the original filename.

How to specify multiple input directories
=========================================

Multiple input directories can be specified using multiple `-input` option. E.g.

  .. code-block:: bash
    
    hadoop jar hadoop-streaming.jar \
      -input '/user/foo/dir1' -input '/user/foo/dir2' 