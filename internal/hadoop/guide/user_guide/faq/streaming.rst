*********
Streaming
*********

.. contents:: Table of Contents
  :local:
  :depth: 4

-----------

Making Hadoop act like Unix (for languages other than Java).

.. seealso:: Follow Apache Hadoop `streaming docs for r2\.10\.0 <https://hadoop.apache.org/docs/r2.10.0/hadoop-streaming/HadoopStreaming.html>`_

How Does Streaming Work?
========================

.. todo:: Find page HadoopStreaming

For an overview, see `Hadoop Streaming <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Grid/HadoopStreaming>`_

Streaming Tips
==============


How to use multiple jars to run my job?
---------------------------------------


* Use ``-libjars`` to specify multiple jars on the command line.
* Set environment variable ``HADOOP_CLASSPATH`` to the jars for your jobs main class.


What's the difference between ``TextInputFormat`` and ``OneLineInputFormat``
----------------------------------------------------------------------------

* ``TextInputFormat`` uses the default implementation of ``getSplits()`` inherited from ``FileInputFormat``. It respects the number of splits determined by the framework based on the size of your input data and the number of nodes available. You can't control the number of map tasks to spawn.

* ``OneLineInputFormat`` overrides ``getSplits()`` such that every single line in your input file(s) becomes an ``InputSplit``. Thus the number of map tasks spawned will be exactly the number of lines in your input file(s). And each map task is fed with a single line.

* Both inputformat classes use ``LineRecordReader`` to read a line into a ``<K,V> pair``. The byte offset of the first character of that line is to be used as the key. And the rest of the line is to be used as the value. However Hadoop Streaming has made it a special case for ``TextInputFormat`` that it passes only the value part to your pipe program. For other inputformat classes, both key and value will be passed to your pipe program, delimited by ``\t``.

Where to find ``OneLineInputFormat``?
--------------------------------------

* ``$HADOOP_HOME/tools/jars/oneLineInputformat.jar``
* ``$SOLUTIONS_HOME/jars/oneLineInputformat.jar`` (on mithril-gold)


How to make mutiple files available to my tasks?
------------------------------------------------

* Use ``-file`` if you have a couple of small files to ship with your job. Those files will be copied into CWD on the task nodes.

* Examples:
    
   * Use ``-cacheFile`` if your files are large.
   * Use ``-cacheArchive`` if your files are large and there are too many of them.   

How to run N-fold cross validation on Hadoop?
---------------------------------------------

* Split your data using n_fold.pl.
* Put all data splits into a Java archive.
  
  .. code-block:: bash

    $JAVA_HOME/bin/jar cvf data.jar fold.*

* Upload data.jar to HDFS

  .. code-block:: bash
  
    hadoop dfs -put data.jar data.jar

* Create a text file (input_file) and put all your cross validation tasks into it, one task per line. This file is going to be used as your job's input.

  .. code-block:: bash
  
    perl cross_validate_task.pl -f 0 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 1 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 2 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 3 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 4 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 5 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 6 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 7 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 8 -c 0.5 -w 0.5 -i data -b .
    perl cross_validate_task.pl -f 9 -c 0.5 -w 0.5 -i data -b .

* Run cross validation using this command:

  .. code-block:: bash
  
     % hadoop dfs -put input_file input_file
     % setenv HADOOP_CLASSPATH  $HADOOP_HOME/tools/jars/oneLineInputformat.jar 
     % hadoop --config config_dir jar \
              -libjars $HADOOP_HOME/tools/jars/oneLineInputformat.jar $HADOOP_HOME/hadoop-streaming.jar \
              -input input_file \
              -output output_dir \
              -mapper run_stdin.pl \
              -file cross_validate_task.pl \
              -file run_stdin.pl \
              -file train \
              -file predict \
              -inputformat com.yahoo.kryptonite.web.OneLineInputFormat \
              -cacheArchive hdfs://kry-nn1:8020/user/dun/data.jar#data

For more information on hadoop streaming please refer to `GridDocStreaming <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/GridDocumentation/GridDocStreaming>`_.

.. todo:: Find page GridDocStreaming

How To Use libyell with Streaming
=================================

Overview
--------

* Grid SE requires all packages to be used in the same way as any other yinst-able package
* In order to access libyell, you need to keep the structure on each grid node as it is running on an openhouse machine

As a grid user, you need to package libyell and distribute it to all your nodes.

Here is an example about how to distribute yinst packages: `StoneCutterOnGrid <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Apex/StoneCutterOnGrid.html/>`_

As of Oct/29. 2015, the choices of libyell are: version 6.12.x on 'stable', version 6.13 on 'current'.

.. todo:: move page StoneCutterOnGrid

Steps for get libyell to runtime node
-------------------------------------

Prepare a libyell tarball for your job
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since libyell also depends on other packages, you need to install it to get all necessary dependenciese available first and then generate one tarball. Here are the steps to generate one tarball.


  .. code-block:: bash
  
    ssh kryptonite-gw.red.ygrid.yahoo.com  #or any gateway machine
    mkdir 4grid
    cd 4grid
    yinst i libyell [-br current] -nosudo -root .
    # replace ./libdata/yell/wseos/webma.conf with the attached webma.conf
    tar -zcvf ../libyell_4grid.tgz bin64 conf include lib lib* share 

:download:`webma.conf </resources/webma.conf>`


Put libyell tarball for distribution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Put the newly generated tarball on HDFS just like any other files to be ready for distibution.
Here is an example cmd:

  .. code-block:: bash
  
    hdfs dfs -put ../libyell_4grid.tgz ./yell_path


* ``libyell_java.jar`` file is under ``$libyell_install_path/lib/jars/``. Insert ``$libyell_install_path/lib`` for ``java.library.path``.

* Here is how QCAT project uses libyell on grid: https://git.corp.yahoo.com/QCAT/qcat-core/tree/master/core/src/main/script/grid

* Here is how Gemini project uses libyell on grid: https://git.corp.yahoo.com/guoqiang/udfs/blob/master/README.md

Use libyell in MapReduce
^^^^^^^^^^^^^^^^^^^^^^^^

#. Add the following lines of codes in your Java code (changing the libyell_install_path accordingly)

    .. code-block:: java
    
        String config_file = libyell_install_path + "/conf/yell/libyell.config";
        yellAPI = new YellAPI();
        yellAPI.yellInit(libyell_install_path, config_file, 0);

        conf.set("mapred.child.java.opts", "-Djava.library.path=$libyell_install_path/lib -Xmx512m");
        conf.set("mapred.child.env", "LD_LIBRARY_PATH=$libyell_install_path/lib");

#. Run your jobs with ``libyell_java.jar``. The working nodes will find the libyell package on the gateway:
   ``-libjars $libyell_install_path/libyell_java.jar``

Using distributed cache
^^^^^^^^^^^^^^^^^^^^^^^^

#. Write your own ``map/reduce`` java code. The following code snippet provides you an example of using libyell in ``map/reduce``,
   Access the libyell with distributedcache

    .. code-block:: java
    
        URI [] urilist = new URI[1];
        try {
          //replacing the URI accordingly, if you are using other clusters
          urilist[0] = new URI ("hdfs://gateway_host_name/yell_path/libyell_4grid.tgz#yell");
        } catch ( Exception e ){
          throw new IOException(StringUtils.stringifyException(e));
        }
        DistributedCache.setCacheArchives(urilist, conf);
        DistributedCache.createSymlink(conf);
        conf.set("mapred.child.java.opts", "-Djava.library.path=./yell/lib -Xmx512m");
        conf.set("mapred.child.env", "LD_LIBRARY_PATH=./yell/lib");

        try {
          DistributedCache.addCacheArchive(new URI("./yell_path/libyell_4grid.tgz"), conf);
          DistributedCache.addFileToClassPath(new Path("./yell_path/lib/jars/libyell_java.jar"), conf);
        } catch (URISyntaxException e) {
          System.out.println(e);
        }

#. In your map/reduce, you can call the yellAPI in this way

    .. code-block:: java
    
        String pwd = System.getenv("PWD");
        String install_path = pwd + "/yell";  
        String config_file = install_path + "/conf/yell/libyell.config";
        try {
          YellAPI yellAPI = new YellAPI();
          yellAPI.yellInit(install_path, config_file, 0);
          YellLang yellLang = yellAPI.yellLangOpen("zh-hans");
          int v[] = yellAPI.yellGetVersion();
          System.out.println("version: " + v[0] + "." + v[1] + "." + v[2]);
        } catch (YellException ex) { 
          System.out.println(ex);
        }

#. Run your job Example:

  .. code-block:: bash
  
     hadoop jar your.jar [mainclass] \
          -libjars $yell_install_path/lib/jars/libyell_java.jar \
          -Dmapred.job.queue.name=queuename ...... [args]  
         

Using libyell
--------------

I packaged a version of libyell that contains ``libyell_xt`` data. ``/user/clementg/yell.zip``

The launching command looks something like this:

  .. code-block:: bash
  
     hadoop jar /grid/0/gs/hadoop/hadoop-0.20.1.3041192001/hadoop-streaming.jar \
          -Dmapred.reduce.tasks=0 -Dmapred.job.queue.name=unfunded \
          -cacheArchive "hdfs://axoniteblue-nn1.blue.ygrid.yahoo.com/user/clementg/yell.zip#yell" \
          -file "../mapperCleaner" -file "../libyell.config" \
          -input /tmp/clementg/referralsNa -output /tmp/clementg/referralsNaClean \
          -mapper mapperCleaner \
          -cmdenv LD_LIBRARY_PATH = ./yell/yell/lib64


Streaming in Pig
================

.. todo:: find stream link

Streaming is also available as Pig operator -- you can combine high-level relational notation with running existing C++ or Perl programs in a single Pig script.
In the Pig Latin Manual, see `Stream <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com:8080/?url=http%3A%2F%2Fhadoop.apache.org%2Fpig%2Fdocs%2Fr0.2.0%2Fpiglatin.html%23STREAM&SIG=11qq394rh>`_ for more details.


Streaming in Python -- Count Dogs Example
=========================================

For those who have absolutely no experience with Hadoop and Map/Reduce ... try this example.

.. todo:: move content from https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Yst/VkMrCtDogs.html

.. _user_guide_faq_streaming_in_python_pymapred:

Streaming in Python -- pymapred Example
=======================================

There is a powerful convenience package that encapsulate interaction with ``Hadoop py`` wrapping it into Python.
A single python script runs both on the gateway and in map/reduce tasks.
On the gateway it generates the necessary Hadoop commands and launches map/reduce jobs.

On the cluster, it does the actual data processing. From the user perspective, the user does not need to know anything but Python and the general concepts of map/reduce computation.

pymapred also supports ``Join`` and ``Parameter Sweep``.

.. todo:: move content from twiki 

see `PymapredMapReduce <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Main/PymapredMapReduce/>`_

Streaming with a recent version of Python
=========================================

.. todo:: move content of YResearch GridTools page from twiki 

The grid team currently only supports Python 2.4, which is quite old and lacks a number of useful modules (e.g., json, defaultdict, etc.). Adding two lines to any Hadoop Streaming or Pig job submission will use the distributed cache mechanism to make a local copy of this library to each mapper or reducer and modify paths accordingly:

  .. code-block:: bash
  
    -Dmapred.child.env=PATH=./gridtools/bin:'${PATH}',LD_LIBRARY_PATH=./gridtools/lib:./gridtools/lib64 \
    -cacheArchive /user/hofman/gridtools.tar.gz#gridtools \

More details are available on the `YResearch GridTools page <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/YResearch/GridTools.html>`_.


How to use map/reduce to run N independent tasks
================================================


GridX - a script for running distributed shared nothing jobs on the grid
-------------------------------------------------------------------------

GridX can be used to run jobs on a set of input files. You can give GridX a command and it will do command substitution to run a command on each of the inputs.


.. code-block:: bash
  
   ARGS:
     -p numParts      // number of jobs to run, each job can use  to identify its partition number, default=1
                      //    partitions are counted from 0 ... p-1

     -file filename   // upload a file to the processing node

     -cacheArchive hdfsfilename#dirname   // upload a file to the processing node via hdfs and unjar it in dirname

     -c command       // (REQUIRED) the command to run.  For example, -c 'echo "partition=$p"; hostname'
                      //    $p is mapped to the partition number
                      //    $pzz is mapped to the partition number with 2-digit zero padding, such as 03
                      //    $pzzz is mapped to the partition number with 3-digit zero padding, such as 003
                      //    $pzzzzz is mapped to the partition number with 5-digit zero padding, such as 00003

   EXAMPLE:  runs getq_orig.gawk on all 24 log files
            (00.log.gz ... 23.log.gz) for a day

   gridx -p 24 -file getq_orig.gawk -c 
      'hadoop dfs -get /data/jasonyz/ks_logs/2008/05/01/$pzz.log.gz .; gunzip $pzz.log.gz; gawk -f getq_orig.gawk .log > $pzz.out ; hadoop dfs -put $pzz.out /user/jasonyz'

.. note::
  - You must have already allocated nodes on the grid with a config dir named hodtmp.
  - be careful to use the 'single quotes' around your command so that the ``'$'`` sign is preserved by the shell and not interpreted if you use any of the macro ``$p`` macro expansions.
    

GridX script
------------

* see :download:`GridX script </resources/gridx.sh.txt>`
* see `Sweeper` class in :ref:`user_guide_faq_streaming_in_python_pymapred`


How to debug streaming jobs
===========================

* Hadoop 0.18 - use ``hadoop dfs -ls hod-logs`` (the logs are placed in your home directory).
* Hadoop 0.20 - use the web interface via the job/task tracker to view the logs

Use streaming to do shell-like work
===================================

A lot of times, we have simple input data that we just want to transform using common Unix tooks like ``awk``, ``grep``, ``sed``. Streaming lends itself well to this application, though you will likely want to iterate over a small sample while you work to get your command perfect. (You do this in the Unix shell too, don't you?)

*Examples:*

Here are some examples specific to working with the XML files from news.yahoo.com, but you can use the same techniques for many applications.

Distributed Cache with Streaming (Python Example)
-------------------------------------------------

  .. code-block:: bash

    yinst install ypython-2.7.0 -nosudo -root /grid/0/tmp/ypython stub/ycron-1.9.0
    cd /grid/0/tmp/ypython
    zip -r ypython-2.7.0 *
    hadoop dfs -put ypython-2.7.0.zip .
    hadoop dfs -ls ypython-2.7.0.zip 
      Found 1 items
      -rw-------   3 peeyushb users  227414492 2011-11-12 11:02 /user/peeyushb/ypython-2.7.0.zip
    cat mapper.py 
      #!./PYTHONROOT/bin/python2.7
      import sys
       
      #--- get all lines from stdin ---
      for line in sys.stdin:
          #--- remove leading and trailing whitespace---
          line = line.strip()
       
          #--- split the line into words ---
          words = line.split()
       
          #--- output tuples [word, 1] in tab-delimited format---
          for word in words: 
              print '%s\t%s' % (word, "1")

    cat reducer.py 
      #!./PYTHONROOT/bin/python2.7
      import sys
       
      # maps words to their counts
      word2count = {}
       
      # input comes from STDIN
      for line in sys.stdin:
          # remove leading and trailing whitespace
          line = line.strip()
       
          # parse the input we got from mapper.py
          word, count = line.split('\t', 1)
          # convert count (currently a string) to int
          try:
              count = int(count)
          except ValueError:
              continue
       
          try:
              word2count[word] = word2count[word]+count
          except:
              word2count[word] = count
       
      # write the tuples to stdout
      # Note: they are unsorted
      for word in word2count.keys():
          print '%s\t%s'% ( word, word2count[word] )


Streaming Command used:

  .. code-block:: bash

    hadoop jar /grid/0/gs/hadoop/current/hadoop-streaming.jar \
      -Dmapred.job.queue.name=grideng \
      -archives hdfs://axoniteblue-nn1.blue.ygrid.yahoo.com:8020/user/peeyushb/ypython-2.7.0.zip#PYTHONROOT \
      -cmdenv LD_LIBRARY?_PATH=PYTHONROOT/lib/ \
      -mapper mapper.py -reducer reducer.py \
      -input input.txt -output streamop \
      -file mapper.py -file reducer.py


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
=================================

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

Is PHP supported in streaming?
==============================

PHP is not installed on the Grid machines.

To run yphp and all possible extension on Grid, you will have to install all things you'll need on your machine, inside a yroot, pack the content of all ``/home/y``, distribute it via Distributed Cache. Then, have a small wrapper that point extension directory as well as library path to the right location.

This is a capture of how I run `YInst:yphp_thoth <http://dist.corp.yahoo.com/by-package/yphp_thoth/>`_ on Grid.
First on your machine:

#. Create a yroot of RHEL 5.6 (since Grid today is running 5.x)
#. Install all the packages you need.
#. Exit the yroot and pack the content
   
    .. code-block:: bash
    
      cd /home/y/var/yroots/<your yroot name>/home/y;
      sudo rm -f *.tgz;
      sudo tar --exclude=var/yinst -zcvpf yphp-yroot.tgz .

  Then copy this `yphp-yroot.tgz` to Grid Gateway, then upload to HDFS (e.g. `/user/pgonzal/y/yphp-yroot.tgz`. In streaming, first you will have to distribute this tgz via Distributed Cache, e.g:

    .. code-block:: bash
    
      -archives "hdfs://nitroblue-nn1.blue.ygrid.yahoo.com:8020/user/pgonzal/y/yphp-yroot.tgz#y"


This will make the content available as `./y/` to the node where you task is launched (note you should change to `viewfs://` and remove the `:8020` part for Hadoop 0.23+). The final touch is your mapper/reducer should be run with a wrapper like this:

  .. code-block:: bash

    #!/bin/sh

    export ROOT=$PWD/y
    export LD_LIBRARY_PATH=$ROOT/lib

    $ROOT/bin/php \
        -d display_errors=stderr \
        -d open_basedir=/ \
        -d extension_dir=$ROOT/lib/php/20060613 \
        -d extension=yahoo_thoth.so \
        ./mapper.php

Change `20060613` to the correct extension directory based on your PHP version.

How to Run Ruby Scripts
=======================

These examples use JRuby instead of the C-Ruby. JRuby (http://jruby.org/) is an open-source pure-Java implementation of ruby. Since JRuby is written in Java, you can use Java to invoke JRuby's Java main class which will run with your Ruby scripts.


**Example 1**

#. Create a command line to invoke scripts.

  .. code-block:: bash

    export JRUBY="java -cp ./lib/jruby-complete.jar:.:${CLASSPATH} org.jruby.main"
    #or
    export JRUBY="java -jar ./lib/jruby-complete.jar "

#. Copy the jruby-complete.jar to your workspace.
#. Modify the configurations of your Hadoop Streaming scripts to include the `jruby-complete.jar` file:
   
  .. code-block:: bash

    hadoop  jar $HADOOP_HOME/hadoop-streaming.jar \
        -mapper "${JRUBY} tp_metrics_filters_mapper.rb" \
        -file ./lib/jruby-complete.jar

**Example 2**

Please replace the HDFS location, `/user/chiac`, with your own location.

#. Copy `jruby-complete.jar` to Hadoop HDFS. You only need to do this one.
   
   .. code-block:: bash

      sudo -u tp hadoop fs -mkdir /user/chiac/tools
      sudo -u tp hadoop fs -put jruby-complete.jar /user/chiac/tools/.
      sudo -u tp hadoop fs -chmod -R a+rx /user/chiac/tools

#. Use these statements in your script.

  .. note:: The following line is NOT a typo; `jruby-complete.jar` will be copied to the cwd instead!

  .. code-block:: bash

    export JRUBY="java -jar ./jruby-complete.jar "
    hadoop jar $HADOOP_HOME/hadoop-streaming.jar \
        -files "hdfs:///user/chiac/tools/jruby-complete.jar" \
        -mapper "${JRUBY} tp_metrics_serves_mapper.rb"

Where did my files (passed in with `-file`) go?
===============================================

You can conveniently pass files to your nodes by using the `-file` argument. These files will generally be placed in the current directory of you streaming job (or a symlink will).
However, some files are special-cased. If you pass a class file, jar file, or zip file, these will not be placed in the current directory, but will instead be placed in a subdirectory (`./classes`, `./lib`, and `./lib`, respectively). This seems to be undocumented, and there may be other files that are similarly special-cased.
