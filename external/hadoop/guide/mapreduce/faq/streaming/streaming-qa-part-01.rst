
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
-----------------------------------------------------------------------------

* ``TextInputFormat`` uses the default implementation of ``getSplits()`` inherited from ``FileInputFormat``. It respects the number of splits determined by the framework based on the size of your input data and the number of nodes available. You can't control the number of map tasks to spawn.

* ``OneLineInputFormat`` overrides ``getSplits()`` such that every single line in your input file(s) becomes an ``InputSplit``. Thus the number of map tasks spawned will be exactly the number of lines in your input file(s). And each map task is fed with a single line.

* Both inputformat classes use ``LineRecordReader`` to read a line into a ``<K,V> pair``. The byte offset of the first character of that line is to be used as the key. And the rest of the line is to be used as the value. However Hadoop Streaming has made it a special case for ``TextInputFormat`` that it passes only the value part to your pipe program. For other inputformat classes, both key and value will be passed to your pipe program, delimited by ``\t``.

Where to find ``OneLineInputFormat``?
-------------------------------------

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
^^^^^^^^^^^^^^^^^^^^^^^

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
-------------

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
------------------------------------------------------------------------

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

