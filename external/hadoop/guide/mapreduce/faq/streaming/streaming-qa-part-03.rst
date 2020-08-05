
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

Please replace the HDFS location, ``/user/chiac``, with your own location.

#. Copy `jruby-complete.jar` to Hadoop HDFS. You only need to do this one.
   
   .. code-block:: bash

      sudo -u tp hadoop fs -mkdir /user/chiac/tools
      sudo -u tp hadoop fs -put jruby-complete.jar /user/chiac/tools/.
      sudo -u tp hadoop fs -chmod -R a+rx /user/chiac/tools

#. Use these statements in your script.

  .. note:: The following line is NOT a typo; ``jruby-complete.jar`` will be copied to the cwd instead!

  .. code-block:: bash

    export JRUBY="java -jar ./jruby-complete.jar "
    hadoop jar $HADOOP_HOME/hadoop-streaming.jar \
        -files "hdfs:///user/chiac/tools/jruby-complete.jar" \
        -mapper "${JRUBY} tp_metrics_serves_mapper.rb"


Where did my files (passed in with `-file`) go?
===============================================

You can conveniently pass files to your nodes by using the ``-file`` argument. These files will generally be placed in the current directory of you streaming job (or a symlink will).
However, some files are special-cased. If you pass a class file, jar file, or zip file, these will not be placed in the current directory, but will instead be placed in a subdirectory (``./classes``, ``./lib``, and ``./lib``, respectively). This seems to be undocumented, and there may be other files that are similarly special-cased.