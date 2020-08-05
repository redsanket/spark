How to run hadoop programs on the gateway machines
=======================================================

Running map-red jobs on the gateways, is now an expert option. (This decision was made because several users had unintentionally run their full-fledged jobs on the gateway machines in the past, when they really wanted to run the jobs on the grid instead.)

Here is how you can test your jobs (on a much smaller dataset, please) on the gateway machine itself:

  .. code-block:: bash

    $ mkdir ~/local
    $ cat > ~/local/hadoop-site.xml
    <configuration>
    <property>
     <name>fs.default.name</name>
     <value>mithrilgold-nn1.inktomisearch.com:8020</value>
     <final>true</final>
     <description>NameNode</description>
    </property>
    <property>
      <name>mapred.job.tracker</name>
      <value>local</value>
      <final>true</final>
      <description>JobTracker</description>
    </property>
    </configuration>
    ^D
    $ export HADOOP_CONF_DIR=~/local
    $ hadoop jar ....

If you want to use the input files from the local file system instead of DFS, you will need to change value for ``fs.default.name`` to ``file:///``


Tutorial for Yarn Web UI
========================


http://hadooptutorial.info/yarn-web-ui/

https://dzone.com/articles/configuring-memory-for-mapreduce-running-on-yarn


When ApplicationMaster? is hitting "Split metadata size exceeded beyond 10000000" or OutOfMemory? error
==============================================================================================================

* If there are too many mappers (in the order of thousands) and you actually don't need them (having way too many mappers is actually bad for performance), try reducing the number of total mapper tasks by having bigger splits by setting:
  ``-Dmapreduce.input.fileinputformat.split.minsize=536870912`` (512M or any higher value, default to block size which is 128M on our grid. For ABF feeds 1G or 2G is good).

* If the file sizes are small, say 128MB, setting s``plit.minsize`` to a higher value like 1G does not help.
  In that case, you can try to combine splits using CombinedInputFormat or if you're using pig, you can set:
  ``-Dpig.maxCombinedSplitSize=536870912`` or higher.
  Use that in conjunction with ``split.minsize setting``.

* If first two options does not work for you and it is hitting "Split metadata size exceeded beyond 10000000" error, please try bumping up AM meta info size by setting ``-Dmapreduce.job.split.metainfo.maxsize=___`` to higher value (default is 10,000,000)

* If first two options does not work for you and if Application Master is hitting OOM due to too many tasks, please try bumping up heapsize of the application master by

    .. code-block:: bash

      #default is 1024m and you can go up to 3584 
      -Dyarn.app.mapreduce.am.command-opts= \
          'Xmx2048m -XX:NewRatio=8 -Djava.net.preferIPv4Stack=true' \
      #default 1536, Max - 4096. Rule of thumb - 512MB higher than the Xmx value
      -Dyarn.app.mapreduce.am.resource.mb=2560