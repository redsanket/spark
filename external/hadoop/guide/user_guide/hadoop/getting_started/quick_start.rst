***********
Quick Start
***********

`Orginal Document twiki <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/GridDocumentation/GridDocGettingStarted.html/>`_

.. todo::
	check if there any relevant instructions can be pulled from `Bdml-guide/Onboarding_to_the_Grid <https://git.ouroath.com/pages/developer/Bdml-guide/Onboarding_to_the_Grid/>`_)

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

Things To Do
============

First
-----

#. Watch Hadoop training sessions - see Grid Training
#. Know where and how to get help - see Grid Support and Escalation Matrix
#. Make sure you can access the Hadoop internal documentation - see Grid Versions, Hadoop/Pig doc links

  * Set proxy ... Mozilla Firefox (tools > network > connection > settings)
  * SOCKS Host: socks.yahoo.com Port: 1080

Second
------

#. Get a User Account (and job queue) - see Grid Onboarding
#. Once you have an account, login to one of the gateway boxes (using your Mail/Exchange password, while grid nodes require the Unix password):

   * ssh to a gateway
   * Examples:
   * For Kryptonite Red cluster gateway rotation name will be kryptonite-gw.red.ygrid.yahoo.com ``ssh kryptonite-gw.red.ygrid.yahoo.com``
   * How do you identify the gateway rotation for a cluster ?

      * Gateway rotations can be derived from the cluster name.
      * A cluster name has two parts : Grid and color. For eg : For mithril red, mithril is grid and red is color.
      * Gateway roations are of the form : ``<grid>-gw.<color>.ygrid.yahoo.com``
      * Continuing the above example for mithril red, it will be ``mithril-gw.red.ygrid.yahoo.com``

#. From the gateway, check the status of your Kerberos tickets:

   * Get a list of your tickets: ``$ klist``
   * If you don't have any tickets, invoke kinit to get a ticket that is good for up to 10 hours:

     .. code-block:: bash

       $ kinit <username>@Y.CORP.YAHOO.COM (e.g. joe@Y.CORP.YAHOO.COM )
       $ Password for <username>@Y.CORP.YAHOO.COM: <pw> (e.g. Enter MS-Exchange password for joe@Y.CORP.YAHOO.COM )

   * .. note::
        Kerberos realm names are case sensitive.
   * See Interacting with Grid for more information about Hadoop Security which uses Kerberos for authentication.

Third
-----

#. Try running a few Hadoop commands - see In a Nutshell below ...
#. Try setting up and running the Hadoop MapReduce and Pig tutorials - see `Grid Versions <http://yo/gridversions/>`_, Hadoop/Pig doc links

In a Nutshell
==============

In the commands below, replace unfunded with your the name of your job queue (see the Scheduler FAQ for information about job queues).

  #. To list the queues you have access. ``mapred queue -showacls``
  #. Most likely, you would have access to queue ``unfunded`` so the examples are described using the ``unfunded`` queue

Try Out HDFS
------------

Perform these simple tasks

#. Log in to a sandbox gateway (using your Exchange password for Sandboxes only)

   * Sandbox: kryptonite-gw.red.ygrid.yahoo.com

#. View the hadoop commands: ``hadoop``

#. View the version of hadoop: ``hadoop version``

#. View the FS shell commands ``hadoop fs``

#. See what data is available on the Hadoop Distributed File System

    .. code-block:: bash

      # (/data might not be available with all the clusters, please check with data page)
      hadoop fs -ls /data
      hadoop fs -ls /data/vespanews

#. Copy ``/usr/share/dict/linux.words`` from the gateway host to a file "linux.words" in your HDFS home directory
   ``hadoop fs -copyFromLocal /usr/share/dict/linux.words linux.words``

#. View the linux.words file: ``hadoop fs -cat linux.words``

Try Out Pig
-----------

Try out a simple Pig script to extract all long words from the file linux.words using the Pig shell "grunt"

#. Local machine (local mode):

   .. code-block:: bash

     # places you in Grunt shell
     pig -x local
     cp /usr/share/dict/linux.words linux.words (creates copy of linux.words in local fs)
     # runs the script
     pig -x local myscript.pig


#. Gateway machine (mapreduce mode):

   .. code-block:: bash

     # places you in Grunt shell
     pig -Dmapred.job.queuename=unfunded
     # runs the script
     pig -Dmapred.job.queuename=unfunded myscript.pig

#. Queue names vary by cluster, try ``unfunded`` or ``adhoc``.

#. Pig latin statements (enter from Grunt shell)

   .. code-block:: pig

      grunt> A = load 'linux.words' using PigStorage() as (word:chararray);
      grunt> B = filter A by SIZE(word) > 19;
      grunt> dump B;
      grunt> quit

#. Pig latin statements (placed in script)

  .. code-block:: pig

    /* myscript.pig */
    A = load 'linux.words' using PigStorage() as (word:chararray);
    B = filter A by SIZE(word) > 19;
    dump B;

#. Check the output and see how many words (with 20 or more letters) you recognize.


Try Out Pig with ABF data
---------------------------

Please refer to `First Pig Program Using ABF <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/GridDocumentation/FirstPigProgramUsingABF/>`_

Try Out Hadoop Streaming
---------------------------

Try out a shell script with hadoop streaming. The following would perform the same task as above.

#. Run the script in the allocated cluster. The results (output files) are written to the dfs directory tempoutput.

   .. code-block:: bash

     hadoop jar $HADOOP_PREFIX/share/hadoop/tools/lib/hadoop-streaming.jar \
           -Dmapred.job.queuename=unfunded -mapper "awk '{if(length(\$0) > 19){print \$0}}'" \
           -reducer NONE -input linux.words -output tempoutput


#. Check the results: ``hadoop fs -cat 'tempoutput/*' | less``

#. Order of the output could be different depending on how many mappers streaming allocated. (Mentioned in the next section).
#. Copy the results from dfs to the gateway directory: ``hadoop fs -copyToLocal 'tempoutput' ./tempoutput``

..  _guide_getting_started_try_out_mapreduce:

Try Out MapReduce
------------------

Try out a pure mapreduce code based on java. Just like running your first "Hello World" java example, mapreduce requires you to have a set of phrases for it to run.

* Following code runs a map-only job that creates an identical copy of the input.
  MyFirstMapOnlyJob.java: Sample map only job that just dumps the copy.

* Download the above file to the gateway, compile and then create a jar file.

  .. code-block:: bash

    $JAVA_HOME/bin/javac -cp $(hadoop classpath) MyFirstMapOnlyJob.java
    $JAVA_HOME/bin/jar -cvf myfirstjob.jar *.class

* Submit the job using the above jar.

  .. code-block:: bash

    hadoop jar myfirstjob.jar MyFirstMapOnlyJob \
          -Dmapred.job.queuename=unfunded linux.words tempoutput2

* If everything goes well, you should see an identical file created under tempoutput2.
* Note that at this point, there is no difference in writing as key or value at ``collect()`` method since there is no reducer.
* Now, try to modify the sample code only to show words longer than 20 letters.


Try Out Oozie
-------------

.. todo:: Move First Oozie Workflow from archive wiki

Please refer to `First Oozie Workflow <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/GridDocumentation/FirstOozieWF.html/>`_

Try Out Hive
------------


Try out a Hive command to query Hive tables.

* Start the Hive CLI.

   * You might need to add ``/home/y/bin`` to ``PATH`` and set ``HIVE_HOME``? to ``/home/y/libexec/hive``.)

   .. code-block:: bash

     [joe@gwrd8000 ~]$ hive
     Logging initialized using configuration in file:/grid/0/y/libexec/hive/conf/hive-log4j.properties
     Hive history file=/homes/joe/hivelogs/hive_job_log_joe_201305212304_1145833011.txt
     hive>

* Prepare a database and indicate where the data will be stored.
  The following creates a new database and stores related data at ``<hdfs-path>``.

  .. code-block:: bash

    # CREATE DATABASE <database> LOCATION '<hdfs-path-to-database>';
    hive> CREATE DATABASE joe LOCATION '/user/joe/mydata';
    OK
    Time taken: 0.11 seconds
    hive> SHOW DATABASES;
    OK
    joe
    Time taken: 0.1 seconds

* Use the database the was just created.

  .. code-block:: bash

    # USE <database>;
    USE <database>;
    hive> USE joe;
    OK
    Time taken: 0.022 seconds

* Prepare a table and indicate how the data is formatted and where the data will be stored. The following creates a table with two string columns 'a' and 'b'.

  .. code-block:: bash

    # CREATE TABLE <tablename> (a string, b string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '<hdfs-path-to-table>';
    hive> CREATE TABLE test (a string, b string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '/user/joe/mydata/test';
    OK
    Time taken: 0.282 seconds
    hive> SHOW TABLES;
    OK
    test
    Time taken: 0.1 seconds

* Load data from the local filesystem into the Hive table. The file is a tab-separated file with two values per line, matching the table definition.

  .. code-block:: bash

    # LOAD DATA LOCAL INPATH '<local-path-to-file>' INTO TABLE <tablename>;
    hive> LOAD DATA LOCAL INPATH 'data.00000' INTO TABLE test;
    Copying data from file:/homes/joe/data.00000
    Copying file: file:/homes/joe/data.00000
    Loading data to table joe.test
    Table joe.test stats: [num_partitions: 0, num_files: 1, num_rows: 0, total_size: 16, raw_data_size: 0]
    OK
    Time taken: 0.843 seconds

* Execute a query to retrieve the data.

  .. code-block:: bash

    # SELECT * FROM test;
    hive> SELECT * FROM test;
    OK
    1       hello
    2       world
    Time taken: 0.21 seconds

* Clean up the table and database.

  .. code-block:: bash

    # DROP TABLE <tablename>;
    # DROP DATABASE <database>;
    hive> DROP TABLE test;
    OK
    Time taken: 0.341 seconds
    hive> DROP DATABASE joe;
    OK
    Time taken: 0.093 seconds


A little advanced examples
==========================

Above examples only used mappers and didn't use the power of map-REDUCE.
Now let's look at a problem which requires some :emphasis:`sorting&shuffling`.


Problem
-------

Let's assume you are working on a user query engine and were asked to predict the word user is typing by looking at the first N letters (based on ``linux.words``).
We need to show the list of candidates when number of matches are less than or equal to 3. As the first step, your task is to come up with pair output for helping the system. For simplicity purposes, we're only going to look for the first 4,5 and 6 letters.

Try Out Pig
--------------

Below script collects tries to solve the above problem. Run it and check the output to see if it makes sense.

  .. code-block:: pig

    A = load 'linux.words' using PigStorage() as (word:chararray);
    B = FOREACH A generate FLATTEN({(SIZE(word) >= 4 ? SUBSTRING(word, 0, 4) : null),
                  (SIZE(word) >= 5 ? SUBSTRING(word, 0, 5) : null),
                  (SIZE(word) >= 6 ? SUBSTRING(word, 0, 6) : null)}) as key, word;
    C = filter B by key is not null;
    D = group C by key;
    E = filter D by COUNT(C) <= 3;
    F = foreach E generate group, C.word;
    dump F;

Try Out Hadoop Streaming
----------------------------

* Before trying on hadoop streaming, try writing your own on local environment.

  .. code-block:: bash

    cat /usr/share/dict/linux.words | [yourscript_here] | ... | ...

* Set ``export LC_ALL?=C`` for sorting case-sensitive way

  .. code-block:: bash

    cat /usr/share/dict/linux.words | awk '{if(length($0) >= 4 ) {print substr($0, 0, 4)" "$0 } if(length($0) >= 5 ) {print substr($0, 0, 5)" "$0} if(length($0) >= 6 ) {print substr($0, 0, 6)" "$0}}' | sort | awk '{curkey=$1; curvalue=$2; if(prevkey == curkey){count+=1; mylist=mylist","curvalue;} else {if(count < 3) {print prevkey" "mylist; } count = 0; mylist=curkey} prevkey=curkey}'

* Using the sample above, we will now split the above example to mapper and reducer.
  Note that 'sort' and implicit shuffle are taken care by the map-reduce framework.

* Instead of directly giving full map and reduce commands, wrapping with shell script in order to avoid extra escaping.

  .. code-block:: bash

    $ cat mymapper.sh
    #!/bin/sh
    awk '{if(length($0) &gt;= 4 ) {print substr($0, 0, 4)" "$0 } if(length($0) &gt;= 5 ) {print substr($0, 0, 5)" "$0} if(length($0) &gt;= 6 ) {print substr($0, 0, 6)" "$0}}'

    $ cat myreducer.sh
    #!/bin/sh
    awk '{curkey=$1; curvalue=$2; if(prevkey == curkey){count+=1; mylist=mylist","curvalue;} else {if(count &lt; 3) {print prevkey" "mylist; } count = 0; mylist=curkey} prevkey=curkey}'

    $ yarn jar /home/gs/hadoop/current/share/hadoop/tools/lib/hadoop-streaming.jar -Dmapreduce.job.queue.name=unfunded -input linux.words -output testoutput3 -mapper mymapper.sh -reducer myreducer.sh -file mymapper.sh -file myreducer.sh

Try Out MapReduce
---------------------

* ''MyFirstMapReduceJob.java'' (see below): MapReduce job listing all values starting with the key(first 4,5&6 letters)
* Follow the same step as the map only job example and check the result.
* Modify the code so that it only prints out when the number of candidates are less than or equal to 3.


.. code-block:: java

  /**
   * MyFirstMapReduceJob.java
   *
   */

  import java.io.IOException;

  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.conf.Configured;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.io.LongWritable;
  import org.apache.hadoop.io.NullWritable;
  import org.apache.hadoop.io.Text;
  import org.apache.hadoop.io.Writable;
  import org.apache.hadoop.mapreduce.Job;
  import org.apache.hadoop.mapreduce.Mapper;
  import org.apache.hadoop.mapreduce.Reducer;
  import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
  import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
  import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
  import org.apache.hadoop.util.Tool;
  import org.apache.hadoop.util.ToolRunner;

  public class MyFirstMapReduceJob extends Configured implements Tool{
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
      @Override
      public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException {
        //ignore the key for this example since it's just the file offset.
        String str = value.toString();
        for( int n=4; n <=6; n++ ) {
          if( str.length() >= n ) {
            Text firstNLetters = new Text(str.substring(0,n));
            context.write(firstNLetters, value);
          }
        }
      }
    }
    static class MyReducer extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce( Text key, Iterable<Text> values, Context context)
          throws IOException, InterruptedException {
        StringBuilder strbuf = new StringBuilder();
        for(Text value : values ) {
          strbuf.append(value.toString());
          strbuf.append(",");
        }
        context.write(key, new Text(strbuf.toString()));
      }
    }

    public int run(String[] args) throws Exception {
      //getConf() is comfing from Configured
      Job job = new Job(getConf());
      job.setJarByClass(MyFirstMapReduceJob.class);
      job.setInputFormatClass(TextInputFormat.class); //default
      job.setOutputFormatClass(TextOutputFormat.class); //default
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      job.setMapperClass(MyMapper.class);
      job.setReducerClass(MyReducer.class);
      job.setNumReduceTasks(1);

      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      return job.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
      int ret = ToolRunner.run(new MyFirstMapReduceJob(), args);
      System.exit(ret);
    }
  }

Try Out Hive
--------------

* More information about Hive at Yahoo can be found at /twiki/twiki.corp.yahoo.com/view/Grid/HiveGettingStarted
* The Apache Project page can be found at https://cwiki.apache.org/confluence/display/Hive/Home
* The Hive QL language manual can be found at https://cwiki.apache.org/confluence/display/Hive/LanguageManual