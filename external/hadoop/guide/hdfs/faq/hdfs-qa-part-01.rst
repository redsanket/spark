
Copying And Moving Files
========================

.. include:: /common/hdfs/data-governance-warning.rst

.. rubric:: DistCp Tool

:hadoop_rel_doc:`DistCp <hadoop-distcp/DistCp.html>` is a tool used for large
inter/intra-cluster copying. It uses MapReduce to effect its distribution, error
handling and recovery, and reporting. It expands a list of files and directories
into input to map tasks, each of which will copy a partition of the files
specified in the source list.

  .. code-block:: bash

    hadoop distcp

For detailed desciption of usage and command line options,
visit Apache Docs - :hadoop_rel_doc:`DistCp Guide <hadoop-distcp/DistCp.html>`.

.. important::
   
   * Do not use :hadoop_rel_doc:`Hadoop shell commands <hadoop-project-dist/hadoop-common/FileSystemShell.html>`
     (such as `cp`, `copyfromlocal`, `put`, `get`) for large copying jobs or you
     may experience I/O bottlenecks.
   * You should no longer use ``hadoop -fs`` to access HDFS.
     Instead use ``hdfs dfs``.
     See Apache Docs - :hadoop_rel_doc:`HDFS Commands Guide <hadoop-project-dist/hadoop-hdfs/HDFSCommands.html>`.
   * When copying between same major versions of Hadoop cluster, use `hdfs`
     protocol. |br| For copying between two different major versions of Hadoop, one will
     usually use `WebHdfsFileSystem`.

Should a map fail to copy one of its inputs, there will be several side-effects. |br|
Unless `-overwrite` is specified, files successfully copied by a previous map on
a re-execution will be marked as “`skipped`”. |br|
If a map fails "`mapreduce.map.maxattempts`" times, the remaining map tasks will be
killed (unless `-i` is set). |br|
If "`mapreduce.map.speculative`" is set to `final` and `true`, the result of the
copy is `unspecified`.


.. rubric:: dfs command

The prefix ``local_`` indicates file path on gateway machine, where prefix
``hdfs_`` indicates file path on HDFS.

  .. code-block:: bash

    hdfs dfs
           [-put <local_src> ... <hdfs_dst>]
           [-copyFromLocal <local_src> ... <hdfs_dst>]
           [-moveFromLocal <local_src> ... <hdfs_dst>]
           [-get [-ignoreCrc] [-crc] <hdfs_src> <local_dst>]
           [-getmerge <hdfs_src> <local_dst> [addnl]]
           [-copyToLocal [-ignoreCrc] [-crc] <hdfs_src> <local_dst>]
           [-moveToLocal [-crc] <hdfs_src> <local_dst>]

The list of `COMMAND_OPTIONS` is defined in
Apache Docs - :hadoop_rel_doc:`File System (FS) shell <hadoop-project-dist/hadoop-common/FileSystemShell.html>`


How To Move Data In/Out of Hadoop
---------------------------------

.. code-block:: bash

   ## for VCG
   ssh -A kessel-gw.gq.vcg.yahoo.com

   ## for YGrid
   ssh -A jet-gw.blue.ygrid.yahoo.com

   ## Get credentials to talk to HDFS
   pkinit-user

   ## Upload a file to HDFS
   $local> scp myfile.txt jet-gw.blue.ygrid.yahoo.com:~/gateway.txt
   $local> ssh -A jet-gw.blue.ygrid.yahoo.com
   
   # now we are on the gateway
   $local> hdfs dfs -put gateway.txt /user/$USER/hdfs.txt
   $local> (cleanup file: e.g. "rm gateway.txt")

   ## Downloading an HDFS file
   ssh -A jet-gw.blue.ygrid.yahoo.com \
       "pkinit-user 1>&2 ; bash -l hadoop fs -cat mydir/myfile" > myfile

How To Get Data Onto/Out-of The Grid
------------------------------------

There are a number of ways.
Visit Bdml-guide - `GDM Cookbook <https://git.vzbuilders.com/pages/developer/Bdml-guide/migrated-pages/GDM_Cookbook/#io>`_

Copying files Within Same HDFS Cluster
---------------------------------------

Use ``hdfs dfs`` command on the gateway machine with appropriate options mentioned below.
For more information, see ``hdfs -h``.

  .. code-block:: bash

     hdfs dfs 
           [-mv <hdfs_src> <hdfs_dst>]
           [-cp <hdfs_src> <hdfs_dst>]

A Simple HDFS Program
---------------------

  .. code-block:: bash

     # Create a directory to store files for this tutorial:
     hdfs dfs -mkdir ./hadoop_tutorial
     # Confirm that the directory was created:
     hdfs dfs -ls -d ./hadoop_tutorial
     # Copy the local file, assume linux.words
     hdfs dfs -copyFromLocal ~/linux.words
     # an alternative to previous command is hdfs dfs -put linux.words
     # Copy the file linux.words from your home directory on HDFS to the directory:
     hdfs dfs -cp linux.words hadoop_tutorial
     # Delete the copy of linux.words
     hdfs dfs -rm linux.words
     # You can change permissions with the DFS command
     hdfs dfs -chmod 644 ./hadoop_tutorial/linux.words

Copying Large Files Between HDFS Clusters
-----------------------------------------

**Usage:**

  .. code-block:: bash

     hadoop distcp hdfs://<src_url> hdfs://<dest_url>

**Example:**

  .. code-block:: bash

     hadoop distcp hdfs://clusterA-nn1.red.ygrid.yahoo.com:8020/user/$USER/hadoop_tutorial \
            hdfs://clusterB-nn1.red.ygrid.yahoo.com:8020/user/$USER/hadoop_tutorial


Copying Files Between HDFS and Object Stores
--------------------------------------------

`DistCp` works with Object Stores such as: Amazon S3, Azure WASB and OpenStack Swift.

.. rubric:: Prequisites

* The JAR containing the object store implementation is on the classpath, along
  with all of its dependencies.
* Unless the JAR automatically registers its bundled filesystem clients, the
  configuration may need to be modified to state the class which implements the
  filesystem schema. All of the ASF’s own object store clients are self-registering.
* The relevant object store access credentials must be available in the cluster
  configuration, or be otherwise available in all cluster hosts.

See Bdml-guide - `On Prem vs. Public Cloud <https://git.vzbuilders.com/pages/developer/Bdml-guide/#moving-data-to-and-from-public-cloud-and-on-prem>`_

Copying Between HDFS and User Machine Without Temp
--------------------------------------------------

HDFS Proxy is the most common way of bringing data onto the grid or out of the
grid.
See `Bdml-guide - GDM Cookbook: When to Use HDFS Proxy <https://git.vzbuilders.com/pages/developer/Bdml-guide/migrated-pages/GDM_Cookbook/#when-to-use-hdfs-proxy>`_

Loading data between HDFS and other products?
---------------------------------------------

Check the following resources:

* Bdml-guide - `Pig script output to HDFS <https://git.vzbuilders.com/pages/developer/Bdml-guide/quickstart/#pig-script-output-to-hdfs>`_
* Bdml-guide - `Run HDFS via Hue <https://git.vzbuilders.com/pages/developer/Bdml-guide/quickstart/#run-hdfs-via-hue>`_
  

What is the right way to ftp a file on hadoop?
----------------------------------------------

*Are there any example/recommendations for downloading a file from external
customer ftp server to grid?*

**Ans:**

For a one-time upload, manually copy the data from your laptop to a gateway and
then copy to HDFS.

.. sidebar:: Check the following discussion...

   See :yahoo_jira:`HADOOPPF-51331 - Right way to ftp a file on hadoop <HADOOPPF-51331>`

Accessing an external customer FTP from a grid job is not permitted without
special approval from paranoids. |br|
A better solution is to have the external customer use our internal FTP dropbox
and then use a grid job to copy from our internal FTP dropbox to HDFS.

* The internal FTP dropbox documentation `yo/dropbox <https://yo/dropbox>`_
* Slack channel :slack:`dropbox_ftps <C6NJDANQ3>` 
* The Apollo team runs a job on the grid that downloads data from the internal
  FTP dropbox and copies it to HDFS
  (See :yahoo_github:`BpmDownloader.java <apollo/apollo_bpm_client/blob/6cd396515b6956a7c113d03fe6a3914122c5d098/src/main/java/com/yahoo/apollo/bpm_client/BpmDownloader.java>`)

  .. literalinclude:: /resources/code/hdfs/BpmDownloader.java
    :language: java
    :caption: Get the ftp file - :yahoo_github:`BpmDownloader.java#L170-L175 <apollo/apollo_bpm_client/blob/6cd396515b6956a7c113d03fe6a3914122c5d098/src/main/java/com/yahoo/apollo/bpm_client/BpmDownloader.java#L170-L175>`
    :lines: 170-175
    :lineno-start: 170
    :linenos:
    :dedent: 12

  .. literalinclude:: /resources/code/hdfs/BpmDownloader.java
    :language: java
    :caption: Upload files to HDFS: :yahoo_github:`BpmDownloader.java-#L205-L211 <apollo/apollo_bpm_client/blob/6cd396515b6956a7c113d03fe6a3914122c5d098/src/main/java/com/yahoo/apollo/bpm_client/BpmDownloader.java#L205-L211>`
    :lines: 205-211
    :lineno-start: 205
    :linenos:
    :dedent: 6

  .. literalinclude:: /resources/code/hdfs/BpmDownloader.java
    :language: java
    :caption: Entry point for oozie java action - :yahoo_github:`BpmDownloader.java#L345-351 <apollo/apollo_bpm_client/blob/master/src/main/java/com/yahoo/apollo/bpm_client/BpmDownloader.java#L345-351>`
    :lines: 345-351
    :lineno-start: 345
    :linenos:
    :dedent: 2

How to Reduce Storage Space for HDFS
====================================

.. sidebar:: Related Topic

   Visit :ref:`mapreduce_compression`

Bdml-guide - :yahoo_github:`HDFS Storage Tips <pages/developer/Bdml-guide/migrated-pages/HDFS_Storage_Tips/#reducing-namespace-usage>`
describes two important tips to reduce storage:

* Reducing Storage Space
* Reducing Namespace Usage


Java Program to Read and Write to HDFS?
=======================================

  .. literalinclude:: /resources/code/hdfs/WriteFileToHDFS.java
      :language: java
      :caption: Java code to write file from HDFS
      :linenos:

#. The HDFS client sends a create request on `DistributedFileSystem` APIs.
#. `DistributedFileSystem` makes an RPC call to the `namenode` to create a new
   file in the file system’s namespace. |br|
   The `namenode` performs various checks to make sure that the file doesn’t
   already exist and that the client has the permissions to create the file.
   |br| When these checks pass, then only the namenode makes a record of the
   new file; otherwise, file creation fails and the client is thrown an
   `IOException`.
#. The `DistributedFileSystem` returns a `FSDataOutputStream` for the client to
   start writing data to. |br|
   As the client writes data, `DFSOutputStream` splits it into packets, which
   it writes to an internal queue, called the data queue. |br|
   The data queue is consumed by the `DataStreamer`, which is responsible for
   asking the `namenode` to allocate new blocks by picking a list of suitable
   `datanodes` to store the replicas.
#. The list of datanodes form a pipeline, and here we’ll assume the
   replication level is three, so there are three nodes in the pipeline. The
   `DataStreamer` streams the packets to the first datanode in the pipeline, which
   stores the packet and forwards it to the second datanode in the pipeline.
   Similarly, the second datanode stores the packet and forwards it to the third
   (and last) datanode in the pipeline.
   (Learn :hadoop_rel_doc:`HDFS Data blocks in detail <hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Data_Blocks>`).
#. `DFSOutputStream` also maintains an internal queue of packets that are
   waiting to be acknowledged by datanodes, called the ack queue. A packet is
   removed from the ack queue only when it has been acknowledged by the datanodes
   in the pipeline. `Datanode` sends the acknowledgment once required replicas are
   created (3 by default). Similarly, all the blocks are stored and replicated on
   the different datanodes; the data blocks are copied in parallel.
#. When the client has finished writing data, it calls `close()` on the stream.
#. This action flushes all the remaining packets to the datanode pipeline and
   waits for acknowledgments before contacting the namenode to signal that the
   file is complete. The namenode already knows which blocks compose the file,
   so it only has to wait for blocks to be minimally replicated before
   returning successfully.      


  .. literalinclude:: /resources/code/hdfs/ReadFileFromHDFS.java
      :language: java
      :caption: Java code to read file from HDFS
      :linenos:


#. Client opens the file it wishes to read by calling `open()` on the
   `FileSystem` object, which for HDFS is an instance of `DistributedFileSystem`.
#. `DistributedFileSystem` calls the `namenode` using RPC to
   determine the locations of the blocks for the first few blocks in the file.
   |br| For each block, the namenode returns the addresses of the datanodes that
   have a copy of that block and the datanode addresses are sorted according to
   their proximity to the client.
#. `DistributedFileSystem` returns a `FSDataInputStream` from which the
   client may read data. `FSDataInputStream`, thus, wraps the `DFSInputStream`
   which manages the datanode and namenode I/O. |br|
   Client calls `read()` on the stream. |br|
   `DFSInputStream` which has stored the datanode addresses then connects to the
   closest datanode for the first block in the file.
#. Data is streamed from the datanode back to the client, as a result client 
   can call `read()` repeatedly on the stream. When the block ends,
   `DFSInputStream` will close the connection to the datanode and then finds the
   best datanode for the next block.
#. If the `DFSInputStream` encounters an error while
   communicating with a datanode, it will try the next closest one for that block.
   `DFSInputStream` will also remember datanodes that have failed so that it doesn't
   needlessly retry them for later blocks. |br|
   The `DFSInputStream` also verifies checksums for the data received from
   from the datanode. When a corrupt block is found, the `DFSInputStream` reports
   this to the namenode before the `DFSInputStream` attempts to read a replica of
   the block from another datanode.
#. The client calls ``close()`` on the stream after finishing reading the data.


How do I make my program's output data available to other users?
================================================================

* By default, the files generated by map/reduce programs have permissions of 700.
  This means, the files that are produced are readable only to the user who ran
  the job. The reason for this is that, the default umask is 077.
  The following example shows how to overwrite the default umask.
  Note that the hadoop option only takes decimal.

  .. code-block:: bash

    [u1@gsgw1022 ~]$ printf "%d\n" 022
     18
    [u1@gsgw1022 ~]$ hadoop dfs -D dfs.umask=18 -put test.txt /user/u1
    [u1@gsgw1022 ~]$ hadoop dfs -ls /user/u1/test.txt
     Found 1 items
    /user/u1/test.txt <r 3>   524     2008-03-26 21:06        rw-r--r--       u1        users
    [u1@gsgw1022 ~]$ hadoop dfs -D dfs.umask=18 -mkdir /user/u1/testdir
    [u1@gsgw1022 ~]$ hadoop dfs -ls /user/u1/ | grep testdir
    /user/u1/testdir  <dir>           2008-03-26 21:07        rwxr-xr-x       u1        users

* For mapred job,

  .. code-block:: bash

    [u1@gsgw1022 ~]$ hadoop --config confdir job -D dfs.umask=18 submit ...
    # OR
    [u1@gsgw1022 ~]$ hadoop jar <jarfile> -Ddfs.umask=18 -Dmapred.job.queue.name=___ ...

.. note:: If this is to be data shared regularly, the data should really get moved
   to a ``/project`` directory and not stored in any particular user's home directory.

Concatenate Multiple Small Files into Single File
=================================================


In order to merge two or more files into one single file and store it in hdfs,
you need to have a folder in the hdfs path containing the files that you want to merge.

  .. code-block:: bash

    hdfs dfs -cat /user/$USER/merge_files/* \
          | hdfs dfs -put - /user/$USER/merged_files

The merged_files folder need not be created manually. It is going to be created
automatically to store your output when you are using the above command.
You can view your output using the following command. |br|
Here `merged_files` is storing my output result.          


  .. code-block:: bash

    hadoop fs -cat merged_files


Removing Empty files on HDFS
============================

.. rubric:: Small number of empty files


.. code-block:: bash

   for f in $(hdfs dfs -ls -R / | awk '$1 !~ /^d/ && $5 == "0" { print $8 }'); do hdfs dfs -rm "$f"; done

*  ``hdfs dfs -ls -R /`` - list all files in HDFS recursively
*  ``awk '$1 !~ /^d/ && $5 == "0" { print $8 }')`` - print full path of those
   being not directories and with size 0
*  ``for f in $(...); do hdfs dfs -rm "$f"; done`` - iteratively remove

.. rubric:: Thousands of empty files

It will be quicker to use xargs

  .. code-block:: bash

     hadoop dfs -lsr | awk  'BEGIN {max=255}{if(($5==0)&&(substr($1,0,1)=="-")){ if (nb%max==0) printf "hadoop dfs -rm " ; printf " "$8; nb++; if (nb%max==0) print ""}}'  > temp.sh

     bash ./temp.sh


Pruning Files and Directories
=============================

There is no automatic purging of old user-created or user-loaded files in HDFS.
If files are not purged regularly, HDFS fills up and stops functioning.

`GDM <https://doppler.cloud.corp.yahoo.com:4443/doppler/gdm>`_
  Tools like `Doppler-GDM <https://doppler.cloud.corp.yahoo.com:4443/doppler/gdm>`_
  can apply retention to your datasets to remove them after a period of time.
`data_disposal <https://git.vzbuilders.com/vzn/data_disposal>`_
  The Java based Data Disposal tool takes in a simple `yaml` configuration
  specifying HDFS directories and Hive tables with customizable retention
  windows and date parsing from a partition or file path. |br|
  You should consider this tool if:

  * you have data in Hadoop HDFS that is made available with `Hive`; and
  * you spend too much time manually cleaning old data or maintaining multiple
    hacked scripts.