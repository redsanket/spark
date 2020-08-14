Configuring Hadoop namenode heap size
=====================================

*Important data structures in namenode:*

BlockInfo 
  used for storing block information, one for every block in the cluster.
INodeFile
  used for storing file information, one for every file on HDFS.
INodeDirectory
  used for storing directory information, one for every directory on HDFS.

*Calculating heap size:*

Java heap is organized into Young and Tenured generation. Young generation heap size is currently chosen as 3GB on production clusters.

*Tenured generation is made of:*

Live objects
  calculated based on number of blocks, files and directories. Additional 20% of space is allocated for other live objects and working heap.
Space for young generation
  Live objects from young generation are moved to Tenured space during minor GC. Young generation heap space needs to be free in the Tenured space assuming the worst case scenario.
Floating garbage
  During concurrent collection of Tenured generation some of the garbage is not collected (because the collection runs concurrently with the application and garbage marked as live could be garbage at the end of marking). To account for it, additional 20% of the Tenured space is allocated.

Use the :download:`excel spreadsheet </resources/NameNodeHeapSize-v4.xls>` to calculate the namenode heap size.


Various data copy scenarios
===========================

``DistCp``
  is a Hadoop utility implemented as a Map/Reduce job to copy files from one HDFS cluster to another HDFS cluster.
  As per `Hadoop Cluster Access Across Colos <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Hadoop/HadoopCrossColoUsers.html/>`_, please use ``webhdfs:`` as the source when the source cluster is cross-colo read *OR* when source and target clusters are on different hadoop versions.

Copying/Moving files from within same HDFS Cluster
--------------------------------------------------

Use ``bin/hadoop fs`` command on the gateway machine with appropriate options mentioned below.
For more information, see ``bin/hadoop fs -help``

  .. code-block:: bash

     bin/hadoop fs 
           [-mv <hdfs_src> <hdfs_dst>]
           [-cp <hdfs_src> <hdfs_dst>]


Copying files from one HDFS Cluster to another HDFS Cluster using webhdfs
-------------------------------------------------------------------------

.. todo:: Review the content because it is outdated. Content has been pulled from `Hadoop Cluster Access Across Colos (and using webhdfs to communciate between .23 and 2.x grids) <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Hadoop/HadoopCrossColoUsers.html>`_

All data being transmitted CROSS-colo must be encrypted. This page documents guidelines on how to access Hadoop grids in view of this initiative.
`INTRA-colo` traffic should still use hdfs! as hdfs for intra-colo is more efficient!
HISTORICAL note, the information below on converting to webhdfs was required as we migrated to 2.x as hdfs rpc is not compatible between .23 and 2.x and as such users should convert to webhdfs for that use case as well.

If at all possible, avoid accessing Hadoop clusters cross-colo
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you need to use data on a particular cluster but don't have resources (like disk quota) on that cluster, please first try to gain the appropriate access and resources on that cluster before you copy the data somewhere else. This avoids duplication of data and wasting of space and network bandwidth.
If you must copy data cross-colo, read on...

Use WebHDFS to read data across colos (and to communicate between .23 and 2.x Clusters)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Cross colo HDFS access must be done via webhdfs. If you are currently reading data from a remote colo, you must change the way you access this data--from Hadoop RPC to Hadoop webhdfs.
For example:

* OLD: hdfs://axonitered-nn1.red.ygrid.yahoo.com:8020/user/nroberts/blah
* NEW: webhdfs://axonitered-nn1.red.ygrid.yahoo.com/user/nroberts/blah

Don't write cross-colo (NO PUSHING)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You should always use a pull model when transferring data cross-colo.
That is, the destination Hadoop cluster should be reading data (pulling) from the source Hadoop cluster. The source Hadoop cluster should not be writing (pushing) data to the destination Hadoop cluster.

For example, with `distcp`,

* Always launch the job from the destination cluster's gateway
* Always use `WebHDFS` to access the source and HDFS to access the destination

.. code-block:: bash

    [ axonite-gw.red ] % hadoop distcp -Dmapred.job.queue.name=${queueName} webhdfs://cobaltblue-nn1/foo hdfs://axonitered-nn1/bar

Don't write via WebHDFS
^^^^^^^^^^^^^^^^^^^^^^^

Writes via WebHDFS are not currently supported, hence the no cross-colo write directive above. Usage of `WebHDFS` should be reads only, so pull models where data is read from `WebHDFS` need to be used instead of push models where data is written via `WebHDFS`.

Don't include a port when using WebHDFS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Including a port on the machine name in a URL is not necessary for either hdfs or webhdfs. Using a port is error prone, so now is a good time to remove it. Users often change ``hdfs://host:8020/`` to ``webhdfs://host:8020/``, or ``webhdfs://host:50070/`` back to ``hdfs://host:50070/`` and experience confusing error messages from hadoop.


For example:

* OLD: hdfs://axonitered-nn1.red.ygrid.yahoo.com:8020/user/nroberts/blah
* NEW: in colo: hdfs://axonitered-nn1.red.ygrid.yahoo.com/user/nroberts/blah
* NEW: cross colo: webhdfs://axonitered-nn1.red.ygrid.yahoo.com/user/nroberts/blah

.. note:: The har filesystem currently has a bug that requires ports to be specified. Ex. ``har://webhdfs-host:50070/path``

Errors encountered when entering bad ports
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* If you try to use a port, but mis-type it, you will get a connection exception (8021 is not a valid port):

  .. code-block:: bash
  
      hadoop distcp webhdfs://gsbl90339:8021/user/myname/afile hdfs://fsbl350n09/user/myname/file4
      14/03/06 18:25:19 ERROR tools.DistCp: Exception encountered 
      java.net.ConnectException: Connection refused
* If you use a port that something is waiting on, you get an IOException (50070 is a valid port, but not for this):

  .. code-block:: bash
  
      % hadoop distcp webhdfs://gsbl90339/user/myname/afile hdfs://fsbl350n09:50070/user/myname/file5
      14/03/06 18:35:12 ERROR tools.DistCp: Exception encountered 
      java.io.IOException: Failed on local exception: java.io.IOException: com.google.protobuf.InvalidProtocolBufferException: Protocol message end-group tag did not match expected tag.; ... 

Never use HFTP
^^^^^^^^^^^^^^

HFTP is a legacy interface for reading Hadoop data. It no longer works.


Provision launches within the same colo as destination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Launchers are generally associated with an cluster. These need to be deployed in the same site as the cluster (BF1/NE1/GQ1). It is almost never a good idea to have a launcher box in one colo launch a job on a cluster that is in another colo.

In Q2, to reduce our dependency on network crypto gear, we would be removing all cross colo launcher acls. Please reach out to grid-ops@ if you have specific need to have launchers in different colo that your primary cluster. Applications can always use Oozie or HDFS Proxy, which provide encrypted access (TLS), for cross colo access.

Connection between the client and ATS Proxies used for encrypting cross colo transfers is not encrypted. By having launchers in an different colo, traffic between hadoop client, running on launcher, and proxy would go through network crypto gear, this is something we would like to avoid.


Copying/Moving files between HDFS and Gateway
---------------------------------------------

Use ``bin/hadoop fs`` command on the gateway machine with appropriate options mentioned below.
For more information, see ``bin/hadoop fs -help``.
The prefix ``local_`` indicates file path on gateway machine, where prefix ``hdfs_`` indicates file path on HDFS.

  .. code-block:: bash

     bin/hadoop fs 
           [-put <local_src> ... <hdfs_dst>]
           [-copyFromLocal <local_src> ... <hdfs_dst>]
           [-moveFromLocal <local_src> ... <hdfs_dst>]
           [-get [-ignoreCrc] [-crc] <hdfs_src> <local_dst>]
           [-getmerge <hdfs_src> <local_dst> [addnl]]
           [-copyToLocal [-ignoreCrc] [-crc] <hdfs_src> <local_dst>]
           [-moveToLocal [-crc] <hdfs_src> <local_dst>]


Copying files between HDFS and User machine via a gateway node (without storing them on gateway)
-------------------------------------------------------------------------------------------------

While the HDFS is a great place to store massive inputs and working sets, sometimes it's good to get data onto and off of the system. But GridServices restricts access to the cluster through the gateway machines, which have relatively small storage capacity. Here's a great way to get data onto and off of the HDFS, without the intermediate step of storing data on the gateway machines' disks.

Remember that the hadoop ``dfs -put`` command accepts - to indicate standard in, and the ``-cat`` command writes to stdout.
You can put this together with SSH to pipe data through the gateway nodes without ever storing any temporary data on the gateway disks. It's easiest if you are running ``ssh-agent`` to handle ssh keys, and dotfiles on the gateway hosts that will automatically set up your ``HADOOP_CONF _DIR`` environment variable. If not, you can still type your password, and you can always supply ``--config`` as part of your hadoop command.

.. note::
  - ``-get`` writing to stdout has been deprecated
  - If you are using a proxy server (such as login1) to pipe the data, your connection may be closed after an hour of transfer. In that case, you will need to find a blessed box to send the data from.


Examples:

  .. code-block:: bash

     #Put a file onto the HDFS
     nohup cat /some/big/file | ssh krygw1000 'time hadoop dfs -put -' /path/in/dfs
     #first copy the contents of /.ssh/*.pub to /.ssh/authorized_keys
     #Get a file from the HDFS
     ssh krygw1000 'hadoop dfs -cat /path/in/dfs ' > /some/big/file
     #Get many files from the HDFS merged into one file (similar to "-getmerge")
     ssh krygw1000 'hadoop dfs -cat "/path/in/dfs/*"' > /some/big/file


Load large data sets from a different data center on to HDFS
------------------------------------------------------------

If you are going to load a large data set (terabytes) from a different data center using a map/reduce job or one of the gateways please notify us (ticket-grid-ops@yahoo-inc.com) AND prod-eng (prod-eng@yahoo-inc.com).

Provide the following information:

* Where you are loading the data from
* When you are loading the data
* The size of the data set
* Instructions for how to stop the process
* Your contact information in case your job causes an issue

.. todo:: Fix email address above


Is there a simple program that reads and writes to HDFS ?
=========================================================

.. todo:: Fix link  `Simple Example to read/write Hadoop DFS <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com:8080/?url=http%3A%2F%2Fwiki.apache.org%2Fhadoop%2FHadoopDfsReadWriteExample&SIG=11r05s6pr>`_



How do I make my program's output data available to other users?
================================================================

* By default, the files generated by map/reduce programs have permissions of 700. This means, the files that are produced are readable only to the user who ran the job. The reason for this is that, the default umask is 077.
  Here's how you can overwrite it. Note that hadoop option only takes decimal.

  .. code-block:: bash

    [knoguchi@gsgw1022 ~]$ printf "%d\n" 022
     18
    [knoguchi@gsgw1022 ~]$ hadoop dfs -D dfs.umask=18 -put test.txt /user/knoguchi
    [knoguchi@gsgw1022 ~]$ hadoop dfs -ls /user/knoguchi/test.txt
     Found 1 items
    /user/knoguchi/test.txt <r 3>   524     2008-03-26 21:06        rw-r--r--       knoguchi        users
    [knoguchi@gsgw1022 ~]$ hadoop dfs -D dfs.umask=18 -mkdir /user/knoguchi/testdir
    [knoguchi@gsgw1022 ~]$ hadoop dfs -ls /user/knoguchi/ | grep testdir
    /user/knoguchi/testdir  <dir>           2008-03-26 21:07        rwxr-xr-x       knoguchi        users

* For mapred job,

  .. code-block:: bash

    [knoguchi@gsgw1022 ~]$ hadoop --config confdir job -D dfs.umask=18 submit ...
    # OR
    [knoguchi@gsgw1022 ~]$ hadoop jar <jarfile> -Ddfs.umask=18 -Dmapred.job.queue.name=___ ...

.. note:: If this is to be data shared regularly, the data should really get moved to a ``/project`` directory and not stored in any particular user's home directory.

Concat Utility: Concatenate a number of small files in a directory into a single large file
============================================================================================


Often we have directories with lots of small files that we can concatenate into a single file (all files within a directory in order). This helps in saving the memory usage of namenode tremendously. Since, all the files in a given directory should be concatenated in order, this is a sequential process.
However, we can carry out these concatenations for different directories in parallel. The following simple perl script when used as the mapper (``-mapper``) of a streaming job does the trick.

This can also be checked out from `out local svn <svn+ssh://yst1001.yst.corp.yahoo.com/export/crawlspace/svn/yst/projects/kryptonite/TRUNK/solutions/concat_utility.pl/>`_.

.. todo:: Fix link  "out local svn"

.. code-block:: perl

  #!/usr/bin/perl

  # The concat utility - This is to be used as the mapper of the
  # streaming job for concatenation of small files in a directory
  # into a single Large file.
  # No Reducers should be used.
  # Mappers have side effects - DO NOT use speculative execution.

  my $next;
  while (defined($next=<STDIN>))
    {
      chomp $next;
      my $hadoop_home = $ENV{'HADOOP_HOME'};
      my $hcmd = $hadoop_home."/bin/hadoop";
      my $output_dir = $ENV{'mapred_output_dir'};
      chomp $output_dir;
      my $tot_size = `$hcmd dfs -dus $next`;
      if ($? != 0)
        {
          die "\n Error in estimating the size of the input directory: $next. Exiting!\n";
        }
      System("$hcmd dfs -cat $next/*");
      my $concatenated_size = `$hcmd dfs -dus $output_dir`;
      if ($? != 0)
        {
          die "\n Error in estimating the size of the concatenated file: $output_dir. Exiting!\n";
        }
      if ($tot_size != $concatenated_size)
        {
          print "\n Error in concatenating (dfs -cat)! Removing $output_dir !!\n";
          System("$hcmd dfs -rmr $output_dir");
          die "\n Exiting! \n";
        }
    }

  sub System
  {
    system (@_) == 0 or die ("system (@_) failed: $?");
  }

Save the above script as ``concat_utility.pl`` and then use it as a mapper of the streaming job as shown below.

   .. code-block:: bash

      hadoop jar $HADOOP_HOME/hadoop-streaming.jar \
                -input concat_input/input_dir_list.txt -output coutput \
                -mapper 'perl concat_utility.pl ' -reducer NONE \
                -file concat_utility.pl

In the above example, ``input_dir_list.txt`` is the input to the streaming job containing the list of directories where we need to concatenate small files into a single large file.
Usage of the example is shown below:

   .. code-block:: bash

      [foo@krygw1000 ~]$ hadoop dfs -cat /user/foo/concat_input/input_dir_list.txt
      /user/foo/cinput
      [foo@krygw1000 ~]$ hadoop dfs -ls /user/foo/cinput
       Found 3 items
      /user/foo/cinput/file1        <r 3>   6       2007-10-29 18:18
      /user/foo/cinput/file2        <r 3>   6       2007-10-29 18:18
      /user/foo/cinput/file3        <r 3>   6       2007-10-29 18:19
      [foo@krygw1000 ~]$
      [foo@krygw1000 ~]$ hadoop jar $HADOOP_HOME/hadoop-streaming.jar \
                        -input /user/foo/concat_input/input_dir_list.txt \
                        -mapper "perl concat_utility.pl" \
                        -file ./concat_utility.pl -output coutput -reducer NONE
      [foo@krygw1000 ~]$ hadoop dfs -ls coutput
       Found 1 items
      /user/foo/coutput/part-00000  <r 3>   21      2007-10-29 20:41
      [foo@krygw1000 ~]$



Joining data in HDFS
====================

``DataJoin`` is a package in hadoop contrib. You have to implement some Java plugin classes to combine multiple records into one.

.. todo:: Fix me: Here is the `doc for the package <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Grid/DataJoinUsingMapReduce/>`_:


Removing thousands of empty files on HDFS
=========================================

To remove empty files on HDFS, use the following awk script.

   .. code-block:: bash

      hadoop dfs -lsr | awk  'BEGIN {max=255}{if(($5==0)&&(substr($1,0,1)=="-")){ if (nb%max==0) printf "hadoop dfs -rm " ; printf " "$8; nb++; if (nb%max==0) print ""}}'  > temp.sh

      bash ./temp.sh

.. todo:: FixME: If you have questions about this script, please contact the author, Eric Crestan.


Pruning files and directories
=============================

There is no automatic purging of old user-created or user-loaded files in HDFS. If files are not purged regularly, HDFS fills up and stops functioning.
You can use the ``dfsprune.pl`` script to purge old files on a regular basis, e.g. from a cron job: `DfsPrune <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Grid/DfsPrune>`_.

.. todo:: FixME: link to DfsPrune

Experiment with large memory pages
==================================

.. todo:: FixME: link to LargeMemoryPageExperiment


`LargeMemoryPageExperiment <https://twiki.corp.yahoo.com/view/Grid/LargeMemoryPageExperiment>`_

Experiment with compressed oops
===============================

.. todo:: FixME: link to CompressedOOPSExperiment

`CompressedOOPSExperiment <https://twiki.corp.yahoo.com/view/Grid/CompressedOOPSExperiment>`_
