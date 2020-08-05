Bash aliases for HDFS commands
==============================

Accessing the HDFS with the ``hadoop fs ...`` commands can be a bit tedious.
Here are some bash aliases that can be included in your ``~/.bashrc`` file to save some typing.

Hope they're useful. (Please updated with improvements, additions, etc. -- curious to see other solutions.) -- `HofmanYahoo <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Main/HofmanYahoo>`_ - 01 Oct 2009

   .. code-block:: bash

      alias hfs='hadoop fs'
      alias hls='hadoop fs -ls'
      alias hrm='hadoop fs -rm'
      alias hrmr='hadoop fs -rmr'
      alias hput='hadoop fs -put'
      alias hget='hadoop fs -get'
      alias hpush='hadoop fs -copyFromLocal'
      alias hpull='hadoop fs -copyToLocal'
      alias hcat='hadoop fs -cat'
      alias hmkdir='hadoop fs -mkdir'
      alias hcp='hadoop fs -cp'
      alias hchmod='hadoop fs -chmod'
      alias hmv='hadoop fs -mv'
      alias hgetmerge='hadoop fs -getmerge'
      alias hkill='hadoop job -kill'
      # below from narayanb
      alias hstream='hadoop jar $HADOOP_HOME/hadoop-streaming.jar -Dmapred.job.queue.name='$QUEUE
      alias hcount='hadoop dfs -count'

      # concatenate all part files in given directory
      hpartcat () { hadoop fs -cat $1/part-* ; }

      # concatenate all part files in given directory, piped to less
      hpartless () { hadoop fs -cat $1/part-* | less ; }

      # get total size of files in given directory
      # note: doesn't recurse, gives only sum of first-level files
      hdu () { hadoop fs -ls $1 | awk '{tot+=$5} END {print tot}' ; }

      # get total size of files in given directory, print human readable
      # human readable code http://bit.ly/O5AWU
      hduh () { hadoop fs -ls $1 | awk '{tot+=$5} END {print tot}' | \
         awk '{sum=$1;
              hum[1024**4]="Tb";hum[1024**3]="Gb";hum[1024**2]="Mb";hum[1024]="Kb";
              for (x=1024**3; x>=1024; x/=1024){
                      if (sum>=x) { printf "%.2f %s\n",sum/x,hum[x];break }
         }}' ; }



Command line auto-completion of Hadoop pathnames
================================================

bash
----

There is a blog post describing how to do this at `Yahoo Reports <http://blog.rapleaf.com/dev/?p=304>`_. I have modified this slightly to partially work with the aliases in the previous section. Also, it now defaults to your ``/user`` directory without an argument, instead of the ``root`` directory.



#. In my  ``~/.bashrc``, I added

   .. code-block:: bash

      if [ -f ~/.hfs_completion ]; then
        . ~/.hfs_completion
      fi

      alias hfs='hadoop fs'

#. In my  ``~/.hfs_completion``, as below.

   .. code-block:: bash

      ### begin of ~/.hfs_completion ###

      _hfs()
      {
        local cur prev

        COMPREPLY=()
        cur=${COMP_WORDS[COMP_CWORD]}
        prev=${COMP_WORDS[COMP_CWORD-1]}

        if [ "$prev" == hfs ]; then
          COMPREPLY=( $( compgen -W '-ls -lsr -du -dus -count -mv -cp -rm \
            -rmr -expunge -put -get -copyFromLocal -moveToLocal -mkdir -setrep \
            -touchz -test -stat -tail -chmod -chown -chgrp -help' -- $cur ) )
        fi

        if [ "$prev" == -ls ] || \
           [ "$prev" == -lsr ] || \
           [ "$prev" == -du ] || \
           [ "$prev" == -dus ] || \
           [ "$prev" == -cat ] || \
           [ "$prev" == -mkdir ] || \
           [ "$prev" == -put ] || \
           [ "$prev" == -get ] || \
           [ "$prev" == -rm ] || \
           [ "$prev" == -rmr ] || \
           [ "$prev" == -tail ] || \
           [ "$prev" == -cp ] || [ "$prev" == hcp ]; then
          if [ -z "$cur" ]; then
            COMPREPLY=( $( compgen -W "$( hfs -ls /user/`whoami`/ 2>&-|grep -v ^Found|awk '{print $8}' )" -- "$cur" ) )
          elif [ `echo $cur | grep \/$` ]; then
            COMPREPLY=( $( compgen -W "$( hfs -ls $cur 2>&-|grep -v ^Found|awk '{print $8}' )" -- "$cur" ) )
          else
            COMPREPLY=( $( compgen -W "$( hfs -ls $cur* 2>&-|grep -v ^Found|awk '{print $8}' )" -- "$cur" ) )
          fi
        fi
      } &&
      complete -F _hfs hfs

      ### End of ~/.hfs_completion ###

#. It works. Note ``hfs`` in the command.
   
    .. code-block:: bash
   
      [ykko@gwgd4005 ~]$ hfs -ls /user/ykko/[TAB][TAB]
      /user/ykko/bk_kw_srch.20091224  /user/ykko/projects
      /user/ykko/hk_spaceid_pv_0101   /user/ykko/tmp
      /user/ykko/hod-logs             /user/ykko/tqd
      /user/ykko/mapredsystem         /user/ykko/.Trash
      /user/ykko/-p


TCSH
----

Tcsh requires the attached python script  :download:`getHdfsCompletions.py </resources/getHdfsCompletions.py.txt>` to be in a directory on the execution path. The ``.cshrc`` lines needed for tcsh are then:

   .. code-block:: bash

    alias hdfs 'hadoop dfs'
    alias hls 'hadoop dfs -ls'

    complete hdfs 'p/1/(-ls -lsr -du -dus -count -mv -cp -rm -rmr -expunge \
                        -put -copyFromLocal -moveToLocal -mkdir -setrep \
                -touchz -test -stat -tail -chmod -chown -chgrp -help)/', \
             'n/-{ls,lsr,du,dus,cat,mkdir,put,rm,rmr,tail,cp,text}/`getHdfsCompletions.py $:-0`/'
    complete hls 'p/1/`getHdfsCompletions.py $:-0`/'


Hadoop Archives
===============

.. todo:: FixME: link to user guide and the external documentation link

.. note:: The deployment of Hadoop Archives is changed in the Security release (20.100)! The feature is now a user library that must be included with any job. See `the user guide <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Hadoop/ArchiveUserGuide.html>`_.

`Hadoop archive external documentation <http://hadoop.apache.org/common/docs/current/hadoop_archives.html>`_

See Usage impact for hadoop archive with Hadoop 20.S (http://twiki.corp.yahoo.com/view/Grid/GridSecurityUserImpact#HAR_usage) User need to download the hadoop archive jar through yinst and include than in ``HADOOP_CLASSPATH`` and as ``-libjars`` option while archiving the HDFS directories or recovering them using ``hadoop fs -cp command``


*Some additional notes to run it on yahoo clusters:*

* Here is an hadoop archive command to run it on yahoo cluster. External docs does not talk about ``–p option``, plus you need to specify appropriate queue name cause this command invokes a M/R job.

   .. code-block:: bash

      mkdir /homes/user_id/my_yinst_dir    # Note: Replace user_id with your own user id
      yinst install -root /homes/user_id/my_yinst_dir hadoop_archive
      export HADOOP_CLASSPATH=/homes/user_id/my_yinst_dir/lib/jars/hadoop_archive.jar:$HADOOP_CLASSPATH
      hadoop archive -Dmapred.job.queue.name=grideng \
            -libjars /homes/user_id/my_yinst_dir/lib/jars/hadoop_archive.jar \
            -archiveName foo1.har \
            -p /user/gogate/testout/ /user/gogate/testout/X /user/gogate/testout/Y /user/gogate

* Option ``-p`` requires you to specify the parent directory. Here ``/user/gogate/testout/`` is a top level parent input directory. Where X and Y are sub-directories of the parent that would be included in the archive. If you don’t specify any sub-directories then everything in the parent directory would be included in the archive. Last ``/user/gogate`` is a destination directory, where ``<foo1.har>`` directory will be created and it contains ``_index``, ``_masterindex`` and aggregated part files (smaller files in the original directory are concatenated to generate small number of larger size files in the archive).

* Retrieving the files from the archive are transparent to fs shell commands. So if you need to retrieve a subdirectory X from the archive use following command (assume ``/user/gogate/foo1.har`` is now your parent directory and X and Y are the sub-directories in the archive). You can also use the distcp to copy files in parallel.

   .. code-block:: bash

      # Note: Replace user_id with your own user id
      mkdir /homes/user_id/my_yinst_dir    
      yinst install -root /homes/user_id/my_yinst_dir hadoop_archive
      export HADOOP_CLASSPATH=/homes/user_id/my_yinst_dir/lib/jars/hadoop_archive-*.jar:$HADOOP_CLASSPATH
      hadoop fs -libjars /homes/user_id/my_yinst_dir/lib/jars/hadoop_archive.jar \
               -cp har:///user/user_id/foo1.har/X /user/user_id/some_new_dir

      ## OR 
      
      hadoop fs -libjars /homes/user_id/my_yinst_dir/lib/jars/hadoop_archive.jar \
                -cp har:///user/user_id/foo1.har /user/user_id/foo1
 

* Estimating number of files in hadoop archive

   * In Hadoop 0.20.x Hadoop archive command creates aggregated part files with max size of 2Gig and so to roughly estimate number of files in the ``archive ~= (size of input HDFS directory)/2GB + 2``. The _index and _masterindex are two additional files per archive.

   * Another trick for more accurate estimate on number of files in the archive, is to run the archive command and once M/R job is started (Map 0%, Reduce 0% complete message displays on console), check the number of map tasks in the job by going to the hadoop job tracker portal, which is same as number of aggregated part files in the archive. You can then kill the job and cleanup the archive directory, if only interested in the estimate.

# Note: Hadoop archive command does not delete or modify the input directories.

Simulated cluster, synthetic load generator
===========================================

.. todo:: FixME: link to SimulatedClusterSyntheticLoad


See `SimulatedClusterSyntheticLoad <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Grid/SimulatedClusterSyntheticLoad>`_ for information on:

* Simulated cluster setup (Hudson jobs and CLI scripts)
* Synthetic load generator setup (Hudson jobs and CLI scripts that run Synthetic load generator on a Simulated cluster)
  
How to set 777 Permission as default
====================================

I need the files I create to be readable and writable by my colleagues.
I set the dir to be 777, but every time I delete the files, and recreate them, they become non-readable again by my colleagues.

**Ans**: This is an HDFS :

The mode of a new file or directory is restricted by the umask set as a configuration parameter. When the existing ``create(path, …)`` method (without the permission parameter) is used, the mode of the new file is ``0666 & ^umask``. When the new ``create(path, permission, …)`` method (with the permission parameter ``P``) is used, the mode of the new file is ``P & ^umask & 0666``. When a new directory is created with the existing ``mkdirs(path)`` method (without the permission parameter), the mode of the new directory is ``0777 & ^umask``. When the new ``mkdirs(path, permission)`` method (with the permission parameter ``P``) is used, the mode of new directory is ``P & ^umask & 0777``.

The umask used when creating files and directories. It can be set by the following configuration ``fs.permissions.umask-mode`` makes all files readable and writeable.

For more details, see :hadoop_rel_doc:`HDFS Permissions Guide <hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html>`
