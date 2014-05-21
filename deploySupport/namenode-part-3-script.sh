#!/bin/sh
export JAVA_HOME=$GSHOME/java/jdk64/current                                                                    

[ -z "$HADOOP_CONF_DIR" ] && export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
[ -z "$HDFSUSER" ] && export HDFSUSER=hdfs

echo $0 -- HDFSUSER=$HDFSUSER

if [ `whoami` != $HDFSUSER ]
then
  echo "failure: need to run $0 as $HDFSUSER." 1>&2
  exit 2
fi
case $1 in
    start+erase)
       export ERASEENABLED=true
       CMD=start
       # echo "starting and erasing first."
       ;;
   startonly)
       export ERASEENABLED=false
       CMD=start
       # echo "starting but not erasing."
       ;;
   stop)
       # echo "stopping, only."
       CMD=stop
       ;;
   *)
       echo "unknown option to $0."
       exit 1
       ;;
esac

mkmapredhdfs() {
   YROOTDIR=$1;
   VERSION=$2;
   TARDR=/tmp/mapredhdfs; 

   rm -rf ${TARDR}
   mkdir ${TARDR}

   # copy jars in classpath
   cd ${YROOTDIR}/share/hadoop-${VERSION}
   find share/hadoop/common share/hadoop/common/lib \
      share/hadoop/hdfs share/hadoop/hdfs/lib \
      share/hadoop/yarn share/hadoop/yarn/lib \
      share/hadoop/mapreduce share/hadoop/mapreduce/lib \
      -maxdepth 1 -name \*.jar -print0 | cpio --null -pvd ${TARDR}

   # copy native libraries
   find lib -depth -print0 | cpio --null -pvd ${TARDR}

   # install gplcompression into tree
   cd ${YROOTDIR}/share/hadoopgplcompression/lib
   find . -maxdepth 1 -name \*.jar -print0 |
      cpio --null -pvdu ${TARDR}/share/hadoop/common
   find . -maxdepth 1 -name \*.so\* -print0 |
      cpio --null -pvdu ${TARDR}/lib/native/Linux-i386-32
   cd ${YROOTDIR}/share/hadoopgplcompression/lib64
   find . -maxdepth 1 -name \*.so\* -print0 |
      cpio --null -pvdu ${TARDR}/lib/native/Linux-amd64-64

   cd ${YROOTDIR} && find conf ! -type l -print0 | cpio --null -pvdu ${TARDR}

   # create tarball
   cd ${TARDR}
   tar zcf ./hadoopmapreduceonhdfs-${VERSION}.tgz *
}

# echo "Part 3: beginning."
if [ $CMD == "start" ]; then
namenode=`hostname`
hadoopversion=`${yroothome}/share/hadoop/bin/hadoop version | sed -n 1p | sed -e 's/Hadoop //' `

    shortname=`expr  $namenode : '(' '\([^\.]*\)\..*$' ')'`
    echo name=$namenode shortname=$shortname
    ktabfile=/etc/grid-keytabs/${shortname}.dev.service.keytab
    # echo '***** NEED TO RUN' kinit to deal with keytab on ${namenode}
    export PATH=/usr/kerberos/bin:$PATH
    case $HDFSUSER in
      hdfsqa|hadoop[0123456789]|hdfs)
      if [  -f "$ktabfile" ]
      then
    kinit -k -t /etc/grid-keytabs/${shortname}.dev.service.keytab hdfs/${namenode}@DEV.YGRID.YAHOO.COM
      else
    kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
      fi
            ;;
        *)
      echo "Do not recognize HDFSUSER -- probably kinit / Kerberos errors will follow."
      kinit -k -t /etc/grid-keytabs/${shortname}.dev.service.keytab hdfs/${namenode}@DEV.YGRID.YAHOO.COM
  ;;
    esac

    st=$?
    [ "$st" -ne 0 ] && exit $st

    klist

    if [ "$ERASEENABLED" = true ]
    then
  echo ============ starting hdfs janitorial services...
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir -p /mapredsystem  /mapredsystem/hadoop /mapred/history/done /jobtracker /mapred/history/done_intermediate /mapred/logs /sharelib/v1/mapred/
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${HDFSUSER}:hadoop /mapredsystem 
  echo ============ chown of /mapredsystem/hadoop to user ${MAPREDUSER}
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${MAPREDUSER}:hadoop /mapredsystem/hadoop /mapred/history/done /jobtracker /mapred/history/done_intermediate
  echo ============ continuing with hdfs janitorial services...
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 755 /mapredsystem
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${HDFSUSER}:hadoop /mapred 
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 755 /mapred
        $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${HDFSUSER}:hadoop /sharelib
        $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 755 /sharelib
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 1777 /mapred/history/done_intermediate
        $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 1777 /mapred/logs
  echo ============ almost done with hdfs janitorial services...
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${MAPREDUSER}:hadoop /mapred/history
        $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${MAPREDUSER}:hadoop /mapred/logs
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir /data
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod 777 /data
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir /tmp
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod 777 /tmp
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir /user
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod 777 /user/
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir /user/hadoopqa
  $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown hadoopqa /user/hadoopqa

    echo =========== Installing mapreduceonhdfs...

    mkmapredhdfs ${yroothome} ${hadoopversion}
    $HADOOP_HDFS_HOME/bin/hdfs  dfs -put /tmp/mapredhdfs/hadoopmapreduceonhdfs-${hadoopversion}.tgz /sharelib/v1/mapred
    $HADOOP_HDFS_HOME/bin/hadoop fs -setrep 50 /sharelib/v1/mapred/hadoopmapreduceonhdfs-${hadoopversion}.tgz
    $HADOOP_HDFS_HOME/bin/hadoop fs -chmod 444 /sharelib/v1/mapred/hadoopmapreduceonhdfs-${hadoopversion}.tgz
    fi 
elif [ $CMD == "stop" ]; then 
    echo "Part 3: the stop is part of part 2; there is no part 3 for stop.."
else
    echo "Usage: namenodescript.sh [startonly|stop|start+erase]"
fi
# echo "Part 3: done."