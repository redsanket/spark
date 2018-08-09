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
   EXCLUDE_PATTERNS='^\./bin/container-executor$|^\./bin/test-container-executor$|^\./include(/|$)|^\./lib/native/ipp(/|$)|^\./sbin(/|$)|^\./share/hadoop/hdfs/webapps(/|$)|^\./share/hadoop/httpfs(/|$)|^\./share/hadoop/kms(/|$)|^\./share/hadoop/yarn/auxservices(/|$)|^\./share/hadoop/yarn/test(/|$)|^\./share/hadoop/tools/sls(/|$)|\.cmd$|/jdiff(/|$)|/jsvc$|/sources(/|$)'

   rm -rf ${TARDR}
   mkdir ${TARDR}

   # copy hadoopcoretree contents
   cd ${YROOTDIR}/share/hadoop-${VERSION}
   find . -depth -print0 | egrep -vz $EXCLUDE_PATTERNS \
       | cpio --null -pvd ${TARDR}

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

   # HACK: Remove the server-site xincludes since these are client configs.
   #       Will not be necessary once QE deploys support separate client/server
   #       configs as production deploys do.
   sed -i -e 's;.*<xi:include href=".*-server-site.xml".*;;' ${TARDR}/conf/hadoop/*-site.xml

   # create tarball
   cd ${TARDR}
   tar zcf ./hadoopmapreduceonhdfs-${VERSION}.tgz *
}

mkhdfslink() {
    YROOTDIR=$1
    LINKPATH=$2
    LINKDEST=$3
    hadoopclasspath=`${YROOTDIR}/share/hadoop/bin/hadoop classpath`
    toolsclasspath="${YROOTDIR}/share/hadoop/share/hadoop/tools/lib/*"
    $JAVA_HOME/bin/java -cp "${hadoopclasspath}:${toolsclasspath}" org.apache.hadoop.tools.SymlinkTool mklink ${LINKDEST} ${LINKPATH}
}

# echo "Part 3: beginning."
if [ $CMD == "start" ]; then
    namenode=`hostname`

    echo "${HADOOP_HDFS_HOME}/bin/hdfs dfsadmin -finalizeUpgrade"
    set -x
    ${HADOOP_HDFS_HOME}/bin/hdfs dfsadmin -finalizeUpgrade
    # gridci-623, if layout version is different this will fail, nn will die on
    # incompatible versions and connection attempts will timeout (eventually)
    # Check if layout is the reason, warn the user 
    RC=$?
    set +x
    if [ $RC -ne 0 ]; then
      set -x
      # check if nn log shows layout errors
      NNLOG="/home/gs/var/log/hdfsqa/hadoop-hdfsqa-namenode-$namenode.log"
      PATTERN="'org.apache.hadoop.hdfs.server.common.IncorrectVersionException|File system image contains an old layout version'" 
      egrep $PATTERN $NNLOG
      RC=$?
      set +x
      if [ $RC -eq 0 ]; then
        echo "ERROR: hdfs layout versions have changed, data cannot be preserved"
        echo "Please rerun the deploy with REMOVEEXISTINGDATA checked" 
        exit 1
      # something else killed the nn
      else
        echo "ERROR: namenode failed to restart, please check logs at: "
        echo "$namenode:$NNLOG" 
        exit 1
      fi
    fi
    
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
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir -p /mapredsystem  /mapredsystem/hadoop /mapred/history/done /jobtracker /mapred/history/done_intermediate /mapred/logs
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${HDFSUSER}:hadoop /mapredsystem 
	echo ============ chown of /mapredsystem/hadoop to user ${MAPREDUSER}
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${MAPREDUSER}:hadoop /mapredsystem/hadoop /mapred/history/done /jobtracker /mapred/history/done_intermediate
	echo ============ continuing with hdfs janitorial services...
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 755 /mapredsystem
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${HDFSUSER}:hadoop /mapred 
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 755 /mapred
	# gridci-1773, change /mapred/history/done to match prod
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 770 /mapred/history/done
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 1777 /mapred/history/done_intermediate
        $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 1777 /mapred/logs
	echo ============ almost done with hdfs janitorial services...
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${MAPREDUSER}:hadoop /mapred/history
        $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${MAPREDUSER}:hadoop /mapred/logs

        DIR_LIST="/data /tmp/ /user /user/dfsload /user/hueadmin"
        for dir in $DIR_LIST; do
            $HADOOP_HDFS_HOME/bin/hdfs dfs -mkdir $dir
            $HADOOP_HDFS_HOME/bin/hdfs dfs -chmod 777 $dir
        done
        $HADOOP_HDFS_HOME/bin/hdfs dfs -chown dfsload /user/dfsload
        $HADOOP_HDFS_HOME/bin/hdfs dfs -chown hueadmin /user/hueadmin

	$HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir /user/hadoopqa
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chown hadoopqa /user/hadoopqa
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir -p /mapred/system
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${MAPREDUSER}:hadoop /mapred/system

	RMSTORE="/mapred/rmstore"
	echo "============ creating resourcemanager state store at $RMSTORE"
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir -p "$RMSTORE"
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chown ${MAPREDUSER}:hadoop "$RMSTORE"
	$HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod 700 "$RMSTORE"
    fi 

    # can't write to sharelib if in safemode
    echo "waiting to exit safe mode"
    $HADOOP_COMMON_HOME/bin/hdfs dfsadmin -safemode wait

    echo =========== Installing mapreduceonhdfs...
    # If upgrading from Hadoop 0.23 will need to make these dirs 
    $HADOOP_HDFS_HOME/bin/hdfs  dfs -mkdir -p /sharelib/v1/mapred/
    $HADOOP_HDFS_HOME/bin/hdfs  dfs -chown -R ${HDFSUSER}:hadoop /sharelib
    $HADOOP_HDFS_HOME/bin/hdfs  dfs -chmod -R 755 /sharelib

    mkmapredhdfs ${yroothome} ${hadoopversion}
    mapredhdfsbasename="hadoopmapreduceonhdfs-${hadoopversion}.tgz"
    mapredhdfsdir="/sharelib/v1/mapred"
    mapredhdfspath="${mapredhdfsdir}/${mapredhdfsbasename}"
    $HADOOP_HDFS_HOME/bin/hdfs  dfs -put /tmp/mapredhdfs/hadoopmapreduceonhdfs-${hadoopversion}.tgz ${mapredhdfspath}
    $HADOOP_HDFS_HOME/bin/hadoop fs -setrep 50 "${mapredhdfspath}"
    $HADOOP_HDFS_HOME/bin/hadoop fs -chmod 444 "${mapredhdfspath}"

    # create a current symlink to the mapreduceonhdfs tarball
    # for 2.5.1 and later releases
    hadoopversionxyz=`echo ${hadoopversion} | cut -d. -f1-3`
    if [ "${hadoopversionxyz}" != "2.5.0" ]
    then
	hadoopversionxy=`echo ${hadoopversion} | cut -d. -f1-2`
	linkpath="${mapredhdfsdir}/hadoopmapreduceonhdfs-${hadoopversionxy}-current.tgz"
	echo "=========== Creating symlink ${linkpath} -> ${mapredhdfsbasename}..."
	mkhdfslink "${yroothome}" "${linkpath}" "${mapredhdfsbasename}"
    fi

    # Directories for ATS
    $HADOOP_HDFS_HOME/bin/hadoop fs -mkdir /mapred/timeline
    $HADOOP_HDFS_HOME/bin/hadoop fs -chmod 755 /mapred/timeline
    $HADOOP_HDFS_HOME/bin/hadoop fs -chown mapredqa:hadoop /mapred/timeline

elif [ $CMD == "stop" ]; then 
    echo "Part 3: the stop is part of part 2; there is no part 3 for stop.."
else
    echo "Usage: namenodescript.sh [startonly|stop|start+erase]"
fi
# echo "Part 3: done."
