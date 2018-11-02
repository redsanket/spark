#!/bin/bash

HADOOP_HOME=/home/gs/hadoop/current
JAVA_HOME=/home/gs/java/jdk64/current/
HADOOP_CONF_DIR=/home/gs/conf/current/
HDFS="$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR"
/usr/kerberos/bin/kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM

function get_dates(){
    DAYS=$1
    if [ ${DAYS} -eq 0 ]; then
        ENDDATE=`date +%F -d "+$((${DAYS} + 1)) days"`
    else
        ENDDATE=`date +%F -d "-$((${DAYS} - 1)) days"`
    fi
    STARTDATE=`date +%F -d "-${DAYS} days"`
    echo "${STARTDATE} ${ENDDATE}"
}

function get_backfill_dates(){
    BACKFILLDATE=$1
    DAYS=$2
    if [ ${DAYS} -eq 0 ]; then
        ENDDATE=`date +%F -d "${BACKFILLDATE} + $((${DAYS} + 1)) days"`
    else
        ENDDATE=`date +%F -d "${BACKFILLDATE} - $((${DAYS} - 1)) days"`
    fi
    STARTDATE=`date +%F -d "${BACKFILLDATE} - ${DAYS} days"`
    echo "${STARTDATE} ${ENDDATE}"
}


function create_folder(){
    dest_pfx=$1
    $HDFS dfs -test -d ${dest_pfx} || $HDFS dfs -Dfs.permissions.umask-mode=027 -mkdir -p ${dest_pfx}
    if [ ! $? -eq 0 ]; then
        echo "`date +%FT%T` error: failed to create folder ${dest_pfx} on HDFS"
        exit 1;
    fi
}

function copy_to_hdfs(){
    SOURCE=$1
    dest_pfx=$2
    FILENAME=$3
    LOGTYPE=$4
    cp ${SOURCE} /grid/0/tmp/"${FILENAME}"

    if [[ ${LOGTYPE} == "fsimage" ]]; then
        gzip -fv /grid/0/tmp/"${FILENAME}"
    else
        bzip2 /grid/0/tmp/"${FILENAME}"
    fi

    if [ ! $? -eq 0 ]; then
        echo "`date +%FT%T` error: failed to generate gzip file from /grid/0/tmp/'${FILENAME}'"
        exit 1;
    fi

    if [[ ${LOGTYPE} == "fsimage" ]]; then
        $HDFS dfs -test -f ${dest_pfx}/"${FILENAME}".gz || $HDFS dfs -copyFromLocal /grid/0/tmp/"${FILENAME}".gz $dest_pfx/.
    else
        $HDFS dfs -test -f ${dest_pfx}/"${FILENAME}".bz2 || $HDFS dfs -copyFromLocal /grid/0/tmp/"${FILENAME}".bz2 ${dest_pfx}/.
    fi

    if [ ! $? -eq 0 ]; then
        echo "`date +%FT%T` error: failed to upload gzip file /grid/0/tmp/'${FILENAME}' to HDFS"
        exit 1;
    fi

    if [[ ${LOGTYPE} == "fsimage" ]]; then
        $HDFS dfs -chmod -R 750 ${dest_pfx}/"${FILENAME}".gz
        rm /grid/0/tmp/"${FILENAME}".gz
    else
        $HDFS dfs -chmod -R 750 ${dest_pfx}/"${FILENAME}".bz2
        rm /grid/0/tmp/"${FILENAME}".bz2
    fi
}