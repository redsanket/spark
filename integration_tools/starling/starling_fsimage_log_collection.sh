#!/bin/bash

set -x

GRIDNAME=`hostname`
DEST_HDFS="hdfs://${GRIDNAME}:8020"
HADOOP_HOME=/home/gs/hadoop/current/
JAVA_HOME=/home/gs/java/jdk64/current/
HADOOP_CONF_DIR=/home/gs/conf/current/
HDFS="$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR"
LOGTYPE="fsimage"
GRID=`echo $DEST_HDFS | cut -d . -f 1 | cut -d '/' -f 3`
LOGPATH="/grid/0/hadoop/var/hdfs/name/current/"

echo "Please enter how many days of logs to collect prior to today?"
echo "(0 for today, 1 for today and yesterday, ...)"
read DAYS

if [[ ${DAYS} =~ ^[\-0-9]+$ ]] && (( ${DAYS} >= 0)); then
    echo "`date +%FT%T` info: Collecting logs for today and ${DAYS} days prior to today"
else
    echo "`date +%FT%T` error: ${DAYS} is not a positive integer"
    exit 1;
fi

/usr/kerberos/bin/kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@YGRID.YAHOO.COM

fsimage_ctime=`stat -c %Y ${LOGPATH}VERSION`
if [ ! -z "$fsimage_ctime" ] ; then
    now=`date +%s`
    if [ $((now-fsimage_ctime)) -gt 3600 ] ; then
        echo "`date +%FT%T` info: +++$+++"
        while [ ${DAYS} -ge 0 ]; do
            if [ ${DAYS} -eq 0 ]; then
                ENDDATE=`date +%F -d "+$((${DAYS} + 1)) days"`
            else
                ENDDATE=`date +%F -d "-$((${DAYS} - 1)) days"`
            fi
            STARTDATE=`date +%F -d "-${DAYS} days"`
            fsimage_files=`find ${LOGPATH} -type f -newermt ${STARTDATE} ! -newermt ${ENDDATE} | grep -E "fsimage_[0-9]*" | grep -v "md5" | cut -d / -f 9`
            if [[ -z $fsimage_files ]]; then
                echo "`date +%FT%T` info: skipping.. no fsimage files present on ${STARTDATE}"
                DAYS=$[${DAYS}-1]
                continue
            fi
            dest_pfx=${DEST_HDFS}/projects/starling/hadoopqa/logs/fsimage/${STARTDATE}
            if ( $HDFS dfs -ls $dest_pfx ) ; then
                echo "`date +%FT%T` info: skipping.. $dest_pfx exists ";
            else
                $HDFS dfs -mkdir $dest_pfx
                for fsimage_file in ${fsimage_files[@]}; do
                    HOUR=`date -r ${LOGPATH}${fsimage_file} +%T | cut -d : -f 1`
                    cp ${LOGPATH}${fsimage_file} /grid/0/tmp/"${GRID}-${LOGTYPE}-${STARTDATE}-${HOUR}"
                    gzip -fv /grid/0/tmp/"${GRID}-${LOGTYPE}-${STARTDATE}-${HOUR}"
                    $HDFS dfs -copyFromLocal /grid/0/tmp/"${GRID}-${LOGTYPE}-${STARTDATE}-${HOUR}".gz $dest_pfx/.
                done
            fi
            DAYS=$[${DAYS}-1]
        done
    else
        echo "`date +%FT%T` info: ${GRIDNAME}:${LOGPATH}VERSION is less than 3600sec old, will try later"
    fi
else
    echo "`date +%FT%T` error: invalid/incomplete storage dir ${GRIDNAME}:${LOGPATH}, may be a checkpoint in progress, skipping"
fi
exit 0;