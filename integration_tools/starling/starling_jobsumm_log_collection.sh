#!/bin/bash

set -x

GRIDNAME=`hostname`
DEST_HDFS="hdfs://${GRIDNAME}:8020"
HADOOP_HOME=/home/gs/hadoop/current
JAVA_HOME=/home/gs/java/jdk64/current/
HADOOP_CONF_DIR=/home/gs/conf/current/
HDFS="$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR"
LOGTYPE="mapred-jobsummary"
GRID=`echo $DEST_HDFS | cut -d . -f 1 | cut -d '/' -f 3`
LOGPATH="/grid/0/hadoop/var/log/mapredqa"

DAYS=0
echo "`date +%FT%T` info: Collecting jobsummary logs for today and ${DAYS} days prior to today"

/usr/kerberos/bin/kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@YGRID.YAHOO.COM

echo "`date +%FT%T` info: +++$+++"
while [ ${DAYS} -ge 0 ]; do
    if [ ${DAYS} -eq 0 ]; then
        ENDDATE=`date +%F -d "+$((${DAYS} + 1)) days"`
    else
        ENDDATE=`date +%F -d "-$((${DAYS} - 1)) days"`
    fi
    STARTDATE=`date +%F -d "-${DAYS} days"`
    jobsummary_files=`find ${LOGPATH} -type f -newermt ${STARTDATE} ! -newermt ${ENDDATE} | grep -E "mapred-jobsummary"`
    if [[ -z $jobsummary_files ]]; then
        echo "`date +%FT%T` info: skipping.. no jobsummary files present on ${STARTDATE}"
        DAYS=$[${DAYS}-1]
        continue
    fi
    dest_pfx=/projects/starling/hadoopqa/logs/jobsumm/${STARTDATE}
    $HDFS dfs -test -d ${dest_pfx} || $HDFS dfs -Dfs.permissions.umask-mode=027 -mkdir -p ${dest_pfx}
    if [ ! $? -eq 0 ]; then
        echo "`date +%FT%T` error: failed to create folder ${dest_pfx} on HDFS"
        exit 1;
    fi
    for jobsummary_file in ${jobsummary_files[@]}; do
        HOUR=`date -r ${jobsummary_file} +%T | cut -d : -f 1`
        cp ${jobsummary_file} /grid/0/tmp/"${LOGTYPE}.${STARTDATE}-${HOUR}"
        bzip2 /grid/0/tmp/"${LOGTYPE}.${STARTDATE}-${HOUR}"
        if [ ! $? -eq 0 ]; then
            echo "`date +%FT%T` error: failed to generate gzip file from /grid/0/tmp/'${LOGTYPE}.${STARTDATE}-${HOUR}'"
            exit 1;
        fi
        $HDFS dfs -test -f ${dest_pfx}/"${LOGTYPE}.${STARTDATE}-${HOUR}".bz2 || $HDFS dfs -copyFromLocal /grid/0/tmp/"${LOGTYPE}.${STARTDATE}-${HOUR}".bz2 $dest_pfx/.
        if [ ! $? -eq 0 ]; then
            echo "`date +%FT%T` error: failed to upload gzip file /grid/0/tmp/'${LOGTYPE}.${STARTDATE}-${HOUR}' to HDFS"
            exit 1;
        fi
        $HDFS dfs -chmod -R 750 ${dest_pfx}/"${LOGTYPE}.${STARTDATE}-${HOUR}".bz2
        rm /grid/0/tmp/"${LOGTYPE}.${STARTDATE}-${HOUR}".bz2
    done
    DAYS=$[${DAYS}-1]
done

exit 0;