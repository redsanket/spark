#!/bin/bash

set -x

GRIDNAME=`hostname`
DEST_HDFS="hdfs://${GRIDNAME}:8020"
HADOOP_HOME=/home/gs/hadoop/current
JAVA_HOME=/home/gs/java/jdk64/current/
HADOOP_CONF_DIR=/home/gs/conf/current/
HDFS="$HADOOP_HOME/bin/hdfs --config $HADOOP_CONF_DIR"
LOGTYPE="hdfs-audit.log"
GRID=`echo $DEST_HDFS | cut -d . -f 1 | cut -d '/' -f 3`
LOGPATH="/home/gs/var/log/hdfs/"

DAYS=0
echo "`date +%FT%T` info: Collecting nnaudit logs for today and ${DAYS} days prior to today"

/usr/kerberos/bin/kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@YGRID.YAHOO.COM

echo "`date +%FT%T` info: +++$+++"
while [ ${DAYS} -ge 0 ]; do
    if [ ${DAYS} -eq 0 ]; then
        ENDDATE=`date +%F -d "+$((${DAYS} + 1)) days"`
    else
        ENDDATE=`date +%F -d "-$((${DAYS} - 1)) days"`
    fi
    STARTDATE=`date +%F -d "-${DAYS} days"`
    nnaudit_files=`find ${LOGPATH} -type f -newermt ${STARTDATE} ! -newermt ${ENDDATE} | rev | cut -d / -f 1 | rev`
    if [[ -z $nnaudit_files ]]; then
        echo "`date +%FT%T` info: skipping.. no nnaudit files present on ${STARTDATE}"
        DAYS=$[${DAYS}-1]
        continue
    fi
    dest_pfx=${DEST_HDFS}/projects/starling/hadoopqa/logs/nnaudit/${STARTDATE}
    $HDFS dfs -test -d ${dest_pfx} || $HDFS dfs -Dfs.permissions.umask-mode=027 -mkdir -p ${dest_pfx}
    if [ ! $? -eq 0 ]; then
        echo "`date +%FT%T` error: failed to create folder ${dest_pfx} on HDFS"
        exit 1;
    fi
    for nnaudit_file in ${nnaudit_files[@]}; do
        TIME=`ls ${LOGPATH}${nnaudit_file} | rev | cut -d - -f 1 | rev`
        if [[ ! ${TIME} =~ ^[\-0-9]+$ ]]; then
            continue
        else
            cp ${LOGPATH}${nnaudit_file} /grid/0/tmp/"${GRID}-${LOGTYPE}.${STARTDATE}-${TIME}"
            bzip2 /grid/0/tmp/"${GRID}-${LOGTYPE}.${STARTDATE}-${TIME}"
            if [ ! $? -eq 0 ]; then
                echo "`date +%FT%T` error: failed to generate gzip file from /grid/0/tmp/'${GRID}-${LOGTYPE}.${STARTDATE}-${TIME}'"
                exit 1;
            fi
            $HDFS dfs -test -f ${dest_pfx}/"${GRID}-${LOGTYPE}.${STARTDATE}-${TIME}".bz2 || $HDFS dfs -copyFromLocal /grid/0/tmp/"${GRID}-${LOGTYPE}.${STARTDATE}-${TIME}".bz2 ${dest_pfx}/.
            if [ ! $? -eq 0 ]; then
                echo "`date +%FT%T` error: failed to upload gzip file /grid/0/tmp/'${GRID}-${LOGTYPE}.${STARTDATE}-${TIME}' to HDFS"
                exit 1;
            fi
            $HDFS dfs -chmod -R 750 ${dest_pfx}/"${GRID}-${LOGTYPE}.${STARTDATE}-${TIME}".bz2
            rm /grid/0/tmp/"${GRID}-${LOGTYPE}.${STARTDATE}-${TIME}".bz2
        fi
    done
    DAYS=$[${DAYS}-1]
done

exit 0;