#!/bin/bash

set -x
PRG=${0}
BASEDIR=`dirname ${PRG}`
source ${BASEDIR}/library/utils.sh

DEST_GRID="openqe95blue"
DEST_HDFS="hdfs://`yinst range -ir "(@grid_re.clusters.${DEST_GRID}.namenode)"`"
LOGTYPE="fsimage"
LOGPATH="/grid/0/hadoop/var/hdfs/name/current/"
GRIDNAME=`hostname | cut -d . -f 1 | cut -d '/' -f 3`

#back fill date should be YYYY-MM-DD
BACKFILLDATE=${1:-`date +%F -d "0 days"`}
#days should be a number
DAYS=${2:-0}
echo "`date +%FT%T` info: Collecting fsimage logs for ${BACKFILLDATE} and ${DAYS} days prior to today"

fsimage_ctime=`stat -c %Y ${LOGPATH}VERSION`
if [ ! -z "$fsimage_ctime" ] ; then
    now=`date +%s`
    if [ $((now-fsimage_ctime)) -gt 3600 ] ; then
        echo "`date +%FT%T` info: +++$+++"
        while [ ${DAYS} -ge 0 ]; do
            read STARTDATE ENDDATE < <(get_backfill_dates ${BACKFILLDATE} ${DAYS})
            fsimage_files=`find ${LOGPATH} -type f -newermt ${STARTDATE} ! -newermt ${ENDDATE} | grep -E "fsimage_[0-9]*" | grep -v "md5" | rev | cut -d / -f 1 | rev`
            if [[ -z $fsimage_files ]]; then
                echo "`date +%FT%T` info: skipping.. no fsimage files present on ${STARTDATE}"
                DAYS=$[${DAYS}-1]
                continue
            fi
            dest_pfx=${DEST_HDFS}/projects/starling/hadoopqa/logs/fsimage/${STARTDATE}
            create_folder ${dest_pfx}
            for fsimage_file in ${fsimage_files[@]}; do
                HOUR=`date -r ${LOGPATH}${fsimage_file} +%T | cut -d : -f 1`
                copy_to_hdfs ${LOGPATH}${fsimage_file} ${dest_pfx} ${GRIDNAME}-${LOGTYPE}-${STARTDATE}-${HOUR} ${LOGTYPE}
            done
            DAYS=$[${DAYS}-1]
        done
    else
        echo "`date +%FT%T` info: ${GRIDNAME}:${LOGPATH}VERSION is less than 3600sec old, will try later"
    fi
else
    echo "`date +%FT%T` error: invalid/incomplete storage dir ${GRIDNAME}:${LOGPATH}, may be a checkpoint in progress, skipping"
fi
exit 0;