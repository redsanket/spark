#!/bin/bash

set -x
PRG=${0}
BASEDIR=`dirname ${PRG}`
source ${BASEDIR}/library/utils.sh

DEST_GRID="openqe95blue"
DEST_HDFS="hdfs://`yinst range -ir "(@grid_re.clusters.${DEST_GRID}.namenode)"`"
LOGTYPE="mapred-jobsummary.log"
LOGPATH="/grid/0/hadoop/var/log/mapredqa"
GRIDNAME=`hostname | cut -d . -f 1 | cut -d '/' -f 3`

DAYS=0
echo "`date +%FT%T` info: Collecting jobsummary logs for today and ${DAYS} days prior to today"

echo "`date +%FT%T` info: +++$+++"
while [ ${DAYS} -ge 0 ]; do
    read STARTDATE ENDDATE < <(get_dates ${DAYS})
    jobsummary_files=`find ${LOGPATH} -type f -newermt ${STARTDATE} ! -newermt ${ENDDATE} | grep -E "mapred-jobsummary"`
    if [[ -z $jobsummary_files ]]; then
        echo "`date +%FT%T` info: skipping.. no jobsummary files present on ${STARTDATE}"
        DAYS=$[${DAYS}-1]
        continue
    fi
    dest_pfx=${DEST_HDFS}/projects/starling/hadoopqa/logs/jobsumm/${STARTDATE}
    create_folder ${dest_pfx}
    for jobsummary_file in ${jobsummary_files[@]}; do
        HOUR=`date -r ${jobsummary_file} +%T | cut -d : -f 1`
        copy_to_hdfs ${jobsummary_file} ${dest_pfx} ${LOGTYPE}.${STARTDATE}-${HOUR} ${LOGTYPE}
    done
    DAYS=$[${DAYS}-1]
done

exit 0;