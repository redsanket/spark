#!/bin/bash

set -x
PRG=${0}
BASEDIR=`dirname ${PRG}`
source ${BASEDIR}/library/utils.sh

DEST_GRID="openqe95blue"
DEST_HDFS="hdfs://`yinst range -ir "(@grid_re.clusters.${DEST_GRID}.namenode)"`"
LOGTYPE="hdfs-audit.log"
LOGPATH="/home/gs/var/log/hdfs/"
GRIDNAME=`hostname | cut -d . -f 1 | cut -d '/' -f 3`

DAYS=0
echo "`date +%FT%T` info: Collecting nnaudit logs for today and ${DAYS} days prior to today"

echo "`date +%FT%T` info: +++$+++"
while [ ${DAYS} -ge 0 ]; do
    read STARTDATE ENDDATE < <(get_dates ${DAYS})
    nnaudit_files=`find ${LOGPATH} -type f -newermt ${STARTDATE} ! -newermt ${ENDDATE} | rev | cut -d / -f 1 | rev`
    if [[ -z $nnaudit_files ]]; then
        echo "`date +%FT%T` info: skipping.. no nnaudit files present on ${STARTDATE}"
        DAYS=$[${DAYS}-1]
        continue
    fi
    dest_pfx=${DEST_HDFS}/projects/starling/hadoopqa/logs/nnaudit/${STARTDATE}
    create_folder ${dest_pfx}
    for nnaudit_file in ${nnaudit_files[@]}; do
        TIME=`ls ${LOGPATH}${nnaudit_file} | rev | cut -d - -f 1 | rev`
        if [[ ! ${TIME} =~ ^[\-0-9]+$ ]]; then
            continue
        else
            copy_to_hdfs ${LOGPATH}${nnaudit_file} ${dest_pfx} ${GRIDNAME}-${LOGTYPE}.${STARTDATE}-${TIME} ${LOGTYPE}
        fi
    done
    DAYS=$[${DAYS}-1]
done

exit 0;