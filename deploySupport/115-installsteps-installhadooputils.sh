echo "Installing ygrid_hadoop_utils ..."

LOCALCONFDIR=/usr/local/conf/hadoop

cmd="[[ -d ${LOCALCONFDIR} ]] || mkdir -p ${LOCALCONFDIR}"
fanout $cmd

cmd="/usr/local/bin/yinst install -root ${LOCALCONFDIR} -yes -branch test ygrid_hadoop_utils"
fanout $cmd

