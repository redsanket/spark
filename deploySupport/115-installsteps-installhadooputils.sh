echo "Installing ygrid_hadoop_utils ..."

LOCALCONFDIR=/usr/local/conf/bouncer

cmd="[[ -d ${LOCALCONFDIR} ]] || mkdir -p ${LOCALCONFDIR}"
fanout $cmd

cmd="/usr/local/bin/yinst install -root ${LOCALCONFDIR} -yes ygrid_hadoop_utils"
fanout $cmd

