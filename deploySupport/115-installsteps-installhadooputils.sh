set +x

echo "Installing ygrid_hadoop_utils ..."
cmd="/usr/bin/yum -y install ygrid-hadoop-utils"
set -x
fanout $cmd
set +x

