set +x

echo "Installing ygrid_hadoop_utils ..."
cmd="/usr/bin/yum clean dbcache; /usr/bin/yum clean metadata; /usr/bin/yum makecache; /usr/bin/yum -y install ygrid-hadoop-utils"
fanout_root "$cmd"
return $?
