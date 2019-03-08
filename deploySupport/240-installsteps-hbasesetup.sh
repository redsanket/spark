#!/usr/local/bin/bash

set +x

if [ "$HBASEVERSION" == "none" ]; then
    echo "Skip hbase setup"
    return 0
fi

set -x
#Workaround to fix /etc/profile.d/grid.sh file in new reimaged cluster node to set HBASE_CONF_DIR to /home/gs/conf/hbase instead of /home/y/libexec/hbase/conf
fanout "sed -i \"s/HBASE_CONF_DIR=\/home\/gs\/conf\/hbase/HBASE_CONF_DIR=\/home\/y\/libexec\/hbase\/conf/g\" /etc/profile.d/grid.sh"
fanout "sed -i \"s/HBASE_CONF_DIR=\/home\/y\/conf\/hbase/HBASE_CONF_DIR=\/home\/y\/libexec\/hbase\/conf/g\" /etc/profile.d/grid.sh"
set +x

if [ "$HBASE_SHORTCIRCUIT" == "true" ]; then
    #Setp proper directory permission for short-circuit enabled
    set -x
    slavefanout "chgrp -R hbaseqa /grid/*/hadoop/var && chmod -R 750 /grid/*/hadoop/var && chmod g+s /grid/*/hadoop/var "
    set +x
fi
return 0 ;
