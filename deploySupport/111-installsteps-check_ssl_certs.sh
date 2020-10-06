#!/bin/bash

set +x

# gridci-2138/gridci-2292, copy jks files from devadm102:/grid/3/dev/ygrid_certs_flubber/*.jks
# to each  cluster node, needed to support webui https/ssl connections on any server run on
# said node, includes certs for Core and HBase

#
# TODO in future we could deliver these certificates using ykeykey/chef
# reference doc at;
#   https://docs.google.com/document/d/1u57rymngcIKUKpw-qfcUv17yOYbPvlHqOP6i3y2Rl0I
#
CERT_REFERENCE_PATH="/grid/3/dev/ygrid_certs_flubber/*"
CERT_HOME="/etc/ssl/certs/prod/_open_ygrid_yahoo_com"

echo "== verify Core SSL certs are in place"

fanout_root "if [ ! -d ${CERT_HOME} ] ; then \
echo \"Going to create ${CERT_HOME}\"; \
mkdir -p ${CERT_HOME}; \
chmod 755 ${CERT_HOME}; \
fi"

NODES=`yinst range -ir @grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway`
transport_files_from_admin $ADM_HOST $CERT_REFERENCE_PATH "$NODES" $CERT_HOME "root:root"
if [ $? -ne 0 ]; then
    echo "Error: Failed to transport JKS files to cluster nodes!"
    exit 1
fi

###### TODO - pdcp would be more efficient because it will parallel scp instead
# of iterate like the while loop, but there's no support for '-S' in pdcp command
# or other efficient way to catch a node error
######
# NODES=`yinst range -ir @grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway|tr '\n' ','|sed 's/.$//'`
#
# $SSH $ADM_HOST "PDSH_SSH_ARGS_APPEND='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' \
#   sudo pdcp -w $NODES $CERT_REFERENCE_PATH $CERT_HOME"
# if [ $? -ne 0 ]; then
#  echo "Error:  failed to scp JKS files!"
#   exit 1
# fi

