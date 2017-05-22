#!/bin/bash

# gridci-2138, copy jks files from devadm102:/dev/shm/ygrid_certs/*.jks to each
# cluster node, needed to support webui https/ssl connections on any server run
# on said node, includes certs for Core and HBase

SSH_OPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"
ADM_HOST=${ADM_HOST:="devadm102.blue.ygrid.yahoo.com"}


CERT_REFERENCE_PATH="/dev/shm/ygrid_certs/*.jks"
CERT_HOME="/etc/ssl/certs/prod/_open_ygrid_yahoo_com"

echo == verify Core SSL certs are in place
 
fanout "if [ ! -d ${CERT_HOME} ] ; then 
           echo "Going to create ${CERT_HOME}";
           mkdir -p ${CERT_HOME};
           chmod 755 ${CERT_HOME};
        fi"

NODES=`yinst range -ir @grid_re.clusters.$CLUSTER`
for NODE in $NODES; do

  $SSH $ADM_HOST "sudo $SCP $CERT_REFERENCE_PATH $NODE:$CERT_HOME"
  if [ $? -ne 0 ]; then
    echo "Error: node $NODE failed to scp JKS files!"
    exit 1
  fi

done

###### TODO - pdcp would be more efficient because it will parallel scp instead
# of iterate like the while loop, but there's no support for '-S' in pdcp command
# or other efficient way to catch a node error
######
# NODES=`yinst range -ir @grid_re.clusters.$CLUSTER|tr '\n' ','|sed 's/.$//'`
#
# $SSH $ADM_HOST "PDSH_SSH_ARGS_APPEND='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' \
#   sudo pdcp -w $NODES $CERT_REFERENCE_PATH $CERT_HOME"
# if [ $? -ne 0 ]; then
#  echo "Error:  failed to scp JKS files!"
#   exit 1
# fi

