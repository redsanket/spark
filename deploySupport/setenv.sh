#!/bin/bash

# To avoid warning messages if host key in /home/hadoopqa/.ssh/known_hosts.
# This can happen if hosts are reimaged and have different IPs
SSH_OPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"
export PDSH_SSH_ARGS_APPEND="$SSH_OPT"

# Set SSH_AUTH_SOCK for sshca to work
export SSH_AUTH_SOCK=/tmp/.sshca_creds_agent/hadoopqa.sock

export yinst=/usr/local/bin/yinst

export PARTITIONHOME=/home
export GSHOME=$PARTITIONHOME/gs
export yroothome=$GSHOME/gridre/yroot.$CLUSTER
export yrootHadoopCurrent=$yroothome/share/hadoop
export yrootHadoopMapred=$yroothome/share/hadoop
export yrootHadoopHdfs=$yroothome/share/hadoop
export yrootHadoopConf=$yroothome/conf/hadoop
export GRIDJDK_VERSION=$GRIDJDK_VERSION

ADM_HOST=${ADM_HOST:="devadm101.blue.ygrid.yahoo.com"}
