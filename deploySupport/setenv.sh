#!/bin/bash

# To avoid warning messages if host key in /home/hadoopqa/.ssh/known_hosts.
# This can happen if hosts are reimaged and have different IPs
SSH_OPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"

# Use the same banner for logging stack component deploy as the other deploy steps
banner() {
    echo "#################################################################################"
    echo "# `date -u +'%a %b %d %Y'` `date -u +'%I:%M:%S %p %Z |'` `TZ=CST6CDT date +'%I:%M:%S %p %Z |'` `TZ=PST8PDT date +'%I:%M:%S %p %Z'`"
    echo "# $*"
    echo "#################################################################################"
}

note() {
    echo "# `TZ=PDT8PDT date '+%H:%M:%S %p %Z'` $*"
}