#!/bin/bash

# To avoid warning messages if host key in /home/hadoopqa/.ssh/known_hosts.
# This can happen if hosts are reimaged and have different IPs
SSH_OPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SSH="ssh $SSH_OPT"
SCP="scp $SSH_OPT"

# Use the same banner for logging stack component deploy as the other deploy steps
banner() {
    echo "*********************************************************************************"
    echo "***" `TZ=PST8PDT date ; echo // ; TZ=  date `
    echo "***" $*
    echo "*********************************************************************************"
}

