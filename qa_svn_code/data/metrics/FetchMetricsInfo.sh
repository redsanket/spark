#!/bin/bash
#set +x
# This script gets the metrics info for the bean attributes provided
# $1 is the Job tracker Process ID
# $2 is the bean attribute interested
# $3 is the queue name where the job is submitted to

myPID=$1
myparam=$2
myqueue=$3
#get -b Hadoop:name=QueueMetrics,q0=root,q1=default,service=ResourceManager AvailableGB
#get -b Hadoop:name=QueueMetrics,q0=root,q1=default,service=ResourceManager AllocatedContainers
#get -b Hadoop:name=QueueMetrics,q0=root,q1=default,service=ResourceManager AllocatedGB
#get -b Hadoop:name=QueueMetrics,q0=root,q1=default,service=ResourceManager ReservedGB
#get -b Hadoop:name=QueueMetrics,q0=root,q1=default,service=ResourceManager PendingGB
(echo open $myPID ; echo get -b Hadoop:name=QueueMetrics,q0=root,q1=$myqueue,service=ResourceManager $myparam) | java -jar /homes/mapred/jmxterm-1.0-SNAPSHOT-uber.jar -n 2>/dev/null | grep $myparam

#set -x
