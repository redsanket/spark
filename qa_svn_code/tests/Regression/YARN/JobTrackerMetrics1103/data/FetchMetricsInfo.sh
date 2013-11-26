#!/bin/bash
#set -x 
# This script gets the metrics info for the bean attributes provided
# $1 is the Job tracker Process ID
# $2 is the bean attribute interested

 myPID=$1
 myparam=$2
#echo $myPID 
#echo $myparam
(echo open $myPID ; echo get -b  Hadoop:name=JobTrackerMetrics,service=JobTracker $myparam) | java -jar /homes/mapred/jmxterm-1.0-SNAPSHOT-uber.jar -n 2>/dev/null |grep $myparam 

