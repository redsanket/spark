#!/bin/bash

SERVICE="yjava_jetty";
if ps ax | grep -v grep | grep $SERVICE > /dev/null
then
    echo "$SERVICE service running, everything is fine"
else
   echo "$SERVICE is not running"
   set -x 
   sudo  /usr/local/bin/yinst  start yjava_jetty -v 3
   if [ $? -eq 0 ]; then
    echo "$SERVICE STARTED SUCCESSFULLY." 
   else
    echo "SERVICE  FAILED TO START. START MANUALLY....!"
   fi
fi
set +x