#!/bin/bash

set +x

yinst i -downgrade -live  yjava_jdk-1.8.0_60.51
yinst i -downgrade  yhdrs-1.24.3 yhdrs_data-2.1.4 yjava_yhdrs-0.32.22
yinst i -downgrade -live yjava_jetty-9.2.15.v20160210_274

SERVICE="yjava_jetty";
if ps ax | grep -v grep | grep $SERVICE > /dev/null
then
    echo "$SERVICE service running, everything is fine"
else
   echo "$SERVICE is not running"
   set -x 
   sudo  /usr/local/bin/yinst  start yjava_jetty
   RC = $?
   set +x
   if [ $RC -eq 0 ]; then
    echo "$SERVICE STARTED SUCCESSFULLY." 
   else
    echo "SERVICE  FAILED TO START. START MANUALLY....!"
   fi
fi
