#!/bin/bash
# This file is used by the task to create a file on the fly and define symlink
echo " ########################### in the process of creating file ###########################"
pwd
#create a file in the tmp folder
touch /tmp/FileCreatedByJob.log > /dev/null 2>&1
#creating a symlink to the file just created
ln -s  /tmp/FileCreatedByJob.log mysymlink.txt > /dev/null 2>&1
sleep 180
echo " ########################### done creating file ###########################"
