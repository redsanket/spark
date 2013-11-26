#!/bin/bash
# This file is used by the job to create a file on the fly and change the permissions 
echo " ########################### in the process of creating file ###########################"
pwd
touch FileCreatedByJob.log > /dev/null 2>&1
chmod 444 FileCreatedByJob.log
sleep 180
echo " ########################### done creating file ###########################"
