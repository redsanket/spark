#!/bin/bash

facetName=$1;
action=$2;
echo "facetName = ${facetName}   action = ${action}"
SUDO_USER=$USER sudo /home/y/bin/yroot  ${facetName} --cmd "HOSTNAME=`hostname` /usr/local/bin/yinst ${action} ygrid_gdm_${facetName}_server" 
if [ "$?" -ne "0" ];then
  echo "Error: restarting  ${facetName} facet failed"
  exit 1
fi

SUDO_USER=$USER sudo /home/y/bin/yroot  ${facetName} --cmd "/usr/local/bin/yinst ${action} yapache_stunnel"
if [ "$?" -ne "0" ];then
  echo "Error: restarting yapache_stunnel failed"
  exit 1
fi

SUDO_USER=$USER sudo /home/y/bin/yroot  ${facetName} --cmd "webctl ${action} yapache"
if [ "$?" -ne "0" ];then
  echo "Error: restarting webctl yapache failed"
  exit 1
fi

SUDO_USER=$USER sudo /home/y/bin/yroot  ${facetName} --cmd "/usr/local/bin/yinst ${action} webctl"
if [ "$?" -ne "0" ];then
  echo "Error: restarting webctl failed"
  exit 1
fi

echo " ${facetName}  ${action} successfully"