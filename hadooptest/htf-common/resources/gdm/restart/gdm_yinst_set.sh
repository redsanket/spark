#!/bin/bash

facetName=$1;
yinstSetting=$2;
yinstValue=$3;
echo "facetName = ${facetName}   yinstSetting=${yinstSetting}   yinstValue=${yinstValue}"
SUDO_USER=$USER  sudo  /home/y/bin/yroot  ${facetName}  --cmd  "/usr/local/bin/yinst   set ${yinstSetting}=${yinstValue}"