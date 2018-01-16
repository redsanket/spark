#!/bin/bash

# if node is rhel7 with docker enabled, make sure dockerd is
# running, if not try to start it 

HOSTNAME=`hostname`
OS_VER=`cat /etc/redhat-release | cut -d' ' -f7`
CLUSTER=`hostname | cut -d- -f1`
DOCKER_YINST_SET=`/usr/local/bin/yinst set -root /home/gs/gridre/yroot."$CLUSTER" | grep TODO_YARN_NODEMANAGER_RUNTIME_LINUX_ALLOWED_RUNTIMES | cut -d: -f2`

echo "Checking if we need to startup Docker daemon on node $HOSTNAME"


function check_dockerd {
  #echo "INFO: Checking if dockerd is running..."

  PS_CHECK=`ps -ef | egrep dockerd | egrep -v grep`
  if [[ "$PS_CHECK" =~ "docker-runc-current" ]]; then
    return 0
  else
    return 1
  fi
}


#
# based on OS version and docker enable yinst setting, decide it we need
# to start dockerd
#
if [[ "$OS_VER" =~ ^7. ]] && [[ "$DOCKER_YINST_SET" =~ "docker" ]]; then
  echo "INFO: OS is $OS_VER and Docker support is enabled, starting dockerd..."

  check_dockerd
  if [ $? -eq 0 ]; then
    echo "dockerd is running ok on node $HOSTNAME"

  else
    echo "WARN: dockerd is not running, attempting to start it..."
    systemctl start docker
    sleep 5
   
    check_dockerd
    if [ $? -eq 0 ]; then
      echo "dockerd is now running ok on node $HOSTNAME"
    else
      echo "ERROR: dockered is still not running after start attempt on node $HOSTNAME !"
      exit 1
    fi
  fi

elif [[ "$OS_VER" =~ "6." ]]; then
  echo "OS is $OS_VER, RHEL6 does not support Docker, not attempting dockerd start"

elif [[ ! "$DOCKER_YINST_SET" =~ "docker" ]]; then
  echo "OS is $OS_VER but Docker support is not enabled in yinst setting, not attempting dockerd start"

else
  echo "WARN: Unknown OS $OS_VER and Docker support setting combination!"
  exit 1
fi
 
