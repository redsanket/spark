#!/bin/bash

# if node is rhel7 with docker enabled, make sure dockerd is
# running, if not try to start it 
#
# gridci-3184, need to be sure we're running 1.13.1-53 after seccomp change
# else see random docker container launch failures

HOSTNAME=`hostname`
OS_VER=`cat /etc/redhat-release | cut -d' ' -f7`
CLUSTER=`hostname | cut -d- -f1`
DOCKER_YINST_SET=`/usr/local/bin/yinst set -root /home/gs/gridre/yroot."$CLUSTER" | grep TODO_YARN_NODEMANAGER_RUNTIME_LINUX_ALLOWED_RUNTIMES | cut -d: -f2`

echo "Checking if we need to startup Docker daemon on node $HOSTNAME"


function check_dockerd {
  #echo "INFO: Checking if dockerd is running..."

  # verify the storage driver in use, should be overlayFS
  CHK_DRIVER=`sudo docker info | grep Storage`


  if [[ "$CHK_DRIVER" =~ "overlay" ]]; then
    echo "Docker storage driver in use is overlay"
  else
    echo "WARN: storage driver is not correct or not found!"

    echo "Setting storage driver to overlay"
    echo "{ \"storage-driver\": \"overlay\" }" > /etc/docker/daemon.json

    echo "Restarting docker service"
    systemctl restart docker
  fi

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
  echo "INFO: OS is $OS_VER and Docker support is up to date and enabled, starting dockerd..."

  echo "Verify we have latest Docker package on node $HOSTNAME"
  yum install -y --enablerepo=latest* docker
  if [ $? -ne 0 ]; then
    echo "ERROR: docker package update failed!"
    exit 1
  fi

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
 
