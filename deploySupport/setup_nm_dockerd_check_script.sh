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
    set -x
    systemctl restart docker
    set +x
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
if [[ "$OS_VER" =~ ^7. ]] && [[ "$DOCKER_YINST_SET" =~ "fsimage" ]]; then
  echo "INFO: OS is $OS_VER and runc support is up to date and enabled, ensuring packages are installed..."

  # Install missing tools if necessary
  # from setup_docker_hdfs.sh
  NEED_PKGS=
  [[ -z $(command -v skopeo) ]] && NEED_PKGS="$NEED_PKGS skopeo"
  # not strictly necessary but useful
  [[ -z $(command -v jq) ]] && NEED_PKGS="$NEED_PKGS jq"
  if [[ -n "$NEED_PKGS" ]]; then
    echo "Installing $NEED_PKGS"
    set -x
    # clean up Docker and dependent packages
    sudo yum -y remove 'docker*' containers-common
    sudo yum -y --enablerepo=epel --enablerepo=non-core install $NEED_PKGS
    set +x
  fi
elif [[ "$OS_VER" =~ "6." ]]; then
  echo "OS is $OS_VER, RHEL6 does not support Docker, not attempting dockerd start"

elif [[ ! "$DOCKER_YINST_SET" =~ "docker" ]]; then
  echo "OS is $OS_VER but Docker support is not enabled in yinst setting, not attempting dockerd start"

else
  echo "WARN: Unknown OS $OS_VER and Docker support setting combination!"
  exit 1
fi
 
