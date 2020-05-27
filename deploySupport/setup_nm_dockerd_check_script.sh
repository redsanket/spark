#!/bin/bash

# if node is rhel7 with docker enabled, make sure dockerd is
# running, if not try to start it 
#
# gridci-3184, need to be sure we're running 1.13.1-53 after seccomp change
# else see random docker container launch failures

HOSTNAME=`hostname`
OS_VER=`cat /etc/redhat-release | cut -d' ' -f7`
CLUSTER=`hostname | cut -d- -f1`
ALLOWED_RUNTIMES=`/usr/local/bin/yinst set -root /home/gs/gridre/yroot."$CLUSTER" | grep TODO_YARN_NODEMANAGER_RUNTIME_LINUX_ALLOWED_RUNTIMES | cut -d: -f2`

#
# based on OS version and docker enable yinst setting, decide it we need
# to start dockerd
#
if [[ "$OS_VER" =~ ^7. ]] && [[ "$ALLOWED_RUNTIMES" =~ "fsimage" ]]; then
  echo "INFO: OS is $OS_VER and runC support is enabled, ensuring packages are installed..."

  # Install missing tools if necessary (from setup_docker_hdfs.sh)
  NEED_PKGS=
  [[ -z $(command -v skopeo) ]] && NEED_PKGS="$NEED_PKGS skopeo"
  # not strictly necessary but useful
  [[ -z $(command -v jq) ]] && NEED_PKGS="$NEED_PKGS jq"
  if [[ -n "$NEED_PKGS" ]]; then
    echo "Installing $NEED_PKGS"
    # clean up Docker and dependent packages
    sudo yum -y remove 'docker*' containers-common
    sudo yum -y --enablerepo=epel --enablerepo=non-core install $NEED_PKGS
  fi
elif [[ "$OS_VER" =~ "7." ]]; then
  echo "OS is $OS_VER, ALLOWED_RUNTIMES is not set for runC containers"
elif [[ "$OS_VER" =~ "6." ]]; then
  echo "OS is $OS_VER, RHEL6 does not support runC containers"
else
  echo "WARN: Unknown OS $OS_VER and ALLOWED_RUNTIMES $ALLOWED_RUNTIMES setting combination!"
  exit 1
fi
 
