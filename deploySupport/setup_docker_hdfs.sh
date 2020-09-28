#!/bin/sh
set -o pipefail
set -o errexit

HADOOP_PREFIX=${HADOOP_PREFIX:-/home/gs/hadoop/current}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/home/gs/conf/current}
DOCKER_SQUASH_SCRIPT="$HADOOP_PREFIX/sbin/docker-to-squash.py"
DOCKER_HDFS_ROOT=$1
DOCKER_IMAGE_URI_AND_TAG=$2

IFS=','
read -ra ADDR <<< "$DOCKER_IMAGE_URI_AND_TAG"
DOCKER_IMAGE_URI="${ADDR[0]}"
DOCKER_IMAGE_TAG="${ADDR[1]}"
IFS=' '

echo "DOCKER IMAGE URI: $DOCKER_IMAGE_URI"
echo "DOCKER IMAGE TAG: $DOCKER_IMAGE_TAG"

# GRIDCI-4920 - default PATH needs to include /usr/sbin so it will find
# /usr/sbin/mksquashfs. This was the case when running on devadm102 as root,
# but not from openre400red-n4.ygridvm.corp.bf1.yahoo.com as hadoopqa
PATH=$PATH:/usr/sbin

if [[ -z "$DOCKER_HDFS_ROOT" || -z "$DOCKER_IMAGE_TAG" ]]; then
  echo "Usage: setup_docker_hdfs.py docker_hdfs_root docker_image_uri,docker_image_tag"
  exit 1
fi
if [[ ! -f "$DOCKER_SQUASH_SCRIPT" ]]; then
  echo "Skipping Docker HDFS setup since $DOCKER_SQUASH_SCRIPT is missing!"
  exit 0
fi

# Install missing tools if necessary
NEED_PKGS=
[[ -z $(command -v skopeo) ]] && NEED_PKGS="$NEED_PKGS skopeo"
[[ -z $(command -v mksquashfs) ]] && NEED_PKGS="$NEED_PKGS squashfs-tools"
[[ -z $(command -v jq) ]] && NEED_PKGS="$NEED_PKGS jq"
if [[ -n "$NEED_PKGS" ]]; then
  echo "Installing $NEED_PKGS"
  set -x
  # clean up Docker and dependent packages
  sudo yum -y remove 'docker*' containers-common
  sudo yum -y --enablerepo=epel --enablerepo=non-core install $NEED_PKGS
  set +x
fi

TAG_TO_HASH_FILE="image-tag-to-hash"
DOCKER_IMAGE_MAGIC_FILE="etc/hadoop-dockerfile-version"
echo "Installing $DOCKER_IMAGE_URI to $DOCKER_HDFS_ROOT as $DOCKER_IMAGE_TAG"
set -x
kinit -kt /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa
python $DOCKER_SQUASH_SCRIPT pull-build-push-update --log=DEBUG --bootstrap --check-magic-file --magic-file="$DOCKER_IMAGE_MAGIC_FILE" --hdfs-root="$DOCKER_HDFS_ROOT" --image-tag-to-hash="$TAG_TO_HASH_FILE" "$DOCKER_IMAGE_URI,$DOCKER_IMAGE_TAG"
set +x
exit 0
