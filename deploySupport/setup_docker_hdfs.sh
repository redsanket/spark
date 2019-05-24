#!/bin/sh
set -o pipefail
set -o errexit

HADOOP_PREFIX=${HADOOP_PREFIX:-/home/gs/hadoop/current}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/home/gs/conf/current}
DOCKER_SQUASH_SCRIPT="$HADOOP_PREFIX/sbin/docker-to-squash.py"
DOCKER_HDFS_ROOT=$1
DOCKER_IMAGE=$2
DOCKER_IMAGE_TAG=$DOCKER_IMAGE

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
  sudo yum -y --enablerepo=epel --enablerepo=non-core install $NEED_PKGS
  set +x
fi

TAG_TO_HASH_FILE="image-tag-to-hash"
DOCKER_IMAGE_MAGIC_FILE="etc/hadoop-dockerfile-version"
echo "Installing $DOCKER_IMAGE to $DOCKER_HDFS_ROOT as $DOCKER_IMAGE_TAG"
kinit -kt /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa
python $DOCKER_SQUASH_SCRIPT pull-build-push-update --log=DEBUG --check-magic-file --magic-file="$DOCKER_IMAGE_MAGIC_FILE" --hdfs-root="$DOCKER_HDFS_ROOT" --image-tag-to-hash="$TAG_TO_HASH_FILE" "$DOCKER_IMAGE,$DOCKER_IMAGE_TAG"

exit 0
