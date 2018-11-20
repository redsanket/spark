#!/bin/sh
set -o pipefail
set -o errexit

HADOOP_PREFIX=${HADOOP_PREFIX:-/home/gs/hadoop/current}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/home/gs/conf/current}
DOCKER_SQUASH_SCRIPT="$HADOOP_PREFIX/sbin/docker-to-squash.sh"
DOCKER_HDFS_ROOT=$1
DOCKER_IMAGE_TAG=$2

if [[ -z "$DOCKER_HDFS_ROOT" || -z "$DOCKER_IMAGE_TAG" ]]; then
  echo "Usage: setup_docker_hdfs.sh docker_hdfs_root docker_image_uri"
  exit 1
fi
if [[ ! -f "$DOCKER_SQUASH_SCRIPT" ]]; then
  echo "Skipping Docker HDFS setup since $DOCKER_SQUASH_SCRIPT is missing!"
  #exit 0
fi

# Install missing tools if necessary
NEED_PKGS=
[[ -z $(command -v skopeo) ]] && NEED_PKGS="$NEED_PKGS skopeo"
[[ -z $(command -v mksquashfs) ]] && NEED_PKGS="$NEED_PKGS squashfs-tools"
[[ -z $(command -v jq) ]] && NEED_PKGS="$NEED_PKGS jq"
if [[ -n "$NEED_PKGS" ]]; then
  echo "Installing $NEED_PKGS"
  sudo yum -y --enablerepo=epel install $NEED_PKGS
fi

echo "Installing $DOCKER_IMAGE_TAG to $DOCKER_HDFS_ROOT"
sh $DOCKER_SQUASH_SCRIPT --hdfs-root="$DOCKER_HDFS_ROOT" "$DOCKER_IMAGE_TAG"

echo "Computing manifest hash for $DOCKER_IMAGE_TAG"
MANIFEST_HASH=$(skopeo inspect --raw "docker://$DOCKER_IMAGE_TAG" | sha256sum | awk '{print $1}')

TAG_TO_HASH_FILE="$DOCKER_HDFS_ROOT/image-tag-to-hash"
echo "Setting up tag to hash mapping file at $TAG_TO_HASH_FILE"
kinit -kt /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa
echo "${DOCKER_IMAGE_TAG}:${MANIFEST_HASH}" | $HADOOP_PREFIX/bin/hadoop fs -put -f - "$TAG_TO_HASH_FILE"
$HADOOP_PREFIX/bin/hadoop fs -chmod 444 "$TAG_TO_HASH_FILE"

exit 0
