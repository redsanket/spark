set +x
#
# Run docker_fetch_image.py on the admin node in order to ensure
# any rhel7 hadoop worker has current docker image files. Any 
# rhel6 and rhel7 non-core worker nodes are ignored. 
#

if [[ $RHEL7_DOCKER_DISABLED == "true" ]]; then
    echo "RHEL7_DOCKER_DISABLED is true. Nothing to do."
    return 0
fi

OS_VER=`ssh $jobtrackernode "cat /etc/redhat-release | cut -d' ' -f7"`
if [[ "$OS_VER" =~ ^6. ]]; then
    echo "SKIP docker setup for cluster with RHEL6 resourcemanager node $jobtrackernode"
    return 0
fi

echo "RHEL7_DOCKER_DISABLED is: $RHEL7_DOCKER_DISABLED"
echo "DOCKER_IMAGE_TAG_TO_USE is: $DOCKER_IMAGE_TAG_TO_USE"
echo "cluster is: $cluster"

DOCKER_TAG_URI="docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/$DOCKER_IMAGE_TAG_TO_USE"

setup_docker_hdfs() {
    echo "=========== Setting up Docker images on HDFS"
    set -x
    scp /grid/0/tmp/setup_docker_hdfs.sh $jobtrackernode:/tmp/
    ssh $jobtrackernode sh /tmp/setup_docker_hdfs.sh /mapred/docker/ "$DOCKER_TAG_URI"
    set +x
}

if [[ $RHEL7_DOCKER_DISABLED == "false" ]]; then
    echo "== update docker image for any rhel7 hadoop worker nodes "
    set -x
    /grid/0/tmp/docker_fetch_image.py $cluster $DOCKER_IMAGE_TAG_TO_USE
    set +x
    setup_docker_hdfs
fi
