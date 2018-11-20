#
# Run docker_fetch_image.py on the admin node in order to ensure
# any rhel7 hadoop worker has current docker image files. Any 
# rhel6 and rhel7 non-core worker nodes are ignored. 
#

echo DOCKER_IMAGE_TAG_TO_USE is: $DOCKER_IMAGE_TAG_TO_USE
echo RHEL7_DOCKER_DISABLED is: $RHEL7_DOCKER_DISABLED
echo cluster is: $cluster

DOCKER_TAG_URI="docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/$DOCKER_IMAGE_TAG_TO_USE"

setup_docker_hdfs() {
  echo =========== Setting up Docker images on HDFS
  scp setup_docker_hdfs.sh $jobtrackernode:/tmp/
  ssh $jobtrackernode sh /tmp/setup_docker_hdfs.sh /mapred/docker/ "$DOCKER_TAG_URI"
}

if [[ $RHEL7_DOCKER_DISABLED == "false" ]]; then

  echo == update docker image for any rhel7 hadoop worker nodes 

  /grid/0/tmp/docker_fetch_image.py $cluster $DOCKER_IMAGE_TAG_TO_USE

  setup_docker_hdfs()

else
 
  echo == docker not enabled, not updating docker image on nodes

fi
