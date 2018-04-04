#!/home/y/bin/python2

##########################################################################################
#
# Tool to fetch docker images for a given flubber cluster, needed if docker image is
# updated and cluster already has an older image of the required type. 
#
# This can take a minute or more per node since images can be >4GB
#
# Needs to run as sudo from admin node, usage;
#
#    docker_fetch_image <CLUSTER_NAME> <IMAGE_NAME>
#    =>  CLUSTER_NAME is flubber cluster, example: openqe77blue
#    =>  IMAGE_NAME can be rhel6 or rhel7
#
#  Basic format for docker pull:
#  sudo docker pull docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/<IMAGE_NAME>
#
##########################################################################################


import argparse, subprocess
import os, sys, tempfile


#
# function: 
# get list of nodes for the given cluster, not including GW
#
def cluster_nodes( CLUSTER ):
  nodes = subprocess.Popen(["yinst", "range", "-ir", "@grid_re.clusters."+CLUSTER], stdout=subprocess.PIPE)
  nodes_list = subprocess.check_output(["tr", "'\n'", "' '"], stdin=nodes.stdout).split()

  return nodes_list

#
# function:
# find rhel7 nodes from a given list of nodes
#
def get_rhel7_nodes( NODE_LIST ):
  os_substring = "release 7."
  rhel7_nodes = []

  try:
    for NODE in NODE_LIST:
        
      print "Checking OS version for node: " + NODE
      output = subprocess.check_output(["ssh", NODE, "cat", "/etc/redhat-release"], stderr=subprocess.STDOUT)

      if os_substring in output:
        rhel7_nodes.append(NODE)
  except subprocess.CalledProcessError as cp_err:
    print "ERROR, got a CalledProcessError: ", cp_err

  return rhel7_nodes
  

#
# process user's args
#
parser = argparse.ArgumentParser()
# get the cluster and image to use 
parser.add_argument("cluster", help="the cluster to update")
parser.add_argument("image", help="docker image name, rhel6 or rhel7")
args = parser.parse_args()

cluster=args.cluster
print "cluster to update is: ", cluster 

image=args.image
print "docker image to use: ", image 
if image != 'rhel6' and image != 'rhel7':
  print "Error, unsupported Docker image type: ", image
  sys.exit(1)


# get the list of nodes for the cluster
nodes_to_update = cluster_nodes(cluster)

# check for only rhel7 nodes
rhel7_nodes = get_rhel7_nodes(nodes_to_update)
print "RHEL7 nodes are: ", rhel7_nodes


# docker command line
cmd = "docker pull docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/" + image


#
# ssh to each rhel7 node and try to run docker pull, need to check return, just
# because a node is rhel7 doesn't mean it's running docker, ie RM, NN, etc.
#
for NODE in rhel7_nodes:
  #for NODE in anode: 
  print "Going to update: " + NODE

  try:
    response = subprocess.check_output(["ssh", NODE, cmd], stderr=subprocess.STDOUT)

  except subprocess.CalledProcessError as cp_err:

    docker_err = "Cannot connect to the Docker daemon"
    #print "Response: " + cp_err.output

    if docker_err in cp_err.output:
      print "NODE " + NODE + " is not running docker, skipping"
    else:
      print "ERROR, " + NODE + " got a CalledProcessError: ", cp_err


print "Update completed"

