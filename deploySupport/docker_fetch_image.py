#!/home/y/bin/python2

##########################################################################################
#
# Tool to fetch docker images for a given flubber cluster, needed if docker image is
# updated and cluster already has an older image of the required type. 
#
# Note: this can take 1+ minutes per node since images can be >4GB
#
# Needs to run as sudo from admin node 
# Needs python2.7 or better for 'subprocess'
#
# Usage;
#    docker_fetch_image <CLUSTER_NAME> <IMAGE_NAME> <debug>
#    =>  CLUSTER_NAME is flubber cluster to update, example: openqe77blue
#    =>  IMAGE_NAME is docker image to fetch, must be 'rhel6' or 'rhel7'
#    =>  debug is an optional literal arg, if supplied will enable debugging info
#
# Example use: docker_fetch_image openqe77blue rhel6 debug
#
# This script is called from 232-installsteps-update-docker.sh in the flubber 
# deploySupport code but can also be run as-is from admin node sudo, to update 
# a cluster on an adhoc basis.
#
#  Basic format for docker pull:
#  sudo docker pull docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/<IMAGE_NAME>
#
##########################################################################################


import argparse, subprocess
import os, sys

#
# function: cluster_nodes 
# get list of nodes for the given cluster, not including GW
#
def cluster_nodes( CLUSTER ):
  nodes = subprocess.Popen(["yinst", "range", "-ir", "@grid_re.clusters."+CLUSTER], stdout=subprocess.PIPE)
  nodes_list = subprocess.check_output(["tr", "'\n'", "' '"], stdin=nodes.stdout).split()

  return nodes_list
# end func cluster_nodes

#
# function: get_rhel7_nodes
# find rhel7 nodes, if any, from a given list of nodes
#
def get_rhel7_nodes( NODE_LIST ):
  os_substring = "release 7."
  rhel7_nodes = []

  try:
    for NODE in NODE_LIST:
        
      print "Info: checking OS version for node: " + NODE
      output = subprocess.check_output(["ssh", NODE, "cat", "/etc/redhat-release"], stderr=subprocess.STDOUT)

      if os_substring in output:
        rhel7_nodes.append(NODE)
  except subprocess.CalledProcessError as cp_err:
    print "Error: got a CalledProcessError: ", cp_err

  return rhel7_nodes
# end func get_rhel7_nodes
  

#
# process user's args
#
parser = argparse.ArgumentParser()
# get the cluster and image to use 
parser.add_argument("cluster", help="the cluster to update")
parser.add_argument("image", help="docker image name, rhel6 or rhel7")
# optional debug flag
parser.add_argument("debug", help="enable debug output", nargs="?", default=False)
args = parser.parse_args()

cluster=args.cluster
print "Info: cluster to update is: ", cluster 

image=args.image
if image != 'rhel6' and image != 'rhel7':
  print "Error: unsupported Docker image type: ", image
  sys.exit(1)
else:
  print "Info: docker image to use is: ", image

if args.debug == 'debug':
  print "Info: debugging is enabled"
else:
  print "Info: debugging is not enabled"


#
# collect node list to work on and setup docker command line
#

# get the list of nodes for the cluster
nodes_to_update = cluster_nodes(cluster)

# check for only rhel7 nodes
rhel7_nodes = get_rhel7_nodes(nodes_to_update)
print "Info: RHEL7 nodes are: ", rhel7_nodes

# docker command line
cmd = "docker pull docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/" + image


#
# ssh to each rhel7 node and try to run docker pull, need to check return, just
# because a node is rhel7 doesn't mean it's running docker, ie RM, NN, etc.
#
for NODE in rhel7_nodes:
  print "Info: Going to update: " + NODE

  try:
    # fetch new images on given node
    response = subprocess.check_output(["ssh", NODE, cmd], stderr=subprocess.STDOUT)

    # get list of docker images on given node
    response = subprocess.check_output(["ssh", NODE, "docker images"], stderr=subprocess.STDOUT)
    if args.debug == 'debug':
      print "Debug: NODE " + NODE + " has docker images:"
      print response + "\n"

  except subprocess.CalledProcessError as cp_err:

    docker_err = "Cannot connect to the Docker daemon"
    #print "Response: " + cp_err.output

    if docker_err in cp_err.output:
      print "Info: NODE " + NODE + " is not running docker, skipping"
    else:
      print "Error: " + NODE + " got a CalledProcessError: ", cp_err


print "Info: Update completed"


