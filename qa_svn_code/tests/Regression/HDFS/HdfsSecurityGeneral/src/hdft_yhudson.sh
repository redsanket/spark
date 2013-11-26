
# supply the var exported from yhudson

if [ -z "$CLUSTER" ] ; then
	#export CLUSTER=omegab
	echo "ERROR: CLUSTER is not set"
	echo "ERROR: CLUSTER is not set"
	echo "ERROR: CLUSTER is not set"
	echo "ERROR: CLUSTER is not set"
	exit
fi

#export SG_WORKSPACE=/home/y/var/builds/workspace/HDFSRegression
# JAVA_HOME is set in conf/hadoop/common_hadoop_env.sh, and sourced in by driver.sh
# export JAVA_HOME=/grid/0/gs/gridre/yroot.${CLUSTER}/share/gridjdk-1.6.0_21

#export HADOOP_VERSION=20
##export HADOOP_VERSION=22

#export CLUSTER=omegaj
#export SG_WORKSPACE=/home/y/var/builds/workspace/HDFSRegression
#export HADOOP_VERSION=20
