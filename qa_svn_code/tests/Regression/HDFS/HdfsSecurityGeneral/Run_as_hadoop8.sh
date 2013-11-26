
# supply the var exported from yhudson

if [ -z "$CLUSTER" ] ; then
	export CLUSTER=JUNKXXXXX
	echo "WARNING: ENV CLUSTER not set. Set to $CLUSTER"
fi

if [ -z "$SG_WORKSPACE" ] ; then
	echo "WARNING: ENV SG_WORKSPACE not set. Set to $SG_WORKSPACE"
fi

if [ -z "$HDFT_TOP_DIR" ] ; then
	echo "WARNING: ENV HDFT_TOP_DIR not set. Set to $HDFT_TOP_DIR"
	echo "WARNING: ENV HDFT_TOP_DIR not set. Set to $HDFT_TOP_DIR"
	echo "WARNING: ENV HDFT_TOP_DIR not set. Set to $HDFT_TOP_DIR"
	
fi

export HDFT_HOME=$HDFT_TOP_DIR

#export CLUSTER=omegaj
#export SG_WORKSPACE=/home/y/var/builds/workspace/HDFSRegression
#export HADOOP_VERSION=20

whoami
pwd
env |grep HADOOP

umask 0002
echo "#### $0: Launching ... $HDFT_TOP_DIR/RunSuHadoop8.sh"
$HDFT_TOP_DIR/RunSuHadoop8.sh
