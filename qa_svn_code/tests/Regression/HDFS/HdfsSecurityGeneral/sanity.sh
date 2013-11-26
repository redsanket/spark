
progName=$0
Argv1=$1

if [ -z "HDFT_HOME" ] ; then
	export HDFT_HOME=/home/y/var/builds/workspace/HDFSRegression
fi
source "${HDFT_HOME}/hdft_include.sh"

#################################################################
###
### Simple routine for usage and debugging
###
#################################################################
hdft_include_dump() {
		echo "=================================================="
		echo "Dump out the key env setting .."
		echo "======= First: JAVA, HADOOP, PATH related  ==================="
		env | egrep 'JAVA|HADOOP|PATH' | sort

		echo "======= Next: HDF* top dir related  ==================="
		env | egrep 'TOP_DIR' | sort

		echo "======= Next: other HDF* related related  ==================="
		env | egrep 'HDFS|HDFT|HDFD' |  grep -v TOP_DIR | sort
}

hdft_include_usage() {
	echo "$progName [-dump|-h]"
	echo "This $progName is intended to be #included by other test program to set up the hadoop hdfs teste environment"
	echo "Invoked by itself with -dump option, will dump the key environment setting, which is useful for debuugin"
	echo "Invoked by itself, it supports the following option: "
	echo "$progName -dump : dumps the key environment setting, which is useful for debugging"
	echo "$progName -sanity : run a simple sanity chedk to verify hadoop and basic data there. Essentially runs HelloWorld.sh"
	echo "$progName -h : echo this help"
}

hdft_include_sanity() {
	echo "################################################ " 
	echo "####### Simple sanity check to see if we have a working HADOOP Env ##########"
	echo "### First use native hadoop command to see if we can run hadoop and verify data are there "
	echo "### ${HDFT_HADOOP_DFS_CMD} -ls /user/hadoopqa/hdfsRegressionData"
	${HDFT_HADOOP_DFS_CMD} -ls /user/hadoopqa/hdfsRegressionData
	echo "### Now use key env variables HDFD_TOP_DIR"
	echo "### ${HDFT_HADOOP_DFS_CMD} -lsr $HDFD_TOP_DIR"
	${HDFT_HADOOP_DFS_CMD} -lsr $HDFD_TOP_DIR
}

hdft_include_processing() {
    if [ -n "$Argv1" ] ; then
	case "$Argv1" in 
	    "-dump")
		hdft_include_dump
		;;
	    "-s")
		hdft_include_sanity
		;;
	    "-sanity")
		hdft_include_sanity
		;;
	    "-h")	
		hdft_include_usage
		;;
	    *)
		hdft_include_sanity
		;;
	esac
    else
		hdft_include_sanity
    fi
}

# when #included in the test, there should be no argument provided to this script
hdft_include_processing
