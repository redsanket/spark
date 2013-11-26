

if [ -z "$HDFT_TOP_DIR" ] ; then
	# export HDFT_HOME=/home/y/var/builds/workspace/HDFSRegression
	export HDFT_TOP_DIR=/tmp/JUNK
fi
export HDFT_HOME=$HDFT_TOP_DIR

HDFT_INCLUDE_SRC=${HDFT_HOME}/src



echo hdft_include.sh source ${HDFT_INCLUDE_SRC}/hdft_hadoop_env.sh
source ${HDFT_INCLUDE_SRC}/hdft_hadoop_env.sh


echo hdft_include.sh source ${HDFT_INCLUDE_SRC}/hdft_setenv.sh
source ${HDFT_INCLUDE_SRC}/hdft_setenv.sh


echo hdft_include.sh source ${HDFT_INCLUDE_SRC}/hdft_testenv.sh
source ${HDFT_INCLUDE_SRC}/hdft_testenv.sh


echo hdft_include.sh source ${HDFT_INCLUDE_SRC}/hdft_globals.sh
source ${HDFT_INCLUDE_SRC}/hdft_globals.sh


echo hdft_include.sh source ${HDFT_INCLUDE_SRC}/hdft_util_lib.sh
source ${HDFT_INCLUDE_SRC}/hdft_util_lib.sh


export HDFT_INCLUDE=1



