
export HDFT_TESTENV_SH=1

## export CLUSTER=omegaj
## export SG_WORKSPACE="/home/y/var/builds/workspace/HDFSRegression"


# common env for test utilities

# 4 sets of directories and files:
# dir/file for local test
# dir/files for hadoop 
# expected results in hdfsMeta
# test results in artifacts

# Prefix used here: the top directories are/can be independently set to different values
# HDFD_ : HDFS test Data on hadoop  side (both reference read-only data and w/r data during testing)
# HDFL_	: HDFS test data on Local side (both reference read-only data and w/r data as well as meta data used as expected results  for comparison)
# HDFT  : for testing directory and script prefix


#####################################################
###
### Top or Key Directory setting
###
#####################################################
#pass in as env variables
#export HDFT_TOP_DIR="/home/y/var/builds/workspace/HDFSRegression"
#export HDFT_TOP_DIR="/homes/cwchung/work/HDFSRegression"
export HDFT_JOB_SCRIPTS_TOP_DIR=$HDFT_TOP_DIR/job_scripts

## input test data local and on HDFS
export HDFD_TOP_DIR="/user/hadoopqa/hdfsRegressionData" 
export HDFL_TOP_DIR=$HDFT_TOP_DIR

## Test run results and expected results
# export HDFL_ARTIFACTS_TOP_DIR="/home/y/var/builds/workspace/HDFSRegression/artifacts"
export HDFL_ARTIFACTS_TOP_DIR="${HDFL_TOP_DIR}/artifacts"
export HDFL_EXPECTED_TOP_DIR="${HDFL_TOP_DIR}/Expected"
export HDFL_DATA_INPUT_TOP_DIR="${HDFL_TOP_DIR}/hdfsRegressionData"

## Some of the test requires writing to HDFS temp dir
export HDFD_TMPWRITE_TOP_DIR=${HDFD_TOP_DIR}/tmpWrite
export HDFD_SUDO_TMPWRITE_TOP_DIR=${HDFD_TOP_DIR}/tmpSudo
export HDFD_SUHADOOP8_TMPWRITE_TOP_DIR=${HDFD_TOP_DIR}/tmpHadoop8

# Libraries
export HDFT_UTIL_LIB="$HDFT_TOP_DIR/src/hdft_util_lib.sh"

# Macros for running hadoop file operations. Actually hadoop fs would work for both.
if [ -n "$HADOOP_VERSION" ] && [ "$HADOOP_VERSION" == '20' ] ; then
	export HDFT_HADOOP_DFS_CMD="hadoop dfs"
else
	export HDFT_HADOOP_DFS_CMD="hadoop fs"
fi

#####################################################
#  Obsolete ########################################
#####################################################
# Meta is obsolete now
export HDFL_HDMETA_TOP_DIR="$HDFL_TOP_DIR/hdfsMeta"
export HDFL_LOMETA_TOP_DIR="$HDFL_TOP_DIR/localMeta"

# Configured env
HDFT_META_PREFIX="z__"
HDFT_META_SUFFIX=".out"

# Coinfigured Tests
export HDFT_OPS_RDDIR="ls lsr du dus stat count"
export HDFT_OPS_RDFILE="ls cat tail"




