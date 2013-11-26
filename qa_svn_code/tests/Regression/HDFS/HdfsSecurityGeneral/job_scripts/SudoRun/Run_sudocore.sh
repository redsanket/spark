#!/bin/sh 

### Layout:
### 	env setting and echo. Global env has specific prefix of variables
###	test routines definition
###	acutal test

### Naming convention
### run_TestFileRdOnly.sh -> run_NameOfTest.sh then NameOfTest will be used 
##	preTestFileRdOnly	any special set up 
##	execTestFileRdOnly	acutally running the test
##	postTestFileRdOnly	post processing of dat
##	checkTestFileRdOnly	compare the data

#XXX#  if [ -z "$HDFT_RUNTEST_SH" ] ; then
	echo "Source env and library from $INCLUDE"
	source "$HDFT_TOP_DIR/hdft_include.sh"
	source "$HDFT_TOP_DIR/src/hdft_util_lib.sh"
##XXX#fi

if [ -n $HDFT_VERBOSE ] && [ $HDFT_VERBOSE == 1 ] ; then
	echo "Verbose mode set: HDFT_VERBOSE is $HDFT_VERBOSE"
	# set -x
fi



# common env for test utilities

# 4 sets of directories and files:
# dir/file for local test
# dir/files for hadoop 
# expected results in hdfsMeta
# test results in artifacts

# export HDFD_TOP_DIR="/user/hadoopqa/hdfsRegressionData" 
# export HDFL_TOP_DIR="/home/y/var/builds/workspace/HDFSRegression/hdfsRegressionData"
# export HDMETA_TOP_DIR="/home/y/var/builds/workspace/HDFSRegression//hdfsRegressionData/hdfsMeta"
# export LOMETA_TOP_DIR="/home/y/var/builds/workspace/HDFSRegression//hdfsRegressionData/localMeta"

echo "HDFD_TOP_DIR=$HDFD_TOP_DIR"
echo "HDFL_TOP_DIR=$HDFL_TOP_DIR"
echo "HDFL_ARTIFACTS_TOP_DIR=$HDFL_ARTIFACTS_TOP_DIR"
echo "HDFL_EXPECTED_TOP_DIR=$HDFL_EXPECTED_TOP_DIR"

# OPS = {ls, lsr, du, dus, stat, count }
echo  "HDFT_OPS_RDDIR=$HDFT_OPS_RDDIR"
echo  "HDFT_OPS_RDFILE=$HDFT_OPS_RDFILE"

# Input parameter: $* hadoop command to run (sands hadoop dfs)
# e.g. hdftExecHadoopDfsCmd -lsr /user/hadoopqa/HDFSRegressionData/

# first one to keep track of return status from exec a job, LOCAL_DIFF_RESULT tracking the number of times output is different
LOCAL_TASKS_RESULT=0
LOCAL_DIFF_RESULT=0
LOCAL_EXEC_RESULT=0

# No good way to find out the display name of the super user. This is the closest. Returned hdfsqa by default
# looking for pattern in : /grid/0/gs/gridre/yroot.omegab/conf/hadoop//deploy.omegab.confoptions.sh:   -set HadoopConfiggeneric10nodeblue.TODO_HDFSUSER=hdfsqa
# another option is to look at the user who runs namenode process on NN
# yet another option is to figure out how  supergroup  maps hdfs group to hdfs or hdfsqa
function junk_hdft_get_HDFSUser {
	F=$HADOOP_CONF_DIR/deploy.${CLUSTER}.confoptions.sh	
	HUser=`cat < $F | fgrep HadoopConfiggeneric10nodeblue.TODO_HDFSUSER= | awk -F= '{print $2}' | awk '{print $1}' ` 
	if [ $? != 0 ] || [ -z "$HUser" ] ; then
		echo "hdfsqa"
	else 
		echo $HUser
	fi	
}

##U=`hdft_get_HDFSUser`
## cho "Found User = $U"

###########################################################################################
###########################################################################################
###########################################################################################
###########################################################################################

# nothing special to do
preTestCoreDfs() {
	echo "" > /dev/null
}
postTestCoreDfs() {
	echo "" > /dev/null
}


checkTestCoreDfs() {
	echo "" > /dev/null
}

execOneCoreDfs() {
	echo "##########################################"
	# CORETEST_ID=$1 ; CORETEST_SUBID=$2 ; CORETEST_OP=$3 ; CORETEST_OUTPUT_ART=$4
	# CORETEST_EXPECTED_RESULT=$5; CORETEST_INPUT_DATA=$6 ; CORETEST_OUTPUT_DATA=$7

	echo "##########################################"
	CORETEST_ID=$1 ; CORETEST_SUBID=$2 ; CORETEST_OP=$3 ; CORETEST_OUTPUT_ART=$4 
	CORETEST_EXPECTED_RESULT=$5; CORETEST_STRING=$6 ; CORETEST_INPUT_DATA=$7 ; CORETEST_OPERAND=$8

	#  default: make use of the current directory relative to job_scripts, and the operations to synthesis the output and expected file name
	#  example for lsr: jobs_scripts/Basic/run_core.sh ==> Basic/run_core/lsr.out. 
	if [ -n "$CORETEST_OUTPUT_ART"  ] && [ "$CORETEST_OUTPUT_ART" == "default" ] ; then
		TASK_OUTPUT_ART=${HDFL_ARTIFACTS_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}.out
	else
		TASK_OUTPUT_ART="${HDFL_ARTIFACTS_TOP_DIR}/${CORETEST_OUTPUT_ART}"
	fi
	rm -f $TASK_OUTPUT_ART	

	if [ $CORETEST_EXPECTED_RESULT == "default" ] ; then
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${LOCAL_DEFAULT_REL_OUTPUT_DIR}/${CORETEST_OP}.out
	else
		TASK_EXPECTED_OUTPUT_ART=${HDFL_EXPECTED_TOP_DIR}/${CORETEST_EXPECTED_RESULT}
	fi

	HDFS_USER=`hdft_get_HDFSUser`

	# These operations requires two operands to hadoop command
	# distcp requires both operands in HDFS. The other three require one in local file, one in HDFS
	TASK_INPUT_DATA="${HDFD_TOP_DIR}/${CORETEST_INPUT_DATA}"
	TASK_OPERAND=$TASK_INPUT_DATA
	TASK_OPERAND_DEST=""
	# if [ $CORETEST_OP == "CopyToLocal" ] || [ $CORETEST_OP == "CopyFromLocal" ] || [ $CORETEST_OP == "distcp" ] || [ $CORETEST_OP == "get" ] ; then
	if [ $CORETEST_OP == "copyFromLocal" ] || [ $CORETEST_OP == "distcp" ] ; then
		case $CORETEST_OP in
		copyFromLocal)
			TASK_OPERAND_SRC=$HDFL_DATA_INPUT_TOP_DIR/$CORETEST_INPUT_DATA/*   
			TASK_OPERAND_DEST=${HDFD_SUDO_TMPWRITE_TOP_DIR}/$CORETEST_INPUT_DATA
			;;
		distcp)
			TASK_OPERAND_SRC=$TASK_INPUT_DATA
			TASK_OPERAND_DEST=${HDFD_SUDO_TMPWRITE_TOP_DIR}/$CORETEST_INPUT_DATA
			;;
		*)
			;;
		esac
	fi

	if [ -n	 "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
		echo "   CORETEST_ID=$CORETEST_ID"
		echo "   CORETEST_SUBID=$CORETEST_SUBID"
		echo "   CORETEST_OP=$CORETEST_OP"
		echo "   CORETEST_OUTPUT_ART=$CORETEST_OUTPUT_ART"
		echo "   CORETEST_EXPECTED_RESULT=$CORETEST_EXPECTED_RESULT"
		echo "   CORETEST_INPUT_DATA=$CORETEST_INPUT_DATA"
		echo "   TASK_INPUT_DATA=$TASK_INPUT_DATA"
		echo "   TASK_OUTPUT_ART=$TASK_OUTPUT_ART"
		echo "   TASK_EXPECTED_OUTPUT_ART=$TASK_EXPECTED_OUTPUT_ART"
		echo "   TASK_OPERAND=$TASK_OPERAND"
		echo "   TASK_OPERAND_SRC=$TASK_OPERAND_SRC"
		echo "   TASK_OPERAND_DEST=$TASK_OPERAND_DEST"
		echo "   HDFS_USER=$HDFS_USER"
	fi

	# create the artifact directory if not exist yet
	mkdir -p `dirname $TASK_OUTPUT_ART`
	chmod -R g+w `dirname $TASK_OUTPUT_ART`
	cmd="-${CORETEST_OP} ${TASK_INPUT_DATA}"

	if [ -n	 "$HDFT_VERBOSE" ] && [ "$HDFT_VERBOSE" == 1 ] ; then
		dumpmsg "    ------------------------"
		dumpmsg "    Exec hdfs dfs ${cmd}  >  "
		dumpmsg "        ${TASK_OUTPUT_ART}"
	fi

	## Some operations require different invocation, or additional operands
	LOCAL_NEED_POST_PROCESSING=1

	case "$CORETEST_OP" in 
	"copyFromLocal"  )
		# Argument is different from other hadoop command

		# First remove the existing file/directory if exist
		$HADOOP_CMD dfs -stat   $TASK_OPERAND_DEST
		#$HADOOP_CMD dfs -rmr  /user/hadoopqa/hdfsRegressionData/tmpSudo/*
		if [ $? == 0 ] ; then
			echo "    DOIT::: hdfs dfs -rmr -skipTrash ${TASK_OPERAND_DEST}"
			$HADOOP_CMD dfs -rmr  "${TASK_OPERAND_DEST}"
		fi

		echo "    $HADOOP_CMD dfs -mkdir -p $TASK_OPERAND_DEST"
		$HADOOP_CMD dfs -mkdir -p $TASK_OPERAND_DEST
		echo "    $HADOOP_CMD dfs -copyFromLocal  $TASK_OPERAND_SRC $TASK_OPERAND_DEST"
		$HADOOP_CMD dfs -copyFromLocal  $TASK_OPERAND_SRC $TASK_OPERAND_DEST
		stat=$?

		LOCAL_NEED_POST_PROCESSING=1
		$HADOOP_CMD dfs -lsr ${TASK_OPERAND_DEST}    > $TASK_OUTPUT_ART
		statExec=$stat
		;;
	"distcp" )
		# First remove the existing file/directory if exist
		$HADOOP_CMD dfs -stat   $TASK_OPERAND_DEST
		if [ $? == 0 ] ; then
			echo "    DOIT::: hdfs dfs -rmr  -skipTrash ${TASK_OPERAND_DEST}"
			$HADOOP_CMD dfs -rmr  "${TASK_OPERAND_DEST}"
		fi

		hadoop distcp -fs "hdfs://${HFDT_NAMENODE}:8020"  -p $TASK_OPERAND_SRC $TASK_OPERAND_DEST
		stat=$?

		LOCAL_NEED_POST_PROCESSING=1
		$HADOOP_CMD dfs -lsr ${TASK_OPERAND_DEST}    > $TASK_OUTPUT_ART
		statExec=$stat
		;;

	"copyToLocal" | "get" )
		echo "    DOIT:::: hdfs dfs -$CORETEST_OP $TASK_INPUT_DATA    ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA    ${TASK_OUTPUT_ART}
		statExec=$?
		;;
	"fsck")
		LOCAL_NEED_POST_PROCESSING=1
		echo "    DOIT::: hdfs     $CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART} "
		$HADOOP_CMD     $CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}
		statExec=$?
		;;
	"setrep")
		LOCAL_NEED_POST_PROCESSING=1
		$HADOOP_CMD dfs -rm  -skipTrash $TASK_OPERAND                                             2> /dev/null
		$HADOOP_CMD dfs -copyFromLocal $HDFT_TOP_DIR/src/data/smallFile14Byte $TASK_OPERAND
		echo "    Expect rep  changed from 3 to 4 to 2:"               >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -setrep 4 $TASK_OPERAND                              >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -setrep 2 $TASK_OPERAND                              >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		statExec=$?
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		;;
	"chown")
		LOCAL_NEED_POST_PROCESSING=1
		$HADOOP_CMD dfs -rm  -skipTrash $TASK_OPERAND                                             2> /dev/null
		$HADOOP_CMD dfs -copyFromLocal $HDFT_TOP_DIR/src/data/smallFile14Byte $TASK_OPERAND
		echo "    Expect owner changed from hdfs/hdfsqa to hadoopqa:"       >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -chown hadoopqa $TASK_OPERAND                        >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		statExec=$?
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		;;
	"chgrp")
		LOCAL_NEED_POST_PROCESSING=1
		$HADOOP_CMD dfs -rm  -skipTrash $TASK_OPERAND                                             2> /dev/null
		$HADOOP_CMD dfs -copyFromLocal $HDFT_TOP_DIR/src/data/smallFile14Byte $TASK_OPERAND
		echo "    Expect group changed from hdfs to users:"            >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -chgrp users $TASK_OPERAND                           >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		statExec=$?
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		;;
	"chmod")
		LOCAL_NEED_POST_PROCESSING=1
		$HADOOP_CMD dfs -rm  -skipTrash $TASK_OPERAND                                             2> /dev/null
		$HADOOP_CMD dfs -copyFromLocal $HDFT_TOP_DIR/src/data/smallFile14Byte $TASK_OPERAND
		echo "    Expect mode to be 600, 664, 666:"                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -chmod 664 $TASK_OPERAND                             >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -chmod 666 $TASK_OPERAND                             >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		statExec=$?
		$HADOOP_CMD dfs -ls $TASK_OPERAND
		;;
	"rm" | "rmr")
		LOCAL_NEED_POST_PROCESSING=1
		echo "    DOIT::: hdfs  dfs   -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART} "
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA 
		stat=$?
		echo "    #########DOIT::: hdfs dfs -lsr $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -lsr $TASK_INPUT_DATA >&  ${TASK_OUTPUT_ART}
		statExec=$stat
		;;
	"trash")
		LOCAL_NEED_POST_PROCESSING=1
		TASK_OPERAND=/tmp/hdft_tmptrash.txt
		TASK_OPERAND_IN_TRASH=/user/$HDFS_USER/.Trash/Current/$TASK_OPERAND
		$HADOOP_CMD dfs -rm  -skipTrash $TASK_OPERAND                                             2> /dev/null
		$HADOOP_CMD dfs -rm  -skipTrash $TASK_OPERAND_IN_TRASH                                    2> /dev/null
		echo "    Create file, then remove, and list in trash dir:"   >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -copyFromLocal $HDFT_TOP_DIR/src/data/smallFile14Byte $TASK_OPERAND
		$HADOOP_CMD dfs -ls $TASK_OPERAND  $TASK_OPERAND_IN_TRASH            >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -rm $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls $TASK_OPERAND                                    >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		$HADOOP_CMD dfs -ls                $TASK_OPERAND_IN_TRASH            >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		statExec=$?
		;;
	"undelete")
		LOCAL_NEED_POST_PROCESSING=1
		TASK_OPERAND=/tmp/hdft_tmptrash.txt
		TASK_OPERAND_IN_TRASH=/user/$HDFS_USER/.Trash/Current/$TASK_OPERAND
		$HADOOP_CMD dfs -rm  -skipTrash $TASK_OPERAND                                             2> /dev/null
		$HADOOP_CMD dfs -stat $TASK_OPERAND_IN_TRASH                                              2> /dev/null
		if [ $? != 0 ] ; then
		    echo " ERROR: Trash file does not exist. Run trash command first."              >> $TASK_OUTPUT_ART
		    statExec=1
		fi

		echo "Expect to see file in .Trash, the undeleted."            >> $TASK_OUTPUT_ART 
		$HADOOP_CMD dfs -ls $TASK_OPERAND  $TASK_OPERAND_IN_TRASH            >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART

		echo "Now move from Trash to undelete file "                   >> $TASK_OUTPUT_ART 
		$HADOOP_CMD dfs -mv $TASK_OPERAND_IN_TRASH  $TASK_OPERAND            >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		statExec=$?

		$HADOOP_CMD dfs -ls $TASK_OPERAND  $TASK_OPERAND_IN_TRASH            >> $TASK_OUTPUT_ART  2>> $TASK_OUTPUT_ART
		;;
	*)
		echo "    DOIT:::: hdfs dfs -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		echo "$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}"
		$HADOOP_CMD dfs -$CORETEST_OP $TASK_INPUT_DATA >  ${TASK_OUTPUT_ART}
		statExec=$?
		;;
	esac

	(( LOCAL_EXEC_RESULT = $LOCAL_EXEC_RESULT + $statExec ))	

	msg="${CORETEST_ID}-${CORETEST_SUBID}"
	hdftShowExecResult "$statExec" "$msg" "dfs-$CORETEST_OP"

	# skip the result file comparison if exec fails?	
	if [ "$LOCAL_NEED_POST_PROCESSING" == 1 ] ; then
		hdftFilterOutput ${TASK_OUTPUT_ART} "$CORETEST_OP"
	fi

	hdftDoDiff ${TASK_OUTPUT_ART} ${TASK_EXPECTED_OUTPUT_ART}
	statDiff=$?; (( LOCAL_DIFF_RESULT = $LOCAL_DIFF_RESULT + $statDiff ))	

	hdftShowDiffResult "$statDiff" "$msg"  "$TASK_OUTPUT_ART" "$TASK_EXPECTED_OUTPUT_ART"

	(( LOCAL_TASKS_RESULT = $statExec + $statDiff ))
	hdftShowTaskResult "$LOCAL_TASKS_RESULT" "$msg" "dfs-$CORETEST_OP"

}

#############################################
### Main test driver
### For each test, pass in
### argument: test-id, sub-id, test op, output dir/file, expected dir/default , input data file/dir
###    default: expected dir/default would be the same file hierarchy as otuput dir/file
###    Input data: if dir is provided and file operation (cat, tail ) is involved, then each of the fileis in the dir is 
#############################################


execOneTestOnly() {
	execOneCoreDfs "SF022" "11"    trash           default default  "NA" /tmp/hdft_tmptrash.txt     default
	execOneCoreDfs "SF022" "12"    undelete        default default  "NA" /tmp/hdft_tmptrash.txt 	default	
}

execAllTests() {
	execOneCoreDfs "SF022" "01"    lsr             default default  "NA" hdfsTestData/basic/           	default
	execOneCoreDfs "SF022" "02"    copyFromLocal   default default  "NA" hdfsTestData/basic/smallFiles	default
	execOneCoreDfs "SF022" "03"    copyToLocal     default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	execOneCoreDfs "SF022" "04"    cat             default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	execOneCoreDfs "SF022" "05"    tail            default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	execOneCoreDfs "SF022" "06"    stat            default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	execOneCoreDfs "SF022" "07"    fsck            default default  "NA" hdfsTestData/basic/           	default

	execOneCoreDfs "SF022" "08"    chmod           default default  "NA" tmpSudo/hdfsTestData/basic/tmp_chmod.txt 	default
	execOneCoreDfs "SF022" "09"    chown           default default  "NA" tmpSudo/hdfsTestData/basic/tmp_chown.txt 	default
	execOneCoreDfs "SF022" "10"    setrep          default default  "NA" tmpSudo/hdfsTestData/basic/tmp_setrep.txt 	default
	execOneCoreDfs "SF022" "11"    trash           default default  "NA" /tmp/hdft_tmptrash.txt     default
	execOneCoreDfs "SF022" "12"    undelete        default default  "NA" /tmp/hdft_tmptrash.txt 	default

	execOneCoreDfs "SF022" "90"    chgrp           default default  "NA" tmpSudo/hdfsTestData/basic/tmp_chgrp.txt 	default
	execOneCoreDfs "SF022" "91"    get             default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	# super user are not allowed to run mapred job. Thus this test case is removed.
	# execOneCoreDfs "SF022" "92"    distcp          default default  "NA" hdfsTestData/basic/smallFiles/smallRDFile755	default
	execOneCoreDfs "SF022" "93"    rm              default default  "NA" tmpSudo/hdfsTestData/basic/smallFiles/smallRDFile755	default
	execOneCoreDfs "SF022" "94"    rmr             default default  "NA" tmpSudo/hdfsTestData/basic	default

}


# handle default directory for artifacts
my_progname=$0
HDFT_JOB_DEFAULT_REL_OUTPUT_DIR="BAD_BAD"
hdftGetDefaultRelOutputDir $my_progname
LOCAL_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR

echo "    HDFT_JOB_DEFAULT_REL_OUTPUT_DIR=$HDFT_JOB_DEFAULT_REL_OUTPUT_DIR; LOCAL_DEFAULT_REL_OUTPUT_DIR = $LOCAL_DEFAULT_REL_OUTPUT_DIR"
hdftSuperuserKInit

preTestCoreDfs

#export RUN_ONE_TEST=1
if [ -n "$RUN_ONE_TEST" ] ; then
	echo "RUN ONE TEST ONLY:  execOneOnly"
	execOneTestOnly
else
	execAllTests
fi
	

postTestCoreDfs
#checkTestCoreDfs

hdftSuperuserKDestroy

(( LOCALY_TASKS_RESULT = $LOCAL_EXEC_RESULT + $LOCAL_DIFF_RESULT ))
echo "Result of running core test is =$LOCAL_TASKS_RESULT"
echo "Result=$LOCAL_TASKS_RESULT"
# echo "Result=0"

exit  "$LOCAL_TASKS_RESULT"

