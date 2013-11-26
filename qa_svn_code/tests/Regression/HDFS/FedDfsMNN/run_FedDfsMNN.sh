source $WORKSPACE/lib/library.sh
source $WORKSPACE/lib/user_kerb_lib.sh
source $WORKSPACE/lib/hdft_util2.sh

OWNER="cwchung"

###################################################
## SETUP & Teardown
###################################################
function setup {
	umask 0002		# create 775 dir/files
	#global, return to framework
	export  COMMAND_EXIT_CODE=0
	export TESTCASE_DESC="None"
	export REASONS="    ####"
	SCRIPT_EXIT_CODE=0
	
	# module-global
	PROGNAME=`basename $0`
	HDFS_USER=$HADOOPQA_USER
	DFS_COMMAND=`basename "$HADOOP_COMMON_CMD"`

	NUM_TESTCASE=0
	NUM_TESTCASE_PASSED=0
	TASK_EXIT_CODE=0

	setKerberosTicketForUser $HDFS_USER

	# gsbl90772.blue.ygrid.yahoo.com
	NNX=$(getDefaultNameNode)

	#get the default FS: hdfs://gsbl.90772.blue.ygrid.yahoo.com:8020
	FS=$(getDefaultFS)

	# gsbl90772
	NN0=$(hdft_getNN 0)
	NN1=$(hdft_getNN 1)
	NN2=$(hdft_getNN 2)

	#dir for federation common dfs opertions on MN
	# for different URI reference
	FED_DFSMNN_DEFAULT_DATA_DIR="hdfsMNNData"
	FED_DFSMNN_SLASH_DATA_DIR="/user/${HDFS_USER}/${FED_DFSMNN_DEFAULT_DATA_DIR}"
	FED_DFSMNN_FS_DATA_DIR="${FS}${FED_DFSMNN_SLASH_DATA_DIR}"
	#FED_DFSMNN_DST_DIR="$TMPDIR/${FED_DFSMNN_DEFAULT_DATA_DIR}"
	FED_DFSMNN_SLASH_DST_DIR="/tmp/${FED_DFSMNN_DEFAULT_DATA_DIR}"
	
	FED_DFSMNN_EXPECTED_DIR="$JOB_SCRIPTS_DIR_NAME/Expected"
	FED_DFSMNN_ARTIFACTS_DIR="$ARTIFACTS_DIR"

	FED_DFSMNN_OUTDIR=/tmp/D1
	#LOCAL_TMPDIR=$TMPDIR/L1
	LOCAL_TMPDIR=/tmp/L1.${CLUSTER}.${USER}

	# create top local dir
	rm -rf $LOCAL_TMPDIR
	mkdir $LOCAL_TMPDIR
	chmod 775 $LOCAL_TMPDIR

	for MM in $NN0 $NN1 $NN2 ; do 
		MM_FS=hdfs://$MM.blue.ygrid.yahoo.com:8020
		#deleteHdfsDir $FED_DFSMNN_DST_DIR $MM_FS
		local cmd="${HADOOP_HDFS_CMD}  dfs -rm -r -skipTrash  ${MM_FS}/${FED_DFSMNN_SLASH_DST_DIR}"
        echo "    #### UCMD= $cmd"
		eval $cmd
	done
}

function teardown {
	for MM in $NN0 $NN1 $NN2 ; do 
		MM_FS=hdfs://$MM.blue.ygrid.yahoo.com:8020
		local cmd="${HADOOP_HDFS_CMD}  dfs -rm -r -skipTrash  ${MM_FS}/${FED_DFSMNN_SLASH_DATA_DIR}"
		echo "    #### Clean up= $cmd"
		eval $cmd
		local cmd="${HADOOP_HDFS_CMD}  dfs -rm -r -skipTrash  ${MM_FS}//user/hadoopqa/tmpOut"
		echo "    #### Clean up= $cmd"
		eval $cmd		
	done
}

function setupData {
	local cmd
	local stat
	local stat0
	for MM in $NN0 $NN1 $NN2 ; do
		MM_FS=hdfs://$MM.blue.ygrid.yahoo.com:8020
		echo  "    ==========  $MM  deleteHdfsDir $FED_DFSMNN_SLASH_DATA_DIR $MM_FS ==="
		#deleteHdfsDir $FED_DFSMNN_SLASH_DATA_DIR $MM_FS
		#createHdfsDir $FED_DFSMNN_SLASH_DATA_DIR $MM_FS
		#copyHdfsTestData $FED_DFSMNN_SLASH_DATA_DIR $MM_FS

		cmd="${HADOOP_HDFS_CMD}  dfs -rm -r -skipTrash  ${MM_FS}${FED_DFSMNN_SLASH_DATA_DIR}"	
		echo "    #### UCMD= $cmd"
		eval $cmd

		cmd="${HADOOP_HDFS_CMD}  dfs -mkdir -p ${MM_FS}${FED_DFSMNN_SLASH_DATA_DIR}"
		#cmd="${HADOOP_HDFS_CMD}  dfs -fs ${MM_FS} -mkdir  ${FED_DFSMNN_SLASH_DATA_DIR}"
		echo "    #### UCMD= $cmd"
		eval $cmd
		stat0=$? ; (( stat = stat + stat0 ))

		cmd="${HADOOP_HDFS_CMD}  dfs -copyFromLocal  /homes/hdfsqa/hdfsMNNData/*  ${MM_FS}${FED_DFSMNN_SLASH_DATA_DIR}"
		echo "    #### UCMD= $cmd"
		eval $cmd
		stat0=$? ; (( stat = stat + stat0 ))

		# do a fast abort if test failed
		if [ $stat != 0 ] ; then
			echo "FATAL ERROR in set up data for FED DFS MNN test with $MM [$cmd]. Test aborted."
			hdftAbortTest "FED_DFS_MNN_SETUP"
		fi
	done
	echo "FED_DFS_MNN_SETUP is OK. Proceed with the tests."
}


function setupOneData {
		#delete directories on hdfs, make sure no old data is there
		deleteHdfsDir $FED_DFSMNN_SLASH_DATA_DIR $FS
		createHdfsDir $FED_DFSMNN_SLASH_DATA_DIR $FS

		createHdfsDir $FED_DFSMNN_TESTS_SRC_DIR $FS
		createHdfsDir $FED_DFSMNN_TESTS_DST_DIR $FS

		copyHdfsTestData $FED_DFSMNN_TESTS_SRC_DIR $FS
}


function copyHdfsTestData {
	DEST_DIR=$1 ; DEST_FF=$2		## PARAM: valid HDFS  directory
	echo "    ## [$PROGNAME] param: DEST_DIR=$DEST_DIR; $DEST_FF=$DEST_FF"
	
	local cmd="${HADOOP_HDFS_CMD}  dfs -copyFromLocal /homes/hdfsqa/hdfsMNNData/* ${DEST_FF}${DEST_DIR}
	echo "    ## [$PROGNAME] setup: $cmd"
	eval $cmd
	TASK_EXIT_CODE="$?"
	if [ $TASK_EXIT_CODE != 0 ] ; then
		echo "    FATAL ERROR: Failed to copy data. [$cmd]. "
	fi
}


function dumpEnv {
	echo "    [$PROGNAME] Env: PROGNAME=$PROGNAME"	
	echo "    [$PROGNAME] Env: FS=$FS"	
	echo "    [$PROGNAME] Env: NNX=$NNX"	
	echo "    [$PROGNAME] Env: NN0=$NN0"	
	echo "    [$PROGNAME] Env: NN1=$NN1"	
	echo "    [$PROGNAME] Env: NN2=$NN2"	
	echo "    [$PROGNAME] Env: TESTCASE_ID=$TESTCASE_ID"	
	echo "    [$PROGNAME] Env: TESTCASE_DESC=$TESTCASE_DESC"	
	echo "    [$PROGNAME] Env: FED_DFSMNN_SLASH_DATA_DIR=$FED_DFSMNN_SLASH_DATA_DIR"	
	echo "    [$PROGNAME] Env: FED_DFSMNN_TESTS_SRC_DIR=$FED_DFSMNN_TESTS_SRC_DIR"	
	echo "    [$PROGNAME] Env: FED_DFSMNN_TESTS_DST_DIR=$FED_DFSMNN_TESTS_DST_DIR"	
	echo "    [$PROGNAME] Env: FED_DFSMNN_EXPECTED_DIR=$FED_DFSMNN_EXPECTED_DIR"	
	echo "    [$PROGNAME] Env: FED_DFSMNN_ARTIFACTS_DIR=$FED_DFSMNN_ARTIFACTS_DIR"	
	echo "    [$PROGNAME] Env: USER=$USER"				# Linux USER
	echo "    [$PROGNAME] Env: HDFS_USER=$HDFS_USER"	# HDFS USER
	echo "    [$PROGNAME] Env: FED_DFSMNN_DEFAULT_DATA_DIR=$FED_DFSMNN_DEFAULT_DATA_DIR"
	echo "    [$PROGNAME] Env: FED_DFSMNN_FS_DATA_DIR=$FED_DFSMNN_FS_DATA_DIR"	

	echo "    [$PROGNAME] Env: WORKSPACE=$WORKSPACE"	
	echo "    [$PROGNAME] Env: TMPDIR=$TMPDIR"	
	echo "    [$PROGNAME] Env: ARTIFACTS=$ARTIFACTS"	
	echo "    [$PROGNAME] Env: ARTIFACTS_DIR=$ARTIFACTS_DIR"	
	echo "    [$PROGNAME] Env: JOB=$JOB"	
	echo "    [$PROGNAME] Env: JOBS_SCRIPTS_DIR=$JOBS_SCRIPTS_DIR"	
	echo "    [$PROGNAME] Env: JOB_SCRIPTS_DIR_NAME=$JOB_SCRIPTS_DIR_NAME"	
	echo "    [$PROGNAME] Env: HADOOP_COMMON_CMD=$HADOOP_COMMON_CMD"	
        echo "    [$PROGNAME] Env: HADOOP_HDFS_CMD=$HADOOP_HDFS_CMD"
	echo "    [$PROGNAME] Env: which hadoop=`which hadoop`"
	
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -ls $FED_DFSMNN_SLASH_DATA_DIR"
	echo "    $cmd"
	eval  $cmd
}

###################################################
## Test Cases: ls and other commands which has one operand
###################################################
function test_dfs_ls {
	T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;		## PARAM
	
	TESTCASE_ID=$TID ; COMMAND_EXIT_CODE=0 ; REASONS="" ; 
	OFILE=${ARTIFACTS_DIR}/$TID
	local TMSG="dfs -${T_OP} ${TFS} ${TDIR}" 
	TESTCASE_DESC=$TMSG
	TESTCASENAME="FED_DFS_MNN_LS"
	displayTestCaseMessage "$TESTCASE_ID"
	
	echo "    #### [$PROGNAME - test_dfs] param: T_OP=$T_OP; TFS=$TFS; TID=$TID; TDIR=$TDIR; TDESC=$TDESC"

	if [ $TFS == "default" ] || [ -s "$TFS" ] ; then
		TFSDIR=$TDIR
	else
		TFSDIR="hdfs://$TFS.blue.ygrid/$TDIR"
	fi
	## RUN the command
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -${T_OP} $TFSDIR 2>/dev/null"
	echo "    #### TCMD: $cmd  > ${OFILE}.tmp"
	eval $cmd >&  ${OFILE}.tmp

	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR failed. "	
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP"

	#COMMAND_EXIT_CODE=$?	
	#displayTestCaseResult $TESTCASE_DESC
	
	echo "cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed > ${OFILE}.out"
	cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed > ${OFILE}.out

	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR fail to run sed to create ${OFILE}.out. "	
	fi

	hdftDoDiff ${OFILE}.out $FED_DFSMNN_EXPECTED_DIR/${TID}.out
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR : actual != expected results "	
	fi
	hdftShowDiffResult "$stat" "$TMSG"  "${OFILE}.out" "$FED_DFSMNN_EXPECTED_DIR/${TID}.out"

	hdftReturnTaskResult $stat  $TID "$cmd" "list "
}

###################################################
## Test Cases: chmod/chgrp and other commands which has one operand, but needs ls afterward to confirm
## Todo: confirm file/dir exist; chmod; confirm ls 
###################################################
function test_dfs_chmod {
	T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;		## PARAM
	
	TESTCASE_ID=$TID
	COMMAND_EXIT_CODE=0
	REASONS=""
	OFILE=${ARTIFACTS_DIR}/$TID
	local TMSG="dfs -${T_OP} ${TFS} ${TDIR}" 
	TESTCASE_DESC=$TMSG
	TESTCASENAME="FED_DFS_MNN_CHMOD"
	displayTestCaseMessage "$TESTCASE_ID"
	
	echo "    #### [$PROGNAME - test_dfs] param: T_OP=$T_OP; TFS=$TFS; TID=$TID; TDIR=$TDIR; TDESC=$TDESC"

	if [ $TFS == "default" ] || [ -s "$TFS" ] ; then
		TFSDIR=$TDIR
	else
		TFSDIR="hdfs://$TFS.blue.ygrid/$TDIR"
	fi

	## confirm file/dir exists
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -test -e  $TFSDIR"
	echo "    #### UCMD: $cmd"
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR - file does not exist."	
	fi

	## RUN the command
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -${T_OP} $TFSDIR"
	echo "    #### TCMD: $cmd  "
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR - op failed. "	
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP"

	## ls the file
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -ls  $TFSDIR"
	echo "    #### UCMD: $cmd  > ${OFILE}.tmp"
	eval $cmd >&  ${OFILE}.tmp

	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR - ls failed to list dir/file. "
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP: ls after op"

	#
	# sed the list, then do diff
	echo "cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed > ${OFILE}.out"
	cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed > ${OFILE}.out

	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR - fail to run sed to create ${OFILE}.out. "	
	fi

	hdftDoDiff ${OFILE}.out $FED_DFSMNN_EXPECTED_DIR/${TID}.out
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR : actual != expected results  "	
	fi
	hdftShowDiffResult "$stat" "$TMSG"  "${OFILE}.out" "$FED_DFSMNN_EXPECTED_DIR/${TID}.out"

	hdftReturnTaskResult $stat  $TID "$cmd" "list "
}



###################################################
## Test Cases: hdfs to local
## Todo: confirm HDFS file/dir exist; then do the opeation, then cehck local existence
###################################################
function test_dfs_hdfs2lo {
	T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;		## PARAM
	
	TESTCASE_ID=$TID
	COMMAND_EXIT_CODE=0
	REASONS=""
	OFILE=${ARTIFACTS_DIR}/$TID
	local TMSG="dfs -${T_OP} ${TFS} ${TDIR}" 
	TESTCASE_DESC=$TMSG
	TESTCASENAME="FED_DFS_MNN_HDFS2LOCAL"
	displayTestCaseMessage "$TESTCASE_ID"
	
	LOCAL_TFILE=$LOCAL_TMPDIR/$TID

	echo "    #### [$PROGNAME - test_dfs] param: T_OP=$T_OP; TFS=$TFS; TID=$TID; TDIR=$TDIR; TDESC=$TDESC"

	if [ $TFS == "default" ] || [ -s "$TFS" ] ; then
		TFSDIR=$TDIR
	else
		TFSDIR="hdfs://$TFS.blue.ygrid/$TDIR"
	fi

	## confirm hdfs file/dir exists
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -test -e  $TFSDIR"
	echo "    #### UCMD: $cmd"
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR - file does not exist."	
	fi

	## RUN the command, add local directory/file
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -${T_OP} $TFSDIR $LOCAL_TFILE"
	echo "    #### TCMD: $cmd  "
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR $LOCAL_TFILE - failed"	
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP"

	## ls the file
	local cmd="ls  $LOCAL_TFILE"
	echo "    #### UCMD: $cmd  > ${OFILE}.tmp"
	eval $cmd >&  ${OFILE}.tmp

	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $LOCAL_TFILE - ls of local file file"
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP : ls after op"

	#COMMAND_EXIT_CODE=$?	
	#displayTestCaseResult $TESTCASE_DESC
	
	echo "cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed   | sed -e s#$LOCAL_TMPDIR/## > ${OFILE}.out"
	cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed   | sed -e "s#$LOCAL_TMPDIR/##" > ${OFILE}.out

	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR fail to run sed to create ${OFILE}.out"	
	fi

	hdftDoDiff ${OFILE}.out $FED_DFSMNN_EXPECTED_DIR/${TID}.out
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR : actual != expected results "	
	fi
	hdftShowDiffResult "$stat" "$TMSG"  "${OFILE}.out" "$FED_DFSMNN_EXPECTED_DIR/${TID}.out"

	hdftReturnTaskResult $stat  $TID "$cmd" "list "
}

###################################################
## Test Cases: local to hdfs
## Todo: rm if hdfs file exist, do the operation, confirm HDFS file/dir exist; then do ls and compared output
## Note that this command has one extra argument then the others
## TDIR: HDFS directory to copied to; T_LOFILE: linux file/dir , copied to $TDIR/$TID
###################################################
function test_dfs_lo2hdfs {
	T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;	T_LOFILE=$6 	## PARAM
	
	TESTCASE_ID=$TID
	COMMAND_EXIT_CODE=0
	REASONS=""
	OFILE=${ARTIFACTS_DIR}/$TID
	local TMSG="dfs -${T_OP} ${TFS} ${TDIR}" 
	TESTCASE_DESC=$TMSG
	TESTCASENAME="FED_DFS_MNN_LOCAL2HDFS"
	displayTestCaseMessage "$TESTCASE_ID"
	
	if [ -z $T_LOFILE ] ; then
		if [ $T_OP == "put" ] ; then
			T_LOFILE="/homes/hdfsqa/hdfsMNNData/helloworld.txt"
		else
			T_LOFILE="/homes/hdfsqa/hdfsMNNData/wordcount_input"
		fi
	fi
	LOCAL_TFILE=$T_LOFILE


	if [ $TFS == "default" ] || [ -s "$TFS" ] ; then
		TFSDIR=$TDIR
	else
		TFSDIR="hdfs://$TFS.blue.ygrid/$TDIR"
	fi

	echo "    #### [$PROGNAME - test_dfs] param: T_OP=$T_OP; TFS=$TFS; TID=$TID; TDIR=$TDIR; TDESC=$TDESC; T_LOFILE=$T_LOFILE"
	echo "    #### [$PROGNAME - test_dfs] param: T_LOFILE=$T_LOFILE; TFSDIR=$TFSDIR"

	## if hdfs file/dir exist, remove it first
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -test -e  $TFSDIR/$TESTCASE_ID"
	echo "    #### UCMD: $cmd"
	eval $cmd 
	stat=$? ; 
	if [ $stat == 0 ]; then
		local cmd="${HADOOP_HDFS_CMD} --config $HADOOP_CONF_DIR  dfs -rm -r -skipTrash  $TFSDIR/$TESTCASE_ID"
		echo "    #### UCMD: $cmd"
		eval $cmd 
		if [ $? != 0 ] ; then
			setFailCase "dfs -${T_OP} $TFSDIR $LOCAL_TFILE - step A: failed to rm file first. "	
		fi
	fi

	## RUN the command, add local directory/file
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -${T_OP} $LOCAL_TFILE $TFSDIR/$TESTCASE_ID"
	echo "    #### TCMD: $cmd  "
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR $LOCAL_TFILE - step B: failed to copy. "	
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP"

	# now make sure file/dir exists
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -test -e  $TFSDIR/$TESTCASE_ID"
	echo "    #### UCMD: $cmd"
	eval $cmd 
	stat=$? ; 
	if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR $LOCAL_TFILE - step C: file/dir does not exist. "
	fi

	## ls the file
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -lsr  $TFSDIR/$TESTCASE_ID"
	echo "    #### UCMD: $cmd  > ${OFILE}.tmp"
	eval $cmd >&  ${OFILE}.tmp

	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR - Step D: file to dfs -lsr file/dir. "
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP : lsr after op"

	# sed  the output results
	echo "cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed   > ${OFILE}.out"
	cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed    > ${OFILE}.out
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR : step E: fail to run sed to create ${OFILE}.out. "	
	fi

	# now do the diff
	hdftDoDiff ${OFILE}.out $FED_DFSMNN_EXPECTED_DIR/${TID}.out
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR : actual != expected results "
	fi
	hdftShowDiffResult "$stat" "$TMSG"  "${OFILE}.out" "$FED_DFSMNN_EXPECTED_DIR/${TID}.out"

	hdftReturnTaskResult $stat  $TID "$cmd" "list "
}


###################################################
## Test Cases: stat and test and other commands which has one operand
###################################################
function test_dfs_stat {
	T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;		## PARAM
	TESTCASE_ID=$TID ; COMMAND_EXIT_CODE=0 ; REASONS="" ; OFILE=${ARTIFACTS_DIR}/$TID
	local TMSG="dfs -${T_OP} ${TFS} ${TDIR}" 
	TESTCASE_DESC=$TMSG
	TESTCASENAME="FED_DFS_MNN_STAT"
	displayTestCaseMessage "$TESTCASE_ID"
	echo "    #### [$PROGNAME - test_dfs] param: T_OP=$T_OP; TFS=$TFS; TID=$TID; TDIR=$TDIR; TDESC=$TDESC"

	if [ $TFS == "default" ] || [ -s "$TFS" ] ; then
		TFSDIR=$TDIR
	else
		TFSDIR="hdfs://$TFS.blue.ygrid/$TDIR"
	fi
	## RUN the command. Stat return 0 if file/directory is exist
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -${T_OP} $TFSDIR"
	echo "    #### TCMD: $cmd "
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR failed. "	
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP"

	hdftReturnTaskResult $stat  $TID "$cmd" "$TMSG "
}

###################################################
## Test Cases: -mkdir and -touchz: create file/dir
###################################################
function test_dfs_mkfile {
	T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;		## PARAM
	TESTCASE_ID=$TID ; COMMAND_EXIT_CODE=0 ; REASONS="" ; OFILE=${ARTIFACTS_DIR}/$TID
	local TMSG="dfs -${T_OP} ${TFS} ${TDIR}" 
	TESTCASE_DESC=$TMSG
	TESTCASENAME="FED_DFS_MNN_MKFILE"
	displayTestCaseMessage "$TESTCASE_ID"
	echo "    #### [$PROGNAME - test_dfs] param: T_OP=$T_OP; TFS=$TFS; TID=$TID; TDIR=$TDIR; TDESC=$TDESC"

	if [ $TFS == "default" ] || [ -s "$TFS" ] ; then
		TFSDIR=$TDIR
	else
		TFSDIR="hdfs://$TFS.blue.ygrid/$TDIR"
	fi
	## RUN the command. Stat return 0 if file/directory is exist
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -${T_OP} $TFSDIR"
	echo "    #### TCMD: $cmd "
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR failed. "	
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP : create file/dir"

	cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -test -e $TFSDIR"
	echo "    #### TCMD: $cmd "
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR failed to create file/dir. "	
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP: test -e after create"

	hdftReturnTaskResult $stat  $TID "$cmd" "$TMSG "
}

###################################################
## Test Cases: -rm and -rm -r : remove file/dir
###################################################
function test_dfs_rm {
	T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;		## PARAM
	TESTCASE_ID=$TID ; COMMAND_EXIT_CODE=0 ; REASONS="" ; OFILE=${ARTIFACTS_DIR}/$TID
	local TMSG="dfs -${T_OP} ${TFS} ${TDIR}" 
	TESTCASE_DESC=$TMSG
	TESTCASENAME="FED_DFS_MNN_RM"
	displayTestCaseMessage "$TESTCASE_ID"
	echo "    #### [$PROGNAME - test_dfs] param: T_OP=$T_OP; TFS=$TFS; TID=$TID; TDIR=$TDIR; TDESC=$TDESC"

	if [ $TFS == "default" ] || [ -s "$TFS" ] ; then
		TFSDIR=$TDIR
	else
		TFSDIR="hdfs://$TFS.blue.ygrid/$TDIR"
	fi
	# first, make sure the file/directory exists
	cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -test -e $TFSDIR"
	echo "    #### TCMD: $cmd "
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR does not exist. "
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP : test -e to verify file/dir should exist"

	## Now run the command. Stat return 0 if file/directory is exist
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -${T_OP} $TFSDIR"
	echo "    #### TCMD: $cmd "
	eval $cmd 
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR failed. "	
	fi
	hdftShowExecResult "$stat" "$TMSG" "$T_OP : the rm operation"

	# first, make sure the file/directory does notexist
	cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -test -e $TFSDIR"
	echo "    #### TCMD: $cmd "
	eval $cmd 
	
	# now expect return stat to be non-zero to indicate the file should not exist. Can use hdftMaptStatus
	stat=$? ; if [ $stat == 0 ]; then
		setFailCase "dfs -${T_OP} $TFSDIR still exist. Failed to delete. "
		stat1=1
	else
		stat1=0
	fi
	hdftShowExecResult "$stat1" "$TMSG" "$T_OP : test -e after rm to verify file/dir not exist"

	hdftReturnTaskResult $stat1  $TID "$cmd" "$TMSG "
}

TIDENT="FED_DFS"		# Ident of this entire set of tests FED_DFS_100, _101, ...
TCASE_TYPE="POS"		# POS or NEG. Stateful. Normally set to POS. If want to do negative test, toggle to NEG, and the toggle back to POS.


function test_dfs {
	#                 op          id +$1                    NS=$2     Directory/file=$3 	comment=$4


#XXX#return
	test_dfs_ls 	"ls"      "${TIDENT}_$(($1 + 10))"  "$2"        "$3"           		"$4"
	test_dfs_ls 	"lsr"     "${TIDENT}_$(($1 + 11))"  "$2"        "$3"           		"$4"
	test_dfs_ls 	"du"      "${TIDENT}_$(($1 + 12))"  "$2"        "$3"           		"$4"
	test_dfs_ls 	"dus"     "${TIDENT}_$(($1 + 13))"  "$2"        "$3"           		"$4"
	test_dfs_ls 	"count"   "${TIDENT}_$(($1 + 14))"  "$2"        "$3"           		"$4"
	test_dfs_ls 	"cat"     "${TIDENT}_$(($1 + 15))"  "$2"        "$3/helloworld.txt" "$4"
	test_dfs_ls 	"tail"    "${TIDENT}_$(($1 + 16))"  "$2"        "$3/wordcount_input/IndustrialDecisionMaking.txt" "$4"
	test_dfs_ls 	"text"    "${TIDENT}_$(($1 + 17))"  "$2"        "$3/helloworld.txt" "$4"

	test_dfs_stat 	"stat"    "${TIDENT}_$(($1 + 20))"  "$2"        "$3"           		"$4"
	test_dfs_stat 	"stat"    "${TIDENT}_$(($1 + 21))"  "$2"        "$3/helloworld.txt" "$4"
	test_dfs_stat 	"test -d" "${TIDENT}_$(($1 + 22))"  "$2"        "$3"           		"$4"
	test_dfs_stat 	"test -e" "${TIDENT}_$(($1 + 23))"  "$2"        "$3/helloworld.txt" "$4"

	test_dfs_mkfile "mkdir -p"   "${TIDENT}_$(($1 + 30))"  "$2"        "$3/../tmpOut/Dir1"           		"$4"
	test_dfs_mkfile "touchz"  "${TIDENT}_$(($1 + 31))"  "$2"        "$3/../tmpOut/Dir1/file1"       	"$4"
	test_dfs_stat 	"test -z" "${TIDENT}_$(($1 + 32))"  "$2"        "$3/../tmpOut/Dir1/file1"        	"$4"
	test_dfs_chmod 	"chmod 755"   "${TIDENT}_$(($1 + 34))"  "$2"    "$3/../tmpOut/Dir1/file1"        	"$4"
	test_dfs_chmod 	"chgrp users" "${TIDENT}_$(($1 + 35))"  "$2"    "$3/../tmpOut/Dir1/file1"        	"$4"

	test_dfs_mkfile "touchz"		"${TIDENT}_$(($1 + 31))"  "$2"        "$3/../tmpOut/Dir1/file2"      		"$4"
	test_dfs_rm 	"rm"      		"${TIDENT}_$(($1 + 40))"  "$2"        "$3/../tmpOut/Dir1/file2"         	"$4"
	test_dfs_rm 	"rm -r"     		"${TIDENT}_$(($1 + 41))"  "$2"        "$3/../tmpOut/Dir1/"               	"$4"

	test_dfs_hdfs2lo "get"         "${TIDENT}_$(($1 + 50))"  "$2"    "$3/helloworld.txt"  	"$4"
	test_dfs_hdfs2lo "copyToLocal" "${TIDENT}_$(($1 + 51))"  "$2"    "$3/wordcount_input" 	"$4"
	test_dfs_hdfs2lo "copyToLocal" "${TIDENT}_$(($1 + 52))"  "$2"    "$3/helloworld.txt" 	"$4"

	test_dfs_lo2hdfs "put"			"${TIDENT}_$(($1 + 55))"  "$2"   	"$3/../tmpOut"     	"$4"
	test_dfs_lo2hdfs "put"			"${TIDENT}_$(($1 + 56))"  "$2"   	"$3/../tmpOut"     	"$4"	"/homes/hdfsqa/hdfsMNNData/distcache_input/jar1.jar"
	test_dfs_lo2hdfs "copyFromLocal" "${TIDENT}_$(($1 + 57))"  "$2" "$3/../tmpOut"     	"$4"
	test_dfs_lo2hdfs "copyFromLocal" "${TIDENT}_$(($1 + 58))"  "$2" "$3/../tmpOut"     	"$4"	"/homes/hdfsqa/hdfsMNNData/har_input/"
	test_dfs_lo2hdfs "copyFromLocal" "${TIDENT}_$(($1 + 59))"  "$2" "$3/../tmpOut"     	"$4"	"/homes/hdfsqa/hdfsMNNData/helloworld.txt"
}

function test1 {
	hdftDisplayTestGroupMsg "Test DFS operations across hdfs in NNs"
	#            $1     $2              $3                           $4       
	test_dfs 	"100"  "default" 	"$FED_DFSMNN_DEFAULT_DATA_DIR"  "dfs"   
#XXX#return
	test_dfs 	"200"  "default" 	"$FED_DFSMNN_SLASH_DATA_DIR" 	"dfs"
	test_dfs 	"300"  "$NN0" 		"$FED_DFSMNN_SLASH_DATA_DIR"	"dfs"
	test_dfs 	"400"  "$NN1" 		"$FED_DFSMNN_SLASH_DATA_DIR"	"dfs"
	#the above test should be enough to test multiple namenode
	#test_dfs 	"500"  "$NN2" 		"$FED_DFSMNN_SLASH_DATA_DIR"	"dfs"
}

################################################
## MAIN
################################################
echo "ENTER MY TEST"
echo "ENTER MY TEST"
echo "ENTER MY TEST"
echo "ENTER MY TEST"
setup
setupData
dumpEnv
test1

teardown
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE

