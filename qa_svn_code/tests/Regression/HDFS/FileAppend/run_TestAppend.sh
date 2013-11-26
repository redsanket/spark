
###################################################
## Exercise File Read/Write/Append/Hflush
## uses a self-build TAppend.jar to do the acutal file read/write/append/hflush
## About 100 test cases in this suite.
###################################################
source $WORKSPACE/lib/library.sh
source $WORKSPACE/lib/user_kerb_lib.sh
source $WORKSPACE/lib/hdft_util2.sh

TAPPEND_FULL_RUN=0		# full production run, last several hours. 

###################################################
## SETUP & Teardown
###################################################
function setup {
	APPEND_OUTDIR=/tmp/M
	TDIR=${APPEND_OUTDIR}
	TEST_APPEND_JAR=$WORKSPACE/lib/Hadoop/Java/TAppend.jar
	TEST_APPEND_ENTRY=TAppend

	OWNER="cwchung"
	USER=$HADOOPQA_USER
	HDFS_USER=$HADOOPQA_USER
	MY_HDFS_CMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR"
	MY_HADOOP_CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR"
	setKerberosTicketForUser $HADOOPQA_USER
	export PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH
	umask 0002		# create 775 dir/files

	#export  COMMAND_EXIT_CODE=0
	#export TESTCASE_DESC="Terasort application read/write across multiple namenodes within one cluster"
	#export REASONS="    ####"

	#global, return to framework
	SCRIPT_EXIT_CODE=0
	NUM_TESTCASE=0
	NUM_TESTCASE_PASSED=0
	TASK_EXIT_CODE=0
	
	# module-global
	PROGNAME=`basename $0`
	DFS_COMMAND=`basename "$HADOOP_COMMON_CMD"`

	# gsbl90772.blue.ygrid.yahoo.com
	NNX=$(getDefaultNameNode)

	#get the default FS: hdfs://gsbl.90772.blue.ygrid.yahoo.com:8020
	FS=$(getDefaultFS)

	# gsbl90772
	NN0=$(hdft_getNN 0)
	NN1=$(hdft_getNN 1)
	NN2=$(hdft_getNN 2)

	JOB_OPTION="-Dmapreduce.job.hdfs-servers=hdfs://$NN0.blue.ygrid:8020,hdfs://$NN1.blue.ygrid:8020,hdfs://$NN2.blue.ygrid:8020 -Djob.mapred.queuename=grideng  -Dmapred.job.numMapTasks=100 -Dmapreduce.job.numReduceTasks=20"
	

	# for different URI reference
	
	APPEND_EXPECTED_DIR="$JOB_SCRIPTS_DIR_NAME/Expected"
	APPEND_ARTIFACTS_DIR="$ARTIFACTS_DIR"
	LOCAL_TMPDIR=/tmp/L1.${CLUSTER}.${USER}
}

function lecho {
	echo "$PROGNAME: $*"
}
function decho {
	CALLER=`caller 0`
	echo "$PROGNAME::$CALLER:: $*"
}

function teardown {
	# deleteHdfsDir $FED_APPMNN_SLASH_DATA_DIR $FS
	lecho "TearDown "
#	${MY_HDFS_CMD} dfs -rm -r -skipTrash $APPEND_OUTDIR
	local NAMENODES=`getNameNodes`	# Get list of namenodes
	NAMENODES=`echo $NAMENODES | tr ';' ' '`
	for nn in $NAMENODES; do
		echo "Delete files from $nn"
		deleteHdfsDir $APPEND_OUTDIR hdfs://$nn 0
	done		
}

function sanityCheck {
	echo "SanityCheck"
}

function setupData {
	TESTCASENAME="SETUP"
	COMMAND_EXIT_CODE=0

	lecho "Set up of Append"
	local cmd="${MY_HDFS_CMD} dfs -test -d $APPEND_OUTDIR"
	lecho "TCMD = $cmd"
	eval $cmd
	if [ $? == 0 ] ; then
		lecho "Output directory $APPEND_OUTDIR already exist."
	else
		${MY_HDFS_CMD} dfs -mkdir -p $APPEND_OUTDIR
		${MY_HDFS_CMD} dfs -chmod 774 $APPEND_OUTDIR
		lecho "Created new Output directory $APPEND_OUTDIR"
	fi

	# sanity check. Abort if cannot write to this directory
	cmd="${HADOOP_COMMON_CMD}  fs -rmr  -skipTrash  $APPEND_OUTDIR"
	eval $cmd
	stat=$?

	cmd="${HADOOP_COMMON_CMD}  fs -mkdir -p $APPEND_OUTDIR"
	eval $cmd
	stat0=$?  ; (( stat = $stat + $stat0 )) ; 

	cmd="${HADOOP_COMMON_CMD}  fs -copyFromLocal  /homes/hdfsqa/hdfsMNNData/*  $APPEND_OUTDIR"
	echo "    #### UCMD= $cmd"
	eval $cmd
	stat0=$? ; (( stat = $stat + $stat0 ))

	hdftReturnTaskResult $stat "000"  "SETUP" "Append "
	if [ $stat != 0 ] ; then
		echo "FATAL ERROR in set up data for File APPEND test [$cmd]. Test aborted."
		hdftAbortTest "RWA_SETUP"
	fi

}

# need to creatre the files to be appended
function setupAppendFiles {
   # create the files for AppendTest
    for F in 2 3 4 5 6 7 8 ; do
		hadoop jar $TEST_APPEND_JAR $TEST_APPEND_ENTRY -create  -n 1 -s 888 -f ${APPEND_OUTDIR}/Z12${F}
		hadoop jar $TEST_APPEND_JAR $TEST_APPEND_ENTRY -create  -n 1 -s 888 -f ${APPEND_OUTDIR}/Z13${F}
		hadoop jar $TEST_APPEND_JAR $TEST_APPEND_ENTRY -create  -n 1 -s 888 -f ${APPEND_OUTDIR}/Z90${F}
    done
    hdfs dfs -ls ${APPEND_OUTDIR}
}


function dumpEnv {
	echo "    [$PROGNAME] Env: PROGNAME=$PROGNAME"	
	echo "    [$PROGNAME] Env: APPEND_OUTDIR=$APPEND_OUTDIR"	
	echo "    [$PROGNAME] Env: FS=$FS"	
	echo "    [$PROGNAME] Env: NNX=$NNX."	
	echo "    [$PROGNAME] Env: NN0=$NN0."	
	echo "    [$PROGNAME] Env: NN1=$NN1."	
	echo "    [$PROGNAME] Env: NN2=$NN2."	
	echo "    [$PROGNAME] Env: TESTCASE_ID=$TESTCASE_ID"	
	echo "    [$PROGNAME] Env: TESTCASE_DESC=$TESTCASE_DESC"	
	echo "    [$PROGNAME] Env: APPEND_EXPECTED_DIR=$FED_APPMNN_EXPECTED_DIR"	
	echo "    [$PROGNAME] Env: APPEND_ARTIFACTS_DIR=$FED_APPMNN_ARTIFACTS_DIR"	
	echo "    [$PROGNAME] Env: USER=$USER"				# Linux USER
	echo "    [$PROGNAME] Env: HDFS_USER=$HDFS_USER"	# HDFS USER

	echo "    [$PROGNAME] Env: WORKSPACE=$WORKSPACE"	
	echo "    [$PROGNAME] Env: TMPDIR=$TMPDIR"	
	echo "    [$PROGNAME] Env: ARTIFACTS=$ARTIFACTS"	DOOP_HDFS_CMD} 
	echo "    [$PROGNAME] Env: ARTIFACTS_DIR=$ARTIFACTS_DIR"	
	echo "    [$PROGNAME] Env: JOB=$JOB"	
	echo "    [$PROGNAME] Env: JOBS_SCRIPTS_DIR=$JOBS_SCRIPTS_DIR"	
	echo "    [$PROGNAME] Env: JOB_SCRIPTS_DIR_NAME=$JOB_SCRIPTS_DIR_NAME"	
	echo "    [$PROGNAME] Env: HADOOP_COMMON_CMD=$HADOOP_COMMON_CMD"	
	echo "    [$PROGNAME] Env: which hadoop=`which hadoop`"
	
	local cmd="${MY_HDFS_CMD}  dfs -ls $APPEND_OUTDIR"
	echo "    $cmd"
	eval  $cmd
}


TIDENT="RWA_"		# Read/Write/Append
TCASE_TYPE="POS"	# POS or NEG. Stateful. Normally set to POS. If want to do negative test, toggle to NEG, and the toggle back to POS.

function doAppend {
	local TID="$TIDENT_${1}"
	local TOPTIONS=$2
	local TFNAME=$3
	TESTCASENAME="$TIDENT_$TID"
	TESTCASE_DESC="$4 [$TOPTIONS]"

	COMMAND_EXIT_CODE=0
	
	echo "Start test $TID  `date` "
	displayTestCaseMessage "$TID ==== $TESTCASE_DESC"

	local StartTime=`date +%s`
	cmd="hadoop jar $TEST_APPEND_JAR $TEST_APPEND_ENTRY $TOPTIONS -f $TFNAME "

	#echo "##############################################################"
	hdfs dfs -ls $TFNAME
	echo "Test Case $1 TCMD = [$cmd]"
	eval $cmd
	stat=$?

	local EndTime=`date +%s`
	local ElpasedTime
	(( ElapsedTime = EndTime - StartTime ))

	if [ $stat == 0 ] ; then
		# echo "[PASS] ---------- STATUS: Test Case $1 OK. (cmd = $cmd).  Took $ElapsedTime seconds."
		hdftReturnTaskResult 0  $TID "$cmd" "Append "
	else
		setFailCase "FAILED in Append $TID . [$cmd]"
		# echo "[FAIL] ---------- STATUS: Test Case $1 BAD. (cmd = $cmd).  Tool $ElapsedTime seconds."
		hdftReturnTaskResult $stat $TID "$cmd" "Append "
	fi
	hdfs dfs -ls $TFNAME
}
#========================================================

function test001_Basic {
	# same as Z101, but exercise the append mode
	doAppend "105" "-wo -n 6 -s 100000 -p 1000" "${TDIR}/Z105"
	
	doAppend "106" "-wo -append -n 6 -s 100000 -p 1000"  "${TDIR}/Z105"

}

# This test just write only and read
# test conditions: larger size of blocks (e.g. 10MB each), do it for 100 to 300 times
# read: always read fully, and reopen file
# variation: writer do hflush, reader read till visible
# variation: writer do hflush, reader read pass visible
# variation: writer do not do hflush:  reader read till visible
# variation: writer do not do hflush:  reader read pass visible
function test900_Bug  {
	hadoop dfs -mkdir -p ${TDIR}
	hadoop dfs -put /etc/group ${TDIR}/Z901
	hadoop dfs -put /etc/group ${TDIR}/Z902
	hadoop dfs -put /etc/group ${TDIR}/Z903
	doAppend 	"901"	"-useFC -append  -alwaysHflush -wo -n 200  -verbose -s 10000000 -p 1000"  "${TDIR}/Z901"  
	doAppend 	"902"	"-useFC -append  -alwaysHflush -wo -n 200  -verbose -s 10000000 -p 1000"  "${TDIR}/Z901"  
	date
	doAppend 	"903"	"-useFC -append  -noHflush -wo -n 200 -verbose  -s 10000000 -p 1000"  "${TDIR}/Z903"  
	doAppend 	"903"	"-useFC -append  -noHflush -wo -n 200 -verbose  -s 10000000 -p 1000"  "${TDIR}/Z903"  
}


# Use FileSystem in this set of test
function test101_CreateFC_RWThread {
	doAppend 	"100" 	"-qt " 	"${TDIR}/Z100"		"Quick Test "

	# read and write 6 chunks of 1KB each. 
	# Writer Hflush on chunk 0, 2, 4, and no hflush on 1, 3, 5. 
	# Reader is in the same thread and verify the correct number of visible bytes by open and reopen the file
	
	# set of test that do both read after write (same thread) to compare the length of a file
	doAppend 	"101"	"-useFC -rw -n 10 -s 1000 -p 1000"  "${TDIR}/Z101"          "Basic RW use FC"
	doAppend 	"102"	"-useFC -rw -n 10 -s 10000 -p 1000"  "${TDIR}/Z102"			"Basic RW use FC"
	doAppend 	"103"	"-useFC -rw -n 10 -s 100000 -p 1000"  "${TDIR}/Z103"	    "Basic RW use FC"
	doAppend 	"104"	"-useFC -rw -n 10 -s 1000000 -p 1000"  "${TDIR}/Z104"	    "Basic RW use FC"

	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"105"	"-useFC -rw -n 10 -s 10000000 -p 1000"  "${TDIR}/Z105"	    "Basic RW use FC"
		doAppend 	"106"	"-useFC -rw -n 10 -s 100000000 -p 1000"  "${TDIR}/Z106"	    "Basic RW use FC"
		doAppend 	"107"	"-useFC -rw -n 10 -s 200000000 -p 1000"  "${TDIR}/Z107"	    "Basic RW use FC"
		doAppend 	"108"	"-useFC -rw -n 10 -s 500000000 -p 1000"  "${TDIR}/Z108"	    "Basic RW use FC"
		#doAppend 	"109"	"-useFC -rw -n 10 -s 5000000000 -p 1000"  "${TDIR}/Z109"    "Basic RW use FC"
	fi
}

function test111_CreateFC_RWThread {


	# set of test that do both read after write (different thread)  to compare the length of a file

	doAppend 	"112"	"-useFC -rw -n 10 -s 10000 -p 1000 -nrt"  "${TDIR}/Z112"       "Basic RW use FC Thread"
	doAppend 	"113"	"-useFC -rw -n 10 -s 100000 -p 1000 -nrt"  "${TDIR}/Z113"      "Basic RW use FC Thread"
	doAppend 	"114"	"-useFC -rw -n 10 -s 1000000 -p 1000 -nrt"  "${TDIR}/Z114"     "Basic RW use FC Thread"

	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"115"	"-useFC -rw -n 10 -s 10000000 -p 1000 -nrt"  "${TDIR}/Z115"    "Basic RW use FC Thread"
		doAppend 	"116"	"-useFC -rw -n 10 -s 100000000 -p 1000 -nrt"  "${TDIR}/Z116"   "Basic RW use FC Thread"
		doAppend 	"117"	"-useFC -rw -n 10 -s 200000000 -p 1000 -nrt"  "${TDIR}/Z117"   "Basic RW use FC Thread"
		# doAppend 	"118"	"-useFC -rw -n 10 -s 500000000 -p 1000 -nrt"  "${TDIR}/Z118"   "Basic RW use FC Thread"
		#doAppend 	"119"	"-useFC -rw -n 10 -s 5000000000 -p 1000 -nrt"  "${TDIR}/Z119"  "Basic RW use FC Thread"
	fi
}

function test401_CreateFC_RW_PosRead_NonFully_Many {

	# set of test that do both read after write (different thread)  amd to do many iterations

	doAppend 	"401"	"-useFC -rw -n 9         -s 100  -p 100  "  "${TDIR}/Z401"       "Basic RW use FC, PosRead, 100B Chunks, many times"
	doAppend 	"402"	"-useFC -rw -n 100       -s 100  -p 100  "  "${TDIR}/Z402"       "Basic RW use FC, PosRead, 100B chunks, many times"

	doAppend 	"411"	"-useFC -rw -n 9         -s 1000 -p 100  "  "${TDIR}/Z411"       "Basic RW use FC, PosRead, 1K  chunks, many times"
	doAppend 	"412"	"-useFC -rw -n 100       -s 1000 -p 100  "  "${TDIR}/Z412"       "Basic RW use FC, PosRead, 1K  chunks, many times"

	doAppend 	"421"	"-useFC -rw -n 9         -s 10000 -p 100  "  "${TDIR}/Z421"       "Basic RW use FC, PosRead, 10K chunks, many times"
	doAppend 	"422"	"-useFC -rw -n 100       -s 10000 -p 100  "  "${TDIR}/Z422"       "Basic RW use FC, PosRead, 10K chunks, many times"

	doAppend 	"431"	"-useFC -rw -n 9         -s 10000 -p 100  "  "${TDIR}/Z431"       "Basic RW use FC, PosRead, 100K chunks, many times"
	doAppend 	"432"	"-useFC -rw -n 100       -s 10000 -p 100  "  "${TDIR}/Z432"       "Basic RW use FC, PosRead, 100K chunks, many times"

	doAppend 	"441"	"-useFC -rw -n 9         -s 1000000 -p 100  "  "${TDIR}/Z441"     "Basic RW use FC, PosRead, 1MB chunks, many times"

	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"403"	"-useFC -rw -n 1000      -s 100  -p 100  "  "${TDIR}/Z403"       "Basic RW use FC, PosRead, 100B chunks, many times"
		doAppend 	"404"	"-useFC -rw -n 10000     -s 100  -p 100  "  "${TDIR}/Z404"       "Basic RW use FC, PosRead, 100B chunks, many times"
		doAppend 	"413"	"-useFC -rw -n 1000      -s 1000 -p 100  "  "${TDIR}/Z413"       "Basic RW use FC, PosRead, 1K  chunks, many times"
		doAppend 	"414"	"-useFC -rw -n 10000     -s 1000 -p 100  "  "${TDIR}/Z414"       "Basic RW use FC, PosRead, 1K  chunks, many times"
		doAppend 	"423"	"-useFC -rw -n 1000      -s 10000 -p 100  "  "${TDIR}/Z423"       "Basic RW use FC, PosRead, 10K chunks, many times"
		doAppend 	"424"	"-useFC -rw -n 10000     -s 10000 -p 100  "  "${TDIR}/Z424"       "Basic RW use FC, PosRead, 10K chunks, many times"
		doAppend 	"433"	"-useFC -rw -n 1000      -s 10000 -p 100  "  "${TDIR}/Z433"       "Basic RW use FC, PosRead, 100K chunks, many times"
		doAppend 	"434"	"-useFC -rw -n 10000     -s 10000 -p 100  "  "${TDIR}/Z434"       "Basic RW use FC, PosRead, 100K chunks, many times"

		doAppend 	"442"	"-useFC -rw -n 100       -s 1000000 -p 100  "  "${TDIR}/Z442"     "Basic RW use FC, PosRead, 1MB chunks, many times"
		doAppend 	"443"	"-useFC -rw -n 1000      -s 1000000 -p 100  "  "${TDIR}/Z443"     "Basic RW use FC, PosRead, 1MB chunks, many times"
		doAppend 	"444"	"-useFC -rw -n 10000     -s 1000000 -p 100  "  "${TDIR}/Z444"     "Basic RW use FC, PosRead, 1MB chunks, many times"
	fi
}

function test451_CreateFC_RW_PosRead_ManyFully {

	# set of test that do both read after write (different thread)  amd to do many iterations

	doAppend 	"451"	"-useFC -readFully -rw -n 9         -s 100  -p 100  "  "${TDIR}/Z451"       "Basic RW use FC, PosRead, 100B Chunks, many times"
	doAppend 	"452"	"-useFC -readFully -rw -n 100       -s 100  -p 100  "  "${TDIR}/Z452"       "Basic RW use FC, PosRead, 100B chunks, many times"

	doAppend 	"461"	"-useFC -readFully -rw -n 9         -s 1000 -p 100  "  "${TDIR}/Z461"       "Basic RW use FC, PosRead, 1K  chunks, many times"
	doAppend 	"462"	"-useFC -readFully -rw -n 100       -s 1000 -p 100  "  "${TDIR}/Z462"       "Basic RW use FC, PosRead, 1K  chunks, many times"

	doAppend 	"471"	"-useFC -readFully -rw -n 9         -s 10000 -p 100  "  "${TDIR}/Z471"       "Basic RW use FC, PosRead, 10K chunks, many times"
	doAppend 	"472"	"-useFC -readFully -rw -n 100       -s 10000 -p 100  "  "${TDIR}/Z472"       "Basic RW use FC, PosRead, 10K chunks, many times"

	doAppend 	"481"	"-useFC -readFully -rw -n 9         -s 10000 -p 100  "  "${TDIR}/Z481"       "Basic RW use FC, PosRead, 100K chunks, many times"
	doAppend 	"482"	"-useFC -readFully -rw -n 100       -s 10000 -p 100  "  "${TDIR}/Z482"       "Basic RW use FC, PosRead, 100K chunks, many times"

	doAppend 	"496"	"-useFC -readFully -rw -n 9         -s 1000000 -p 100  "  "${TDIR}/Z496"     "Basic RW use FC, 1MB chunks, many times"

	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"453"	"-useFC -readFully -rw -n 1000      -s 100  -p 100  "  "${TDIR}/Z453"       "Basic RW use FC, PosRead, 100B chunks, many times"
		doAppend 	"454"	"-useFC -readFully -rw -n 10000     -s 100  -p 100  "  "${TDIR}/Z454"       "Basic RW use FC, PosRead, 100B chunks, many times"
		doAppend 	"463"	"-useFC -readFully -rw -n 1000      -s 1000 -p 100  "  "${TDIR}/Z463"       "Basic RW use FC, PosRead, 1K  chunks, many times"
		doAppend 	"464"	"-useFC -readFully -rw -n 10000     -s 1000 -p 100  "  "${TDIR}/Z464"       "Basic RW use FC, PosRead, 1K  chunks, many times"
		doAppend 	"473"	"-useFC -readFully -rw -n 1000      -s 10000 -p 100  "  "${TDIR}/Z473"       "Basic RW use FC, PosRead, 10K chunks, many times"
		doAppend 	"474"	"-useFC -readFully -rw -n 10000     -s 10000 -p 100  "  "${TDIR}/Z474"       "Basic RW use FC, PosRead, 10K chunks, many times"
		doAppend 	"483"	"-useFC -readFully -rw -n 1000      -s 10000 -p 100  "  "${TDIR}/Z483"       "Basic RW use FC, PosRead, 100K chunks, many times"
		doAppend 	"484"	"-useFC -readFully -rw -n 10000     -s 10000 -p 100  "  "${TDIR}/Z484"       "Basic RW use FC, PosRead, 100K chunks, many times"

		doAppend 	"497"	"-useFC -readFully -rw -n 100       -s 1000000 -p 100  "  "${TDIR}/Z497"     "Basic RW use FC, 1MB chunks, many times"
		doAppend 	"498"	"-useFC -readFully -rw -n 1000      -s 1000000 -p 100  "  "${TDIR}/Z498"     "Basic RW use FC, 1MB chunks, many times"
		doAppend 	"494"	"-useFC -readFully -rw -n 10000     -s 1000000 -p 100  "  "${TDIR}/Z494"     "Basic RW use FC, 1MB chunks, many times"
	fi
}

function test501_CreateFC_RW_SeqRead_NonFully_Many {

	# set of test that do both read after write (different thread)  amd to do many iterations

	doAppend 	"501"	"-useFC -seqRead -rw -n 9         -s 100  -p 100  "  "${TDIR}/Z501"       "Basic RW use FC, SeqRead, 100B Chunks, many times"
	doAppend 	"502"	"-useFC -seqRead -rw -n 100       -s 100  -p 100  "  "${TDIR}/Z502"       "Basic RW use FC, SeqRead, 100B chunks, many times"

	doAppend 	"511"	"-useFC -seqRead -rw -n 9         -s 1000 -p 100  "  "${TDIR}/Z511"       "Basic RW use FC, SeqRead, 1K  chunks, many times"
	doAppend 	"512"	"-useFC -seqRead -rw -n 100       -s 1000 -p 100  "  "${TDIR}/Z512"       "Basic RW use FC, SeqRead, 1K  chunks, many times"

	doAppend 	"521"	"-useFC -seqRead -rw -n 9         -s 10000 -p 100  "  "${TDIR}/Z521"       "Basic RW use FC, SeqRead, 10K chunks, many times"
	doAppend 	"522"	"-useFC -seqRead -rw -n 100       -s 10000 -p 100  "  "${TDIR}/Z522"       "Basic RW use FC, SeqRead, 10K chunks, many times"

	doAppend 	"531"	"-useFC -seqRead -rw -n 9         -s 10000 -p 100  "  "${TDIR}/Z531"       "Basic RW use FC, SeqRead, 100K chunks, many times"
	doAppend 	"532"	"-useFC -seqRead -rw -n 100       -s 10000 -p 100  "  "${TDIR}/Z532"       "Basic RW use FC, SeqRead, 100K chunks, many times"

	doAppend 	"541"	"-useFC -seqRead -rw -n 9         -s 1000000 -p 100  "  "${TDIR}/Z541"     "Basic RW use FC, SeqRead, 1MB chunks, many times"
	doAppend 	"542"	"-useFC -seqRead -rw -n 100       -s 1000000 -p 100  "  "${TDIR}/Z542"     "Basic RW use FC, SeqRead, 1MB chunks, many times"

	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"503"	"-useFC -seqRead -rw -n 1000      -s 100  -p 100  "  "${TDIR}/Z503"       "Basic RW use FC, SeqRead, 100B chunks, many times"
		doAppend 	"504"	"-useFC -seqRead -rw -n 10000     -s 100  -p 100  "  "${TDIR}/Z504"       "Basic RW use FC, SeqRead, 100B chunks, many times"
		doAppend 	"513"	"-useFC -seqRead -rw -n 1000      -s 1000 -p 100  "  "${TDIR}/Z513"       "Basic RW use FC, SeqRead, 1K  chunks, many times"
		doAppend 	"514"	"-useFC -seqRead -rw -n 10000     -s 1000 -p 100  "  "${TDIR}/Z514"       "Basic RW use FC, SeqRead, 1K  chunks, many times"
		doAppend 	"523"	"-useFC -seqRead -rw -n 1000      -s 10000 -p 100  "  "${TDIR}/Z523"       "Basic RW use FC, SeqRead, 10K chunks, many times"
		doAppend 	"524"	"-useFC -seqRead -rw -n 10000     -s 10000 -p 100  "  "${TDIR}/Z524"       "Basic RW use FC, SeqRead, 10K chunks, many times"
		doAppend 	"533"	"-useFC -seqRead -rw -n 1000      -s 10000 -p 100  "  "${TDIR}/Z533"       "Basic RW use FC, SeqRead, 100K chunks, many times"
		doAppend 	"534"	"-useFC -seqRead -rw -n 10000     -s 10000 -p 100  "  "${TDIR}/Z534"       "Basic RW use FC, SeqRead, 100K chunks, many times"

		doAppend 	"543"	"-useFC -seqRead -rw -n 1000      -s 1000000 -p 100  "  "${TDIR}/Z543"     "Basic RW use FC, SeqRead, 1MB chunks, many times"
		doAppend 	"544"	"-useFC -seqRead -rw -n 10000     -s 1000000 -p 100  "  "${TDIR}/Z544"     "Basic RW use FC, SeqRead, 1MB chunks, many times"
	fi
}

function test551_CreateFC_RW_SeqRead_ManyFully {

	# set of test that do both read after write (different thread)  amd to do many iterations

	doAppend 	"551"	"-useFC -readFully -seqRead -rw -n 9         -s 100  -p 100  "  "${TDIR}/Z551"       "Basic RW use FC, SeqRead, 100B Chunks, many times"
	doAppend 	"552"	"-useFC -readFully -seqRead -rw -n 100       -s 100  -p 100  "  "${TDIR}/Z552"       "Basic RW use FC, SeqRead, 100B chunks, many times"

	doAppend 	"561"	"-useFC -readFully -seqRead -rw -n 9         -s 1000 -p 100  "  "${TDIR}/Z561"       "Basic RW use FC, SeqRead, 1K  chunks, many times"
	doAppend 	"562"	"-useFC -readFully -seqRead -rw -n 100       -s 1000 -p 100  "  "${TDIR}/Z562"       "Basic RW use FC, SeqRead, 1K  chunks, many times"

	doAppend 	"571"	"-useFC -readFully -seqRead -rw -n 9         -s 10000 -p 100  "  "${TDIR}/Z571"       "Basic RW use FC, SeqRead, 10K chunks, many times"
	doAppend 	"572"	"-useFC -readFully -seqRead -rw -n 100       -s 10000 -p 100  "  "${TDIR}/Z572"       "Basic RW use FC, SeqRead, 10K chunks, many times"

	doAppend 	"581"	"-useFC -readFully -seqRead -rw -n 9         -s 10000 -p 100  "  "${TDIR}/Z581"       "Basic RW use FC, SeqRead, 100K chunks, many times"
	doAppend 	"582"	"-useFC -readFully -seqRead -rw -n 100       -s 10000 -p 100  "  "${TDIR}/Z582"       "Basic RW use FC, SeqRead, 100K chunks, many times"

	doAppend 	"596"	"-useFC -readFully -seqRead -rw -n 9         -s 1000000 -p 100  "  "${TDIR}/Z596"     "Basic RW use FC, 1MB chunks, many times"
	doAppend 	"597"	"-useFC -readFully -seqRead -rw -n 100       -s 1000000 -p 100  "  "${TDIR}/Z597"     "Basic RW use FC, 1MB chunks, many times"

	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"553"	"-useFC -readFully -seqRead -rw -n 1000      -s 100  -p 100  "  "${TDIR}/Z553"       "Basic RW use FC, SeqRead, 100B chunks, many times"
		doAppend 	"554"	"-useFC -readFully -seqRead -rw -n 10000     -s 100  -p 100  "  "${TDIR}/Z554"       "Basic RW use FC, SeqRead, 100B chunks, many times"
		doAppend 	"563"	"-useFC -readFully -seqRead -rw -n 1000      -s 1000 -p 100  "  "${TDIR}/Z563"       "Basic RW use FC, SeqRead, 1K  chunks, many times"
		doAppend 	"564"	"-useFC -readFully -seqRead -rw -n 10000     -s 1000 -p 100  "  "${TDIR}/Z564"       "Basic RW use FC, SeqRead, 1K  chunks, many times"
		doAppend 	"573"	"-useFC -readFully -seqRead -rw -n 1000      -s 10000 -p 100  "  "${TDIR}/Z573"       "Basic RW use FC, SeqRead, 10K chunks, many times"
		doAppend 	"574"	"-useFC -readFully -seqRead -rw -n 10000     -s 10000 -p 100  "  "${TDIR}/Z574"       "Basic RW use FC, SeqRead, 10K chunks, many times"
		doAppend 	"583"	"-useFC -readFully -seqRead -rw -n 1000      -s 10000 -p 100  "  "${TDIR}/Z583"       "Basic RW use FC, SeqRead, 100K chunks, many times"
		doAppend 	"584"	"-useFC -readFully -seqRead -rw -n 10000     -s 10000 -p 100  "  "${TDIR}/Z584"       "Basic RW use FC, SeqRead, 100K chunks, many times"

		doAppend 	"598"	"-useFC -readFully -seqRead -rw -n 1000      -s 1000000 -p 100  "  "${TDIR}/Z598"     "Basic RW use FC, 1MB chunks, many times"
		doAppend 	"594"	"-useFC -readFully -seqRead -rw -n 10000     -s 1000000 -p 100  "  "${TDIR}/Z594"     "Basic RW use FC, 1MB chunks, many times"
	fi
}

# Use FileSystem in this set of test
function test151_CreateFC_RWFully {

	# read and write 6 chunks of 1KB each. 
	# Writer Hflush on chunk 0, 2, 4, and no hflush on 1, 3, 5. 
	# Reader is in the same thread and verify the correct number of visible bytes by open and reopen the file
	
	# set of test that do both read after write (same thread) to compare the length of a file
	doAppend 	"151"	"-useFC -rw -readFully -n 10 -s 1000 -p 1000"  "${TDIR}/Z151"          "Basic RW use FC Read Fully"
	doAppend 	"152"	"-useFC -rw -readFully -n 10 -s 10000 -p 1000"  "${TDIR}/Z152"			"Basic RW use FC Read Fully"
	doAppend 	"153"	"-useFC -rw -readFully -n 10 -s 100000 -p 1000"  "${TDIR}/Z153"	    "Basic RW use FC Read Fully"
	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"154"	"-useFC -rw -readFully -n 10 -s 1000000 -p 1000"  "${TDIR}/Z154"	    "Basic RW use FC Read Fully"
		doAppend 	"155"	"-useFC -rw -readFully -n 10 -s 10000000 -p 1000"  "${TDIR}/Z155"	    "Basic RW use FC Read Fully"
		doAppend 	"156"	"-useFC -rw -readFully -n 10 -s 100000000 -p 1000"  "${TDIR}/Z156"	    "Basic RW use FC Read Fully"
		doAppend 	"157"	"-useFC -rw -readFully -n 10 -s 200000000 -p 1000"  "${TDIR}/Z157"	    "Basic RW use FC Read Fully"
		doAppend 	"158"	"-useFC -rw -readFully -n 10 -s 500000000 -p 1000"  "${TDIR}/Z158"	    "Basic RW use FC Read Fully"
		#doAppend 	"159"	"-useFC -rw -n 10 -s 5000000000 -p 1000"  "${TDIR}/Z159"    "Basic RW use FC Read Fully"
	fi
}

function test221_AppendFC_WO {

	# read and write 6 chunks of 1KB each. 
	# Writer Hflush on chunk 0, 2, 4, and no hflush on 1, 3, 5. 
	# Reader is in the same thread and verify the correct number of visible bytes by open and reopen the file
	
	# set of test that do both read after write (same thread) to compare the length of a file
	doAppend 	"221"	"-useFC -wo -append -n 6 -s 1000 -p 1000"        "${TDIR}/Z221"	"Basic Append WriteOnly use FC"
	doAppend 	"222"	"-useFC -wo -append -n 10 -s 10000 -p 1000"      "${TDIR}/Z222"	"Basic Append WriteOnly use FC"
	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"223"	"-useFC -wo -append -n 10 -s 100000 -p 1000"     "${TDIR}/Z223"	"Basic Append WriteOnly use FC"
		doAppend 	"224"	"-useFC -wo -append -n 10 -s 1000000 -p 1000"    "${TDIR}/Z224"	"Basic Append WriteOnly use FC"
		doAppend 	"225"	"-useFC -wo -append -n 10 -s 10000000 -p 1000"   "${TDIR}/Z225"	"Basic Append WriteOnly use FC"
		doAppend 	"226"	"-useFC -wo -append -n 10 -s 100000000 -p 1000"  "${TDIR}/Z226"	"Basic Append WriteOnly use FC"
		##doAppend 	"227"	"-useFC -wo -append -n 10 -s 200000000 -p 1000"  "${TDIR}/Z227"	"Basic Append WriteOnly use FC"
		##doAppend 	"228"	"-useFC -wo -append -n 10 -s 500000000 -p 1000"  "${TDIR}/Z228"	"Basic Append WriteOnly use FC"
	fi
}

function test231_AppendFC_WONoHflush {

	# set of test that do both read after write (different thread)  to compare the length of a file
	doAppend 	"231"	"-useFC -wo -append -n 6 -s 1000 -p 1000 -noHflush"        "${TDIR}/Z231"
	doAppend 	"232"	"-useFC -wo -append -n 10 -s 10000 -p 1000 -noHflush"      "${TDIR}/Z232"
	doAppend 	"233"	"-useFC -wo -append -n 10 -s 100000 -p 1000 -noHflush"     "${TDIR}/Z233"
		if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"234"	"-useFC -wo -append -n 10 -s 1000000 -p 1000 -noHflush"    "${TDIR}/Z234"
		doAppend 	"235"	"-useFC -wo -append -n 10 -s 10000000 -p 1000 -noHflush"   "${TDIR}/Z235"
		doAppend 	"236"	"-useFC -wo -append -n 10 -s 100000000 -p 1000 -noHflush"  "${TDIR}/Z236"
		##doAppend 	"237"	"-useFC -wo -append -n 10 -s 200000000 -p 1000 -noHflush"  "${TDIR}/Z237"
		##doAppend 	"238"	"-useFC -wo -append -n 10 -s 500000000 -p 1000 -noHflush"  "${TDIR}/Z238"
	fi
}


function test241_AppendFC_WOHflush {

	# set of test that do both read after write (different thread)  to compare the length of a file
	doAppend 	"241"	"-useFC -wo -append -n 6 -s 1000 -p 1000 -alwaysHflush"        "${TDIR}/Z241"
	doAppend 	"242"	"-useFC -wo -append -n 10 -s 10000 -p 1000 -alwaysHflush"      "${TDIR}/Z242"
	doAppend 	"243"	"-useFC -wo -append -n 10 -s 100000 -p 1000 -alwaysHflush"     "${TDIR}/Z243"

	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"244"	"-useFC -wo -append -n 10 -s 1000000 -p 1000 -alwaysHflush"    "${TDIR}/Z244"
		doAppend 	"245"	"-useFC -wo -append -n 10 -s 10000000 -p 1000 -alwaysHflush"   "${TDIR}/Z245"
		doAppend 	"246"	"-useFC -wo -append -n 10 -s 100000000 -p 1000 -alwaysHflush"  "${TDIR}/Z246"
		##doAppend 	"247"	"-useFC -wo -append -n 10 -s 200000000 -p 1000 -alwaysHflush"  "${TDIR}/Z247"
		##doAppend 	"248"	"-useFC -wo -append -n 10 -s 500000000 -p 1000 -alwaysHflush"  "${TDIR}/Z248"
	fi
}

function test301_ReadFC_OCFully {
#Run this after test101 test, as this depend on the file created in that test
	hdftDisplayTestGroupMsg "Test Read operation from on filesbeginning to end"
	doAppend 	"301"	"-useFC -ro -oc -n 10 -p 1000 -readFully"  "${TDIR}/Z101"      "Basic RO use FC OC readFully"
	doAppend 	"302"	"-useFC -ro -oc -n 100 -p 1000 -readFully"  "${TDIR}/Z102"		"Basic RO use FC OC readFully"
	if [ $TAPPEND_FULL_RUN  == 1 ] ; then
		doAppend 	"303"	"-useFC -ro -oc -n 1000 -p 1000 -readFully"  "${TDIR}/Z103"	    "Basic RO use FC OC readFully"
		doAppend 	"304"	"-useFC -ro -oc -n 10000 -p 1000 -readFully"  "${TDIR}/Z104"	    "Basic RO use FC OC readFully"
	fi

}

setup
setupData
setupAppendFiles
dumpEnv
test101_CreateFC_RWThread 
test401_CreateFC_RW_PosRead_NonFully_Many
test451_CreateFC_RW_PosRead_ManyFully 
test501_CreateFC_RW_SeqRead_NonFully_Many
test551_CreateFC_RW_SeqRead_ManyFully 
test301_ReadFC_OCFully
displayTestSuiteResult     
teardown 
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE

setup
setupData
test101_CreateFC_RWThread 
# test111_CreateFC_RWThread 
# test121_AppendFC_WO 
# test131_AppendFC_WONoHflush 
# test141_AppendFC_WOHflush 


function xxxx {
setUp
setupAppendFiles
dumpEnv
doFCCreateTest
doFCCreateTest
doAppendTest

exit
setupAppendFiles
doAppendTest
teardown

}







displayTestSuiteResult

teardown
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE




	
		

# Documentation Here
function doHelpx {
$  hadoop jar lib/Hadoop/Java/TAppend.jar TAppend

Usage: [-useFS|-useFC] [-qt|-ro|-wo|-rw] [-overwrite]  [-append] [-create] [-oc] [-nrt] [-readfully] [-verbose] [-p pmsec] [-n ntimes] [-s nbyte] [-f filename] 

Default: -p = 1000 for 1 sec; -n = 1. All boolean default to false.
The -f filename must be provided.
[-useFS | -useFC]: use FileSystem or FileContext (default) in file operations
-qt: quick test
-ro: read-only; -wo: write-only; -rw: read and write
-overwrite: overwrite if file already exist
-append: append if file already exist
-create: create if file does not exist
-oc: open and close file during each read or write cycle
-nrt: spawn new read thread for read during -rw test. No effect in -ro test.
-readFully: if set, reader will read from begin of file to end. Default is to read 1K from end.
-alwaysHflush: if set, all write will be followed by hflush (default is do hflush on alternate writes).
-noHflush: if set, no hflush will be invoked. (default is to do hflush on alternate writes).
-readPassVEnd: if set, read pass Visible End. (default is to read till Visible).
-n: iterate ntimes for read or write
[-posRead | -seqRead]: Use Sequential Read or the default position read to read data
-blockLoc: dump out the block location and host of the files around the last block
-p pause pmsec: pause pmsec during each iteration in msec. (0 means no pause).
-s chunkSize: size of write at a time in byte (default 100 byte)
-verbose: verbose mode for debugging


reader permutations:: 
	readFully vs not
	readPassVEnd vs not
	posRead vs SeqRead
	useFS vs useFC (does not seem to make any difference)
	not reopen the file : some test cases
writer permutations
	create/append
	flush/no flush/alternate flush
}
