
###################################################
## Exercise dfsamin on multiple NNs within a cluster, using -fs option
## Just sanity check to make sure dfsadmin works on different fs
##  relying on return status of command to tell if the command works or not.
## 
## This is not a regression test for dfsadmin.
## 
###################################################
source $WORKSPACE/lib/library.sh
source $WORKSPACE/lib/user_kerb_lib.sh
source $WORKSPACE/lib/hdft_util2.sh

###################################################
## SETUP & Teardown
###################################################
function setup {
	OWNER="smilli"
	USER=$HADOOPQA_USER
	HDFS_USER=$HDFS_SUPER_USER
	setKerberosTicketForUser $HDFS_USER

	MY_HDFS_CMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR"
	MY_HADOOP_CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR"
	export PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH
	umask 0002		# create 775 dir/files

	#global, return to framework
	export  COMMAND_EXIT_CODE=0
	export TESTCASE_DESC="Wordcount application read/write across multiple namenodes within one cluster. Exercise various form of URI also"
	export REASONS="    ####"
	SCRIPT_EXIT_CODE=0
	
	# module-global
	PROGNAME=`basename $0`
	DFS_COMMAND=`basename "$HADOOP_COMMON_CMD"`

	NUM_TESTCASE=0
	NUM_TESTCASE_PASSED=0
	TASK_EXIT_CODE=0

	# this returnes gsbl90772.blue.ygrid.yahoo.com
	NNX=$(getDefaultNameNode)

	#get the default FS: e.g. hdfs://gsbl90772.blue.ygrid.yahoo.com:8020
	FS=$(getDefaultFS)

	# e.g. gsbl90772
	NN0=$(hdft_getNN 0)
	NN1=$(hdft_getNN 1)
	NN2=$(hdft_getNN 2)
	if [ -z "$NN1" ] ; then
		NN1=$NN0
	fi
	if [ -z "$NN2" ] ; then
		NN2=$NN1
	fi

	### JOB_OPTION="-Dmapreduce.job.hdfs-servers=hdfs://$NN0.blue.ygrid:8020,hdfs://$NN1.blue.ygrid:8020,hdfs://$NN2.blue.ygrid:8020 -Djob.mapred.queuename=grideng  -Dmapred.job.numMapTasks=100 -Dmapreduce.job.numReduceTasks=20"
	JOB_OPTION="-Djob.mapred.queuename=grideng  -Dmapred.job.numMapTasks=100 -Dmapreduce.job.numReduceTasks=20"
	

	#dir for federation common dfs opertions on MN
	# for different URI reference
	FED_APPMNN_DEFAULT_DATA_DIR="hdfsMNNData"
	FED_APPMNN_SLASH_DATA_DIR="/user/${HDFS_USER}/${FED_APPMNN_DEFAULT_DATA_DIR}"
	FED_APPMNN_FS_DATA_DIR="${FS}${FED_APPMNN_SLASH_DATA_DIR}"
	#FED_APPMNN_DST_DIR="$TMPDIR/${FED_APPMNN_DEFAULT_DATA_DIR}"
	FED_APPMNN_SLASH_DST_DIR="/tmp/${FED_APPMNN_DEFAULT_DATA_DIR}"
	
	FED_APPMNN_EXPECTED_DIR="$JOB_SCRIPTS_DIR_NAME/Expected"
	FED_APPMNN_ARTIFACTS_DIR="$ARTIFACTS_DIR"

	FED_APPMNN_OUTDIR=/tmp/D1
	FED_TGEN_OUTDIR=$FED_APPMNN_OUTDIR/TGEN
	FED_TSORT_OUTDIR=$FED_APPMNN_OUTDIR/TSORT
	FED_TVER_OUTDIR=$FED_APPMNN_OUTDIR/TVER

	#LOCAL_TMPDIR=$TMPDIR/L1
	LOCAL_TMPDIR=/tmp/L1.${CLUSTER}.${USER}

	# Input and output directories
    FED_WC_INDIR_TOP_REL="hdfsMNNData"
    FED_WC_INDIR_REL="hdfsMNNData/wordcount_input"     
    FED_WC_INDIR_SLASH="/user/$HDFS_USER/$FED_WC_INDIR_REL"
    FED_WC_INHAR_REL="hdfsMNNData/har_input/wordcount_input.har"   
    FED_WC_INHAR_SLASH="/user/$HDFS_USER/$FED_WC_INHAR_REL"

	#FED_WC_OUTDIR_REL="FEDMNN_output"
	#FED_WC_OUTDIR_SLASH="/user/$HDFS_USER/$FED_WC_OUTDIR_REL"   
	FED_WC_DATA_SRC="/homes/hdfsqa/hdfsMNNData"
	FED_WC_OUTDIR_REL="MNNDC_output"
	FED_WC_OUTDIR_SLASH="/user/$HDFS_USER/$FED_WC_OUTDIR_REL"   


	TIDENT="FED_ADM"	# Ident of this entire set of tests FED_DFsAdmin
	TCASE_TYPE="POS"	# POS or NEG. Stateful. Normally set to POS. If want to do negative test, toggle to NEG, and the toggle back to POS.

}

function teardown {
	# deleteHdfsDir $FED_APPMNN_SLASH_DATA_DIR $FS
	echo " "
}

# Input param: msg to be echo
function fatalErrorExit {
	echo "ERROR: $*. Test suite [$TIDENT] aborted."
	echo "ERROR: $*. Test suite [$TIDENT] aborted."
	SCRIPT_EXIT_CODE=1
	echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
	exit $SCRIPT_EXIT_CODE
}


# For each NN, create the necessary data 
# TODO
# assert that FED_WC_DATA_SRC exist
# remove dest data dir
# copyFromLocal
# make sure does exist
# else return failure
function setupData {
	if [ -z "$NNX" ] || [ -z "$NN0" ] ; then
		fatalErrorExit "Failed to get default Name node of the cluster"
	fi		
	if [ ! -d "$FED_WC_DATA_SRC" ] ; then
		fatalErrorExit "Data directory on Linux does not exist [$FED_WC_DATA_SRC]."
	fi

	echo "    Preparing data for $NN0 $NN1 $NN2"
	for MM in $NN0 $NN1 $NN2 ; do
		hdftCpf2HdfsDirs $FED_WC_DATA_SRC "hdfs://$MM.blue.ygrid/user/${HDFS_USER}/${FED_WC_INDIR_TOP_REL}"  ## $FED_WC_INDIR_TOP_REL
	done			
}


function dumpEnv {
	echo "    [$PROGNAME] Env: PROGNAME=$PROGNAME"	
	echo "    [$PROGNAME] Env: FS=$FS"	
	echo "    [$PROGNAME] Env: NNX=$NNX."	
	echo "    [$PROGNAME] Env: NN0=$NN0."	
	echo "    [$PROGNAME] Env: NN1=$NN1."	
	echo "    [$PROGNAME] Env: NN2=$NN2."	

	echo "    [$PROGNAME] Env: USER=$USER"				# Linux USER
	echo "    [$PROGNAME] Env: HDFS_USER=$HDFS_USER"	# HDFS USER
	echo "    [$PROGNAME] Env: OWNER=$OWNER"	

	echo "    [$PROGNAME] Env: TESTCASE_ID=$TESTCASE_ID"	
	echo "    [$PROGNAME] Env: TESTCASE_DESC=$TESTCASE_DESC"	
	echo "    [$PROGNAME] Env: CLUSTER=$CLUSTER"	
	echo "    [$PROGNAME] Env: WORKSPACE=$WORKSPACE"	
	echo "    [$PROGNAME] Env: TMPDIR=$TMPDIR"	
	echo "    [$PROGNAME] Env: ARTIFACTS=$ARTIFACTS"	DOOP_HDFS_CMD} 
	echo "    [$PROGNAME] Env: ARTIFACTS_DIR=$ARTIFACTS_DIR"	
	echo "    [$PROGNAME] Env: JOB=$JOB"	
	echo "    [$PROGNAME] Env: JOBS_SCRIPTS_DIR=$JOBS_SCRIPTS_DIR"	
	echo "    [$PROGNAME] Env: JOB_SCRIPTS_DIR_NAME=$JOB_SCRIPTS_DIR_NAME"	
	echo "    [$PROGNAME] Env: HADOOP_COMMON_CMD=$HADOOP_COMMON_CMD"	
	echo "    [$PROGNAME] Env: which hadoop=`which hadoop`"
	echo "    [$PROGNAME] Env: MY_HDFS_CMD=$MY_HDFS_CMD"	
	echo "    [$PROGNAME] Env: MY_HADOOP_CMD=$MY_HADOOP_CMD"	

	echo "    [$PROGNAME] Env: FED_WC_INDIR_TOP_REL=$FED_WC_INDIR_TOP_REL"	
	echo "    [$PROGNAME] Env: FED_WC_INDIR_SLASH=$FED_WC_INDIR_SLASH"	
	echo "    [$PROGNAME] Env: FED_WC_INHAR_REL=$FED_WC_INHAR_REL"	
	echo "    [$PROGNAME] Env: FED_WC_INHAR_SLASH=$FED_WC_INHAR_SLASH"	
	echo "    [$PROGNAME] Env: FED_WC_OUTDIR_REL=$FED_WC_OUTDIR_REL"	
	echo "    [$PROGNAME] Env: FED_WC_OUTDIR_SLASH=$FED_WC_OUTDIR_SLASH"	
	echo "    [$PROGNAME] Env: FED_WC_DATA_SRC=$FED_WC_DATA_SRC"	

}


###################################################
## Test Cases: ls and other commands which has one operand
###################################################
function run_ls {
	T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;		## PARAM
	
	TESTCASE_ID=$TID ; COMMAND_EXIT_CODE=0 ; REASONS="" ; 
	OFILE=${ARTIFACTS_DIR}/$TID
	local TMSG="dfs -${T_OP} ${TFS} ${TDIR}" 
	TESTCASE_DESC=$TMSG
	TESTCASENAME="FED_DFS"
	displayTestCaseMessage "$TESTCASE_ID"
	
	echo "    #### [$PROGNAME - test_dfs] param: T_OP=$T_OP; TFS=$TFS; TID=$TID; TDIR=$TDIR; TDESC=$TDESC"

	if [ -z "$TFS" ] || [ "$TFS" == "DEFAULT" ]  ; then
		TFSDIR=$TDIR
	else
		TFSDIR="hdfs://$TFS.blue.ygrid:8020/$TDIR"
	fi
	## RUN the command
	local cmd="${HADOOP_COMMON_CMD} --config $HADOOP_CONF_DIR  fs -${T_OP} $TFSDIR"
	echo "    #### TCMD:$TID= [$cmd]  > ${OFILE}.tmp"
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
	hdftReturnTaskResult $stat  $TID "$cmd" "list "

	return

	hdftDoDiff ${OFILE}.out $FED_DFSMNN_EXPECTED_DIR/${TID}.out
	stat=$? ; if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR fail to run sed to create ${OFILE}.out. "	
	fi
	hdftShowDiffResult "$stat" "$TMSG"  "${OFILE}.out" "$FED_DFSMNN_EXPECTED_DIR/${TID}.out"

	hdftReturnTaskResult $stat  $TID "$cmd" "list "
}


	#                $1         $2      $3                     $4                        $5 		      $6                                      $7
    #                Ops        ID      fs           dir of src (mostly not used)  dir of dest       descriptions	                		Additional Options 
	#run_dfsadmin 	"-report"  "110"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin on default host" 	            ""

function run_dfsadmin {

	TOP=$1
	TID="${TIDENT}_${2}"	# testId
	INFS=${3}				# simple gsbl name
	MY_INDIR=${4}			# fully qualified input Dir of where teragen output the data
	MY_OUTDIR="${5}/output_${2}"		# fully qualified output dir of where terasort output the data
	TESTCASE_DESC=$6
	TOPTION=$7
	TESTCASENAME="${TIDENT}"
	OFILE=${ARTIFACTS_DIR}/$TID

	if [ -z "$INFS" ] || [ "$INFS" == "DEFAULT" ] ; then
		MY_INFS=""
	else
		MY_INFS="-fs hdfs://${INFS}.blue.ygrid:8020"
	fi

	echo "    #### [$PROGNAME - test_run_dfsadmin] param: $*"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: TID=$TID"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: MY_INFS=$MY_INFS"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: MY_INDIR=$MY_INDIR"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: MY_OUTDIR=$MY_OUTDIR"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: TOiP=$TOP"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: TOPTION=$TOPTION"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: JOB_OPTION=$JOB_OPTION"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: MY_INDIR_FQ=$MY_INDIR_FQ"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: TESTCASE_DESC=$TESTCASE_DESC"
	echo "    #### [$PROGNAME - test_run_dfsadmin] param: OFILE=$OFILE"

	displayTestCaseMessage "$TID ==== $TESTCASE_DESC"

	hdftRmfHdfsDirs $MY_OUTDIR
	if [ $? != 0 ] ;then
	    setFailCase "FAILED to remove directories $MY_OUTDIR [1]."
	    hdftReturnTaskResult 1 $TID "Failed to remove existing directory $MY_OUTDIR for $TESTCASE_DESC"
		return 1
	fi


	################################################
	# Begins 
	################################################

	local 	cmd="${MY_HDFS_CMD} dfsadmin  $MY_INFS  $TOP $TOPTION "
	echo "    ######## TCMD1:$TID=[$cmd]"
	eval $cmd > ${OFILE}.tmp
	local stat=$?
	if [ $stat != 0 ] ; then
	    setFailCase "FAILED in exec dfsadmin [3]. [$cmd]"
	    hdftReturnTaskResult 1 $TID "$cmd" "dfsadmin $TOP FAIL"
		return 1
	else
		hdftShowExecResult "$stat" "$cmd" "dfsadmin $T_OP PASS"
	fi
	
	echo "cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed > ${OFILE}.out"
	cat < ${OFILE}.tmp  | sed -f $WORKSPACE/lib/hdft_filter.sed > ${OFILE}.out

	stat=$? ; 
	if [ $stat != 0 ]; then
		setFailCase "dfs -${T_OP} $TDIR fail to run sed to create ${OFILE}.out. "	
	fi
	hdftReturnTaskResult $stat  $TID "$cmd" "dfsadmin $TOP"

}

# Note: mapred job does not seem to support -fs option yet, such as:
# hadoop jar hadoop-examples.jar  teragen  -Dmapreduce.job.hdfs-servers=hdfs://gsbl90773.blue.ygrid:8020  -fs hdfs://gsbl90773.blue.ygrid:8020  10000  /tmp/D1/TGEN_120_90773
#

function doit {
	for MM in DEFAULT $NN0 $NN1 $NN2	 ; do
		echo "##################################################################################"
		echo "#########################$MM #####################################################"
		   if [ $MM == "DEFAULT" ] ; then
				FS=""
		   else
			   FS="-fs hdfs://$MM.blue.ygrid"
		   fi
           hadoop dfsadmin $FS -report	## XXXX
           hadoop dfsadmin $FS  -safemode get
           hadoop dfsadmin $FS  -safemode enter 
           hadoop dfsadmin $FS  -safemode get
           hadoop dfsadmin $FS  -safemode leave 
           hadoop dfsadmin $FS  -safemode get
           hadoop dfsadmin $FS  -safemode wait
           hadoop dfsadmin $FS  -saveNamespace
           hadoop dfsadmin $FS  -restoreFailedStorage check    # true false check
           hadoop dfsadmin $FS  -refreshNodes
           hadoop dfsadmin $FS  -finalizeUpgrade         
           hadoop dfsadmin $FS  -upgradeProgress status #   | details | force
           hadoop dfsadmin $FS  -metasave YYYsave        
           hadoop dfsadmin $FS  -refreshServiceAcl       
           hadoop dfsadmin $FS  -refreshUserToGroupsMappings
           hadoop dfsadmin $FS  -refreshSuperUserGroupsConfiguration
           hadoop dfsadmin $FS  -printTopology
           hadoop dfsadmin $FS  -refreshNamenodes       #datanodehost:port
	done
}

# do a full combination of running distcp with different src and dest
# 		Note that hftp://  can be used in src but not dest
# 		default directory, default NN can be used in both dir or dest
# 		fully qualified hdfs can be used in both src and dir
#		to distcp, har: is just another file. Thus no need to test har: specifically

#  fs
function do_test1 {

	hdftDisplayTestGroupMsg "Dfsadmin on default fs"
####T_OP=$1; TID=$2; TFS=$3; TDIR=$4; TDESC=$5;  
	run_ls          "dus"      "101"   "DEFAULT"     "/"               "dfs -dus of default fs"
	run_ls          "dus"      "102"   "$NN0"        "/"               "dfs -dus of NN0 fs"
	run_ls          "dus"      "103"   "$NN1"        "/"               "dfs -dus of NN1 fs"
	run_ls          "dus"      "104"   "$NN2"        "/"               "dfs -dus of NN2 fs"

	#                $1         $2      $3                     $4                        $5 		      $6                                      $7
    #                Ops        ID      fs           dir of src (mostly not used)  dir of dest       descriptions	                		Additional Options 
	run_dfsadmin 	"-report"  "110"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin on default host" 	            ""
	run_dfsadmin 	"-report"  "111"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -report on NN0 host"    ""
	run_dfsadmin 	"-report"  "112"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -report on NN1 host"    ""
	run_dfsadmin 	"-report"  "113"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -report on NN2 host"    ""

	run_dfsadmin 	"-safemode get"  "120"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -safemode get on default host" 	            ""
	run_dfsadmin 	"-safemode get"  "121"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -safemode get  on NN0 host"    ""
	run_dfsadmin 	"-safemode get"  "122"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -safemode get  on NN1 host"    ""
	run_dfsadmin 	"-safemode get"  "123"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -safemode get  on NN2 host"    ""

        #In order to run saveNamespace test , safemode should be on
	run_dfsadmin 	"-safemode enter"  "130_0"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -safemode enter on default host" 	            ""
	run_dfsadmin 	"-saveNamespace"  "130_1"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -saveNamespace on default host" 	            ""
	run_dfsadmin 	"-safemode leave"  "130_2"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -safemode leave on default host" 	            ""
	run_dfsadmin 	"-safemode enter"  "131_0"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -safemode enter on NN0 host"    ""
	run_dfsadmin 	"-saveNamespace"  "131_1"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -saveNamespace on NN0 host"    ""
	run_dfsadmin 	"-safemode leave"  "131_2"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -safemode leave on NN0 host"    ""
	run_dfsadmin 	"-safemode enter"  "132_0"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -safemode enter  on NN1 host"    ""
	run_dfsadmin 	"-saveNamespace"  "132_1"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -saveNamespace on NN1 host"    ""
	run_dfsadmin 	"-safemode leave"  "132_2"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -safemode leave  on NN1 host"    ""
	run_dfsadmin 	"-safemode enter"  "133_0"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -safemode enter on NN2 host"    ""
	run_dfsadmin 	"-saveNamespace"  "133_1"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -saveNamespace on NN2 host"    ""
	run_dfsadmin 	"-safemode leave"  "133_2"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -safemode leave on NN2 host"    ""

	run_dfsadmin 	"-restoreFailedStorage check"  "140"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -restoreFailedStorage on default host" 	            ""
	run_dfsadmin 	"-restoreFailedStorage check"  "141"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -restoreFailedStorage check on NN0 host"    ""
	run_dfsadmin 	"-restoreFailedStorage check"  "142"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -restoreFailedStorage check on NN1 host"    ""
	run_dfsadmin 	"-restoreFailedStorage check"  "143"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -restoreFailedStorage check on NN2 host"    ""

	run_dfsadmin 	"-refreshNodes"  "150"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -refreshNodes on default host" 	            ""
	run_dfsadmin 	"-refreshNodes"  "151"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -refreshNoderefreshNodes on NN0 host"    ""
	run_dfsadmin 	"-refreshNodes"  "152"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -refreshNodes on NN1 host"    ""
	run_dfsadmin 	"-refreshNodes"  "153"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -refreshNodes on NN2 host"    ""

	run_dfsadmin 	"-finalizeUpgrade"  "160"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -finalizeUpgrade on default host" 	            ""
	run_dfsadmin 	"-finalizeUpgrade"  "161"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -finalizeUpgrade on NN0 host"    ""
	run_dfsadmin 	"-finalizeUpgrade"  "162"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -finalizeUpgrade on NN1 host"    ""
	run_dfsadmin 	"-finalizeUpgrade"  "163"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -finalizeUpgrade on NN2 host"    ""

	run_dfsadmin 	"-upgradeProgress status"  "170"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -upgradeProgress status on default host" 	            ""
	run_dfsadmin 	"-upgradeProgress status"  "171"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -upgradeProgress status on NN0 host"    ""
	run_dfsadmin 	"-upgradeProgress status"  "172"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -upgradeProgress status on NN1 host"    ""
	run_dfsadmin 	"-upgradeProgress status"  "173"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -upgradeProgress status on NN2 host"    ""

	run_dfsadmin 	"-metasave YYYsave"  "180"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -metasave on  default host" 	            ""
	run_dfsadmin 	"-metasave YYYsave"  "181"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -metasave on NN0 host"    ""
	run_dfsadmin 	"-metasave YYYsave"  "182"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -metasave on NN1 host"    ""
	run_dfsadmin 	"-metasave YYYsave"  "183"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -metasave on NN2 host"    ""

	run_dfsadmin 	"-refreshServiceAcl"  "190"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -refreshServiceAcl on default host" 	            ""
	run_dfsadmin 	"-refreshServiceAcl"  "191"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -refreshServiceAcl on NN0 host"    ""
	run_dfsadmin 	"-refreshServiceAcl"  "192"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -refreshServiceAcl on NN1 host"    ""
	run_dfsadmin 	"-refreshServiceAcl"  "193"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -refreshServiceAcl on NN2 host"    ""

	run_dfsadmin 	"-refreshUserToGroupsMappings"  "200"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -refreshUserToGroupsMappings on default host" 	            ""
	run_dfsadmin 	"-refreshUserToGroupsMappings"  "201"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -refreshUserToGroupsMappings on NN0 host"    ""
	run_dfsadmin 	"-refreshUserToGroupsMappings"  "202"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -refreshUserToGroupsMappings on NN1 host"    ""
	run_dfsadmin 	"-refreshUserToGroupsMappings"  "203"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -refreshUserToGroupsMappings on NN2 host"    ""

	run_dfsadmin 	"-refreshSuperUserGroupsConfiguration"  "210"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -refreshSuperUserGroupsConfiguration on default host" 	            ""
	run_dfsadmin 	"-refreshSuperUserGroupsConfiguration"  "211"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -refreshSuperUserGroupsConfiguration on NN0 host"    ""
	run_dfsadmin 	"-refreshSuperUserGroupsConfiguration"  "212"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -refreshSuperUserGroupsConfiguration on NN1 host"    ""
	run_dfsadmin 	"-refreshSuperUserGroupsConfiguration"  "213"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -refreshSuperUserGroupsConfiguration on NN2 host"    ""

	run_dfsadmin 	"-printTopology"  "220"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -printTopology on default host" 	            ""
	run_dfsadmin 	"-printTopology"  "221"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN0 -printTopology on NN0 host"    ""
	run_dfsadmin 	"-printTopology"  "222"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN1 -printTopology on NN1 host"    ""
	run_dfsadmin 	"-printTopology"  "223"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "dfsadmin -fs $NN2 -printTopology on NN2 host"    ""

        #To run refreshNamenodes datanode:port should be supplied as arguments
        dns=$(getDataNodes)
        dns_list=(`echo $dns | tr ";" " "`)
        i=0
        for dn in ${dns_list[@]}
        do
                echo "run refreshNamenodes on $dn:8020 "

                run_dfsadmin    "-refreshNamenodes $dn:8020"  "230_$i"    "DEFAULT"    $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH       "dfsadmin -refreshNamenodeson $dn:8020 default host."                    ""
                run_dfsadmin    "-refreshNamenodes $dn:8020"  "231_$i"    "$NN0"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH       "dfsadmin -fs $NN0 -refreshNamenodes $dn:8020 on NN0 host."    ""
                run_dfsadmin    "-refreshNamenodes $dn:8020"  "232_$i"    "$NN1"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH       "dfsadmin -fs $NN1 -refreshNamenodes $dn:8020 on NN1 host."    ""
                run_dfsadmin    "-refreshNamenodes $dn:8020"  "233_$i"    "$NN2"       $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH       "dfsadmin -fs $NN2 -refreshNamenodes $dn:8020 on NN2 host."    ""
                (( i = $i+1 ))
        done



### YYY

return

}

################################################
## MAIN
################################################
echo "ENTER MY TEST"
echo "ENTER MY TEST"
echo "ENTER MY TEST"
echo "ENTER MY TEST"
echo "     getKerberosTicketForUser $HDFS_SUPER_USER"

#getKerberosTicketForUser $HDFS_SUPER_USER
setup
#setupData
dumpEnv
do_test1

displayTestSuiteResult

teardown
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE






