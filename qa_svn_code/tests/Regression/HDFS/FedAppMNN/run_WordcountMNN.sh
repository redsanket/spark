
###################################################
## Exercise wordcount on multiple NNs within a cluster
## Also uses various permutation of hdfs://, hftp://, har:
## Note that by design, -fs is not suported by mapred app. Thus default home dir and default / works only for the default NN
###################################################
source $WORKSPACE/lib/library.sh
source $WORKSPACE/lib/user_kerb_lib.sh
source $WORKSPACE/lib/hdft_util2.sh

###################################################
## SETUP & Teardown
###################################################
function setup {
	OWNER="cwchung"
	USER=$HADOOPQA_USER
	HDFS_USER=$HADOOPQA_USER
	MY_HDFS_CMD="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR"
	MY_HADOOP_CMD="$HADOOP_COMMON_CMD --config $HADOOP_CONF_DIR"
	setKerberosTicketForUser $HADOOPQA_USER
	export PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH
	umask 0002		# create 775 dir/files

	#global, return to framework
	export  COMMAND_EXIT_CODE=0
	export TESTCASE_DESC="Wordcount application read/write across multiple namenodes within one cluster. Exercise various form of URI also"
	export REASONS="    ####"
	export SCRIPT_EXIT_CODE=0
	
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
	FED_WC_OUTDIR_REL="FEDMNN_output"
	FED_WC_OUTDIR_SLASH="/user/$HDFS_USER/$FED_WC_OUTDIR_REL"   

	FED_WC_DATA_SRC="/homes/hdfsqa/hdfsMNNData"

	TIDENT="FED_WCOUNT"		# Ident of this entire set of tests FED_DFS_100, _101, ...
	TCASE_TYPE="POS"	# POS or NEG. Stateful. Normally set to POS. If want to do negative test, toggle to NEG, and the toggle back to POS.

	MD5SUM_INPUT_BestManagementQuotationI="7571a425ee7f31818028b07120fc24f7  -"
	MD5SUM_INPUT_IndustrialDecisionMaking="04351241ebb8296802c219a1aba03b64  -"
	MD5SUM_INPUT=SydneySheldonMasterOfTheGame="245c05e3741dc2b8dde84d55d5aaefac  -"
	MD5SUM_WORDCOUNT_OUTPUT1="ab9d060d653492d65b460562d0b1e815  -"
	MD5SUM_WORDCOUNT_OUTPUT2="17dbc9e2a5b789d38db584385d82c200  -"
}

function teardown {
	local NAMENODES=`getNameNodes`	# Get list of namenodes
	NAMENODES=`echo $NAMENODES | tr ';' ' '`
	for nn in $NAMENODES; do
		echo "Delete files from $nn"
		deleteHdfsDir $FED_APPMNN_SLASH_DATA_DIR hdfs://$nn 0
		deleteHdfsDir $FED_WC_OUTDIR_SLASH hdfs://$nn 0
	done	
}

# Input param: msg to be echo
function fatalErrorExit {
	echo "ERROR: $*. Test suite [$TIDENT] aborted."
	echo "ERROR: $*. Test suite [$TIDENT] aborted."
	echo "SCRIPT_EXIT_CODE=1"
	exit 1
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

###########################################
# The equivalent of rm -f on a hdfs system
# Input: a list of files/dir to be remove -f
# output: 0 if all removed; non-zero=> failure
##########################################
# parameters: $1 Linux local dir; $2 HDFS dir
function hdftCpf2HdfsDirs {
	local localDir=$1
	local hdfsDir=$2
    local stat=0        # non-zero => failure
    local cmd

	echo "    Working on copying from local file/dir $localDir to HDFS $hdfsDir..."
	if [ ! -e "$localDir" ] ; then
		fatalErrorExit "Data Src $localDir does not exist"
	fi

	${MY_HDFS_CMD}  dfs -test -e $hdfsDir
	if [ $? == 0 ] ; then
		echo "    Dir/file exists. First remove $hdfsDir"
		cmd="${MY_HDFS_CMD}  dfs -rmr -skipTrash $hdfsDir"
		echo "    $cmd"
		eval $cmd

		# Now make sure the directories does not exist now. Failure
		if [ $? != 0 ] ; then
			fatalErrorExit "Fail to remove dir/file $hdfsDir"
		fi

		cmd="${MY_HDFS_CMD}  dfs -test -e $hdfsDir"
		echo "    $cmd"
		eval $cmd
	fi

	cmd="${MY_HDFS_CMD}  dfs -copyFromLocal  $localDir $hdfsDir"
	echo "    #### Ucmd1: $cmd"
	eval $cmd
	local stat=$?
	if [ $stat != 0 ]; then
		fatalErrorExit "Fail to copy data from local $localDir to HDFS $hdfsDir: [$cmd]"
	fi

	${MY_HDFS_CMD}  dfs -test -e $hdfsDir
	if [ $? != 0 ]  ; then
		fatalErrorExit "Fail to copy data from local $localDir to HDFS $hdfsDir. [$cmd]"
	fi

	echo "    Done copying from local file/dir $localDir to HDFS $hdfsDir."
	return 0
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



#                     $1     $2                           $3                          $4                                    $5
#	run_wordcount 	"100" "DEFAULT"                      $FED_WC_INDIR_REL     $FED_WC_OUTDIR_REL	     "Default NN-home src to default NN-home dest"
# MAPRED_OPTION=-Dmapreduce.job.hdfs-servers=hdfs://gsbl90773.blue.ygrid.yahoo.com:8020
function run_wordcount {

	TID="${TIDENT}_${1}"	# testId
	MY_INFS=${2}			# fully qualitifed input Dir of where teragen output the data
	MY_INDIR=${3}			# fully qualitifed input Dir of where teragen output the data
	MY_OUTDIR="${4}/output_${1}"		# fully qualified output dir of where terasort output the data
	TESTCASE_DESC=$5
	TESTCASENAME="${TID}"

	if [ -z "$MY_INFS" ] || [ "$MY_INFS" == "DEFAULT" ] ; then
		MY_INDIR_FQ="$MY_INDIR"
	else
		MY_INDIR_FQ="${MY_INFS}${MY_INDIR}"
	fi

	echo "    #### [$PROGNAME - test_run_wordcount] param: $*"
	echo "    #### [$PROGNAME - test_run_wordcount] param: TID=$TID"
	echo "    #### [$PROGNAME - test_run_wordcount] param: MY_INFS=$MY_INFS"
	echo "    #### [$PROGNAME - test_run_wordcount] param: MY_INDIR=$MY_INDIR"
	echo "    #### [$PROGNAME - test_run_wordcount] param: MY_OUTDIR=$MY_OUTDIR"
	echo "    #### [$PROGNAME - test_run_wordcount] param: JOB_OPTION=$JOB_OPTION"
	echo "    #### [$PROGNAME - test_run_wordcount] param: MY_INDIR_FQ=$MY_INDIR_FQ"
	echo "    #### [$PROGNAME - test_run_wordcount] param: TESTCASE_DESC=$TESTCASE_DESC"

	displayTestCaseMessage "$TID ==== $TESTCASE_DESC"

	hdftRmfHdfsDirs $MY_OUTDIR
	if [ $? != 0 ] ;then
	    setFailCase "FAILED to remove directories $MY_OUTDIR [1]."
	    hdftReturnTaskResult 1 $TID "Failed to remove existing directory $MY_OUTDIR for $TESTCASE_DESC"
		return 1
	fi

 	${MY_HDFS_CMD} dfs -test -e "$MY_INDIR_FQ"
	if [ $? != 0 ] ;then
	    setFailCase "Src file/directories does not exist [2]."
	    hdftReturnTaskResult 1 $TID "Src file/directories does not exist"
		return 1
	fi

	################################################
	# Begins 
	################################################

	local 	cmd="${MY_HADOOP_CMD} jar $HADOOP_EXAMPLES_JAR  wordcount  $JOB_OPTION  $MY_INDIR_FQ  $MY_OUTDIR"
	echo "    #### TCMD1=$cmd"
	eval $cmd
	if [ $? != 0 ] ; then
	    setFailCase "FAILED in exec wordcount [3]. [$cmd]"
	    hdftReturnTaskResult 1 $TID "$cmd" "wordcount "
		return 1
	fi

	# return 0 if dir exists
	${MY_HDFS_CMD} dfs -test -d $MY_OUTDIR
	if [ $? != 0 ] ; then
	    setFailCase "FAILED to create wordcount output dir [4]. [$cmd]"
	    hdftReturnTaskResult 1  $TID "$cmd" "wordcount "
		return 1
	fi
	${MY_HDFS_CMD} dfs -test -e "$MY_OUTDIR/part-r-00000"
	if [ $? != 0 ] ; then
	    setFailCase "FAILED to create wordcount output file $MY_OUTDIR/part-r-00000 [5]. [$cmd]"
	    hdftReturnTaskResult 1  $TID "$cmd" "wordcount "
		return 1
	fi

	cmd="${MY_HDFS_CMD} dfs -lsr $MY_OUTDIR"
	echo "    #### UCmd2: $cmd"
	eval $cmd

	# now compute the checksum
	cmd="${MY_HDFS_CMD} dfs -cat $MY_OUTDIR/part-r-00000 | md5sum "
	echo "    #### UCmd3: $cmd "
	OUTPUT_MD5=`eval $cmd`
	
	if [ $? != 0 ] ;then
	    setFailCase "FAILED to cat  wordcount output file to md5sum [6]."
	    hdftReturnTaskResult 1  $TID "$cmd" "FAILED to cat  wordcount output file to md5sum"
		return 1
	fi

    if [ "$MD5SUM_WORDCOUNT_OUTPUT1"  == "$OUTPUT_MD5" ] || [ "$MD5SUM_WORDCOUNT_OUTPUT2" == "$OUTPUT_MD5" ] ; then
		echo "    Checksum matches ($OUTPUT_MD5). Test Pass"
	    hdftReturnTaskResult 0  $TID "$cmd" "wordcount PASS"
        return 0
    else
	    setFailCase "FAILED: MD5sum of wordcount output is $OUTPUT_MD5. Expected $MD5SUM_WORDCOUNT_OUTPUT1 or $MD5SUM_WORDCOUNT_OUTPUT2[7]."
	    hdftReturnTaskResult 1  $TID "$cmd" "FAILED: MD5sum of wordcount output is $OUTPUT_MD5. Expected $MD5SUM_WORDCOUNT_OUTPUT1"
		return 1
    fi

}

# Note: mapred job does not seem to support -fs option yet, such as:
# hadoop jar hadoop-examples.jar  teragen  -Dmapreduce.job.hdfs-servers=hdfs://gsbl90773.blue.ygrid:8020  -fs hdfs://gsbl90773.blue.ygrid:8020  10000  /tmp/D1/TGEN_120_90773
#

# do a full combination of source and destination in running wordcount app
function do_test1 {
	#                $1     $2                            $3                         $4                                          $5 
    #                ID    fs of src                      dir of src            dir of dest                                     descriptions
	hdftDisplayTestGroupMsg "Test wordcount across multiple NNs, with different URI. Src from default host NNX/NN0. Output to same defaul thost"
	run_wordcount 	"100" "DEFAULT"                      $FED_WC_INDIR_REL     $FED_WC_OUTDIR_REL	     "Default NN-home src to default NN-home dest"
	run_wordcount 	"110" "DEFAULT"                      $FED_WC_INDIR_SLASH   $FED_WC_OUTDIR_SLASH	     "Default NN src to default NN dest"
	run_wordcount 	"120" "hdfs://$NN0.blue.ygrid"       $FED_WC_INDIR_SLASH   "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hdfs default NN src to FQ default NN dest"
	run_wordcount 	"130" "DEFAULT"                      $FED_WC_INHAR_REL     "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"140" "DEFAULT"                      $FED_WC_INHAR_SLASH   "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"150" "hdfs://$NN0.blue.ygrid"       $FED_WC_INHAR_SLASH   "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"160" "hftp://$NN0.blue.ygrid:50070" $FED_WC_INDIR_SLASH   "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hftp NN1 src to FQ NN2 dest"

	hdftDisplayTestGroupMsg "Test wordcount across multiple NNs, with different URI. Src from NN1 or NN2. Output to same host."
	run_wordcount 	"200" "hdfs://$NN1.blue.ygrid"       $FED_WC_INDIR_SLASH   "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hdfs default NN src to FQ default NN dest"
	run_wordcount 	"210" "hdfs://$NN1.blue.ygrid"       $FED_WC_INHAR_SLASH   "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"230" "hftp://$NN1.blue.ygrid:50070" $FED_WC_INDIR_SLASH   "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hftp NN1 src to FQ NN2 dest"
	run_wordcount 	"250" "hdfs://$NN2.blue.ygrid"       $FED_WC_INDIR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hdfs default NN src to FQ default NN dest"
	run_wordcount 	"260" "hdfs://$NN2.blue.ygrid"       $FED_WC_INHAR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"270" "hftp://$NN2.blue.ygrid:50070" $FED_WC_INDIR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hftp NN1 src to FQ NN2 dest"

	hdftDisplayTestGroupMsg "Test wordcount across multiple NNs, with different URI. Src from default host NNX/NN0. Output to NN1/NN2"
	run_wordcount 	"300" "DEFAULT"                      $FED_WC_INDIR_REL     "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "Default NN-home src to default NN-home dest"
	run_wordcount 	"310" "DEFAULT"                      $FED_WC_INDIR_SLASH   "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "Default NN src to default NN dest"
	run_wordcount 	"320" "hdfs://$NN0.blue.ygrid"       $FED_WC_INDIR_SLASH   "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hdfs default NN src to FQ default NN dest"
	run_wordcount 	"330" "DEFAULT"                      $FED_WC_INHAR_REL     "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"340" "DEFAULT"                      $FED_WC_INHAR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"350" "hdfs://$NN0.blue.ygrid"       $FED_WC_INHAR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"360" "hftp://$NN0.blue.ygrid:50070" $FED_WC_INDIR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hftp NN1 src to FQ NN2 dest"


	hdftDisplayTestGroupMsg "Test wordcount across multiple NNs, with different URI. Src from NN1/NN2. Output to NN0"
	run_wordcount 	"400" "hdfs://$NN1.blue.ygrid"       $FED_WC_INDIR_SLASH    $FED_WC_OUTDIR_REL	                            "Default NN-home src to default NN-home dest"
	run_wordcount 	"410" "hdfs://$NN1.blue.ygrid"       $FED_WC_INDIR_SLASH   "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hdfs default NN src to FQ default NN dest"
	run_wordcount 	"420" "hftp://$NN1.blue.ygrid:50070" $FED_WC_INDIR_SLASH   "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hftp NN1 src to FQ NN2 dest"
	run_wordcount 	"430" "hdfs://$NN2.blue.ygrid"       $FED_WC_INDIR_SLASH    $FED_WC_OUTDIR_SLASH	                        "Default NN src to default NN dest"
	run_wordcount 	"440" "hdfs://$NN2.blue.ygrid"       $FED_WC_INHAR_SLASH   "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"450" "hftp://$NN2.blue.ygrid:50070" $FED_WC_INDIR_SLASH   "hdfs://$NN0.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hftp NN1 src to FQ NN2 dest"

	hdftDisplayTestGroupMsg "Test wordcount across multiple NNs, with different URI. Src from NN1/NN2. Output to NN1/NN2"
	run_wordcount 	"500" "hdfs://$NN1.blue.ygrid"       $FED_WC_INDIR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "Default NN-home src to default NN-home dest"
	run_wordcount 	"510" "hdfs://$NN1.blue.ygrid"       $FED_WC_INDIR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hdfs default NN src to FQ default NN dest"
	run_wordcount 	"520" "hftp://$NN1.blue.ygrid:50070" $FED_WC_INDIR_SLASH   "hdfs://$NN2.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hftp NN1 src to FQ NN2 dest"
	run_wordcount 	"530" "hdfs://$NN2.blue.ygrid"       $FED_WC_INDIR_SLASH   "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "Default NN src to default NN dest"
	run_wordcount 	"540" "hdfs://$NN2.blue.ygrid"       $FED_WC_INHAR_SLASH   "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ har NN1 src to FQ NN2 dest"
	run_wordcount 	"550" "hftp://$NN2.blue.ygrid:50070" $FED_WC_INDIR_SLASH   "hdfs://$NN1.blue.ygrid/$FED_WC_OUTDIR_SLASH"    "FQ hftp NN1 src to FQ NN2 dest"

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
do_test1

displayTestSuiteResult

teardown
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit $SCRIPT_EXIT_CODE






