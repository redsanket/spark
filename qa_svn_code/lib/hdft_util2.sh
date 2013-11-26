#!/bin/sh
export  HDFT_UTIL_LIB_SH=1 


#DFS_CMD=$HADOOP_HOME/bin/hadoop
DFS_CMD=$HADOOP_HDFS_HOME/bin/hdfs


###########################################
# Function to do klist: just show the principal, unless -l option is passed in
###########################################
function hdftKList {
	arg=$1		## PARAM

	if [ -n $arg ] ; then
		klist | cat -n 
	fi
	P=`klist |grep 'Default principal'`
	if [ $? == 0 ] ; then
		echo "    KLIST: Currently running as effective user:  [$P]"
	else
		echo "    KLIST: NO effective user principal $P running."
	fi
}


### 		echo "      kinit -c  /tmp/hdft_kbr012345 -kt $HDFT_KEYTAB_DIR/${KUSER}.dev.headless.keytab  $*   ${KUSER}@DEV.YGRID.YAHOO.COM"

HDFT_KEYTAB_DIR=/homes/hdfsqa/etc/keytabs
###########################################
# Function to do kinitas a user (first argument). Accept additional argument
###########################################
function hdftKInitAsUser {
	KUSER=$1		## PARAM
	shift
	arg=$*
	echo "    hdftKInitAsUser: run by $KUSER; Arg= $arg"

	export EFFECTIVE_KB_USER=$KUSER

	case $KUSER in 
	# earlier version use mktemp to generate temp file for kb ticket, but now use a fixed file for easy trouble-shooting
	"hadoopqa")
		echo "    kinit  $HDFT_KEYTAB_DIR/hadoopqa.dev.headless.keytab $*   hadoopqa@DEV.YGRID.YAHOO.COM"
		          kinit  -kt $HDFT_KEYTAB_DIR/hadoopqa.dev.headless.keytab $*   hadoopqa@DEV.YGRID.YAHOO.COM
		;;
	"hdfs" | "hadoop9" )
		export EFFECTIVE_KB_USER='hdfs'
		echo "    kinit -k -t $HDFT_KEYTAB_DIR/etc/keytabs/hdfs.dev.headless.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
		          kinit -k -t $HDFT_KEYTAB_DIR/etc/keytabs/hdfs.dev.headless.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
		;;
	*)
		export EFFECTIVE_KB_USER=$KUSER
		echo "      kinit  -kt $HDFT_KEYTAB_DIR/${KUSER}.dev.headless.keytab  $*   ${KUSER}@DEV.YGRID.YAHOO.COM"
		            kinit -kt $HDFT_KEYTAB_DIR/${KUSER}.dev.headless.keytab  $*   ${KUSER}@DEV.YGRID.YAHOO.COM
		;;
	esac

	echo "    Env: User $WHOAMI: run as KB credential EFFECTIVE_KB_USER=$EFFECTIVE_KB_USER"
	hdftKList |grep -i principal
}


# re-written after reorg of the keytabs, now accessible in /homes/hdfsq/etc/keytabs/ in a consistent manner (except hdfs)
function hdftKInit {
	arg=$*		## PARAM
	WHOAMI=`whoami`
	echo "    hdftKInit: run by $WHOAMI; Arg= $arg"

	export EFFECTIVE_KB_USER=$WHOAMI

	case $WHOAMI in 
	# earlier version use mktemp to generate temp file for kb ticket, but now use a fixed file for easy trouble-shooting
	"hdfs" | "hadoop9" )
		export EFFECTIVE_KB_USER="hdfs"
		#    kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
		echo "    kinit -k -t $HDFT_KEYTAB_DIR/etc/keytabs/hdfs.dev.headless.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
		          kinit -k -t $HDFT_KEYTAB_DIR/etc/keytabs/hdfs.dev.headless.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
		;;
	"cwchung" | "hadoop8" )	# special use hadoopqa by default
		export EFFECTIVE_KB_USER="hadoopqa"
		echo "    kinit  -kt $HDFT_KEYTAB_DIR/hadoopqa.dev.headless.keytab $*   hadoopqa@DEV.YGRID.YAHOO.COM"
		          kinit  -kt $HDFT_KEYTAB_DIR/hadoopqa.dev.headless.keytab $*   hadoopqa@DEV.YGRID.YAHOO.COM
		;;

	*)	# any one else use their own KB identity
		export EFFECTIVE_KB_USER="$WHOAMI"
		#echo "    kinit -c  /tmp/hdft_kbr012345 -kt ~/hadoopqa.dev.headless.keytab $*   hadoopqa"
		     #kinit -c  /tmp/hdft_kbr012345 -kt $HDFT_KEYTAB_DIR/${WHOAMI}.dev.headless.keytab  $*   $WHOAMI
		echo "    kinit -kt ~/hadoopqa.dev.headless.keytab $*   hadoopqa"
		          kinit -kt $HDFT_KEYTAB_DIR/${WHOAMI}.dev.headless.keytab  $*   ${WHOAMI}@DEV.YGRID.YAHOO.COM
		;;
	esac
	echo "    Env: User $WHOAMI: run as KB credential EFFECTIVE_KB_USER=$EFFECTIVE_KB_USER"
	hdftKList 
}


function hdftKDestroy {
	WHOAMI=`whoami`
	case $WHOAMI in 
	"hadoop8")
		kdestroy 
		;;
	"haoopqa")
		echo "kdestroy -c  /tmp/hdft_kbr012345"
		kdestroy -c  /tmp/hdft_kbr012345
		kdestroy
		;;
	esac
}

###########################################
function hdftSuperuserKInit {
	WHOAMI=`whoami`
	# allow any one can become KB super user
	WHOAMI=hadoop8
	if [ $WHOAMI == "hadoop8" ] || [ $WHOAMI == "hdfs" ] || [ $WHOAMI == "hadoop9" ] || [ $WHOAMI == "cwchung" ] ; then
		export EFFECTIVE_KB_USER="hdfs"
		# echo "    sudo -u hdfs kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
		# sudo -u hdfs kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM

		echo "    kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
		# kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
		# kinit -k -t /homes/hdfsqa/etc/keytabs/hdfsqa.dev.headless.keytab $*   hdfsqa@DEV.YGRID.YAHOO.COM
		kinit -k -t /homes/hdfsqa/etc/keytabs/hdfs.dev.headless.keytab $*   hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
		klist | grep "Default principal: hdfs"		# work for both hdfs and hdfsqa
		stat=$?
		if [ $stat == 0 ] ; then
			echo "    Effective KB user: hdfs"
		else 
			echo "    ERROR: Effective KB user is  not hdfs"
			echo "    ERROR: Effective KB user is  `klist | grep Default principal`"
		fi
	else
		echo "ERROR: Invalid user $WHOAMI to try to be a superuser in hdfs"
		echo "ERROR: Invalid user $WHOAMI to try to be a superuser in hdfs"
	fi
	hdftKList
}

function hdftSuperuserKDestroy {
	WHOAMI=`whoami`
	if [ $WHOAMI == "hadoop8" ] || [ $WHOAMI == "hdfs" ]  || [ $WHOAMI == "hadoop9" ] || [ $WHOAMI == "cwchung" ]  || [ $WHOAMI == "hadoopqa" ] ; then
	      kdestroy
	else
		echo "ERROR: Invalid user $WHOAMI to try to be a superuser in hdfs"
		echo "ERROR: Invalid user $WHOAMI to try to be a superuser in hdfs"
	fi
	hdftKList
}

###########################################
# Function to display a message on console
###########################################
function hdft_dumpmsg {
  local MSG=$*			## PARAM
  #local DT=`date '+%Y-%m-%d %H:%M:%S '`
  echo "${DT}${MSG}"
}


  
###########################################
# Function to dump the current HADOOP ENV, CLASSPATH, 
###########################################
function hdftDumpEnv {
	echo "## ------------ DUMP of KEY ENV VARIABLES --------------------------"
	echo "SG_WORKSPACE = $SG_WORKSPACE"
	echo "CLUSTER = $CLUSTER"
	echo "HDFT_HOME = $HDFT_HOME"
	echo "HDFT_TOP_DIR = $HDFT_TOP_DIR"
	echo "HADOOP_CONF_DIR = $HADOOP_CONF_DIR"
	echo "HADOOP_COMMON_HOME = $HADOOP_COMMON_HOME"
	echo "HADOOP_MAPRED_HOME = $HADOOP_MAPRED_HOME"
	echo "HADOOP_HDFS_HOME = $HADOOP_HDFS_HOME"
	echo "HADOOP_EXAMPLES_JAR = $HADOOP_EXAMPLES_JAR"
	echo "HADOOP_STREAMING_JAR = $HADOOP_STREAMING_JAR"
	echo "CLUSTER, SG_WORKSPACE, HADOOP and JAVA ENV:"
	echo "Other env variables"
	env | egrep "HADOOP|JAVA|CLUSTER|SG_WORKSPACE" | sort

	echo "PATH and CLASSPATH:"
	env | grep PATH	 | sort


	echo "HDFT/HDFD/HDFL:"
	env | egrep "HDFT|HDFD|HDFL" | sort

	echo "Which hadoop: `which hadoop`"
	echo "Which java: `which java`"

}
    
###########################################
# Function to show the result of Stderr (checking for a particular error string)
# hdftShowStderrResult  - subroutine input param: status, task id, pattern in stderr, [success msg], [failure msg].  Use JOB env. Will update the global HDFT_TASKS_RESULT
#     failmsg is optional. If not provided, passmsg will be used
###########################################
function hdftShowStderrResult {
    stat=$1 ; taskid=$2; stderrPattern=$3; passmsg=$4 ; failmsg=$5			## PARAM

    if [ -z "$failmsg" ] ; then
	failmsg=$passmsg	# passmsg could be "" too
    fi

    # consider an error if stat is not defined or null
    if [ -z "$stat" ] ; then
	stat=1
    fi

    (( HDFT_TASKS_RESULT = $HDFT_TASKS_RESULT + $stat ))

    if [ "$stat" == 0 ] ; then
	hdft_dumpmsg "    [Pass - $taskid]: OK  in xact [subtask cker $JOB $taskid - pass test: $passmsg; got stderr string: $stderrPattern]"
    else
	hdft_dumpmsg "    [Fail - $taskid]: BAD in xact [subtask cker $JOB $taskid - fail test: $failmsg; did not get stderr string: $stderrPattern]"
    fi
}

###########################################
# Function to show the result of exec of task 
# hdftShowExecResult  - subroutine input param: status, task id, [success msg], [failure msg].  Use JOB env. Will update the global HDFT_TASKS_RESULT
#     failmsg is optional. If not provided, passmsg will be used
###########################################
function hdftShowExecResult {
    stat=$1 ; taskid=$2; passmsg=$3 ; failmsg=$4			## PARAM

    if [ -z "$failmsg" ] ; then
		failmsg=$passmsg	# passmsg could be "" too
    fi

    # consider an error if stat is not defined or null
    if [ -z "$stat" ] ; then
		stat=1
    fi

    (( HDFT_TASKS_RESULT = $HDFT_TASKS_RESULT + $stat ))

    if [ "$stat" == 0 ] ; then
		hdft_dumpmsg "    [Pass - $taskid]: OK  in xact [subtask exec ${JOB}- $passmsg]"
    else
		hdft_dumpmsg "    [Fail - $taskid]: BAD in xact [subtask exec ${JOB}- $failmsg]"
    fi
}


###########################################
# Given two md5sum, generate the appropriate msg 
###########################################
function hdftDiffMd5 {
    taskid=$1; md5Expected=$2; md5Actual=$3 ; passmsg=$4; failmsg=$5			## PARAM

    if [ -z "$failmsg" ] ; then
		failmsg=$passmsg	# passmsg could be "" too
    fi

    (( HDFT_TASKS_RESULT = $HDFT_TASKS_RESULT + $stat ))

    if  [ "$md5Expected" == "$md5Actual" ] ; then
		hdft_dumpmsg "    [Pass - $taskid]: OK  in xact [subtask data $JOB - pass test: md5sum matches expected $md5Expected. $passmsg]"
		returnStat=0
    else
		hdft_dumpmsg "    [Fail - $taskid]: BAD in xact [subtask data $JOB - fail test: md5sum mismatch: expected $md5Expected. Got $md5Actual. $failmsg]"
		returnStat=1
    fi
	return $returnStat
}

###########################################
# Function to display the result of cmp of data. Data is typically generated during runtime, and already compared against certain pattern 
# hdftShowDataResult  - subroutine input param: status, task id, pattern, passmsg, failmsg.  Use JOB env. Will update the global HDFT_TASKS_RESULT
###########################################
function hdftShowDataResult {
    stat=$1 ; taskid=$2; pattern=$3; passmsg=$4 ; failmsg=$5			## PARAM

    if [ -z "$failmsg" ] ; then
		failmsg=$passmsg	# passmsg could be "" too
    fi

	patternPassMsg=""
	patternFailMsg=""
	if [ -n "$pattern" ] ; then
		patternPassMsg="$pattern found."
		patternFailMsg="$pattern not found."
	fi

    # consider an error if stat is not defined or null
    if [ -z "$stat" ] ; then
		stat=1
    fi

    (( HDFT_TASKS_RESULT = $HDFT_TASKS_RESULT + $stat ))

    if [ "$stat" == 0 ] ; then
		hdft_dumpmsg "    [Pass - $taskid]: OK  in xact [subtask data $JOB - pass test: $patternPassMsg $passmsg]"
    else
		hdft_dumpmsg "    [Fail - $taskid]: BAD in xact [subtask data $JOB - fail test: $patternFailMsg $failmsg]"
    fi
}


###########################################
# Function to show the result of diff of results
# hdftShowDiffResult  - subroutine input param: status, task id, [first file], [second file].  Use JOB env. Will update the global HDFT_TASKS_RESULT
###########################################
function hdftShowDiffResult {
    stat=$1 ; taskid=$2; file1=$3 ; file2=$4			## PARAM

    if [ -z "$failmsg" ] ; then
	failmsg=$passmsg
    fi

    (( HDFT_TASKS_RESULT = $HDFT_TASKS_RESULT + $stat ))

    if [ "$stat" == 0 ] ; then
		hdft_dumpmsg "    [Pass - $taskid]:  OK  in xact [diff $JOB $taskid - Pass test: $passmsg]"
		#hdft_dumpmsg "Pass in [$taskid - diff]:  OK  in $JOB subtask diff - [pass $taskid $passmsg]"
    else
	case $stat in 
	1)
		hdft_dumpmsg "    [Fail - $taskid]:  BAD in xact [subtask diff $JOB $taskid - fail test: $passmsg: output file mismatch $file1  $file2]"
		;;
	2)
		hdft_dumpmsg "    [Fail - $taskid]:  BAD in xact [subtask diff $JOB $taskid - fail test: $passmsg: file1 does not exist $file1]"
		;;
	3)
		hdft_dumpmsg "    [Fail - $taskid]:  BAD in xact [subtask diff $JOB $taskid - fail test: $passmsg: file2 does not exist $file2]"
		;;
	*)	
		hdft_dumpmsg "    [Fail - $taskid]:  BAD in xact [subtask diff $JOB $taskid - fail test: $passmsg: unknown error $file1  $file2]"
		;;
	esac
    fi
}

###########################################
###########################################
# Function to check the result of a test task run
# hdftCheckTaskResult  - subroutine input param: status, taskid, msg on success, msg on failure. Use JOB env. Will update the global HDFT_TASKS_RESULT
#
###########################################
function hdftShowTaskResult {
    stat=$1; taskid=$2; passmsg=$3; failmsg=$4			## PARAM

    if [ -z "$failmsg" ] ; then
		failmsg=$passmsg
    fi

    if [ -z "$stat" ] ; then
		stat=1
    fi 

    (( HDFT_TASKS_RESULT = $HDFT_TASKS_RESULT + $stat ))

    if [ "$stat" == 0 ] ; then
		hdft_dumpmsg "    [PASS - $taskid]: OK  in XACT [run  $JOB $taskid - pass test: $passmsg]"
		hdft_dumpmsg "#TK#|PASS | $taskid| $cmdmsg | $passmsg |"

    else
		hdft_dumpmsg "    [FAIL - $taskid]: BAD in XACT [run  $JOB $taskid - fail test: $failmsg]"
		hdft_dumpmsg "#TK#|FAIL | $taskid| $cmdmsg | $failmsg |"
    fi
	$HDFT_TOP_DIR/hdft_reportStatus.sh $stat  $taskid $cmdmsg $failmsg

}

# call $HDFT_TOP_DIR/hdft_reportStatus.sh COMMAND_EXIT_CODE=$1 TESTCASENAME=$2 TESTCASE_DESC=$3 REASONS=$4

function hdftShowBriefTaskResult {
    stat=$1; taskid=$2; cmdmsg=$3; passmsg=$4; failmsg=$5			## PARAM

    if [ -z "$failmsg" ] ; then
		failmsg=$passmsg
    fi

    if [ -z "$stat" ] ; then
		stat=1
    fi 

    (( HDFT_TASKS_RESULT = $HDFT_TASKS_RESULT + $stat ))

    if [ "$stat" == 0 ] ; then
		hdft_dumpmsg "    [PASS - $taskid]: OK  in XACT [$cmdmsg] [$passmsg]"
		hdft_dumpmsg "#TK#|PASS | $taskid| $cmdmsg | $passmsg |"
    else
		hdft_dumpmsg "    [FAIL - $taskid]: BAD in XACT [$cmdmsg] [$failmsg]"
		hdft_dumpmsg "#TK#|FAIL | $taskid| $cmdmsg | $failmsg |"
    fi

	echo "    $HDFT_TOP_DIR/hdft_reportStatus.sh $stat  $taskid $cmdmsg $failmsg"
	$HDFT_TOP_DIR/hdft_reportStatus.sh $stat  $taskid $cmdmsg $failmsg

}

# most param are passed as 'global'
function hdftReturnTaskResult {
    stat=$1; taskid=$2; cmdmsg=$3; passmsg=$4; failmsg=$5			## PARAM
    # stat=$1; passmsg=$2; failmsg=$5			## PARAM

    if [ -z "$failmsg" ] ; then
		failmsg=$passmsg
    fi

    if [ -z "$stat" ] ; then
		stat=1
    fi 

    (( HDFT_TASKS_RESULT = $HDFT_TASKS_RESULT + $stat ))

    if [ "$stat" == 0 ] ; then
		hdft_dumpmsg "    [PASS - $taskid]: OK  in XACT [$cmdmsg] [$passmsg]"
		hdft_dumpmsg "#TK#|PASS | $taskid| $cmdmsg | $passmsg |"
    else
		hdft_dumpmsg "    [FAIL - $taskid]: BAD in XACT [$cmdmsg] [$failmsg]"
		hdft_dumpmsg "#TK#|FAIL | $taskid| $cmdmsg | $failmsg |"
		setFailCase "FAILED in $taskid [$cmdmsg]"
    fi

	#echo "    $HDFT_TOP_DIR/hdft_reportStatus.sh $stat  $taskid $cmdmsg $failmsg"

	#XX# COMMAND_EXIT_CODE=$stat
	#XX# REASONS=$cmdmsg

 	# For Testcase name starts with DUP or NOP, they are really just helper test: no need to report
 	TID3=`echo "$taskid" | cut -c1-3`
 	if [ -z "$TID3" ] || [ "$TID3" == "NOP" ] || [ "$TID3" == "DUP" ] ; then
 		echo "    NOP/DUP cases $tsskid. Ignored in the tally of total cases."
 		return
 	fi

	echo "    $taskid REASONS = $REASONS"

	#displayTestCaseResult $taskid [$TESTCASENAME]   # rely on env var TESTCASENAME and COMMAND_EXIT_CODE (set by setFailCase)
	# The following check is to eliminate the redundant id like: FED_DCPY_100_FED_DCPY_100
	if [ -z "$TESTCASENAME" ] || [ "$TESTCASENAME" != "$taskid" ] ; then
		displayTestCaseResult $taskid 
	else
		displayTestCaseResult ""
	fi
	# $HDFT_TOP_DIR/hdft_reportStatus.sh $stat  $taskid $cmdmsg $failmsg

}

# display msg for a new test group  (a script can contain several test groups, each group contains many tests)
function hdftDisplayTestGroupMsg {
		local msg=$1		## PARAM
		hdft_dumpmsg "############################### Test Group: $msg ##############################################"
		hdft_dumpmsg "############################### Test Group: $msg ##############################################"
		hdft_dumpmsg "#TH#|*Status* | *TestId*| *Test Command for ${msg}*| *Test Notes* |"
}




###########################################
# A thin wrapper to do print out the status
# hdftDisplayTasksStatus  - subroutine input param: status taskName
# Example of call: hdftDisplayTasksStatus $LOCAL_DIFF_RESULT checkTestDirRdOnly
###########################################
function hdftDisplayTasksStatus {
	stat=$1; taskName=$2;		## PARAM
	if [ "$stat" == 0 ] ; then
	   hdft_dumpmsg "____XACT____ TASKS PASSED: $taskName"
	else
	   hdft_dumpmsg "____XACT____ TASKS FAILED: $taskName. Result=$stati"	
	fi
}


###########################################
# Given an output file, do a sed on item that print out date/time, and normalize the date/time string for comparison
# NOTE: Change is saved back to the original file
###########################################

function hdftFilterOutput {
	file=$1; type=$2;		## PARAM
	if [ -z "$file" ] ; then
		return 1
	fi
	if [ -z "$type" ] ; then
		type="std"
	fi

	TFILE=`/bin/mktemp -p /tmp "hdftmp_XXXXX"`

	hdft_dumpmsg "    hdftFilterOutput -- type=$type; mv $file $TFILE "
	mv $file $TFILE

	# fsck output can only grep on status: HEALTH. Anything else may change from day to day
	case $type in 
	"fsck")
		fgrep '....Status: HEALTHY' $TFILE   > $file
		;;
	
	# filter out the WARNING related to deprecated items, then do sed to normalize date/time
	"std" | *)
		hdft_dumpmsg "    hdftFilterOutput -- sed -f ${HDFT_TOP_DIR}/src/hdft_date.sed $TFILE | fgrep -v -f ${HDFT_TOP_DIR}/src/etc/grep-v.txt \> $file"
		sed -f ${HDFT_TOP_DIR}/src/hdft_date.sed $TFILE  | fgrep -v -f ${HDFT_TOP_DIR}/src/etc/grep-v.txt > $file
		## sed -f ${HDFT_TOP_DIR}/src/hdft_date.sed $TFILE  | fgrep -v 'WARN conf.Configuration: mapred.task.id is deprecated.' > $file
		;;
	esac
	stat=$?
	rm $TFILE
	return $stat
}


###########################################
# Do a diff with check on the existence of the files
# hdftDoDiff  - subroutine input param: file1 file2. Do not use any Env Var
# example of call: hdftDoDiff file1 file2
# Return: 0 on total match; 1 on diff; 2: first file does not exist; 3: second file does not exist
###########################################
function hdftDoDiff {
	file1=$1; file2=$2; 		## PARAM
	
	stat=0
	if [ -n "HDFT_VERBOSE" ] && [ "HDFT_VERBOSE" == 1 ] ; then
		hdft_dumpmsg  "    === do diff $file1 $file2"
	fi
	if [ -z "$file1" ] || [ ! -f "$file1" ] ; then
	    hdft_dumpmsg "    error in hdftDoDiff: first file [$file1] does not exist"
	    stat=2
	fi
	if [ -z "$file2" ] || [ ! -f "$file2" ] ; then
	    hdft_dumpmsg "    error in hdftDoDiff: second file [$file2] does not exist"
	    stat=3
	fi
	if [ $stat != 0  ]; then
		return $stat
	fi

	diff -q $file1 $file2
	stat=$?
	if [ $stat != 0 ] ; then
		diff $file1 $file2 | head -10 | cat -n
	fi

	return $stat
}

###########################################
# Do a diff after sed with the appropriate sed filter
# hdftDoDiff  - subroutine input param: file1 file2. Do not use any Env Var
# example of call: hdftDoDiff file1 file2
# Return: 0 on total match; 1 on diff; 2: first file does not exist; 3: second file does not exist
###########################################
function hdftDoFilteredDiff {
	file1=$1; file2=$2; type=$3; stat=0; 	## PARAM

	if [ -n "HDFT_VERBOSE" ] && [ "HDFT_VERBOSE" == 1 ] ; then
		hdft_dumpmsg  "    === do diff $file1 $file2"
	fi
	if [ -z "$file1" ] || [ ! -f "$file1" ] ; then
	    hdft_dumpmsg "    error in hdftDoDiff: first file [$file1] does not exist"
	    stat=2
	fi
	if [ -z "$file2" ] || [ ! -f "$file2" ] ; then
	    hdft_dumpmsg "    error in hdftDoDiff: second file [$file2] does not exist"
	    stat=3
	fi
	if [ $stat != 0  ]; then
		return $stat
	fi
	hdftFilterOutput $file1 $type
	hdftFilterOutput $file2 $type
	diff -q $file1 $file2
	stat=$?
	if [ $stat != 0 ] ; then
		diff $file1 $file2 | head -10 | cat -n
	fi

	return $stat
}

###########################################
# Map stat to either 0 (OK/PASS) or 1 (BAD/FAIL), based on type of mapping
# Input: stat, "diff" | "neg" | "kbneg" |permneg; return mapped status
###########################################
function hdftMapStatus {
	stat=$1; maptype=$2;		## PARAM
	case "$maptype" in 
	"kbneg")		# kb negative case expect 255
		if [ $stat == 255 ] ; then
			return 0
		else
			return 1
		fi
		;;
	"neg"|"NEG")			# reverse the status for negative test. 
		if [ $stat == 0 ] ; then
			return 1
		else
			return 0
		fi
		;;
	"diff")			# mapp 1, 2, 3 and * to 1
		if [ $stat == 0 ] ; then
			return 0;
		else
			return 1
		fi
		;;
	"perm")		# standard status == 0 ==> good
		if [ $stat == 0 ] ; then
		 	return 0
		else
			return 1
		fi
		;;
	"permneg")
		if [ $stat == 255 ] ; then
		 	return 0
		else
			return 1
		fi
		;;
	*)
		if [ ! -z $stat ] && [ $stat == '0' ] ; then
			return 0;
		else
			return 1;
		fi
		;;

	esac

}

###########################################
# Grep if the patern is in the file. May need to go through sed
# Input: type of pattern, file
###########################################
function hdftGrepPattern {
	typeOfPattern=$1; file=$2		## PARAM
	if [ $typeOfPattern == "kbsec" ]; then
		echo fgrep -f $HDFT_TOP_DIR/src/etc/grep-kbsecurity.txt $file
		fgrep -f $HDFT_TOP_DIR/src/etc/grep-kbsecurity.txt $file
	fi
	return $?
}


###########################################
# parm: one of the three: no, expired, long-expired
###########################################
function hdftSetupBadKInit {
	crentialType=$1		## PARAM

	echo "    Exec kdestroy to remove KB tickets"
	sleep 3
	hdftKDestroy
	klist 

	case $crednetialType in
	"no")
		return
		;;
	"expired")
		NDAY=1
		;;
	"long-expired")
		NDAY=10
		;;
	*)
		NDAY=1
	esac

	LOCAL_CUR_DATE=`date "+%s"`
	(( LOCAL_nDAY_AGO = $LOCAL_CUR_DATE - ${NDAY} * 86400 ))
	LOCAL_DATE_nDAY_AGO=`date -d@${LOCAL_nDAY_AGO} "+%Y%m%d%H%M%S" `
	echo "Current Date=`date`; epoch=$LOCAL_CUR_DATE; epoch 24 hours ago: $LOCAL_nDAY_AGO; date 24 hours ago: $(LOCAL_DATE_nDAY_AGO)"

	# get 30 seconds lease, start 24 hr ago - thus this would have been expired
	##kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab -s ${LOCAL_DATE_1DAY_AGO} -l 30s hadoopqa
	echo "    hdftSetupBadKInit: Getting Special KB Ticket: hdftKInit -s ${LOCAL_DATE_nDAY_AGO} -l 30s "
	hdftKInit -s ${LOCAL_DATE_nDAY_AGO} -l 30s 

	# echo "    hdftSetupBadKInit: Exec klist to show that KB Ticket should expire N hours ago:"
	klist
	sleep 3


}

###########################################
function hdftCleanupBadKDestroy {
	hdftKDestroy
	klist
	#hdftKInit
}

###########################################
function hdft_getHadoopVersion {
	ID_OUTPUT=`hadoop version`
	if [ $? != 0 ] ; then
		echo "BAD_HADOOP_VERSION";
		return 127
	else
		HVERSION=`echo $ID_OUTPUT | grep 'Hadoop 0' | awk -F. '{print $2}'	`
		echo $HVERSION
		return 0
	fi
}

###########################################
#return hadoop for version 20, and hdfs for version 22
# adding case for version 23
###########################################
function hdft_getDfsCommand {
	HVERSION=`hdft_getHadoopVersion`
	case $HVERSION in 
	20)
		echo 'hadoop'
		;;
	*)
		echo 'hdfs'
		;;	
	esac

}


###########################################
function hdft_getNN {
	PARAM=$1
	HVERSION=`hdft_getHadoopVersion`
	case $HVERSION in 
	20)
		hdft_getNN_V20 $PARAM
		;;
	*)
		hdft_getNN_V22 $PARAM
		;;	
	esac
}

###########################################
# two ways to look at: core-site.xml for fs.default.name like: hdfs://gsbl90609.blue.ygrid.yahoo.com:8020
# or hdfs-site.xml for dfs.http.address which <value>gsbl90609.blue.ygrid.yahoo.com:50070</value>
###########################################
function hdft_getNN_V20 {
	NN_dfs=`cat < $HADOOP_CONF_DIR/hdfs-site.xml | grep -A 3 dfs.http.address | fgrep '<value>' | awk -F. '{print $1}' | awk -'F>' '{print $2}' `
	NN_fs=`cat < $HADOOP_CONF_DIR/core-site.xml  | grep -A 3 fs.default.name  | fgrep '<value>' | awk -F. '{print $1}' | awk -'F/' '{print $3}' `
	# echo "    hdft_getNN_V20: NN_dfs=$NN_dfs"
	# echo "    hdft_getNN_V20: NN_fs=$NN_fs"
	if [ "$NN_dfs" == "$NN_fs" ] ; then
		echo "$NN_dfs"
	else
		# echo "WARNING: NN_dfs=$NN_dfs is different from NN_fs $NN_fs. Choose NN_dfs"
		if [ -n "$NN_fs" ] ; then
			echo $NN_fs
		else 
			echo $NN_dfs
		fi
	fi
}

###########################################
# use the new hdfs getcong -namenodes command 
###########################################
function hdft_getNN_V22 {
        # <value>gsbl90772,gsbl90773,gsbl90774</value>
        ##echo "getNN:: grep -A 2 dfs.federation.nameservices $HADOOP_CONF_DIR/${CLUSTER}.namenodeconfigs.xml "
        NN_List=`grep -A 2 dfs.federation.nameservices $HADOOP_CONF_DIR/${CLUSTER}.namenodeconfigs.xml | fgrep '<value>' | sed -e 's#<value>##' -e 's#</value>##'`
		NN_List=`hdfs --config $HADOOP_CONF_DIR  getconf -namenodes`
		MM0=`echo $NN_List | awk  '{print $1}' `
        case "$1" in		## PARAM. Also HADOOP_CONF_DIR and CLUSTER
        "0"|""|"default")
                NN=`echo $NN_List | awk  '{print $1}' `
                ;;
        "1")
                NN=`echo $NN_List | awk  '{print $2}' `
                ;;
        "2")
                NN=`echo $NN_List | awk  '{print $3}' `
                ;;
        *)
                NN=`echo $NN_List | awk  '{print $1}' `
                ;;
        esac
		# a system may have less than 3 NN. In that case, return the default NN (first one)
		if [ -z "$NN" ] ; then
			NN=$MM0
		fi
		# exit if no default name node can be found
		if [ -z "$NN" ] ; then
			echo "FATAL ERROR: hdft_getNN_V22 default namenode is null. Test aborted"
			echo "FATAL ERROR: hdft_getNN_V22 default namenode is null. Test aborted"
			echo "FATAL ERROR: hdft_getNN_V22 default namenode is null. Test aborted"
			exit 1
		fi

		# now strip off the .blue.ygrid.yahoo.com
        MM=`echo $NN | awk -F. '{print $1}'`
		echo "$MM"
}

###########################################
# No good way to find out the display name of the super user. This is the closest. Returned hdfsqa by default
# looking for pattern in : /grid/0/gs/gridre/yroot.omegab/conf/hadoop//deploy.omegab.confoptions.sh:   -set HadoopConfiggeneric10nodeblue.TODO_HDFSUSER=hdfsqa
# another option is to look at the user who runs namenode process on NN
# yet another option is to figure out how  supergroup  maps hdfs group to hdfs or hdfsqa
###########################################
function hdft_get_HDFSUser {
        F=$HADOOP_CONF_DIR/deploy.${CLUSTER}.confoptions.sh     	## PARAM: 
        HUser=`cat < $F | fgrep HadoopConfiggeneric10nodeblue.TODO_HDFSUSER= | awk -F= '{print $2}' | awk '{print $1}' `
        if [ $? != 0 ] || [ -z "$HUser" ] ; then
                echo "hdfsqa"
        else 
                echo $HUser
        fi      
}

###########################################
# The equivalent of rm -f on a hdfs system
# Input: a list of files/dir to be remove -f
# output: 0 if all removed; non-zero=> failure
##########################################
# parameters: a list of files/dir to be remove, like rm -f
function hdftRmfHdfsDirs {

    local stat=0        # non-zero => failure
    for Dir in  $* ; do
        echo "    Working on Dir/file $Dir..."
        ${MY_HDFS_CMD}  dfs -test -e $Dir
        if [ $? == 0 ] ; then
            echo "    Dir/file exists. First remove $Dir"
            ${MY_HDFS_CMD}  dfs -rmr -skipTrash $Dir
   
            # Now make sure the directories does not exist now. Failure
            if [ $? != 0 ] ; then
                echo "    Fail to remove dir/file $Dir"
                (( stat = stat + 1 ))
            else
                ${MY_HDFS_CMD}  dfs -test -e $Dir
                    # should not exist after rm
                    if [ $? == 0 ]  ; then
                        (( stat = stat + 1 ))
                    fi
            fi
        fi
    done
    return $stat
}

function hdftSetPath {
	if [ "$HADOOPQE_TEST_BRANCH" == "0.20" ] ; then
		export PATH=$HADOOP_HOME/bin:$PATH
	else
		export PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_MAPRED_HOME/bin:$HADOOP_HDFS_HOME/bin:$PATH
	fi	
}


# Abort test upon fatal error: param1: test case name
function hdftAbortTest {
			TESTCASENAME=$1
			displayTestCaseMessage $TESTCASENAME
			echo "FATAL ERROR in $TESTCASENAME. Abort Test."
			echo "FATAL ERROR in $TESTCASENAME. Abort Test."
			setFailCase "$TESTCASENAME failed"
			hdftReturnTaskResult 1 "TESTCASENAME" "$cmd" "Initial set up data of $MM failed"
			export SCRIPT_EXIT_CODE=1
			echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
			exit $SCRIPT_EXIT_CODE
}


# unit test
function test1nn {
	echo "Hdfs_getNN returns:  $(hdft_getNN )"
	echo "Hdft_getNN default returns : $(hdft_getNN default)"

	echo "Hdfs_getNN 0 returns: $(hdft_getNN 0)"
	echo "Hdfs_getNN 1 returns: $(hdft_getNN 1)"
	echo "Hdfs_getNN 2 returns: $(hdft_getNN 2)"
	echo "Hdfs_getNN 3 returns: $(hdft_getNN 3)"
}

hdftSetPath
test1nn
hdft_dumpmsg "OK from hdft_util2 lib"
