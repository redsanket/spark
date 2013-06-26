#!/bin/bash

function usage {
    echo
    echo "USAGE"
    echo "-----------------------------------------------------------------------------------"
    echo "/bin/sh $0 <option1> <values1> <option2> <values2> ...
The options
        -c|--cluster <cluster name>          : cluster name
        [ -f|            <conf file>       ] : hadooptest configuration file
        [ -g|--gateway   <aateway>         ] : remote gateway host
        [ -i|--install                     ] : transport packages to remote host only
        [ -j|--java                        ] : run tests via java directly instead of via maven
        [ -m|--mvn                         ] : run tests via maven instead of via java directly
        [ -n|--nopasswd                    ] : no password prompt
        [ -p|--profile                     ] : maven profile
        [ -s|--source    <source root dir> ] : source root directory
        [ -t|--test      <test suite(s)    ] : test suite name(s)
        [ -u|--user      <remote user>     ] : remote user; e.g. hadoopqa
        [ -w|--workspace <workspace>       ] : remote workspace. Default remote workspace is 
                                               "/tmp/hadooptest-<REMOTE USER>-<CLUSTER>"
        [ -h|--help                        ] : help

Example:
$ run_hadooptest_remote --cluster theoden --gateway gwbl2003.blue.ygrid.yahoo.com --install
$ run_hadooptest_remote --cluster theoden --gateway gwbl2003.blue.ygrid.yahoo.com --mvn --install
$ run_hadooptest_remote --cluster theoden --java --install
$ run_hadooptest_remote --cluster theoden --mvn --install -u hadoopqa
$ run_hadooptest_remote --cluster theoden --mvn --workspace /tmp/foo --install -u hadoopqa
$ run_hadooptest_remote --cluster theoden --gateway gwbl2003.blue.ygrid.yahoo.com --mvn --test SleepJobRunner
$ run_hadooptest_remote -c theoden -i
$ run_hadooptest_remote -c theoden -n
$ run_hadooptest_remote -c theoden -s /home/hadoopqa/git/hadooptest/hadooptest -i
$ run_hadooptest_remote -c theoden -p clover -t TestVersion -u hadoopqa
"
    exit 1
}

for arg; do 
    [[ "${arg:0:1}" == "-" ]] && delim="" || delim="\""
    if [ "${arg:0:2}" == "--" ]; 
       then args="${args} -${arg:2:1}" 
       else args="${args} ${delim}${arg}${delim}"
    fi
done

# reset the incoming args
eval set -- $args

INSTALL_ONLY=0
USE_MVN=1
while getopts "c:f:g:s:t:u:w:ijmnp:h" opt; do
        case $opt in
                c) CLUSTER=$OPTARG;;
                f) CONF=$OPTARG;;
                g) REMOTE_HOST=$OPTARG;;
                i) INSTALL_ONLY=1;;
                j) USE_MVN=0;;
                m) USE_MVN=1;;
                n) PASSWORD_OPTION="-n";;
                s) LOCAL_WORKSPACE=$OPTARG;;
                p) PROFILE=$OPTARG;;
                t) TESTS=$OPTARG;;
                u) REMOTE_USER=$OPTARG;;
                w) REMOTE_WORKSPACE=$OPTARG;;
                h) usage;;
                *) usage;;
        esac
done

#shift $(($OPTIND - 1))
#echo "Remaining arguments are: %s\n" "$*"
#exit;

if [ -z $CLUSTER ]; then
   echo "ERROR: Required CLUSTER value not defined!!!";
   exit 1;
fi

# DEFAULT_REMOTE_HOST="gwbl2005.blue.ygrid.yahoo.com"
if [ -z $REMOTE_HOST ]; then
	rolename="grid_re.clusters.$CLUSTER.gateway"
	echo "gateway unspecified: fetch host from igor role: $rolename";
	if [ -f /home/y/bin/igor ];then
		DEFAULT_REMOTE_HOST=`/home/y/bin/igor fetch -members $rolename`
	else
		DEFAULT_REMOTE_HOST=`ssh re101.ygrid.corp.gq1.yahoo.com /home/y/bin/igor fetch -members $rolename`
	fi
fi
REMOTE_HOST=${REMOTE_HOST:=$DEFAULT_REMOTE_HOST}
echo "remote gateway host=$DEFAULT_REMOTE_HOST";

USER=`whoami`
REMOTE_USER=${REMOTE_USER:=`whoami`}
REMOTE_WORKSPACE=${REMOTE_WORKSPACE:="/tmp/hadooptest-$REMOTE_USER-$CLUSTER"}

if [ "$REMOTE_USER" == "hadoopqa" ]; then
# 	ssh -t $REMOTE_HOST "sudo rm -rf $REMOTE_WORKSPACE;";
	set -x
	ssh -t $REMOTE_HOST "if [ -d $REMOTE_WORKSPACE ]; then sudo /bin/rm -rf $REMOTE_WORKSPACE; fi";
	set +x
else
	set -x
	ssh -t $REMOTE_HOST "if [ -d $REMOTE_WORKSPACE ]; then /bin/rm -rf $REMOTE_WORKSPACE; fi";
	set +x
fi

set -x
ssh -t $REMOTE_HOST "/bin/mkdir -p $REMOTE_WORKSPACE";
set +x

LOCAL_WORKSPACE=${LOCAL_WORKSPACE:="/Users/$USER/git/hadooptest/hadooptest"}
LOCAL_WORKSPACE_TARGET_DIR="$LOCAL_WORKSPACE/target"

echo "CLUSTER='$CLUSTER'"
echo "LOCAL_WORKSPACE='$LOCAL_WORKSPACE'"
echo "REMOTE_HOST='$REMOTE_HOST'"
echo "REMOTE_WORKSPACE='$REMOTE_WORKSPACE'"
echo "TESTS='$TESTS'"
echo "USE_MVN='$USE_MVN'"
echo "INSTALL_ONLY='$INSTALL_ONLY'"

TGZ_DIR=/tmp
TGZ_FILE=hadooptest.tgz

set -x
tar -zcf $TGZ_DIR/$TGZ_FILE --exclude='target' -C $LOCAL_WORKSPACE .
scp $TGZ_DIR/$TGZ_FILE $REMOTE_HOST:$REMOTE_WORKSPACE
ssh -t $REMOTE_HOST "/bin/gtar fx $REMOTE_WORKSPACE/$TGZ_FILE -C $REMOTE_WORKSPACE";
set +x

set -x
ssh -t $REMOTE_HOST "/bin/mkdir -p $REMOTE_WORKSPACE/target";
scp $LOCAL_WORKSPACE_TARGET_DIR/*.jar $REMOTE_HOST:$REMOTE_WORKSPACE/target
set +x

if [ -z $TESTS ]
then
    TESTS_PARAM=""
else
    TESTS_PARAM="--test $TESTS"
fi

if [ -z $PROFILE ]
then
    PROFILE_PARAM=""
else
    PROFILE_PARAM="--profile $PROFILE"
fi

if [ -z $CONF ]
then
    CONF_PARAM=""
else
    CONF_PARAM="-f $CONF"
fi

if [ "$REMOTE_USER" == "hadoopqa" ]; then
	ssh -t $REMOTE_HOST "sudo chown -R hadoopqa $REMOTE_WORKSPACE;";
fi


COMMON_ARGS="--cluster $CLUSTER --workspace $REMOTE_WORKSPACE $PASSWORD_OPTION $PROFILE_PARAM $CONF_PARAM $TESTS_PARAM"

if [ "$INSTALL_ONLY" -ne 1 ]; then
    if [ "$USE_MVN" -eq 1 ]; then
      set -x
      rm -rf $LOCAL_WORKSPACE_TARGET_DIR/surefire-reports/
      ssh -t $REMOTE_HOST "cd $REMOTE_WORKSPACE; $REMOTE_WORKSPACE/scripts/run_hadooptest --mvn $COMMON_ARGS";
      # WE NEED TO COPY THE TEST RESULTS FROM THE GATEWAY BACK TO THE BUILD HOST
      scp -r $REMOTE_HOST:$REMOTE_WORKSPACE/target/surefire-reports $LOCAL_WORKSPACE_TARGET_DIR/
      if [ -n $PROFILE ]
      then
          scp -r $REMOTE_HOST:$REMOTE_WORKSPACE/target/clover $LOCAL_WORKSPACE_TARGET_DIR/
      fi
      set +x
    else 
      set -x
      ssh -t $REMOTE_HOST "$REMOTE_WORKSPACE/scripts/run_hadooptest --java $COMMON_ARGS";
      set +x
    fi
fi
