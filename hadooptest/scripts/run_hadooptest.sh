#!/bin/bash

function usage {
    echo 
    echo "USAGE"
    echo "-----------------------------------------------------------------------------------"
    echo "/bin/sh $0 <option1> <values1> <option2> <values2> ...
The options
        -c|--cluster    <cluster name> : cluster name
        [ -f|           <conf file>        ] : hadooptest configuration file
        [ -j|--java                        ] : run tests via java directly instead of via maven
        [ -m|--mvn                         ] : run tests via maven instead of via java directly
        [ -n|--nopasswd                    ] : no password prompt
        [ -p|--profile                     ] : maven profile
        [ -t|--test      <test suite(s)    ] : test suite name(s). use delimitor comma for mvn, and space for java
        [ -w|workspace  <workspace>        ] : workspace
        [ -h|--help                        ] : help

Example:
$ run_hadooptest --cluster theoden
$ run_hadooptest --cluster theoden --test TestSleepJobCLI
$ run_hadooptest --cluster theoden --mvn --test TestSleepJobCLI
$ run_hadooptest --cluster theoden --mvn --test TestSleepJobCLI -f /homes/hadoopqa/hadooptest.conf
$ run_hadooptest -c theoden -j -t hadooptest.regression.TestVersion -n
$ run_hadooptest -c theoden -p clover -m TestVersion
"
    exit 1
}

for arg; do 
    [[ "${arg:0:1}" == "-" ]] && delim="" || delim="\""
    if [[ "${arg:0:2}" == "--" ]]; 
       then args="${args} -${arg:2:1}" 
       else args="${args} ${delim}${arg}${delim}"
    fi
done

# reset the incoming args
eval set -- $args

PASSWORD_OPTION=${PASSWORD_OPTION:-"-password"}
DEFAULT_WS=`dirname $0`"/.."
WORKSPACE=${WORKSPACE:=$DEFAULT_WS"}

for arg; do 
    [[ "${arg:0:1}" == "-" ]] && delim="" || delim="\""
    if [[ "${arg:0:2}" == "--" ]]; 
       then args="${args} -${arg:2:1}" 
       else args="${args} ${delim}${arg}${delim}"
    fi
done

# reset the incoming args
eval set -- $args

USE_MVN=1
while getopts "c:f:t:w:jmnp:h" opt; do
	case $opt in
		c) CLUSTER=$OPTARG;;
		f) CONF=$OPTARG;;
        j) USE_MVN=0;;
        m) USE_MVN=1;;
		n) PASSWORD_OPTION="no-password";;
        p) PROFILE=$OPTARG;;
		t) TESTS=$OPTARG;;
		w) WORKSPACE=$OPTARG;;		
		h) usage;;
		*) usage;;
	esac
done

if [[ -z $CLUSTER ]]; then
   echo "ERROR: Required CLUSTER value not defined!!!";
   exit 1;
fi

DEFAULT_CONF=`ls -d ~/`"/hadooptest.conf"
CONF=${CONF:="$DEFAULT_CONF"}

DEFAULT_TESTS="\
hadooptest.regression.TestVersion,\
hadooptest.regression.yarn.TestSleepJobAPI\
"
# TESTS=${TESTS:="$DEFAULT_TESTS"}

# Setup Hadoop Env Variables (e.g. HADOOP_COMMON_HOME, HADOOP_CONF_DIR, etc

CLASSPATH="\
/home/y/lib/jars/junit.jar:\
/home/gs/gridre/yroot.$CLUSTER/conf/hadoop/:"

if [[ "$USE_MVN" -eq 0 ]]; then
    CLASSPATH="$CLASSPATH:\
$WORKSPACE/target/hadooptest-1.0-SNAPSHOT.jar:\
$WORKSPACE/target/hadooptest-1.0-SNAPSHOT-tests.jar"
fi

HADOOP_INSTALL="/home/gs/gridre/yroot.$CLUSTER"
HADOOP_JAR_PATHS="
$HADOOP_INSTALL/share/hadoop/share/hadoop/common/lib \
$HADOOP_INSTALL/share/hadoop-*/share/hadoop/common \
$HADOOP_INSTALL/share/hadoop-*/share/hadoop/common/lib \
$HADOOP_INSTALL/share/hadoop-*/share/hadoop/hdfs \
$HADOOP_INSTALL/share/hadoop-*/share/hadoop/hdfs/lib \
$HADOOP_INSTALL/share/hadoop-*/share/hadoop/yarn \
$HADOOP_INSTALL/share/hadoop-*/share/hadoop/yarn/lib \
$HADOOP_INSTALL/share/hadoop-*/share/hadoop/mapreduce \ 
$HADOOP_INSTALL/share/hadoop-*/share/hadoop/hdfs
"

for i in $HADOOP_JAR_PATHS
do
  CLASSPATH=${CLASSPATH}:`echo -en ${i}`/*
done
echo "CLASSPATH=$CLASSPATH"

# TODO: long term this should move into the java test framework
if [[ "$PASSWORD_OPTION" != 'no-password' ]]; then 
   USER=`whoami`
   if [[ $USER == 'hadoopqa' ]];then
      echo "Headless user 'hadoopqa' requires no password."
   else 
      stty -echo
      /usr/kerberos/bin/kinit $USER@DS.CORP.YAHOO.COM
      stty echo
   fi
fi

if [[ -n $TESTS ]]; then
	TESTS_OPT="-Dtest=$TESTS"
else 
	TESTS_OPT=""
fi

CLOVER_OPT="-Pclover -Djava.awt.headless=true"
if [[ -n $PROFILE ]]; then
    if [[ $PROFILE == "clover" ]]; then
      PROFILE_OPT=$CLOVER_OPT
    else
       PROFILE_OPT="-D$PROFILE"
    fi
else 
    PROFILE_OPT=""
fi

POM_OPT="-f pom-ci.xml"
MVN_SETTINGS_OPT="-gs $WORKSPACE/resources/yjava_maven/settings.xml.gw"
JAVA_LIB_PATH="-Djava.library.path=/home/gs/gridre/yroot.${CLUSTER}/share/hadoop/lib/native/Linux-amd64-64/"
COMMON_ARGS="\
-DCLUSTER_NAME=$CLUSTER \
-DWORKSPACE=$WORKSPACE \
-Dhadooptest.config=$CONF \
"

if [[ "$USE_MVN" -eq 1 ]]; then
    # RUN TESTS VIA MAVEN
    set -x
    if [[ ! -f /home/y/bin/mvn ]];then
		/usr/local/bin/yinst install yjava_maven -br test -yes
	fi    
    
# HADOOP_SHARE=/grid/0/gshome/gridre/yroot.${CLUSTER}/share/hadoop-${HADOOP_VERSION}/share/hadoop/ \
    CLUSTER=$CLUSTER \
HADOOP_VERSION=`ls -l /home/gs/gridre/yroot.${CLUSTER}/share/hadoop|cut -d">" -f2|cut -d"-" -f2` \
HADOOP_SHARE=/home/gs/gridre/yroot.${CLUSTER}/share/hadoop-${HADOOP_VERSION}/share/hadoop/ \
/home/y/bin/mvn $POM_OPT $MVN_SETTINGS_OPT $PROFILE_OPT clean test -DfailIfNoTests=false $TESTS_OPT $COMMON_ARGS
    set +x
else
    # RUN TESTS VIA JAVA
    # /home/y/bin/mvn clean package -Pdist -Dtar
    set -x
    /home/y/bin/java -cp $CLASSPATH $COMMON_ARGS $JAVA_LIB_PATH org.junit.runner.JUnitCore $TESTS
    set +x
fi
