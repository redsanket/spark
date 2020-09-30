#!/bin/bash

# This is the top level script called by the Jenkin's job for hadoop deployment.

# Set the CLUSTER value using the first command line argument if it is passed in.
# Otherwise, CLUSTER will default to the environment variable value.
if [ $# -gt 0 ]; then
    CLUSTER=$1
    export CLUSTER=$CLUSTER
fi

# Remove spaces in cluster name, make lowercase
CLUSTER=`echo $CLUSTER|tr -d ' '|tr '[:upper:]' '[:lower:]'`

# CLUSTER and cluster are used interchangeably
cluster=$CLUSTER

# Set DEBUG to 0 if not defined via the environment
DEBUG=${DEBUG:=0}

case "$CLUSTER" in
    *Fill*in*your*cluster*)
	echo "ERROR: Required CLUSTER value is not defined.  Exiting!!!"
	exit 1
	;;
    open*)
	export confpkg=HadoopConfigopenstacklargedisk
	export localconfpkg=hadooplocalconfigsopenstacklarge
	;;
    *)
	export confpkg=HadoopConfiggeneric10nodeblue
	export localconfpkg=hadooplocalconfigs
	;;
esac

# Check for other required parameters
if [ -z "$HADOOP_RELEASE_TAG" ]; then
    echo "ERROR: Required HADOOP_RELEASE_TAG value is not defined.  Exiting!!!"
    exit 1
fi

export PATH=$PATH:/home/y/bin64:/home/y/bin:/usr/bin:/usr/local/bin:/bin:/sroot:/sbin
[ $DEBUG -eq 1 ] && echo "PATH='$PATH'"

export DATESTRING=`date +%y%m%d%H%M`
set -o pipefail

# ROOT DIR. i.e where the deploy hadoop scripts are located. (.../deploySupport/)
if [ $WORKSPACE ]; then
    ROOT_DIR=$WORKSPACE/deploySupport
else
    ROOT_DIR=`dirname $(readlink -f $0)`
fi
cd $ROOT_DIR

# Setup a new artifacts directory
WORK_DIR="$ROOT_DIR/deployment"
[ -d $WORK_DIR ] && rm -rf $WORK_DIR
mkdir -p $WORK_DIR

echo "source $ROOT_DIR/setenv.sh"
source $ROOT_DIR/setenv.sh

# Default values, if not set by a Hudson/user environment variable.
echo "source $ROOT_DIR/deploy_default.sh"
source $ROOT_DIR/deploy_default.sh

source ${ROOT_DIR}/deploy_lib.sh
source ${ROOT_DIR}/000-shellfunctions.sh

# show environment variables
[ $DEBUG -eq 1 ] && /bin/env

# Check if dist_tag is valid. If not, exit.
# dist_tag list take on average about 3 minutes to run
# /home/y/bin/dist_tag basically fetch the content from
# http://edge.dist.corp.yahoo.com:8000/dist_get_tag?t=HADOOP_2_8_0_LATEST&os=rhel&q=1&ls=1
# The '&ls=1' is the part that takes a very long time.
# So we will curl the content directly ourselves.

# cmd="dist_tag list $HADOOP_RELEASE_TAG -timeout 300 -os rhel"
cmd="curl -s \"http://edge.dist.corp.yahoo.com:8000/dist_get_tag?t=${HADOOP_RELEASE_TAG}&os=rhel&q=1\"|cut -d' ' -f2- | sed 's/ /-/'"
echo "Process dist tag '$HADOOP_RELEASE_TAG'"
echo "$cmd"
DIST_TAG_LIST=`eval "$cmd"`
if [[ $? != "0" ]] || [[ -z $DIST_TAG_LIST ]]; then
    echo "ERROR: dist_tag list '$HADOOP_RELEASE_TAG' failed: '$DIST_TAG_LIST'; Exiting!!!"
    exit 1;
fi

# Parse the hadoop version
export FULLHADOOPVERSION=`echo $DIST_TAG_LIST | grep -o hadoopcoretree-[^\ ]* | cut -d'-' -f2`
if [ -z "$FULLHADOOPVERSION" ]; then
    echo "ERROR: Cannot determine hadoop version!!! Exiting!!!"
    exit 1
fi

# Parse the hadoop short version: e.g 2.6
export HADOOPVERSION=`echo $FULLHADOOPVERSION|cut -d. -f1,2`

YJAVA_JDK_VERSION=${YJAVA_JDK_VERSION:='qedefault'}

# need to get test version of coretree and hadoopCommonsDaemon for rhel7, 
# so pass it in explicitly later on, rhel6 installs should be fine with this...
# HADOOP_CORE_PKGS="hadoopcoretree hadoopgplcompression hadoopCommonsDaemon ytez_yarn_shuffle"
HADOOP_CORE_PKGS="hadoopgplcompression ytez_yarn_shuffle"

# For stack component deploys, make sure we have tools to talk to artifactory.
# We also dertermine the yspark_yarn_shuffle version using artifactory.
yinst i hadoop_releases_utils
if [ $? -ne 0 ]; then
    echo "Error: failed to install hadoop_releases_utils!"
    exit 1
fi

#################################################################################
# Deploy Hadoop
#################################################################################
whoami=`whoami`
hostname=`hostname`
pwd=`pwd`
timeline="$WORK_DIR/timeline.log"
export BUILD_DESC="Deploy to $CLUSTER with Hadoop version $FULLHADOOPVERSION ($HADOOP_RELEASE_TAG) from $hostname as $whoami"
echo "$BUILD_DESC"
echo "$BUILD_DESC" > $timeline
echo "Running ${pwd}/$0"

#################################################################################
# Set default values for SPARK parameters if not set via Jenkins
#################################################################################
# STACK_COMP_VERSION_SPARK
# Install Spark component on the Spark node using a reference cluster's package version(s)
# none - do not install (DEFAULT VALUE)
# LATEST - use version from Artifactory LATEST
# axonitered - use same version as on AR cluster.
STACK_COMP_VERSION_SPARK=${STACK_COMP_VERSION_SPARK:='none'}
SPARK_SHUFFLE_DIST_TAG=${SPARK_SHUFFLE_DIST_TAG:='same_as_STACK_COMP_VERSION_SPARK'}

if [[ $SPARK_SHUFFLE_DIST_TAG != "same_as_STACK_COMP_VERSION_SPARK" ]]; then
    export SPARK_SHUFFLE_VERSION=`dist_tag list $SPARK_SHUFFLE_DIST_TAG | head -1 | awk '{print $1}' | cut -d- -f2`
elif [[ $STACK_COMP_INSTALL_SPARK == true && $STACK_COMP_VERSION_SPARK != "none" ]]; then
    export SPARK_SHUFFLE_VERSION=`/home/y/bin/query_releases -c $STACK_COMP_VERSION_SPARK -b spark -p SPARK_DOT_LATEST`
elif [[ $SPARK_SHUFFLE_VERSION == "" ]]; then
    HADOOP_CORE_PKGS+=" yspark_yarn_shuffle"
fi

# if jdk is coming from Dist tag, add it to the HADOOP_CORE_PKGS list now so that its
# version can be pulled along with rest of core pkgs
if [[ $YJAVA_JDK_VERSION == "disttag" ]]; then
    echo "Using JDK version from Dist tag"
    HADOOP_CORE_PKGS+=" yjava_jdk"
fi

export HADOOP_MVN_PKGS="hadoop_mvn_auth hadoop_mvn_common hadoop_mvn_hdfs"
HADOOP_INSTALL_STRING=''
HADOOP_MVN_INSTALL_STRING_PKG=''
# JDK_QEDEFAULT=yjava_jdk-1.8.0_102.70
# need rhel7 compatible flavor of jdk
JDK_QEDEFAULT=yjava_jdk-8.0_8u232b09.2641196

for pkg in $HADOOP_CORE_PKGS; do
    HADOOP_INSTALL_STRING_PKG=`echo $DIST_TAG_LIST|grep -o $pkg-[^\ ]*`
    HADOOP_INSTALL_STRING+=" $HADOOP_INSTALL_STRING_PKG"
done

# gridci-1557, make jdk8 u102 the 'qedefault', so changing YJAVA_JDK_VERSION behavior to be:
#              if param empty OR 'qedefault', use u102
#              elif param has 'disttag', use Dist tagged pkg
#              else use the version sent in from jenkins
# gridci-1465, allow testing yjava_jdk version 8u102
if [[ $YJAVA_JDK_VERSION == "qedefault"  ]]; then
    echo "Using yjava_jdk $JDK_QEDEFAULT"
    HADOOP_INSTALL_STRING+=" $JDK_QEDEFAULT"
elif [[ $YJAVA_JDK_VERSION != "disttag" ]]; then 
    # use arbitrary jdk version sent in from jenkins
    echo "Using yjava_jdk $YJAVA_JDK_VERSION"
    # note: can't use '^', jenkins is inserting some chars in front of the choice list item
    if [[ $YJAVA_JDK_VERSION =~ "yjava_jdk-8" ]]; then
	HADOOP_INSTALL_STRING+=" $YJAVA_JDK_VERSION"
    else
	echo "Error: invalid YJAVA_JDK_VERSION value, expected starts-with yjava_jdk-8, got: $YJAVA_JDK_VERSION"
	exit 1
    fi
    # if neither qedefault or an arbitrary jdk was sent in, the base pkg 'yjava_jdk' was
    # set earlier and has already been added to HADOOP_CORE_PKGS
fi

# explicitly set coretree and hadoopCommonsDaemon for rhel7
# must do this to get the rhel7 compat version on test branch
# Again, rhel6 installs should be good with this
HADOOP_INSTALL_STRING+=" hadoopcoretree-$FULLHADOOPVERSION hadoopCommonsDaemon "

if [ -n "$SPARK_SHUFFLE_VERSION" ]; then
    HADOOP_INSTALL_STRING+=" yspark_yarn_shuffle-$SPARK_SHUFFLE_VERSION"
fi

# gridci-1566, remove the unneeded "|sed 's/ *//'" from below 'echo', this does 
# nothing and 'echo' itself reduces whitespace
HADOOP_INSTALL_STRING=`echo $HADOOP_INSTALL_STRING`
export HADOOP_INSTALL_STRING=$HADOOP_INSTALL_STRING

for pkg in $HADOOP_MVN_PKGS; do
    HADOOP_MVN_INSTALL_STRING_PKG=`echo $DIST_TAG_LIST|grep -o $pkg-[^\ ]*`
    HADOOP_MVN_INSTALL_STRING+=" $HADOOP_MVN_INSTALL_STRING_PKG"
done
HADOOP_MVN_INSTALL_STRING=`echo $HADOOP_MVN_INSTALL_STRING`
export HADOOP_MVN_INSTALL_STRING=$HADOOP_MVN_INSTALL_STRING

export HADOOP_CORETREE_INSTALL_STRING=`echo $DIST_TAG_LIST | grep -o hadoopcoretree-[^\ ]*`
export HADOOP_CONFIG_INSTALL_STRING=`echo $DIST_TAG_LIST | grep -o $confpkg-[^\ ]*`
export LOCAL_CONFIG_INSTALL_STRING=`echo $DIST_TAG_LIST | grep -o $LOCAL_CONFIG_PKG_NAME-[^\ ]*`

echo "Process dist tags for any applicable components"
if [ -n "$TEZ_DIST_TAG" ]; then
    export TEZVERSION=`dist_tag list $TEZ_DIST_TAG | grep ytez- | cut -d' ' -f1 | cut -d'-' -f2`
fi

if [ -n "$SPARK_DIST_TAG" ]; then
    export SPARKVERSION=`dist_tag list $SPARK_DIST_TAG | head -1 |awk '{print $1}' | cut -d- -f2`
fi

if [ -n "$SPARK_HISTORY_SERVER_DIST_TAG" ]; then
    export SPARK_HISTORY_VERSION=`dist_tag list $SPARK_HISTORY_SERVER_DIST_TAG | head -1 | awk '{print $1}' | cut -d- -f2`
fi

AUTO_CREATE_RELEASE_TAG=${AUTO_CREATE_RELEASE_TAG:=0}
if [ $AUTO_CREATE_RELEASE_TAG = 1 ]; then
    if [ -n "$CUST_DIST_TAG" ]; then
        export NEW_DIST_TAG="$CUST_DIST_TAG"_${DATESTRING}
    else
        export NEW_DIST_TAG=hadoop_2_0_${DATESTRING}
    fi
    echo "`date +%H:%M:%S` Clone dist tag '$HADOOP_RELEASE_TAG' to '$NEW_DIST_TAG':"
    dist_tag clone $HADOOP_RELEASE_TAG $NEW_DIST_TAG
    dist_tag add $NEW_DIST_TAG $HADOOP_INSTALL_STRING $HADOOP_CONFIG_INSTALL_STRING $LOCAL_CONFIG_INSTALL_STRING
fi
echo "Completed dist tag processing."

indent=30
echo "==="
printf "%-${indent}s %s\n" "===  Dist Tag" $HADOOP_RELEASE_TAG
printf "%-${indent}s %s\n" "===  Hadoop Version (full)" $FULLHADOOPVERSION
printf "%-${indent}s %s\n" "===  Hadoop Version (short)" $HADOOPVERSION
printf "%-${indent}s %s\n" "===  Tez Version" $TEZVERSION
printf "%-${indent}s %s\n" "===  Spark Version" $SPARKVERSION
printf "%-${indent}s %s\n" "===  Spark History Version" $SPARK_HISTORY_VERSION
printf "%-${indent}s %s\n" "===  Requested packages" "$HADOOP_INSTALL_STRING"
printf "%-${indent}s %s\n" "===  Requested configs" "$HADOOP_CONFIG_INSTALL_STRING"
printf "%-${indent}s %s\n" "===  Requested MVN packages" "$HADOOP_MVN_INSTALL_STRING"
echo "==="

export RUNSIMPLETEST=true

# side note: this removes any leftover cruft from a previous run. Hudson does not start 'clean'.
rm -f *.tgz > /dev/null 2>&1

# Make sure there is sufficient disk space before we install
banner "Make sure there is sufficient disk space before installation"
set -x
DU_THRESHOLD=${DU_THRESHOLD:=80}
set +x
# Proceed only if DU_THRESHOLD is a number between 0 and 100
if [[ "$DU_THRESHOLD" =~ ^[0-9]+$ ]] && [[ $DU_THRESHOLD -lt 100 ]] && [[ $DU_THRESHOLD -gt 0 ]] ; then
    banner "Make sure there is sufficient disk space before installation: Use% should be less than threshold of ${DU_THRESHOLD}% "
    set -x
    PDSH_SSH_ARGS_APPEND="$SSH_OPT" \
        /home/y/bin/pdsh -S -r @grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway "\
[[ \$(cat /etc/redhat-release | cut -d' ' -f7) =~ ^7 ]] && sudo yum install -y perl-Test-Simple; \
yinst install -br test -yes hadoop_qa_utils && sudo /home/y/bin/disk_usage -c -t $DU_THRESHOLD"
    RC=$?
    set +x
    if [[ $RC -ne 0 ]]; then
        echo "ERROR: Insufficient disk space on the cluster for install!!!"
        exit 1
    fi
fi

# Make sure rocl is installed on all nodes
banner "Make sure rocl is installed on all the nodes"
set -x
PDSH_SSH_ARGS_APPEND="$SSH_OPT" \
    /home/y/bin/pdsh -S -r @grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway 'if [ ! -x /home/y/bin/rocl ]; then yinst install -br test -yes rocl; fi'
RC=$?
set +x
if [[ $RC -ne 0 ]]; then
    echo "ERROR: rocl failed to installed on all the nodes"
    exit 1
fi

#################################################################################
# The 'set -e' option will cause the script to terminate immediately on error.
# The intent is to exit when the first build errors occurs.
# We do want to trap the error so it can be handle more gracefully, and provide
# information on the origination of the error.
#################################################################################
function error_handler {
    LASTLINE="$1"
    # echo "ERROR: Trapped error signal from caller [${BASH_SOURCE} line ${LASTLINE}]"
    # echo "ERROR: Trapped error signal from caller [${BASH_SOURCE[*]} line ${LASTLINE}]"

    # If BASH_SOURCE array size is greater than 1, show the next to last one
    if [ "${#BASH_SOURCE[*]}" -gt 1 ]; then
	echo "ERROR: Trapped error signal from caller [${BASH_SOURCE[-2]} line ${LASTLINE}]"
    else
	echo "ERROR: Trapped error signal from caller [${BASH_SOURCE[0]} line ${LASTLINE}]"
    fi
}
trap 'error_handler ${LINENO}' ERR

#################################################################################
# Deploy Hadoop
#################################################################################

#################################################################################
# Ported over from installgrid.sh
#################################################################################
export PATH="/home/y/bin64:/home/y/bin:$PATH"
echo "PATH=$PATH"

export MANIFEST=${WORK_DIR}/manifest.txt

rocl_cmd="/home/y/bin/rocl -P /var/lib/sia/keys/hadoopqa.jenkins.key.pem -C /var/lib/sia/certs/hadoopqa.jenkins.cert.pem"
$rocl_cmd -r %grid_re.clusters.${CLUSTER}% > ${WORK_DIR}/${CLUSTER}.rolelist.txt

# Initialize the nodes variables from rolesdb roles
# TODO: Pass STACK_COMP_INSTALL settings to cluster-list.sh so they can be
# used to determine if component nodes should be added to the host list.

# cluster
export role_list="$WORK_DIR/$CLUSTER.rolelist.txt"
cluster_list="$WORK_DIR/hostlist.$CLUSTER.txt"
cluster_list_tmp="$WORK_DIR/hostlist.$CLUSTER.txt.1"
worker_list="$WORK_DIR/slaves.$CLUSTER.txt"

# namenodes
nn_haalias_list="$WORK_DIR/namenodehaalias.$CLUSTER.txt"
nns_list="$WORK_DIR/nn.$CLUSTER.txt"
sns_list="$WORK_DIR/sn.$CLUSTER.txt"
nn_list="$WORK_DIR/namenodes.$CLUSTER.txt"
sn_list="$WORK_DIR/secondarynamenodes.$CLUSTER.txt"
all_nn_list="$WORK_DIR/allnamenodes.$CLUSTER.txt"

# hbase
hbase_list="$WORK_DIR/hbasemasternodes.$CLUSTER.txt"
hbase_region_list="$WORK_DIR/regionservernodes.$CLUSTER.txt"
hbase_zk_list="$WORK_DIR/hbasezookeepernodes.$CLUSTER.txt"

echo "source ${ROOT_DIR}/cluster-list.sh"
GDMVERSION=$GDMVERSION \
HBASEVERSION=$HBASEVERSION \
HCATVERSION=$HCATVERSION \
HDFSPROXYVERSION=$HDFSPROXYVERSION \
OOZIEVERSION=$OOZIEVERSION \
source ${ROOT_DIR}/cluster-list.sh
st=$?
[ "$st" -ne 0 ] && exit $st
setGridParameters $CLUSTER

webui_file="$WORK_DIR/webui.html"
generate_webui $webui_file

ALLHOSTS=`cat $cluster_list`
#echo "hostlist = '$ALLHOSTS'" | fmt
ALLNAMENODES=`cat $nn_list`
ALLSECONDARYNAMENODES=`cat $sn_list`
ALLNAMENODESAndSecondaries=`cat $all_nn_list`

NAMENODE_Primary=`head -1 $nn_list`
NAMENODEHAALIAS=`cat $nn_haalias_list`
HOSTLISTNOGW1=`grep -v $gateway $cluster_list`
HOSTLISTNOGW=`echo $HOSTLISTNOGW1 | tr ' ' ,`

[ -e $hbase_list ]    	  && export HBASEMASTERNODE=`cat $hbase_list`
[ -e $hbase_region_list ] && export REGIONSERVERNODES=`cat $hbase_region_list`
[ -e $hbase_zk_list ] 	  && export HBASEZOOKEEPERNODE=`cat $hbase_zk_list`

ALLSLAVES=`cat $worker_list`
# echo "slavelist = '$ALLSLAVES'" | fmt

if [ -z "$NAMENODE_Primary" ]; then
    echo "failure: No namenodes indicated in list."
    exit 1
#else
#        echo "namenode (primary): " $NAMENODE_Primary
#        echo "namenodes: " $ALLNAMENODES
fi

export WCALL=$cluster_list

yr=`date +%Y`
errs=0

export HOSTLIST=`echo $ALLHOSTS| tr ' ' ,`
export SLAVELIST=`echo $ALLSLAVES| tr ' ' ,`
export ALLNAMENODESLIST=`echo $ALLNAMENODES  | tr ' ' ,`
export ALLSECONDARYNAMENODESLIST=`echo $ALLSECONDARYNAMENODES  | tr ' ' ,`
export ALLNAMENODESAndSecondariesList=`echo $ALLNAMENODESAndSecondaries  | tr ' ' ,`

indent=30
			     printf "%-${indent}s %s\n" "===  namenode (primary)" $NAMENODE_Primary
			     printf "%-${indent}s %s\n" "===  namenode (all)" $ALLNAMENODES
[ -n "$NAMENODEHAALIAS" ] && printf "%-${indent}s %s\n" "===  namenode_alias" $NAMENODEHAALIAS
                             printf "%-${indent}s %s\n" "===  confpkg" $confpkg
                             printf "%-${indent}s %s\n" "===  HOSTLIST (all nodes)" $HOSTLIST
                             printf "%-${indent}s %s\n" "===  worker nodes (all)" $SLAVELIST

banner "Installing grid cluster $cluster"
index=1
START_STEP=${START_STEP:="0"}

scriptdir=/grid/0/tmp/deploy.$CLUSTER
[ -d $scriptdir ] || mkdir -p $scriptdir
#
# Review:  All scripts are copied to /grid/0/tmp/deploy.$CLUSTER.  This should be
# no problem, even if multiple deploys are happening, unless someone updates one
# of the scripts at just the 'right' moment before a second job starts. Still,
# it could be a slight exposure for concurrency.
#
echo "Copying scripts from ${YINST_ROOT}/conf/hadoop/hadoopAutomation/ to $scriptdir"
cp $ROOT_DIR/*.sh $scriptdir
cp $ROOT_DIR/*.pl $scriptdir

set -e

######### DEBUG ##############################
# ./001-create-conf-installscript.sh
# ./002-create-conf-runas.sh
# ./003-create-conf-killprocs.sh
# ./004-create-conf-rmoldpackage.sh
# ./005-create-conf-rmoldgateway.sh
banner "installgrid.sh: run create conf scripts to auto-generate scripts to be run later"
for script in ${ROOT_DIR}/[0-9][0-9]*-create-conf-*.sh; do
    banner "Running eval . $script"
    eval ". $script"
done

export EXIT_ON_ERROR=true

index=1
START_STEP=${START_STEP:="0"}
RUN_SINGLE_STEP=${RUN_SINGLE_STEP:="0"}

# Install Tez if enabled
if [[ "${INSTALL_TEZ}" == only ]]; then
    deploy_spark
    exit $?
fi

# Be careful not to name any variable in any downstream scripts with the name
# $script because it will override the $script variable here.
# for script in ${base}/[0-9][0-9]*-installsteps-*.sh
# Skip over HIT tests. HIT tests are not being run anymore.
for script in ${ROOT_DIR}/[0-9][0-9]*-installsteps-[^HIT]*.sh; do
    if [[  -e $script ]]; then
	script_basename=`basename $script`
	current_step=`echo $script_basename|cut -d'-' -f1|bc`
	if [[ $current_step -lt $START_STEP ]];then
            echo "SKIP deploy script: ${script_basename}: less than starting step '$START_STEP'"
            continue;
	fi

	set +x
	banner "START INSTALL STEP #$index: $script_basename"

	start=`date +%s`
	h_start=`date +%Y/%m/%d-%H:%M:%S`

	echo "Running $script"

	# For general shutdown and cleanup scripts, temporarily disable exit on failure.
	# also skip error on 170 since it returns nonzero on GW ssl cert update for kms
	# unfortunately +/-e in the step still propogated fail out
	SKIP_ERROR_ON_STEP="false"
	if ([[ $script =~ "100-" ]] || [[ $script =~ "101-" ]] || [[ $script =~ "140-" ]] || [[ $script =~ "170-" ]]); then
            echo "Skip exit on failure for script $script_basename"
            SKIP_ERROR_ON_STEP="true"
            set +e
	fi

	max_script_num=500
	################################################################################
	# $ ls -1 [0-9][0-9]*-installsteps-[^HIT]*.sh
	# 000-installsteps-explanation.sh
	# 100-installsteps-examineiptables.sh
	# 101-installsteps-killprocess.sh
	# 105-installsteps-createnewclusterkeytab.sh # nothing to do
	# 110-installsteps-checksshconnectivity.sh   # nothing to do
	# 111-installsteps-check_ssl_certs.sh
	# 115-installsteps-installhadooputils.sh
	# 120-installsteps-removerpmpackages.sh      # nothing to do
	# 140-installsteps-removeoldyinstpkgs.sh
	# 145-installsteps-mkyroot-gw.sh             # nothing to do
	# 150-installsteps-yinstselfupdate.sh        # nothing to do
	# 160-installsteps-removeexistingdata.sh
	# 165-installsteps-creategshome.sh
	# 167-installsteps-removeexistinglogs.sh
	# 170-installsteps-installmainpackages.sh
	# 180-installsteps-installlocalsave.sh
	# 189-installsteps-namenodeExplanation.sh
	# 190-installsteps-runNNkinit.sh
	# 200-installsteps-configureNN.sh
	# 205-installsteps-getClusterid.sh            # nothing to do
	# 210-installsteps-startNN.sh
	# 219-installsteps-jobtrackerExplanation.sh
	# 220-installsteps-configureJT.sh
	# 229-installsteps-installTez.sh
	# 230-installsteps-update-docker.sh
	# 231-installsteps-startyarn-RM.sh
	# 235-installsteps-jdk-gateway.sh
	# 240-installsteps-hbasesetup.sh               # nothing to do
	# 250-installsteps-testNN.sh
	# 255-installsteps-setstickybit.sh
	# 256-installsteps-KmsAndZookeeper.sh
	# 258-installsteps-runTezWordCount.sh
	# 261-installsteps-testRM.sh
	# 500-installsteps-runsimpletest.sh

	script_num=`echo $script_basename|cut -d'-' -f1`

	# Execute steps that are smaller than or equal to the max script num
	if (($script_num <= $max_script_num)); then
	    time . "$script" 2>&1 |tee "$WORK_DIR/${script_basename}.log"
	else
	    echo "Nothing to do: script_num=$script_num is less than max_script_num=$max_script_num"
	fi
	st=$?
	end=`date +%s`
	h_end=`date +%Y/%m/%d-%H:%M:%S`
	runtime=$((end-start))

        # exported value of CLUSTERID from 205-installsteps-getClusterid.sh is not sticking
	if ((($script_num == 205)) && [ -f /tmp/$cluster.clusterid.txt ]); then
	    export CLUSTERID=`cat /tmp/$cluster.clusterid.txt`
	    rm -rf /tmp/$cluster.clusterid.txt
	    echo "CLUSTERID=$CLUSTERID"
	fi

	# Turn exit on failure back on now
	if [[ "$SKIP_ERROR_ON_STEP" == "true" ]]; then
            set -e
	fi

	if [ "$st" -eq 0 ]; then
            status='PASSED'
	else
            status='FAILED'
	fi

	echo
	banner "END INSTALL STEP #$index: $status: $script_basename: status=$st"
	echo

	banner "CURRENT COMPLETED EXECUTION STEPS:"
	printf "# %-2s %-7s %-43s : %.0f min (%3.0f sec) : %s : %s : %s\n" \
	    $index $status $script_basename $(echo "scale=2;$runtime/60" | bc) $runtime $h_start $h_end $st >> $timeline
	cat $timeline
	echo

	if [ "$st" -ne 0 ]; then
            echo "EXIT_ON_ERROR=$EXIT_ON_ERROR"
            if [ "$EXIT_ON_ERROR" = "true" ]; then
		echo ">>>>>>>> EXIT ON ERROR <<<<<<<<<<" && exit $st
            fi
	fi
	index=$((index+1))

	if [ "$RUN_SINGLE_STEP" == true ]; then
	    break
	fi
    else
	echo "WARNING!!! deploy script $script not found!!!"
    fi
done
echo FinalStatus: $st

#################################################################################
# Install Stack Components
#################################################################################
banner "CHECK IF WE NEED TO INSTALL STACK COMPONENTS: pig, hive, spark, oozie"

# Deploy Pig
deploy_stack pig $STACK_COMP_VERSION_PIG pig-install-check.sh

# Deploy Hive
deploy_stack hive $STACK_COMP_VERSION_HIVE hive-install-check.sh

# Deploy Spark
if [[ $STACK_COMP_INSTALL_SPARK == true ]]; then
    deploy_spark
fi

# Deploy oozie after installing other stack components as it relies on them
deploy_stack oozie $STACK_COMP_VERSION_OOZIE oozie-install-check.sh

set -x
ls -l $WORK_DIR
set +x

# Review: note that the exit-status of the deploy is indeterminate, and seems to reflect the success of that final 'yinst-remove'.
exit $?
