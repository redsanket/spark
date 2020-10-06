set +x

# "start the job tracker."
#
#
# (1) WORKAROUND for capacity-scheduler.jar
# (2) then ssh to the right machine and run job-tracker startup.
# (2) We run a small perl(1) script to get the output and parse it.
#
# Inputs: $STARTNAMENODE	(boolean)
# Inputs: $REMOVEEXISTINGDATA	(boolean)
# Inputs: $NAMENODE_Primary (set by installgrid.sh)
# Inputs: $cluster
#

if [ "$STARTYARN" != true ]; then
    echo "STARTYARN is not enabled. Nothing to do."
    return 0
fi

echo "== starting up yarn servers."

# GRIDCI-440, from RM node need to ssh to each NM as $MAPREDUSER with StrictHostKeyChecking=no
# in order to create known_hosts, else RM access fails
# Load the mapredqa credentials
set -x
ssh $jobtrackernode "sudo bash -c \"sshca-creds -u mapredqa load\""
ssh $jobtrackernode "sudo -su $MAPREDUSER bash -c \"\
export SSH_AUTH_SOCK=/tmp/.sshca_creds_agent/mapredqa.sock && PDSH_SSH_ARGS_APPEND='-o StrictHostKeyChecking=no' && $PDSH -w $SLAVELIST \\\"whoami && hostname\\\"\
\""
set +x

# GRIDCI-444 - nm health check for openstack
fanoutscp "$scriptdir/setup_nm_health_check_script.sh" "/tmp" "$SLAVELIST"
fanout_workers_root "sh /tmp/setup_nm_health_check_script.sh"

# GRIDCI-2885 - nm dockerd check for rhel7 nodes with docker enabled
fanoutscp "$scriptdir/setup_nm_dockerd_check_script.sh" "/tmp" "$SLAVELIST"
fanout_workers_root "sh /tmp/setup_nm_dockerd_check_script.sh"

# Install runc on all the nodemanagers that are not RHEL6
fanout_workers_root '[[ $(cut -d" " -f7 < /etc/redhat-release) =~ ^6. ]] || yum -y install runc'

# Setup cgroups on the worker nodes
tmpsetupfile=/tmp/setup_nm_cgroups.sh.$$
(
    echo "mkdir -p /cgroup/cpu"
    echo "if [[ \$(uname -r) =~ ^2\\. ]] && ! mountpoint -q /cgroup/cpu; then"
    echo "  set -x"
    echo "  mount -t cgroup -o cpu none /cgroup/cpu"
    echo "  mkdir /cgroup/cpu/hadoop-yarn"
    echo "  chown $MAPREDUSER:hadoop /cgroup/cpu/hadoop-yarn"
    echo "  set +x"
    echo "else"
    echo "  for i in \$(grep ^cgroup /proc/mounts | awk '{print \$2}'); do"
    echo "    set -x"
    echo "    mkdir -p \$i/hadoop-yarn"
    echo "    chown $MAPREDUSER:hadoop \$i/hadoop-yarn"
    echo "    set _x"
    echo "  done"
    echo "fi"
) > $tmpsetupfile
fanoutscp "$tmpsetupfile" "/tmp/setup_nm_cgroups.sh" "$SLAVELIST"

fanout_workers_root "sh /tmp/setup_nm_cgroups.sh"
# echo == "note short-term workaround for capacity scheduler (expires Sept 9)"
# echo "(cd ${yroothome}/share/hadoop ; cp contrib/capacity-scheduler/hadoop-*-capacity-scheduler.jar  .)" | ssh $jobtrackernode

fanout_root "/usr/local/bin/yinst set -root ${yroothome} \
$confpkg.TODO_CLIENTFACTORYMETHOD=org.apache.hadoop.mapred.YarnClientFactory \
$confpkg.TODO_MAPRED_CLIENTFACTORY_CLASS_NAME=mapreduce.clientfactory.class.name"

fanoutGW_root "/usr/local/bin/yinst set -root ${yroothome} \
$confpkg.TODO_CLIENTFACTORYMETHOD=org.apache.hadoop.mapred.YarnClientFactory \
$confpkg.TODO_MAPRED_CLIENTFACTORY_CLASS_NAME=mapreduce.clientfactory.class.name"


tmpfile="/tmp/xx.$$"
JAVA_HOME="$GSHOME/java/jdk64/current"
(
    echo "set -x"
    echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
    echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
    echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
    echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
    # workaround for one-day with older name for common, expires sept 9
    echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
    echo "export YARN_HOME=${yroothome}/share/hadoop"
    echo "export MAPREDUSER=$MAPREDUSER"
    echo "export USER=$MAPREDUSER"
    echo "export JAVA_HOME=$JAVA_HOME"
    echo "export SSH_AUTH_SOCK=/tmp/.sshca_creds_agent/mapredqa.sock"
    echo "export HADOOP_CLASSPATH=${yroothome}/share/hadoop/share/hadoop/server/*:${yroothome}/share/hadoop/share/hadoop/server/lib/*"
) > $tmpfile

set -x
(
    cat $tmpfile
    echo '$YARN_HOME/sbin/start-yarn.sh'
)  | ssh $jobtrackernode sudo -su $MAPREDUSER
set +x

echo "== starting up yarn JobHistoryServer."
set -x
(
    cat $tmpfile
    echo 'export YARN_OPTS="$YARN_OPTS -Dmapred.jobsummary.logger=INFO,JSA"'

    if [[ "$HADOOPVERSION" =~ ^3. ]]; then
      echo '$YARN_HOME/bin/mapred --daemon start historyserver'
    elif [[ "$HADOOPVERSION" =~ ^2. ]]; then
      echo '$YARN_HOME/sbin/mr-jobhistory-daemon.sh start historyserver'
    else
        echo "ERROR: Unknown HADOOPVERSION $HADOOPVERSION"
        exit 1
    fi

)  | ssh $jobtrackernode sudo -su $MAPREDUSER
set +x

echo "== starting up yarn TimelineServer."
set -x
(
    cat $tmpfile
    if [[ "$HADOOPVERSION" =~ ^3. ]]; then
      echo '$YARN_HOME/bin/yarn --daemon start timelineserver'
    elif [[ "$HADOOPVERSION" =~ ^2. ]]; then
      echo '$YARN_HOME/sbin/yarn-daemon.sh start timelineserver'
    else
        echo "ERROR: Unknown HADOOPVERSION $HADOOPVERSION"
        exit 1
    fi

)   | ssh $jobtrackernode sudo -su $MAPREDUSER
set +x
