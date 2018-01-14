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
if [ "$STARTYARN" = true ]
then
    echo == starting up yarn servers.

    # GRIDCI-440, from RM node need to ssh to each NM as $MAPREDUSER with StrictHostKeyChecking=no
    # in order to create known_hosts, else RM access fails
    ( echo "PDSH_SSH_ARGS_APPEND='-o StrictHostKeyChecking=no' $PDSH -w $SLAVELIST hostname" ) |\
ssh  $jobtrackernode su - $MAPREDUSER

    # GRIDCI-444 - nm health check for openstack
    fanoutcmd "scp $scripttmp/setup_nm_health_check_script.sh __HOSTNAME__:/tmp/" "$SLAVELIST"
    slavefanout "sh -x /tmp/setup_nm_health_check_script.sh" "$SLAVELIST"

    # GRIDCI-2885 - nm dockerd check for rhel7 nodes with docker enabled
    fanoutcmd "scp $scripttmp/setup_nm_dockerd_check_script.sh __HOSTNAME__:/tmp/" "$SLAVELIST"
    slavefanout "sh -x /tmp/setup_nm_dockerd_check_script.sh" "$SLAVELIST"

# echo == "note short-term workaround for capacity scheduler (expires Sept 9)"
#    echo "(cd ${yroothome}/share/hadoop ; cp contrib/capacity-scheduler/hadoop-*-capacity-scheduler.jar  .)" | ssh $jobtrackernode

    fanout "/usr/local/bin/yinst set -root ${yroothome} $confpkg.TODO_CLIENTFACTORYMETHOD=org.apache.hadoop.mapred.YarnClientFactory   $confpkg.TODO_MAPRED_CLIENTFACTORY_CLASS_NAME=mapreduce.clientfactory.class.name"
    fanoutGW "/usr/local/bin/yinst set -root ${yroothome} $confpkg.TODO_CLIENTFACTORYMETHOD=org.apache.hadoop.mapred.YarnClientFactory   $confpkg.TODO_MAPRED_CLIENTFACTORY_CLASS_NAME=mapreduce.clientfactory.class.name"
    tmpfile=/tmp/xx.$$

    JAVA_HOME="$GSHOME/java8/jdk64/current"

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
        echo "export JAVA_HOME=$JAVA_HOME"
     ) > $tmpfile

    set -x
    (
        cat $tmpfile
        echo '$YARN_HOME/sbin/start-yarn.sh'
    )  | ssh $jobtrackernode su - $MAPREDUSER
   set +x

    echo == starting up yarn JobHistoryServer.
    set -x
    (
        cat $tmpfile
        echo 'export YARN_OPTS="$YARN_OPTS -Dmapred.jobsummary.logger=INFO,JSA"'
        echo '$YARN_HOME/bin/mapred --daemon start historyserver'
    )  | ssh $jobtrackernode su - $MAPREDUSER
   set +x

   echo == starting up yarn TimelineServer.
   set -x
   (
	cat $tmpfile
	echo '$YARN_HOME/bin/yarn --daemon start timelineserver'
   )   | ssh $jobtrackernode su - $MAPREDUSER
   set +x
fi
