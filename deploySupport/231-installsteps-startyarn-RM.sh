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

# echo == "note short-term workaround for capacity scheduler (expires Sept 9)"
#    echo "(cd ${yroothome}/share/hadoop ; cp contrib/capacity-scheduler/hadoop-*-capacity-scheduler.jar  .)" | ssh $jobtrackernode

    fanout "/usr/local/bin/yinst set -root ${yroothome} $confpkg.TODO_CLIENTFACTORYMETHOD=org.apache.hadoop.mapred.YarnClientFactory   $confpkg.TODO_MAPRED_CLIENTFACTORY_CLASS_NAME=mapreduce.clientfactory.class.name"
    fanoutGW "/usr/local/bin/yinst set -root ${yroothome} $confpkg.TODO_CLIENTFACTORYMETHOD=org.apache.hadoop.mapred.YarnClientFactory   $confpkg.TODO_MAPRED_CLIENTFACTORY_CLASS_NAME=mapreduce.clientfactory.class.name"
    tmpfile=/tmp/xx.$$
    (
        echo "export HADOOP_COMMON_HOME=${yroothome}/share/hadoop"
        echo "export HADOOP_PREFIX=${yroothome}/share/hadoop"
        echo "export HADOOP_CONF_DIR=${yroothome}/conf/hadoop"
        echo "export YARN_CONF_DIR=${yroothome}/conf/hadoop"
        # workaround for one-day with older name for common, expires sept 9
        echo "export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop"
        echo "export YARN_HOME=${yroothome}/share/hadoop"
    	echo "export MAPREDUSER=$MAPREDUSER"
        echo "export JAVA_HOME=$GSHOME/java/jdk64/current"
     ) > $tmpfile

    (
        cat $tmpfile
        echo '$YARN_HOME/sbin/start-yarn.sh'
    )  | ssh $jobtrackernode su - $MAPREDUSER

echo == starting up yarn JobHistoryServer.
    (
        cat $tmpfile
        echo 'export YARN_OPTS="$YARN_OPTS -Dmapred.jobsummary.logger=INFO,JSA"'
        echo '$YARN_HOME/sbin/mr-jobhistory-daemon.sh start historyserver'
    )  | ssh $jobtrackernode su - $MAPREDUSER

fi
