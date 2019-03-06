set +x
##----------- option #1: ALWAYS kill processes before starting.
if [ "$KILLALLPROCESSES" = true ]; then
	# Step 1: kill any running processes.
	echo "Step 1: Kill any running processes."

	# Step 1a: kill job tracker script.
	echo "Step 1a: kill job tracker script, if running."

        # need to use correct syntax depending on Core 2.x or 3.x
        if [[ "$HADOOPVERSION" =~ ^3. ]]; then
            set -x
            echo ${yrootHadoopMapred}/bin/yarn --daemon stop resourcemanager | $SSH $jobtrackernode su - $MAPREDUSER
	    echo ${yrootHadoopMapred}/bin/mapred --daemon stop historyserver | $SSH $jobtrackernode su - $MAPREDUSER
	    echo ${yrootHadoopMapred}/bin/yarn --daemon stop timelineserver | $SSH $jobtrackernode su - $MAPREDUSER
            set +x
        elif [[ "$HADOOPVERSION" =~ ^2. ]]; then
            set -x
	    echo ${yrootHadoopMapred}/sbin/yarn-daemon.sh --config $yroothome/conf/hadoop  stop resourcemanager | $SSH $jobtrackernode su - $MAPREDUSER
            echo ${yrootHadoopMapred}/sbin/mr-jobhistory-daemon.sh --config $yroothome/conf/hadoop  stop historyserver | $SSH $jobtrackernode su - $MAPREDUSER
            echo ${yrootHadoopMapred}/sbin/yarn-daemon.sh --config $yroothome/conf/hadoop  stop timelineserver | $SSH $jobtrackernode su - $MAPREDUSER
            set +x
        else
            echo "ERROR: Unknown HADOOPVERSION $HADOOPVERSION"
            exit 1
        fi

        # removing su - $HDFSUSER as we might have started the jt as mapred/mapredqa users
        JAVA_HOME="$GSHOME/java/jdk64/current"
	echo "Step 1b: kill namenode, if running."

        if [[ "$HADOOPVERSION" =~ ^3. ]]; then
            set -x
            fanoutNN  "su - $HDFSUSER -c '\
            export JAVA_HOME=$JAVA_HOME && \
            export HADOOP_PREFIX=${yroothome}/share/hadoop && \
            export HADOOP_CONF_DIR=${yroothome}/conf/hadoop && \
            ${yrootHadoopMapred}/bin/hdfs --daemon stop namenode '"
            set +x
        elif [[ "$HADOOPVERSION" =~ ^2. ]]; then
            set -x
            fanoutNN  "su - $HDFSUSER -c '\
            export JAVA_HOME=$JAVA_HOME && \
            export HADOOP_PREFIX=${yroothome}/share/hadoop && \
            export HADOOP_CONF_DIR=${yroothome}/conf/hadoop && \
            ${yrootHadoopCurrent}/sbin/hadoop-daemon.sh stop namenode '"
            set +x
        else
            echo "ERROR: Unknown HADOOPVERSION $HADOOPVERSION"
            exit 1
        fi

	echo "Step 1c: stop data nodes, if running."
        set -x
	fanoutcmd "scp /grid/0/tmp/scripts.deploy.$cluster/datanode-script.sh __HOSTNAME__:/tmp/datanode-script.sh" "$SLAVELIST"
	$PDSH -w "$SLAVELIST" "export HDFSUSER=$HDFSUSER && sh /tmp/datanode-script.sh stop $cluster > /dev/null 2>&1"
        set +x

	# kill any running processes.
        set -x
        fanoutcmd "scp /grid/0/tmp/deploy.$cluster.processes.to.kill.sh __HOSTNAME__:/tmp/deploy.$cluster.processes.to.kill.sh" "$HOSTLISTNOGW"
        fanoutnogw "sh /tmp/deploy.$cluster.processes.to.kill.sh && rm /tmp/deploy.$cluster.processes.to.kill.sh  > /dev/null 2>&1"
        fanoutnogw "rm -rf /tmp/logs"
        set +x
else
    echo "KILL ALL PROCESSES is not enabled. Nothing to do."
fi
