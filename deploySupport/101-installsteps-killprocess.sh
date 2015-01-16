##----------- option #1: ALWAYS kill processes before starting.
if [ "$KILLALLPROCESSES" = true ]
then
	#
	# Step 1: kill any running processes.
	echo "Step 1: Kill any running processes."

	# Step 1a: kill job tracker script.
	echo "Step 1a: kill job tracker script, if running."
	echo ${yrootHadoopMapred}/sbin/yarn-daemon.sh --config $yroothome/conf/hadoop  stop resourcemanager | ssh $jobtrackernode su - $MAPREDUSER
        echo ${yrootHadoopMapred}/sbin/mr-jobhistory-daemon.sh --config $yroothome/conf/hadoop  stop historyserver | ssh $jobtrackernode su - $MAPREDUSER
        echo ${yrootHadoopMapred}/sbin/yarn-daemon.sh --config $yroothome/conf/hadoop  stop timelineserver | ssh $jobtrackernode su - $MAPREDUSER

  # removing su - $HDFSUSER as we might have started the jt as mapred/mapredqa users 
	echo "Step 1b: kill namenode, if running."
	fanoutNN  "su - $HDFSUSER -c '${yrootHadoopCurrent}/sbin/hadoop-daemon.sh stop namenode  '"

	echo "Step 1c: stop data nodes, if running."
    	pdsh -w "$SLAVELIST"  "scp  $ADMIN_HOST:/grid/0/tmp/scripts.deploy.$cluster/datanode-script.sh  /tmp/datanode-script.sh  && export HDFSUSER=$HDFSUSER &&  sh /tmp/datanode-script.sh stop  $cluster > /dev/null 2>&1"

	# kill any running processes.

        fanoutnogw "scp  $ADMIN_HOST:/grid/0/tmp/deploy.$cluster.processes.to.kill.sh   /tmp/deploy.$cluster.processes.to.kill.sh && sh /tmp/deploy.$cluster.processes.to.kill.sh && rm /tmp/deploy.$cluster.processes.to.kill.sh  > /dev/null 2>&1"
        fanoutnogw "rm -rf /tmp/logs"
fi
