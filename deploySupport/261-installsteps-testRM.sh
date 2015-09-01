#
# "Test that basic namenode/datanode operations work."
# 
#
# (1) Create a script that runs 'hdfs put / hdfs ls'.
# (2) On each gateway/yroot:
#        rsync the file, run it.
# (3) if each JT has a good exit-status, record the results in a MANIFEST.
#
# Inputs: $scripttmp    (for rsync)
# Inputs: $scriptaddr    (for rsync)
# Inputs: $REMOVEEXISTINGDATA    (boolean)
# Inputs: $cluster
# Inputs: $namenode    (set by cluster-list.sh)
# Inputs: $jobtrackernode    (set by cluster-list.sh)


if [ "$STARTYARN" = true ]
then
     banner  running Yarn tests: \$PREFERREDJOBPROCESSOR set to yarn.
debug=
cat > $scripttmp/$cluster.testYarndeploy.sh <<zz
cd ${yroothome}
   export HADOOP_HDFS_HOME=${yroothome}/share/hadoop
   export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
   export YARN_HOME=$HADOOP_MAPRED_HOME
   export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop
   export JAVA_HOME=$GSHOME/java/jdk64/current
   if [ -e share/hadoop-current ]
   then
       export HADOOP_COMMON_HOME=${yroothome}/share/hadoop-current
       export HADOOP_PREFIX=${yroothome}/share/hadoop
   else
    export HADOOP_COMMON_HOME=${yroothome}/share/hadoop
    export HADOOP_PREFIX=${yroothome}/share/hadoop
   fi
   export PATH=/usr/kerberos/bin:\$PATH

#
# Strategy: run a Yarn-operation for each of the N namenodes.
# then run an "ls" on the expected-output for each of them.
#
# "you should see the same number of attempts and then files found in 'ls'."
#
# If a namenode is down, it isn't counted IF it was consistently
# unavailable for both the "put" and the "ls".
#
# At least one namenode must be able to work with the JT.
#
kinit -k -t /$HOMEDIR/mapred/mapred.dev.headless.keytab mapred
export written=0
export read=0
tmpfile=tmp-\`date +%y%m%d%H%M\`
for n in `echo $namenode`
do

   if [ "$REMOVEEXISTINGDATA" = true ]
   then
        fname=hdfs://\${n}:8020/user/hadoopqa/rw.\$n.\$tmpfile.1mb

        echo "==== running randomwriter to \${n}:8020"

        cd \$YARN_HOME
        m=\`echo ${yroothome}/share/hadoop/modules/yarn-mapreduce-client-0.1.11*.jar \`
        echo \$m
        echo === to test yarn, we will eventually want to run the folloiwng:
        echo sleep 20
echo \$HADOOP_COMMON_HOME/bin/hadoop dfs -rmr /tmp/yarn
        echo sleep 20
	echo ./bin/yarn jar ./modules/yarn-mapreduce-app-*.jar org.apache.hadoop.mapreduce.SleepJob -libjars file://\$m -Dmapreduce.job.user.name=mapred -m 4 -r 2 -mt 2700 

		[ $? -eq 0 ] && export written=\`expr \$written + 1\`
   fi
done
exit 0
for n in `echo $namenode`
do

   if [ "$REMOVEEXISTINGDATA" = true ]
   then
       fname=hdfs://\${n}:8020/user/hadoopqa/rw.\$n.\$tmpfile.1mb

       echo "==== testing JT run to \${n}:8020"
       $debug \$HADOOP_COMMON_HOME/bin/hadoop fs -ls -R \$fname
       [ $? -eq 0 ] && export read=\`expr \$read + 1\`
   fi
done
[ "\$written" -gt 0  -a "\$read" = "\$written" ]
zz
fanoutcmd "scp $scripttmp/$cluster.testYarndeploy.sh __HOSTNAME__:/tmp/" "$gateway"
fanoutGW "mount gridnfs-b.blue.ygrid.yahoo.com:/vol/gridhomevol/mapred
  ~mapred ; chsh -s /bin/bash mapred ; && su mapred -c 'sh  /tmp/$cluster.testYarndeploy.sh' " # > /dev/null 2>&1"
[ $? -eq 0 ] && (
   rm -fr /tmp/$cluster.*.handoff.txt
   for c in mapred yarn
   do
      scp ${jobtrackernode}:${yroothome}/share/hadoop${c}/handoff.txt /tmp/$cluster.$c.handoff.txt
      recordpkginstall  hadoop$c `cat /tmp/$cluster.$c.handoff.txt`
      banner SUCCESS: hadoop-$c is correctly installed: ver=`cat /tmp/$cluster.$c.handoff.txt`
   done
)
else
   banner  not running Yarn tests: \$STARTYARN set to $STARTYARN.
   return 0
fi
