#
# "Test that basic namenode/datanode operations work."
# 
#
# (1) Create a script that runs 'hdfs put / hdfs ls'.
# (2) On each gateway/yroot:
#        rsync the file, run it.
# (3) if each NN has a good exit-status, record the results in a MANIFEST.
#
# Inputs: $scripttmp	(for rsync)
# Inputs: $scriptaddr	(for rsync)
# Inputs: $REMOVEEXISTINGDATA	(boolean)
# Inputs: $cluster
# Inputs: $namenode	(set by cluster-list.sh)
# Inputs: $NAMENODE_Primary	(set by installgrid.sh)



debug=
JAVA_HOME="$GSHOME/java8/jdk64/current"

cat > $scripttmp/$cluster.testNNdeploy.sh <<zz
cd ${yroothome}
   export HADOOP_HDFS_HOME=${yroothome}/share/hadoop
   export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
   export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop
   export JAVA_HOME=$JAVA_HOME
if [ -e share/hadoop-current ]
then
	export HADOOP_COMMON_HOME=${yroothome}/share/hadoop-current
        export HADOOP_PREFIX=${yroothome}/share/hadoop
else
	export HADOOP_COMMON_HOME=${yroothome}/share/hadoop
        export HADOOP_PREFIX=${yroothome}/share/hadoop
fi
export PATH=/usr/kerberos/bin:\$PATH

\$HADOOP_COMMON_HOME/bin/hadoop fs -ls -R /user
kinit -k -t $HOMEDIR/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa
export cnt=0
for i in `echo $namenode`
do

   if [ "$REMOVEEXISTINGDATA" = true ]
   then
	fname=hdfs://\${i}/user/hadoopqa/passwd.`date +%y%m%d%H%M`.\${i}.txt
        echo "==== bin/hadoop fs -put... test that namenode \${i} is up."
	$debug \$HADOOP_COMMON_HOME/bin/hadoop fs -put /etc/passwd \$fname
   else
	fname=hdfs://\${i}/user
   fi
   echo "==== bin/hadoop fs -ls... test that namenode node for \${i} are up."
   $debug \$HADOOP_COMMON_HOME/bin/hadoop fs -ls -R \$fname
   [ $? -eq 0 ] && export cnt=\`expr \$cnt + 1\`
done
echo \$cnt namenodes up.
zz
fanoutcmd "scp $scripttmp/$cluster.testNNdeploy.sh __HOSTNAME__:/tmp/" "$gateway"
fanoutGW "su hadoopqa -c 'sh  /tmp/$cluster.testNNdeploy.sh' > /dev/null 2>&1"

# TODO: The following is failing with the error message:
# scp: /home/gs/gridre/yroot.openqe2blue/share/hadoopcommon/handoff.txt: No such file or directory
# ver shows empty string, even in our classic deploy logs!!!
#
# [ $? -eq 0 ] && (
#    rm -fr /tmp/$cluster.*.handoff.txt
#    for c in common hdfs
#    do
#       scp ${NAMENODE_Primary}:${yroothome}/share/hadoop${c}/handoff.txt /tmp/$cluster.$c.handoff.txt
#       recordpkginstall  hadoop$c `cat /tmp/$cluster.$c.handoff.txt`
#       banner SUCCESS: hadoop-$c is correctly installed: ver=`cat /tmp/$cluster.$c.handoff.txt`
#    done
# )
