
####                                                                    new config dir    cluster NN0 (default)  NN2     NN3        NN4
####   rm -rf v20_viewfs_omegam_fred81 ; sh  genViewfsConfig.sh V20  v20_viewfs_omegam_fred81 omegam   gsbl90470     gsbl90440 gsbl90440 gsbl90470
## sh genViewfsConfig.sh V22 v22_viewfs_config omegab gsbl90772 gsbl90772 gsbl90772 gsbl90772

source $WORKSPACE/lib/library.sh  
source $WORKSPACE/lib/user_kerb_lib.sh
source $WORKSPACE/lib/hdft_util2.sh

# Usage: cluster NN-name NewConfigDirName
# Action:
#     mkidr NewConfigDirName
#	  copy from ${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop NewConfigDirName
#	  run sed to add xs!include NewConfigDirName/core-site.xml, and to change the default to viewfs:///
#	  copy the mnttable and update with the NN
# 	  may also need to update mapred-site.xml
# Up to NN0 is mandatory. NN0 is default NN (gsbl90470 for omegam, for eaxmple).
# If NN2 is not provided, it will be set to NN0. If NN3 is not provided, it will be set to  NN2, and so on
genVersion=$1		# either V20 or V22
newConfigDir=$2
CLUSTER=$3
NN0=$4
NN2=$5
NN3=$6
NN4=$7
hadoopConfigDir=${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop
export HADOOP_HOME=${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/share/hadoop-current
export HADOOP_CONF_DIR=$hadoopConfigDir
: ${NN2:=$NN0}
: ${NN3:=$NN2}
: ${NN4:=$NN3}

echo "   Env/input: genVersion=$genVersion"
echo "   Env/input: CLUSTER=$CLUSTER"
echo "   Env/input: Default Name Node NN0=$NN0"
echo "   Env/input: Default Name Node NN0=$NN0"
echo "   Env/input: NN0=$NN0"
echo "   Env/input: NN2=$NN2"
echo "   Env/input: NN3=$NN3"
echo "   Env/input: NN4=$NN4"
echo "   Env/input: newConfigDir=$newConfigDir"
echo "   Env/input: hadoopConfigDir=$hadoopConfigDir"
echo "   Env/input: HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
echo "   Env/input: HADOOP_HOME=$HADOOP_HOME"

function checkParam {
	if [ -z "$CLUSTER" ] || [ -z "$NN0"  ] || [ -z "$newConfigDir" ] ; then
		echo "ERROR: Bad Input param"
		exit
	fi

	if [ -f $newConfigDir ] ; then
		echo "ERROR: Directory $newConfigDir already exist. Pls give a new config directory name. "
		exit
	fi
}

# this works for .20
function getDefaultNN {
	H=`grep -A 3 fs.default.name ${HADOOP_QA_ROOT}/gs/gridre/yroot.${CLUSTER}/conf/hadoop/core-site.xml | fgrep '<value>' | head -1 | awk -F/ '{print $3}' | awk -F. '{print $1}'`
	echo $H
}

function genNNConfigStub {
	rm -f viewfsNNconfig.sh
	echo "export NN0=$NN0" 	> viewfsNNconfig.sh
	echo "export NN2=$NN2" 	>> viewfsNNconfig.sh
	echo "export NN3=$NN3" 	>> viewfsNNconfig.sh
	echo "export NN4=$NN4" 	>> viewfsNNconfig.sh
}

function genViewfsConfigDir {
	genNNConfigStub

	# copy basic
 	rm -f ${genVersion}_viewfs_config		# this is the default directory that the test script is using
	mkdir $newConfigDir
 	ln -s $newConfigDir  ${genVersion}_viewfs_config	
	cp -p $HADOOP_CONF_DIR/* $newConfigDir
	ls -l $HADOOP_CONF_DIR/core-site.xml

	###############################################
	# now update the core-site.xml
	###############################################
	# first sed: update fs.default.name to <value>viewfs:////</value>
	# add the xi:include to include the mount table name
	#
	if [ "$genVersion" == "V20" ] ; then
		DFS_DEFAULT_NAME="fs.default.name"
	else
		DFS_DEFAULT_NAME="fs.defaultFS"
	fi

	rm -f new/core-site-XXX.xml
	echo "Now updating core-site.xml ..."
	cat $HADOOP_CONF_DIR/core-site.xml | \
		sed -e "/$DFS_DEFAULT_NAME/ { N; N; N; s#<value>[a-zA-Z0-9/\:\.]*</value>#<value>viewfs:///</value># ; }" |\
		sed -e "/$DFS_DEFAULT_NAME/ { 
			N; N; N; N; N; N; N; N;
			s#</property>#</property>\n\n<xi:include href=\""local.mountfs.xml\""/>#  
		}"    | \
		sed -e "s#local.mountfs.xml#${NN0}.mountfs.xml#" >   new/core-site-XXX.xml
		cp -pf new/core-site-XXX.xml $newConfigDir/core-site.xml
		#sed -e "s#local.mountfs.xml#${NN0}.mountfs.xml#" >!   $newConfigDir/core-site.xml

	###############################################
	# now generate the gsbl00000.mountfs.xml
	###############################################
	rm -f new/mountfs-XXX.xml
	echo "Now generating ${NN0}.mountfs.xml..."
	cat new/local-mountfs.xml | sed  -e "{
		s/NN0/$NN0/
		s/NN2/$NN2/
		s/NN3/$NN3/
	s/NN4/$NN4/
	}"	 > new/mountfs-XXX.xml
	cp -pf new/mountfs-XXX.xml $newConfigDir/${NN0}.mountfs.xml

	###############################################
	# now update the mapred-site.xml
	###############################################
	echo "Now prepare MSERVER"
	MSERVER="\n<property>\n<name>mapreduce.job.hdfs-servers</name>\n<value>hdfs://${NN0}.blue.ygrid:8020,hdfs://${NN2}.blue.ygrid:8020,hdfs://${NN3}.blue.ygrid:8020,hdfs://${NN4}.blue.ygrid:8020</value>\n<description>See this or else viewfs/csmt won;t work across two clusters. Right now set to omemam and omegal. </description>\n</property>"

	echo "MSERVER=$MSERVER"
	echo "Now updating mapred-site.xml ..."

	cat $HADOOP_CONF_DIR/mapred-site.xml | \
		sed -e "/io.sort.mb/ { N; N; N; N; s|</property>|</property> ${MSERVER} | ; }" >! $newConfigDir/mapred-site.xml
}	 

function junkk {
	echo "Now updating core-site.xml ..."
	cat $HADOOP_CONF_DIR/core-site.xml | \
		sed -e '/fs.default.name/ { N; N; N; s#<value>[a-zA-Z0-9/\:\.]*</value>#<value>viewfs:///</value># ; }' |\
		sed -e '/fs.default.name/ { 
			N; N; N; N; N; N; N; N;
			s#</property>#</property>\n\n<xi:include href="local.mountfs.xml"/>#  
		}'    | \
		sed -e "s#local.mountfs.xml#${NN0}.mountfs.xml#" >   $newConfigDir/core-site.xml
}

function junk {
cat $HADOOP_CONF_DIR/mapred-site.xml | \
    sed -e "/io.sort.mb/ { 
	N; N; N; N;
	s#</property>#</property>	\
<property>\
<name>mapreduce.job.hdfs-servers</name>	\
<value>hdfs://${NN0}.blue.ygrid:8020,hdfs://${NN2}.blue.ygrid:8020,hdfs://${NN3}.blue.ygrid:8020,hdfs://${NN4}.blue.ygrid:8020</value>	\
<description>See this or else viewfs/csmt won;t work across two clusters. Right now set to omemam and omegal. </description>	\
</property>	\
	# ; 
	} "  	> $newConfigDir/mapred-site.xml

}

function genHdfsDir {
	echo "Now mkdir hdfs "
	rm -f ${genVersion}_hdfs_config		# this is the default directory used by the script
	mkdir ${genVersion}_hdfs_${CLUSTER}
	ln -s  ${genVersion}_hdfs_${CLUSTER}  ${genVersion}_hdfs_config
	cp -p $HADOOP_CONF_DIR/* ${genVersion}_hdfs_${CLUSTER}
	cat new/my-hdfs-scheme-only.xml  | sed -e "s/gsbl00000/${NN0}/" > ${genVersion}_hdfs_${CLUSTER}/my-hdfs-scheme-only.xml
}


###############################################################
### Starts here 
##############################################################
checkParam
genViewfsConfigDir
genHdfsDir

echo "Done Updating. Now check results...."

ls -ltr $newConfigDir | tail -4

for f in core-site.xml mapred-site.xml hdfs-site.xml ; do
	echo "=== Doing diff of $HADOOP_CONF_DIR/$f $newConfigDir .."
	diff $HADOOP_CONF_DIR/$f $newConfigDir   | head -20
	echo " "
done
