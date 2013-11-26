
whoami
date
hostname
OWNER="cwchung"
PWD=`pwd`
## sudo su -c "RunSudo.sh"
## sudo su - hadoop8 -c "RunSuHadoop8.sh"
echo "RunAll.sh: ENV CLUSTER=$CLUSTER"
echo "RunAll.sh: ENV SG_WORKSPACE=$SG_WORKSPACE"
echo "RunAll.sh: ENV HDFT_TOP_DIR=$HDFT_TOP_DIR"
echo "RunAll.sh: ENV PWD=$PWD"
echo "RunAll.sh: ENV HADOOP_VERSION=$HADOOP_VERSION"

SUMMARY_LOG=log/All-summary.log

umask 0002

rm -f $SUMMARY_LOG
touch $SUMMARY_LOG
chmod 777 $SUMMARY_LOG

# make sure the artifats are wriatble by user group
rm -rf artifacts/
mkdir artifacts
chgrp users artifacts

echo "    #### $0 LAUNCH::  RunTest.sh as user hadoopqa"
echo "    #### $0 LAUNCH::  RunTest.sh as user hadoopqa"
echo "XXXXXXXXX uncomment this to Run Basic  sh RunTestInDev.sh | tee log/All-TestInDev.log"
sh RunTest.sh 2>&1 | tee log/All-RunTest.log

echo "RUNNING Superuser test as user hadoop8"
echo "    #### $0 LAUNCH:: RunAll.sh: DOIT: sudo su - hadoop8 -c cd $PWD; CLUSTER=$CLUSTER SG_WORKSPACE=$SG_WORKSPACE HDFT_TOP_DIR=$HDFT_TOP_DIR  $PWD/Run_as_hdfs.sh"  
echo "    #### $0 LAUNCH:: RunAll.sh: DOIT: sudo su - hadoop8 -c cd $PWD; CLUSTER=$CLUSTER SG_WORKSPACE=$SG_WORKSPACE HDFT_TOP_DIR=$HDFT_TOP_DIR  $PWD/Run_as_hdfs.sh"  

#sudo su - hadoop8 -c "cd $PWD; CLUSTER=$CLUSTER SG_WORKSPACE=$SG_WORKSPACE HDFT_TOP_DIR=$HDFT_TOP_DIR  $PWD/Run_as_hdfs.sh"  | tee log/All-RunSudo.log
sh Run_as_hdfs.sh 2>&1  | tee log/All-RunSudo.log

echo "RUNNING Negative test as user hadoop8"
echo "    #### $0 LAUNCH:: RunAll.sh: DOIT: sudo su - hadoop8 -c cd $PWD; CLUSTER=$CLUSTER SG_WORKSPACE=$SG_WORKSPACE HDFT_TOP_DIR=$HDFT_TOP_DIR $PWD/Run_as_hadoop8.sh" 
echo "    #### $0 LAUNCH:: RunAll.sh: DOIT: sudo su - hadoop8 -c cd $PWD; CLUSTER=$CLUSTER SG_WORKSPACE=$SG_WORKSPACE HDFT_TOP_DIR=$HDFT_TOP_DIR $PWD/Run_as_hadoop8.sh" 
## sudo su - hadoop8 -c "cd $PWD; CLUSTER=$CLUSTER SG_WORKSPACE=$SG_WORKSPACE HDFT_TOP_DIR=$HDFT_TOP_DIR $PWD/Run_as_hadoop8.sh" | tee log/All-RunSuHadoop8.log
sh Run_as_hadoop8.sh 2>&1 | tee log/All-RunSuHadoop8.log

echo "    ##### RUNNING Done"
echo "    ##### RUNNING Done"

TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_XACT=0
#for Run in RunTest RunSudo RunSuHadoop8 ; do
for Run in RunTest RunSudo RunSuHadoop8; do
	LOG=log/All-$Run.log
	grep 'XACT'  $LOG | grep 'SUMMARY:' >> $SUMMARY_LOG
	NPASS=`grep 'XACT'  $LOG | grep  'SUMMARY:' | grep PASS | wc -l `
	NFAIL=`grep 'XACT'  $LOG | grep  'SUMMARY:' | grep FAIL | wc -l `
	NXACT=`grep 'XACT'  $LOG | grep  'SUMMARY:' | wc -l `
	(( TOTAL_PASS = $TOTAL_PASS + $NPASS ))
	(( TOTAL_FAIL = $TOTAL_FAIL + $NFAIL ))
	(( TOTAL_XACT = $TOTAL_XACT + $NXACT ))
done

echo ""
echo "Done by `whoami` at `date` on host `hostname`:`pwd -P` "
echo "CLUSTER=$CLUSTER; SG_WORKSPACE=$SG_WORKSPACE"
$HADOOP_COMMON_HOME/bin/hadoop version
echo ""
echo "#####################################################################################"
echo "###################### TEST SUMMARY #################################################"
echo "#####################################################################################"
cat $SUMMARY_LOG
echo "#####################################################################################"
echo "GRAND SUMMARY: TOTAL TESTS=$TOTAL_XACT; TOTAL PASS=$TOTAL_PASS: TOTAL FAIL=$TOTAL_FAIL"
echo "#####################################################################################"

