Please see 
- The bug to track automation of this test http://bug.corp.yahoo.com/show_bug.cgi?id=4352401
- The bug to track bug fixing of the feature http://bug.corp.yahoo.com/show_bug.cgi?id=4422879

To semi-automatically test this feature,  
1. go to all the namenodes and
	- export LOOPBACKSIZE=10000
	- dd if=/dev/zero of=/tmp/test-img bs=1024 count=$LOOPBACKSIZE
	- sudo /sbin/losetup `sudo /sbin/losetup -f` /tmp/test-img
	- /sbin/mkfs -t ext3 -q /tmp/test-img
	- mkdir -p /tmp/checkpointDir
	- sudo mount -o loop /tmp/test-img /tmp/checkpointDir
2. Use driver.sh to run the test. e.g./bin/sh bin/driver.sh -c $CLUSTER -w /tmp/raviprak -s /tmp/raviprak/tests/Regression/HDFS/restoreCheckpointStorage/run_restoreCheckpointStorage.sh -n

Currently the feature is failing. Todo : after http://bug.corp.yahoo.com/show_bug.cgi?id=4422879 gets fixed automate RestoreFailedStorage_05 from http://twiki.corp.yahoo.com/pub/Grid/HdfsCommonTestPlan_0_22_0/DfsAdminRestoreFailedStorageTestPlan.html