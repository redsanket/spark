#!/bin/sh

export CLUSTER=ankh

if [ -d /grid/0/tmp/validate.$CLUSTER ]
then
	rm -rf /grid/0/tmp/validate.$CLUSTER 
fi
yinst install -root /grid/0/tmp/validate.$CLUSTER -nosudo -yes  -branch test  hadoopvalidation

export PATH=/usr/kerberos/bin:$PATH


kinit -k -t $HOMEDIR/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa
cd /grid/0/tmp/validate.$CLUSTER/share/hadoopvalidation/
ls
if [ -f bin/wordcount-simple ]
then
	if [ -f bin/wordcount-simple-20.102 ]
	then
		rm  -f bin/wordcount-simple
	else
		mv  -f bin/wordcount-simple bin/wordcount-simple-20.102
	fi
	ln bin/wordcount-simple-20.103 bin/wordcount-simple
fi
 chmod +x bin/*
 bash bin/main3.sh gsbl20003.blue.ygrid.yahoo.com gsbl91039.blue.ygrid.yahoo.com:50070 gwbl9010.blue.ygrid.yahoo.com:9999 ${GSHOME}/gridre/yroot.$CLUSTER/share/hadoop-current
