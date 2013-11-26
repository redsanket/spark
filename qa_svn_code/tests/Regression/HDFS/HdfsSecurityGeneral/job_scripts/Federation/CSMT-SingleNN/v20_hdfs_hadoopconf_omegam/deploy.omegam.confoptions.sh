[ -x /usr/local/bin/yinst ] && export yinst=/usr/local/bin/yinst 
[ -x /usr/y/bin/yinst ] && export yinst=/usr/y/bin/yinst 
if [ -n "$TARGET_YROOT" ] 
then
      export rootdirparam="-yroot $TARGET_YROOT    -set HadoopConfiggeneric10nodeblue.TODO_RUNMKDIRS=false "
else
   export rootdirparam="-root /grid/0/gs/gridre/yroot.omegam  "  
fi
echo ======= installing config-package using 'HadoopConfiggeneric10nodeblue:hadoopXXX20X203Xlatest'
/usr/local/bin/yinst install -yes $rootdirparam \
   -set HadoopConfiggeneric10nodeblue.TODO_NAMENODE_HOSTNAME=gsbl90470.blue.ygrid.yahoo.com \
   -set HadoopConfiggeneric10nodeblue.TODO_NAMENODE_SHORTHOSTNAME=gsbl90470 \
   -set HadoopConfiggeneric10nodeblue.TODO_SECONDARYNAMENODE_HOSTNAME=gsbl90471.blue.ygrid.yahoo.com \
   -set HadoopConfiggeneric10nodeblue.TODO_SECONDARYNAMENODE_SHORTHOSTNAME=gsbl90471 \
   -set HadoopConfiggeneric10nodeblue.TODO_JTNODE_HOSTNAME=gsbl90472.blue.ygrid.yahoo.com \
   -set HadoopConfiggeneric10nodeblue.TODO_MAPREDUSER=mapred \
   -set HadoopConfiggeneric10nodeblue.TODO_HDFSUSER=hdfs \
   -set HadoopConfiggeneric10nodeblue.TODO_JTNODE_SHORTHOSTNAME=gsbl90472 \
   -set HadoopConfiggeneric10nodeblue.TODO_HDFSCLUSTER_NAME=omegam -set HadoopConfiggeneric10nodeblue.TODO_MAPREDCLUSTER_NAME=omegam \
   -set HadoopConfiggeneric10nodeblue.TODO_KERBEROS_ZONE=dev \
   HadoopConfiggeneric10nodeblue:hadoopXXX20X203Xlatest
/usr/local/bin/yinst install -yes $rootdirparam -branch test hadoop_qa_restart_config datanode namenode jobtracker tasktracker hadoopviewfs
