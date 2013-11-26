# Set Hadoop-specific environment variables here.

# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=/grid/0/gs/java/jdk64/current

if [ -z "" ]
then
    export spareNNOpts=-Dhadoop.chop.id=
else
    export spareNNOpts=-Dhadoop.chop.id=
fi
export SPARE_NN_OPTS="-Xms25000m -XX:CMSInitiatingOccupancyFraction=84 -XX:+UseCMSInitiatingOccupancyOnly"
export SPARE_JT_OPTS=""

	

# Extra Java CLASSPATH elements.  Adjusted to automatically insert capacity-scheduler.
for f in $HADOOP_HOME/hadoop-capacity-scheduler-*.jar /grid/0/gs/hadoopviewfs/current/*.jar ;     do
  if [ "$HADOOP_CLASSPATH" ]; then
    export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
  else
    export HADOOP_CLASSPATH=$f
  fi
done

# Hadoop Configuration Directory
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:="/grid/0/gs/conf/current"}

# The maximum amount of heap to use, in MB. Default is 1000.
#export HADOOP_HEAPSIZE=
#export HADOOP_NAMENODE_INIT_HEAPSIZE=""

# Extra Java runtime options.  Empty by default.
export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true ${HADOOP_OPTS}"

# Command specific options appended to HADOOP_OPTS when specified
export HADOOP_NAMENODE_OPTS="-server -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -XX:ErrorFile=/grid/0/hadoop/var/$USER/hs_err_pid%p.log -XX:NewSize=3G -XX:MaxNewSize=3G -Xloggc:/grid/0/hadoop/var/log/$USER/gc.log-`date +'%Y%m%d%H%M'` -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps ${HADOOP_NAMENODE_INIT_HEAPSIZE} -Xmx45000m -Dsecurity.audit.logger=INFO,DRFAS -Dhdfs.audit.logger=INFO,DRFAAUDIT ${HADOOP_NAMENODE_OPTS} ${SPARE_NN_OPTS} $spareNNOpts"
HADOOP_JOBTRACKER_OPTS="-server -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -XX:ErrorFile=/grid/0/hadoop/var/log/$USER/hs_err_pid%p.log -XX:NewSize=1G -XX:MaxNewSize=1G -Xloggc:/grid/0/hadoop/var/log/$USER/gc.log-`date +'%Y%m%d%H%M'` -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xmx14000m -Dsecurity.audit.logger=INFO,DRFAS -Dmapred.audit.logger=INFO,MRAUDIT -Dmapred.jobsummary.logger=INFO,JSA ${HADOOP_JOBTRACKER_OPTS}"
export HADOOP_JOB_HISTORYSERVER_OPTS="-Xmx4000m -Dcom.sun.management.jmxremote $HADOOP_JOB_HISTORYSERVER_OPTS"


HADOOP_TASKTRACKER_OPTS="-server -Xmx1024m -Dsecurity.audit.logger=ERROR,console -Dmapred.audit.logger=ERROR,console ${HADOOP_TASKTRACKER_OPTS}"
HADOOP_DATANODE_OPTS="-Xmx1024m -Dsecurity.audit.logger=ERROR,DRFAS ${HADOOP_DATANODE_OPTS} $spareNNOpts"
HADOOP_BALANCER_OPTS="-server -Xmx45000m ${HADOOP_BALANCER_OPTS}"

export HADOOP_SECONDARYNAMENODE_OPTS="-server -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -XX:ErrorFile=/grid/0/hadoop/var/$USER/hs_err_pid%p.log -XX:NewSize=3G -XX:MaxNewSize=3G -Xloggc:/grid/0/hadoop/var/log/$USER/gc.log-`date +'%Y%m%d%H%M'` -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps ${HADOOP_NAMENODE_INIT_HEAPSIZE} -Xmx45000m -Dsecurity.audit.logger=INFO,DRFAS -Dhdfs.audit.logger=INFO,DRFAAUDIT ${HADOOP_SECONDARYNAMENODE_OPTS}"

# The following applies to multiple commands (fs, dfs, fsck, distcp etc)
export HADOOP_CLIENT_OPTS="-Xmx128m ${HADOOP_CLIENT_OPTS}"
#HADOOP_JAVA_PLATFORM_OPTS="-XX:-UsePerfData ${HADOOP_JAVA_PLATFORM_OPTS}"

# On secure datanodes, user to run the datanode as after dropping privileges
export HADOOP_SECURE_DN_USER=hdfs

# Extra ssh options.  Empty by default.
export HADOOP_SSH_OPTS="-o ConnectTimeout=5 -o SendEnv=HADOOP_CONF_DIR"

# Where log files are stored.  $HADOOP_HOME/logs by default.
export HADOOP_LOG_DIR=/grid/0/hadoop/var/log/${HADOOP_IDENT_STRING-$USER}

# Where log files are stored in the secure data environment.
export HADOOP_SECURE_DN_LOG_DIR=/grid/0/hadoop/var/log/$HADOOP_SECURE_DN_USER

# File naming remote slave hosts.  $HADOOP_HOME/conf/slaves by default.
# export HADOOP_SLAVES=${HADOOP_HOME}/conf/slaves

# host:path where hadoop code should be rsync'd from.  Unset by default.
# export HADOOP_MASTER=master:/home/$USER/src/hadoop

# Seconds to sleep between slave commands.  Unset by default.  This
# can be useful in large clusters, where, e.g., slave rsyncs can
# otherwise arrive faster than the master can service them.
# export HADOOP_SLAVE_SLEEP=0.1

# The directory where pid files are stored. /tmp by default.
export HADOOP_PID_DIR=/grid/0/hadoop/var/run/$USER
export HADOOP_SECURE_DN_PID_DIR=/grid/0/hadoop/var/run/$HADOOP_SECURE_DN_USER

# A string representing this instance of hadoop. $USER by default.
export HADOOP_IDENT_STRING=$USER

# The scheduling priority for daemon processes.  See 'man nice'.
# export HADOOP_NICENESS=10
