
export HDFT_SETENV_SH=1

# test script to source this script before running the test
# this script can also be source for manual testing
# By this time, the following should have already been set up
# CLUSTER. SG_WORKSPACE, HDFT_TOP_DIR, HADOOP_CONF_DIR, (HADOOP_HOME or HADOOP_COMMON_HOME, HADOOP_MAPRED_HOME, HADOOP_HDFS_HOME)

#=== start of automatically maintained lines (do not delete)===

# This one is not appropriate for regression test. Setenv probably shoudl have two scripts: one for interactive, one for regressioin test run

# .bashrc, sourced by interactive login shells
#===   end of automatically maintained lines (do not delete)===

if [ -z "$CLUSTER" ] ; then
	echo "WARNING: ENV CLUSTER is NOT set. export CLUSTER=omegab"
	export CLUSTER=OMEGAXXXXX
fi
	
if [ -z "$HDFT_TOP_DIR" ] ; then
	export HDFT_TOP_DIR=/tmp/JUNK-XXXXX
	echo "WARNING: ENV HDFT_TOP_DIR is NOT set. export $HDFT_TOP_DIR"
	# export HDFT_TOP_DIR=/tmp/junk
fi

export HDFT_HOME=$HDFT_TOP_DIR

if [ -n "$HADOOP_VERSION" ] ; then
	# not as reliable as running hadoop version, but you need this before setting up the path
	if [ -f $HADOOP_CONF_DIR/${CLUSTER}.namenodeconfigs.xml ] ; then
		export HADOOP_VERSION="22"
	else
		export HADOOP_VERSION="20"
	fi
fi

## export HADOOP_HOME=/grid/0/gs/gridre/yroot.${CLUSTER}/share/hadoop-current
## export HADOOP_CONF_DIR=/grid/0/gs/gridre/yroot.${CLUSTER}/conf/hadoop/

if [ -n "$HADOOP_VERSION" ] && [ "$HADOOP_VERSION" == "20" ] ; then
	PATH=$HADOOP_HOME/bin:$PATH
	CLASSPATH=$HADOOP_HOME:$HADOOP_HOME/lib
else
	PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH
	CLASSPATH=$HADOOP_COMMON_HOME:$HADOOP_COMMON_HOME/lib:$HADOOP_HDFS_HOME:$HADOOP_HDFS_HOME/lib:$HADOOP_MAPRED_HOME:$HADOOP_MAPRED_HOME/lib
fi

# JAVA_HOME is set in conf/hadoop/common_hadoop_env.sh, and sourced in by driver.sh
export PATH=${PATH}:/home/y/bin:/usr/local/bin:/bin:/usr/sbin:/sbin:/usr/bin:/usr/X11R6/bin:$HOME/bin:.:$HOME/cron:/usr/kerberos/bin
export CLASSPATH=${CLASSPATH}:${JAVA_HOME}/lib:$JAVA_HOME/lib/tools.jar

# print the abbreviated  hostname (without domain name) on yroot host, and full hostname otherwise
# -H print the entire hostname like gwbl2003.blue.ygrid.yahoo.com. -n below just the host name: gwbl2003
if [ -n "$PS1" ]; then
  W=`whoami`
  PS1="${W}@\h \t \W \$  "
  if [ -n "$YROOT_NAME" ]; then
    PS1="${W}@[$YROOT_NAME] \h \t \W \$ "
  fi
fi

export S=svn+ssh://svn.corp.yahoo.com/
export SVN_SSH=/usr/local/bin/yssh

# svn co  $S/yahoo/messenger/server/services/ESMediaService

alias a="alias"
alias ls="ls -F"
alias h="hadoop"
alias hf="hadoop fs"
alias hfl="hadoop fs -ls /user/hadoopqa/hdfsRegressionData"
export HQ="/user/hadoopqa/hdfsRegressionData"
