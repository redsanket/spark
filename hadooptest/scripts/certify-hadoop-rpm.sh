#!/bin/sh

set -x
#####################################################
# Purpose: Certify Hadoop RPM
# Notes:   Base path is /opt/oath/hadoop
#
#####################################################

ARCH_REPO=https://edge.artifactory.ouroath.com:4443/artifactory/hadoop_rpms/hadoop/6Server/x86_64/Packages
NOARCH_REPO=https://edge.artifactory.ouroath.com:4443/artifactory/hadoop_rpms/hadoop/6Server/noarch/Packages
CORE=hadoopcoretree
CONFIG=hadoopconfigopenqe63blue
GPL=hadoopgplcompression-0.23.3.1343915340-1.el6.x86_64
NOARCH=1.el6.noarch
ARCH=1.el6.x86_64

# Cleanup existing RPMs
echo "Removing existing RPMs..."
cmd="echo y | sudo yum remove $CONFIG*"
$cmd
if [ $? -eq 1 ];then
  echo "Something went wrong running $cmd. Exiting!"
  exit -1
fi

cmd="echo y | sudo yum remove $CORE*"
$cmd
if [ $? -eq 1 ];then
  echo "Something went wrong running $cmd. Exiting!"
  exit -1
fi

cmd="echo y | sudo yum remove $GPL*"
$cmd
if [ $? -eq 1 ];then
  echo "Something went wrong running $cmd. Exiting!"
  exit -1
fi

# Set Hadoop variables
export HADOOP_HOME=/opt/oath/hadoop
export HADOOP_CONF_DIR=/opt/oath/hadoop/conf

# Authenticate hadoopqa
kinit -k -t /homes/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa
VERSION=`$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR version | head -1 | cut -d' ' -f2`
echo "HADOOP_VERSION=$VERSION"

# Install rpms
cmd="echo y | sudo yum install $ARCH_REPO/$GPL.rpm"
$cmd
if [ $? -eq 1 ];then
  echo "Something went wrong running $cmd. Exiting!"
  exit -1
fi

cmd="echo y | sudo yum install $ARCH_REPO/$CORE-$VERSION-$ARCH.rpm"
$cmd
if [ $? -eq 1 ];then
  echo "Something went wrong running $cmd. Exiting!"
  exit -1
fi

cmd="echo y | sudo yum install $NOARCH_REPO/$CONFIG-$VERSION-$NOARCH.rpm"
$cmd
if [ $? -eq 1 ];then
  echo "Something went wrong running $cmd. Exiting!"
  exit -1
fi

echo "List rpms installed..."
rpm -qa | grep hadoop

# Run Basic Hadoop command
cmd="$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR fs -ls /"
$cmd
if [ $? -eq 1 ];then
  echo "Something went wrong running $cmd. Exiting!"
  exit -1
fi

if [ -e /tmp/rpmtest.txt ];then
  rm /tmp/rpmtest.txt
fi
echo "TEST CONTENT FOR RPM" > /tmp/rpmtest.txt
cmd="$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR fs -put /tmp/rpmtest.txt /user/hadoopqa/"
$cmd
if [ $? -eq 1 ];then
  echo "Something went wrong running $cmd. Exiting!"
  exit -1
fi
