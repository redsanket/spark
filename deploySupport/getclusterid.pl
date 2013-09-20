#!/bin/env perl

#
# Simple script to run in the /home/y (or similar) in a hadoop-0.22
# area, to run a specific command:
#	$hadoop_hdfs_home/bin/hdfs   namenode   -genclusterid
# 
# Output (success): if the 'hdfs' command has expected output, then this
# will output the cluster-id and a 0 exit-code.

# Output (fail): if the 'hdfs' fails or has not got expected output,
# it will output the 'hdfs' output and a 1 exit-code.

# usage:
#     clusterid=`cd /home/y &&  perl genclusterid.pl`
#

my $t = `pwd`;
chomp($t);

$ENV{"HADOOP_CONF_DIR"} = $t . "/conf/hadoop";
$ENV{"HADOOP_COMMON_HOME"} = $t . "/share/hadoop";
$ENV{"HADOOP_HDFS_HOME"} = $t . "/share/hadoop";
$ENV{"HADOOP_MAPRED_HOME"} = $t . "/share/hadoop";

foreach $k ("HADOOP_CONF_DIR", "HADOOP_COMMON_HOME", "HADOOP_HDFS_HOME", "HADOOP_MAPRED_HOME") {
     die $ENV{$k} . " was not found."	if ( ! -e ($ENV{$k} ) );
}

my $cmd = $ENV{"HADOOP_HDFS_HOME"} . "/bin/hdfs namenode -genclusterid ";
my $ret = `$cmd  2>&1`;

$re = qr{Generating new cluster id:
(.*)}m;

if ($ret =~ $re) {
	print "$1\n";
        exit 0;
} else {
	print $ret;
	exit 1;
}

