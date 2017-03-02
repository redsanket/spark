#!/usr/bin/env perl

# This script can be used to collect the installed versions of stack
# components from a Hadoop cluster with Igor configuration.
#
# Parameters:
#
#    cluster - the name of your cluster (like omegaf)
#    workspace - the directory where you want the version output file
#                to be written to.  The name of the file will be
#                installed_versions.txt.

use strict;
use warnings;

my $cluster = $ARGV[0];
my $workspace = $ARGV[1];

my $rolename;
my $result;
my $host;

sub getIgorHost {
    my $role = $_[0];
    
    my $roclhost = `rocl -r $role -m`;
    chomp($roclhost);
    return $roclhost;
}

# PIG
$rolename = "grid_re.clusters.$cluster.gateway";
$host = getIgorHost($rolename);
$result = `ssh -t $host \"SUDO_USER=hadoopqa sudo yinst ls -root /home/y/var/yroots/$cluster/home/gs/gridre/yroot.$cluster | grep pig"`;
print $result;
my ($pig_version) = $result =~ /^pig-(.*)/;
print "PIG = " . $pig_version . "\n";

# HIVE
$rolename = "grid_re.clusters.$cluster.hive_client";
$host = getIgorHost($rolename);
$result = `ssh -t $host \"yinst ls | grep hive\"`;
print $result;
my ($hive_version) = $result =~ /^hive-(.*)/;
print "HIVE = " . $hive_version . "\n";

# OOZIE
$rolename = "grid_re.clusters.$cluster.oozie";
$host = getIgorHost($rolename);
$result = `ssh -t $host \"yinst ls | grep yoozie\"`;
print $result;
my ($oozie_version) = $result =~ /^yoozie-(.*)/;
print "OOZIE = " . $oozie_version . "\n";

# HCAT
$rolename = "grid_re.clusters.$cluster.hcat";
$host = getIgorHost($rolename);
$result = `ssh -t $host \"yinst ls | grep hcat\"`;
print $result;
my ($hcat_version) = $result =~ /^hcat_common-(.*)/;
print "HCAT = " . $hcat_version . "\n";

# HBASE
$rolename = "hbase.qe.cluster.$cluster.hbase_client";
$host = getIgorHost($rolename);
$result = `ssh -t $host \"yinst ls | grep hbase\"`;
print $result;
my ($hbase_version) = $result =~ /^hbase-(.*)/;
print "HBASE = " . $hbase_version . "\n";

# HDFSPROXY
$rolename = "grid_re.clusters.$cluster.hdfsproxy";
$host = getIgorHost($rolename);
$result = `ssh -t $host \"yinst ls -yroot hit_hp | grep hdfsproxy\"`;
print $result;
my ($hdfsproxy_version) = $result =~ /ygrid_hdfsproxy-(.*)/;
print "HDFSPROXY = " . $hdfsproxy_version . "\n";

# HADOOP
$rolename = "grid_re.clusters.$cluster.jobtracker";
$host = getIgorHost($rolename);
$result = `ssh -t $host \"hadoop version\"`;
print $result;
my ($hadoop_version) = $result =~ /^Hadoop (.*)/;
print "HADOOP = " . $hadoop_version . "\n";

# JAVA
$rolename = "grid_re.clusters.$cluster.jobtracker";
$host = getIgorHost($rolename);
$result = `ssh -t $host \"/home/gs/java/jdk/bin/java -version\"`;
print $result;
my ($java_version) = $result =~ /^java version (.*)/;
print "JAVA = " . $java_version . "\n";

print "\n\n";
print localtime . "\n";
print "CLUSTER = " . $cluster . "\n";
print "JAVA = " . $java_version . "\n";
print "HADOOP = " . $hadoop_version . "\n";
print "HDFSPROXY = " . $hdfsproxy_version . "\n";
print "HBASE = " . $hbase_version . "\n";
print "HCAT = " . $hcat_version . "\n";
print "OOZIE = " . $oozie_version . "\n";
print "HIVE = " . $hive_version . "\n";
print "PIG = " . $pig_version . "\n";

`mkdir -p $workspace`;
my $outputfile = ">$workspace\/installed_versions.txt";
print "OUTPUT FILE = $outputfile\n\n";
open (VERSIONFILE, $outputfile);
print VERSIONFILE localtime . "\n";
print VERSIONFILE "CLUSTER = " . $cluster . "\n";
print VERSIONFILE "JAVA = " . $java_version . "\n";
print VERSIONFILE "HADOOP = " . $hadoop_version . "\n";
print VERSIONFILE "HDFSPROXY = " . $hdfsproxy_version . "\n";
print VERSIONFILE "HBASE = " . $hbase_version . "\n";
print VERSIONFILE "HCAT = " . $hcat_version . "\n";
print VERSIONFILE "OOZIE = " . $oozie_version . "\n";
print VERSIONFILE "HIVE = " . $hive_version . "\n";
print VERSIONFILE "PIG = " . $pig_version . "\n";
close (VERSIONFILE);
