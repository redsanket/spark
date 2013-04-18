#!/usr/bin/env perl 

use strict;
use warnings;
use FindBin qw($Bin $Script);
use Test::More;

my %options=();

#
# Setup shell environment on a remote host
#

sub usage {
    my ($err_msg) = @_;
    print STDERR << "EOF";

  Usage: $Script -c cluster <option1> <values1> <option2> <values2> ...
-----------------------------------------------------------------------------------
The options
        -c|--cluster    <cluster name> : cluster name
        [ -f|           <conf file>         ] : hadooptest configuration file
        [ -j|--java                         ] : run tests via java directly instead of via maven
        [ -m|--mvn                          ] : run tests via maven instead of via java directly
        [ -n|--nopasswd                     ] : no password prompt
        [ -w|workspace  <workspace>         ] : workspace
        [ -h|--help                         ] : help

Pass Through options
        [ -P<profile>                       ] : maven profile
        [ -Pclover -Djava.awt.headless=true ]: activate clover profile
        [ -Dthread.count=<threadcount>      ] : maven thread count
	[ -Dtest=<test>...                  ] : test suite name(s). use delimitor comma for mvn, and space for java

Example:
\$ run_hadooptest --cluster theoden
\$ run_hadooptest --cluster theoden -Dtest=TestSleepJobCLI
\$ run_hadooptest --cluster theoden --mvn -Dtest=TestSleepJobCLI
\$ run_hadooptest --cluster theoden --mvn -f /homes/hadoopqa/hadooptest.conf -Dtest=TestSleepJobCLI 
\$ run_hadooptest -c theoden -j -n -Dtest=hadooptest.regression.TestVersion
\$ run_hadooptest -c theoden -m -Pclover -Djava.awt.headless=true -Dtest=TestVersion
        
EOF
    die($err_msg) if ($err_msg);
    exit 0;
}

sub execute {
    my ($command) = @_;
    note($command);
    system($command);
}

my $cluster;
my $conf = glob("~/hadooptest.conf");
my $use_mvn = 1;
my $workspace = "$Bin/..";
my $username = getpwuid($<);
my $nopasswd = ($username eq "hadoopqa") ? 1 : 0;
my $test;

#
# Command line options processing
#
use Getopt::Long;
&Getopt::Long::Configure( 'pass_through');
my $result = 
GetOptions(\%options,
    "cluster|c=s"        => \$cluster,
    "conf|f=s"           => \$conf,
    "mvn|m"              => sub { $use_mvn = 1 },
    "java|j"             => sub { $use_mvn = 0 },
    "nopasswd|n"         => \$nopasswd,
    "workspace|w=s"      => \$workspace,
    "test|t=s"          => \$test,
    "help|h|?"
    ) or usage(1);
usage() if $options{help};
usage("Invalid arguments!!!") if (!$result);
usage("ERROR: Required cluster value not defined!!!") if (!defined($cluster));

note("cluster='$cluster'");
note("conf = $conf");
note("workspace='$workspace'");
note("use_mvn='$use_mvn'");

my @tests = ($test) ? split(",", $test) : 
    (
     "hadooptest.regression.TestVersion",
     "hadooptest.regression.yarn.TestSleepJobAPI"
    );

my @classpath= (
    "/home/y/lib/jars/junit.jar",
    "/home/gs/gridre/yroot.$cluster/conf/hadoop/"
    );

unless ($use_mvn) {
    push(@classpath, "$workspace/target/hadooptest-1.0-SNAPSHOT.jar");
    push(@classpath, "$workspace/target/hadooptest-1.0-SNAPSHOT-tests.jar");
}

my $hadoop_install="/home/gs/gridre/yroot.$cluster";
my @hadoop_jar_paths = (
    "$hadoop_install/share/hadoop/share/hadoop/common/lib",
    "$hadoop_install/share/hadoop-*/share/hadoop/common",
    "$hadoop_install/share/hadoop-*/share/hadoop/common/lib",
    "$hadoop_install/share/hadoop-*/share/hadoop/hdfs",
    "$hadoop_install/share/hadoop-*/share/hadoop/hdfs/lib",
    "$hadoop_install/share/hadoop-*/share/hadoop/yarn",
    "$hadoop_install/share/hadoop-*/share/hadoop/yarn/lib",
    "$hadoop_install/share/hadoop-*/share/hadoop/mapreduce",
    "$hadoop_install/share/hadoop-*/share/hadoop/hdfs"
    );

foreach my $path (@hadoop_jar_paths) {
    push(@classpath, `echo -en $path`.'/*');
};

note("CLASSPATH=".join(":",@classpath));

# TODO: long term this should move into the java test framework
execute("stty -echo; /usr/kerberos/bin/kinit $username\@DS.CORP.YAHOO.COM; stty echo")
    unless ($nopasswd);

my $pom_opt="-f pom-ci.xml";
my $mvn_settings_opt="-gs $workspace/resources/yjava_maven/settings.xml.gw";
my $java_lib_path="-Djava.library.path=/home/gs/gridre/yroot.$cluster/share/hadoop/lib/native/Linux-amd64-64/";
my @common_args = (
    "-DCLUSTER_NAME=$cluster",
    "-DWORKSPACE=$workspace",
    "-Dhadooptest.config=$conf",
    join(" ", @ARGV)
    );

my $test_opt = ($use_mvn) ?
    "-Dtest=".join(",",@tests) :
    join(" ", @tests);

if ($use_mvn) {
    # RUN TESTS VIA MAVEN
    execute("/usr/local/bin/yinst install yjava_maven -br test -yes") unless (-e "/home/y/bin/mvn");

    my $cmd = "ls -l /home/gs/gridre/yroot.$cluster/share/hadoop|cut -d'>' -f2|cut -d'-' -f2";
    note($cmd);
    chomp(my $hadoop_version = `$cmd`);

    execute("CLUSTER=$cluster ".
            "HADOOP_VERSION=$hadoop_version ".
            "HADOOP_SHARE=/home/gs/gridre/yroot.$cluster/share/hadoop-$hadoop_version/share/hadoop/ ".
            "/home/y/bin/mvn $pom_opt $mvn_settings_opt clean test -DfailIfNoTests=false ".
            join(" ", @common_args)." ".
            $test_opt);

} else {
    # RUN TESTS VIA JAVA
    execute("/home/y/bin/java -cp ".
            join(":",@classpath)." ".
            join(" ",@common_args)." ".
            "$java_lib_path ".
            "org.junit.runner.JUnitCore ".
            $test_opt);
}






