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

  Usage: $Script
-----------------------------------------------------------------------------------
The options
        -c|--cluster <cluster name>          : cluster name
        [ -i|--install_only                ] : transport packages to remote host only
        [ -u|--user <remote user>          ] : remote user; e.g. hadoopqa
        [ -r|--remote_host <gateway>       ] : remote gateway host
        [ -s|--source    <source root dir> ] : source root directory
        [ -w|--workspace <workspace>       ] : remote workspace. Default remote workspace is 
                                               "/tmp/hadooptest-<REMOTE USER>-<CLUSTER>"
        [ -h|--help                        ] : help

Pass Through options
        [ -f|            <conf file>       ] : hadooptest configuration file
        [ -p|--profile <profile>           ] : maven profile
        [ -Dthread.count=<threadcount>     ] : maven thread count
        [ -t|--test      <test suite(s)    ] : test suite name(s)
        [ -j|--java                        ] : run tests via java directly instead of via maven
        [ -m|--mvn                         ] : run tests via maven instead of via java directly
        [ -n|--nopasswd                    ] : no password prompt

Example:
\$ run_hadooptest_remote --cluster theoden --gateway gwbl2003.blue.ygrid.yahoo.com --install
\$ run_hadooptest_remote --cluster theoden --gateway gwbl2003.blue.ygrid.yahoo.com --mvn --install
\$ run_hadooptest_remote --cluster theoden --java --install
\$ run_hadooptest_remote --cluster theoden --mvn --install -u hadoopqa
\$ run_hadooptest_remote --cluster theoden --mvn --workspace /tmp/foo --install -u hadoopqa
\$ run_hadooptest_remote --cluster theoden --gateway gwbl2003.blue.ygrid.yahoo.com --mvn --test SleepJobRunner
\$ run_hadooptest_remote -c theoden -i
\$ run_hadooptest_remote -c theoden -n
\$ run_hadooptest_remote -c theoden -s /home/hadoopqa/git/hadooptest/hadooptest -i
\$ run_hadooptest_remote -c theoden -p clover -t TestVersion -u hadoopqa
        
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
my $remote_host;
my $install_only = 0;
my $use_mvn = 1;
my $local_workspace;
my $username = getpwuid($<);
my $remote_username = $username;
my $remote_workspace;

#
# Command line options processing
#
use Getopt::Long;
&Getopt::Long::Configure( 'pass_through');
my $result = 
GetOptions(\%options,
    "install_only"       => \$install_only,
    "cluster|c=s"        => \$cluster,
    "remote_host=s"      => \$remote_host,
    "local_workspace=s"  => \$local_workspace,
    "remote_workspace=s" => \$remote_workspace,
    "user|u=s"           => \$remote_username,
    "help|h|?"
    ) or usage(1);
usage() if $options{help};
usage("Invalid arguments!!!") if (!$result);
usage("ERROR: Required cluster value not defined!!!") if (!defined($cluster));

my $igor = "/home/y/bin/igor";
if (!$remote_host) {
    my $rolename="grid_re.clusters.$cluster.gateway";
    note("fetch unspecified gateway host from igor role: '$rolename'");
    $remote_host = (-e $igor) ? 
        `/home/y/bin/igor fetch -members $rolename` :
        `ssh re101.ygrid.corp.gq1.yahoo.com $igor fetch -members $rolename`;
    chomp($remote_host);
}
note("remote gateway host = '$remote_host'");

$remote_workspace = "/tmp/hadooptest-$remote_username-$cluster"
    unless ($remote_workspace);

my $command;
if ($remote_username eq "hadoopqa") {
    $command = "ssh -t $remote_host \"if [ -d $remote_workspace ]; then sudo /bin/rm -rf $remote_workspace; fi\"";
} else {
    $command = "ssh -t $remote_host \"if [ -d $remote_workspace ]; then /bin/rm -rf $remote_workspace; fi\"";
}
execute($command);

execute("ssh -t $remote_host \"/bin/mkdir -p $remote_workspace\"");

$local_workspace = "/Users/$username/git/hadooptest/hadooptest" unless ($local_workspace);
my $local_workspace_target_dir="$local_workspace/target";
my $tgz_dir="/tmp";
my $tgz_file="hadooptest.tgz";

note("cluster='$cluster'");
note("local_workspace='$local_workspace'");
note("remote_host='$remote_host'");
note("remote_workspace='$remote_workspace'");
note("use_mvn='$use_mvn'");
note("install_only='$install_only'");

execute("tar -zcf $tgz_dir/$tgz_file --exclude='target' -C $local_workspace .");
execute("scp $tgz_dir/$tgz_file $remote_host:$remote_workspace");
execute("ssh -t $remote_host \"/bin/gtar fx $remote_workspace/$tgz_file -C $remote_workspace\"");
execute("ssh -t $remote_host \"/bin/mkdir -p $remote_workspace/target\"");
execute("scp $local_workspace_target_dir/*.jar $remote_host:$remote_workspace/target");
execute("ssh -t $remote_host \"sudo chown -R hadoopqa $remote_workspace;\"") if ($remote_username eq "hadoopqa");

my $user_mvn = (("-m" ~~ @ARGV ) || ( "-mvn" ~~ @ARGV ) || ( "--mvn" ~~ @ARGV )) ? 1 : 0;
my $common_args = "--cluster $cluster --workspace $remote_workspace ".join(" ", @ARGV);
unless ($install_only) {
    if ($use_mvn) {
        execute("rm -rf $local_workspace_target_dir/surefire-reports/");
        execute("ssh -t $remote_host \"cd $remote_workspace; $remote_workspace/scripts/run_hadooptest $common_args\"");

        # WE NEED TO COPY THE TEST RESULTS FROM THE GATEWAY BACK TO THE BUILD HOST
        execute("scp -r $remote_host:$remote_workspace/target/surefire-reports $local_workspace_target_dir/");

        # execute("scp -r $remote_host:$remote_workspace/target/clover $local_workspace_target_dir/")
        #   if (( "-p" ~~ @ARGV ) || ( "-profile" ~~ @ARGV ) || ( "--profile" ~~ @ARGV ));
    } else {
        execute("ssh -t $remote_host \"$remote_workspace/scripts/run_hadooptest $common_args\"");
    }
}

