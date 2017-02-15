#!/usr/bin/env perl

use strict;
use 5.010;
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
        [ -s|--local_ws <source root dir>  ] : local source root directory
        [ -w|--remote_ws <workspace>       ] : remote workspace. Default remote workspace is
                                               "/grid/0/tmp/hadooptest-<REMOTE USER>-<CLUSTER>"
        [ -o|--resultsdir <results dir>    ] : test output results directory to copy to on calling host
        [ -b|--screwdriver                     ] : if specified, will use ssh and scp options for screwdriver user
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
\$ run_hadooptest_remote --cluster theoden --remote_host gwbl2003.blue.ygrid.yahoo.com --install
\$ run_hadooptest_remote --cluster theoden --remote_host gwbl2003.blue.ygrid.yahoo.com --mvn --install
\$ run_hadooptest_remote --cluster theoden --java --install
\$ run_hadooptest_remote --cluster theoden --mvn --install -u hadoopqa
\$ run_hadooptest_remote --cluster theoden --mvn --workspace /grid/0/tmp/foo --install -u hadoopqa
\$ run_hadooptest_remote -c theoden -r gwbl2003.blue.ygrid.yahoo.com -m -t SleepJobRunner
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
my $local_ws_ht;
my ($remote_ws, $remote_ws_ht);
my $username = getpwuid($<);
my $remote_username = $username;
my $test_results_dir;
my $use_screwdriver = 0;
my $screwdriver_ssh_opts = "";
my $screwdriver_scp_opts = "";

#
# Command line options processing
#
use Getopt::Long;
&Getopt::Long::Configure( 'pass_through');
my $result =
GetOptions(\%options,
    "install_only"         => \$install_only,
    "cluster|c=s"          => \$cluster,
    "remote_host|r=s"      => \$remote_host,
    "local_ws|s=s"         => \$local_ws_ht,
    "remote_ws|w=s"        => \$remote_ws,
    "user|u=s"             => \$remote_username,
    "resultsdir|o=s"       => \$test_results_dir,
    "screwdriver|b"        => sub { $use_screwdriver = 1 },
    "help|h|?"
    ) or usage(1);
usage() if $options{help};
usage("Invalid arguments!!!") if (!$result);
usage("ERROR: Required cluster value not defined!!!") if (!defined($cluster));

if ($use_screwdriver) {
    $screwdriver_ssh_opts = "-l hadoopqa -oStrictHostKeyChecking=no";
    $screwdriver_scp_opts = "hadoopqa\@";
}

# Retrieve the cluster gateway from Igor if not specified by remote_host.
my $igor = "/home/y/bin/igor";
my $re_host = "re103.ygrid.corp.gq1.yahoo.com";
if (!$remote_host) {
    my $rolename="grid_re.clusters.$cluster.gateway";
    note("fetch unspecified gateway host from igor role: '$rolename'");
    $remote_host = (-e $igor) ?
        `/home/y/bin/igor fetch -members $rolename` :
        `ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  $screwdriver_ssh_opts $re_host $igor fetch -members $rolename`;
    chomp($remote_host);
}
note("remote gateway host = '$remote_host'");
unless ($remote_host) {
    warn("ERROR: Missing required remote gateway host!!!");
    exit 1;
}

# my $tmp="/tmp"
my $tmp="/grid/0/tmp";
$remote_ws = "$tmp/hadooptest-$remote_username-$cluster"
    unless ($remote_ws);
$remote_ws_ht = "$remote_ws/hadooptest";

# CLEAN UP EXISTING WORKSPACE DIRECTORY
my $command;
if (($remote_username eq "hadoopqa") && ($username ne "hadoopqa")) {
    $command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $screwdriver_ssh_opts -t $remote_host \"if [ -d $remote_ws ]; then sudo /bin/rm -rf $remote_ws; fi\"";
} else {
    $command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  $screwdriver_ssh_opts -t $remote_host \"if [ -d $remote_ws ]; then /bin/rm -rf $remote_ws; fi\"";
}
execute($command);

# CREATE NEW WORKSPACE DIRECTORY
execute("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $screwdriver_ssh_opts -t $remote_host \"/bin/mkdir -p $remote_ws\"");
execute("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $screwdriver_ssh_opts -t $remote_host \"/bin/mkdir -p $remote_ws_ht\"");

$use_mvn = ( grep( /-j/, @ARGV ) ) ? 0 : 1;
$local_ws_ht = "/Users/$username/git/hadooptest/hadooptest" unless ($local_ws_ht);
my $tgz_dir="/tmp";
my $tgz_file_ht="hadooptest.tgz";

note("cluster='$cluster'");
note("local_ws_ht='$local_ws_ht'");
note("remote_host='$remote_host'");
note("remote_ws='$remote_ws'");
note("use_mvn='$use_mvn'");
note("install_only='$install_only'");

my $os=$^O;
my $mvn = ($os eq 'linux') ? "/home/y/bin/mvn" : "/usr/bin/mvn";

my $SSH_OPT='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null';

# INSTALL HADOOPTEST FRAMEWORK
execute("HTF_WORKSPACE=/ignore $mvn clean -Dmaven.compiler.source=1.8 -Dmaven.compiler.target=1.8 -f $local_ws_ht/pom.xml") if ($use_mvn);
execute("tar -zcf $tgz_dir/$tgz_file_ht --exclude='target' -C $local_ws_ht .");
execute("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  $SSH_OPT $tgz_dir/$tgz_file_ht $screwdriver_scp_opts$remote_host:$remote_ws_ht");
execute("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $SSH_OPT $screwdriver_ssh_opts -t $remote_host \"/bin/gtar fx $remote_ws_ht/$tgz_file_ht -C $remote_ws_ht\"");
execute("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $SSH_OPT $screwdriver_ssh_opts -t $remote_host \"/bin/mkdir -p $remote_ws_ht/target\"");
execute("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $SSH_OPT $local_ws_ht/target/*.jar $screwdriver_scp_opts$remote_host:$remote_ws_ht/target");
execute("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $SSH_OPT $screwdriver_ssh_opts -t $remote_host \"$remote_ws_ht/scripts/yinst_perl_support\"");

execute("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $screwdriver_ssh_opts -t $remote_host \"sudo chown -R hadoopqa $remote_ws;\"")
    if (($remote_username eq "hadoopqa") && ($username ne "hadoopqa"));

# EXECUTE TESTS
my $common_args = "--cluster $cluster --workspace $remote_ws_ht";
$common_args .= " ".join(" ", @ARGV);
unless ($install_only) {

    if ($use_mvn) {
        #########################
        # Execute tests via maven
        #########################
        execute("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $screwdriver_ssh_opts -t $remote_host \"cd $remote_ws_ht; $remote_ws_ht/scripts/run_hadooptest $common_args\"");

        # COPY THE TEST RESULTS BACK TO THE BUILD HOST FROM THE GATEWAY
        if ($test_results_dir) {
            execute("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -rp $screwdriver_scp_opts$remote_host:$remote_ws_ht/htf-common/target/surefire-reports/*.xml $test_results_dir");
        }
        else {
            execute("/bin/mkdir -p $local_ws_ht/target");
            execute("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -rp $screwdriver_scp_opts$remote_host:$remote_ws_ht/htf-common/target/surefire-reports $local_ws_ht/target");
        }

    	# COPY BACK THE FINGER PRINT FILE (IF IT EXISTS SO IT CAN BE GROUPED
    	# TOGETHER WITH APPLICABLE JENKINS JOBS)
    	execute("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r $screwdriver_scp_opts$remote_host:$remote_ws_ht/artifacts.stamp $local_ws_ht/target/");

        # LIST THE BUILD HOST TARGET DIR
        # execute("/usr/bin/tree $local_ws_ht/target/");
        execute("ls -lR $local_ws_ht/target/*");

        # COPY THE CLOVER CODE COVERAGE FILE BACK IF APPLICABLE
        # execute("scp -r $screwdriver_scp_opts$remote_host:$remote_ws_ht/target/clover $local_ws_ht/target/")
        #   if (( "-p" ~~ @ARGV ) || ( "-profile" ~~ @ARGV ) || ( "--profile" ~~ @ARGV ));
        execute("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r $screwdriver_scp_opts$remote_host:$remote_ws_ht/target/clover $local_ws_ht/target/")
            if ( "clover" ~~ @ARGV );

        # COPY THE JACOCO CODE COVERAGE FILE BACK IF APPLICABLE
        execute("scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -r $screwdriver_scp_opts$remote_host:$remote_ws_ht/target/site $local_ws_ht/target/")
            if ( "jacoco" ~~ @ARGV );
    }
    else {
        #########################
        # Execute tests via java
        #########################
        execute("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null $screwdriver_ssh_opts -t $remote_host \"cd $remote_ws_ht; $remote_ws_ht/scripts/run_hadooptest $common_args\"");
    }
}
