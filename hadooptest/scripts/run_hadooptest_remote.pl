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
                                               "/tmp/hadooptest-<REMOTE USER>-<CLUSTER>"
        [ -build_coretest                  ] : build coretest
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
\$ run_hadooptest_remote --cluster theoden --mvn --workspace /tmp/foo --install -u hadoopqa
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
my $build_coretest = 0;
my ($local_ws_ht, $local_ws_ct);
my ($remote_ws, $remote_ws_ht, $remote_ws_ct);
my $username = getpwuid($<);
my $remote_username = $username;

#
# Command line options processing
#
use Getopt::Long;
&Getopt::Long::Configure( 'pass_through');
my $result = 
GetOptions(\%options,
    "install_only"         => \$install_only,
    "build_coretest"       => \$build_coretest,
    "cluster|c=s"          => \$cluster,
    "remote_host|r=s"      => \$remote_host,
    "local_ws|s=s"         => \$local_ws_ht,
    "remote_ws|w=s"        => \$remote_ws,
    "user|u=s"             => \$remote_username,
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

$remote_ws = "/tmp/hadooptest-$remote_username-$cluster"
    unless ($remote_ws);
$remote_ws_ht = "$remote_ws/hadooptest";
$remote_ws_ct = "$remote_ws/coretest";

# CLEAN UP EXISTING WORKSPACE DIRECTORY
my $command;
if (($remote_username eq "hadoopqa") && ($username ne "hadoopqa")) {
    $command = "ssh -t $remote_host \"if [ -d $remote_ws ]; then sudo /bin/rm -rf $remote_ws; fi\"";
} else {
    $command = "ssh -t $remote_host \"if [ -d $remote_ws ]; then /bin/rm -rf $remote_ws; fi\"";
}
execute($command);

# CREATE NEW WORKSPACE DIRECTORY
execute("ssh -t $remote_host \"/bin/mkdir -p $remote_ws\"");
execute("ssh -t $remote_host \"/bin/mkdir -p $remote_ws_ht\"");
execute("ssh -t $remote_host \"/bin/mkdir -p $remote_ws_ct\"");

$use_mvn = ( grep( /-j/, @ARGV ) ) ? 0 : 1;
$local_ws_ht = "/Users/$username/git/hadooptest/hadooptest" unless ($local_ws_ht);
$local_ws_ct = "$local_ws_ht/../../coretest/coretest";
my $tgz_dir="/tmp";
my $tgz_file_ht="hadooptest.tgz";
my $tgz_file_ct="coretest.tgz";

note("cluster='$cluster'");
note("local_ws_ht='$local_ws_ht'");
note("remote_host='$remote_host'");
note("remote_ws='$remote_ws'");
note("use_mvn='$use_mvn'");
note("install_only='$install_only'");

my $os=$^O;
my $mvn = ($os eq 'linux') ? "/home/y/bin/mvn" : "/usr/bin/mvn";

# INSTALL HADOOPTEST FRAMEWORK 
execute("$mvn clean") if ($use_mvn);
execute("tar -zcf $tgz_dir/$tgz_file_ht --exclude='target' -C $local_ws_ht .");
execute("scp $tgz_dir/$tgz_file_ht $remote_host:$remote_ws_ht");
execute("ssh -t $remote_host \"/bin/gtar fx $remote_ws_ht/$tgz_file_ht -C $remote_ws_ht\"");
execute("ssh -t $remote_host \"/bin/mkdir -p $remote_ws_ht/target\"");
execute("scp $local_ws_ht/target/*.jar $remote_host:$remote_ws_ht/target");

# INSTALL CORETEST FRAMEWORK 
if ($build_coretest) {
    execute("cd $local_ws_ct; $mvn clean") if ($use_mvn);
    execute("tar -zcf $tgz_dir/$tgz_file_ct --exclude='target' -C $local_ws_ct .");
    execute("scp $tgz_dir/$tgz_file_ct $remote_host:$remote_ws_ct");
    execute("ssh -t $remote_host \"/bin/gtar fx $remote_ws_ct/$tgz_file_ct -C $remote_ws_ct\"");
    execute("ssh -t $remote_host \"/bin/mkdir -p $remote_ws_ct/target\"");
    execute("scp $local_ws_ct/target_dir/*.jar $remote_host:$remote_ws_ct/target");
}

execute("ssh -t $remote_host \"sudo chown -R hadoopqa $remote_ws;\"") if ($remote_username eq "hadoopqa");

# EXECUTE TESTS
my $common_args = "--cluster $cluster --workspace $remote_ws_ht";
$common_args .= " --build_coretest" if ($build_coretest);
$common_args .= " ".join(" ", @ARGV);
unless ($install_only) {

    if ($use_mvn) {
        #########################
        # Execute tests via maven
        #########################
        execute("rm -rf $local_ws_ct/target/surefire-reports/");
        execute("ssh -t $remote_host \"cd $remote_ws_ht; $remote_ws_ht/scripts/run_hadooptest $common_args\"");

        # COPY THE TEST RESULTS BACK TO THE BUILD HOST FROM THE GATEWAY 
        execute("scp -r $remote_host:$remote_ws_ht/target/surefire-reports $local_ws_ht/target/");

    	# COPY BACK THE FINGER PRINT FILE (IF IT EXISTS SO IT CAN BE GROUPED
    	# TOGETHER WITH APPLICABLE JENKINS JOBS)
    	execute("scp -r $remote_host:$remote_ws_ht/artifacts.stamp $local_ws_ht/target/");

        # LIST THE BUILD HOST TARGET DIR
        # execute("/usr/bin/tree $local_ws_ht/target/");
        execute("ls -l /usr/bin/tree $local_ws_ht/target/*");

        # COPY THE CLOVER CODE COVERAGE FILE BACK IF APPLICABLE
        # execute("scp -r $remote_host:$remote_ws_ht/target/clover $local_ws_ht/target/")
        #   if (( "-p" ~~ @ARGV ) || ( "-profile" ~~ @ARGV ) || ( "--profile" ~~ @ARGV ));
        execute("scp -r $remote_host:$remote_ws_ht/target/clover $local_ws_ht/target/")
            if ( "clover" ~~ @ARGV );

        # COPY THE JACOCO CODE COVERAGE FILE BACK IF APPLICABLE
        execute("scp -r $remote_host:$remote_ws_ht/target/site $local_ws_ht/target/")
            if ( "jacoco" ~~ @ARGV );
    }
    else {
        #########################
        # Execute tests via java 
        #########################
        execute("ssh -t $remote_host \"$remote_ws_ht/scripts/run_hadooptest $common_args\"");
    }
}

