#
# Copyright Yahoo! 2011
#

=pod

=head1 NAME

package Hadoop::Test

=head1 DESCRIPTION

This module contains the test base class used by the Hadoop Tests.

=head1 FUNCTIONS

The following functions are implemented:

=cut

package Hadoop::Test;

use strict;
use warnings;
use FindBin qw($Bin $Script);
use Hadoop::Config;
use POSIX;
use Math::Round;
use Test::More;
use String::ShellQuote;
use IO::CaptureOutput qw/capture_exec/;

# This section will override the Test::More functions to integrate them into
# the QA test automation framework.
# These functions will first call the displayTestCaseResult() function to write
# the framework artifacts and then call the Test::More version of the function.
BEGIN {
    require Test::More;

    # Save off original function pointers.
    *Test::More::ok_orig = \&Test::More::ok;
    *Test::More::is_orig = \&Test::More::is;
    *Test::More::is_deeply_orig = \&Test::More::is_deeply;
    *Test::More::isnt_orig = \&Test::More::isnt;
    *Test::More::like_orig = \&Test::More::like;

    # QA test framework needs a test name. Take the name of the test file and
    # append a number to construct the test name. This variable increments each
    # time one of the overridden functions is called.
    our $testnum = 1;

    sub displayTestCaseResult {
        my ($msg, $exitcode) = @_;

        # Construct the test name.
        my $basename = $Script;
        $basename =~ /run_(.*)\.[t|pl]/;
        my $name = "$1_${testnum}";

        $ENV{TESTCASENAME}      = $name;
        $ENV{TESTCASE_DESC}     = $msg;
        $ENV{COMMAND_EXIT_CODE} = $exitcode;
        $ENV{OWNER}             = $ENV{USER} unless defined($ENV{OWNER});
        $ENV{REASONS}           = "Failed test: $msg";

        my $bugfile = (($ENV{'KNOWNBUGS_FILE'}) && (-d $ENV{'KNOWNBUGS_FILE'})) ? 
            $ENV{'WORKSPACE'}."/conf/test/".$ENV{'KNOWNBUGS_FILE'} : 
            $ENV{'WORKSPACE'}."/conf/test/known_bugs.txt";

        my $known_bug = `/bin/grep -w $name $bugfile|grep -v ^#`;
        unless (($exitcode == 0) && ($known_bug eq '')) {
            use Carp qw(longmess);
            my $mess = longmess();
            note("stack trace:",explain($mess));
        }

        invoke_shell_lib("", "displayTestCaseResult");
        $testnum++;
    }

    # INVESTIGATE:
    # The value of $exitcode can be different than the value of the $exitcode
    # returned by the method print_test_summary in the end block which is then
    # used for the SCRIPT_EXIT_CODE by library.sh. This happens when the number
    # of tests planned is greater than the number executed. When this scenario
    # happens, failcount is not incremented but the script exit code is set to 1.

    sub my_ok ($;$) {
        my ($exp, $msg) = @_;
        # The following is needed so that the correct line number is printed
        # when the test fails.
        local $Test::Builder::Level = $Test::Builder::Level + 1;

        # call Test::More::ok and retrieve exit code so can pass to artifacts.
        # 0 means success, 1 means failure.
        my $result = Test::More::ok_orig(@_);
        my $exitcode = ($result?0:1);

        # Write test case summary to the artifacts file.
        displayTestCaseResult($msg, $exitcode) if defined($ENV{ARTIFACTS_DIR});
        return $result;
    }

    sub my_is ($$;$) {
        my ($got, $expected, $msg) = @_;
        # The following is needed so that the correct line number is printed
        # when the test fails.
        local $Test::Builder::Level = $Test::Builder::Level + 1;

        # call Test::More::is and retrieve exit code so can pass to artifacts.
        # 0 means success, 1 means failure.
        my $result = Test::More::is_orig(@_);
        my $exitcode = ($result?0:1);

        # Wriet test case summary to the artifacts file.
        displayTestCaseResult($msg, $exitcode) if defined($ENV{ARTIFACTS_DIR});
        return $result;
    }

    sub my_isnt ($$;$) {
        my ($got, $expected, $msg) = @_;
        # The following is needed so that the correct line number is printed
        # when the test fails.
        local $Test::Builder::Level = $Test::Builder::Level + 1;

        # call Test::More::isnt and retrieve exit code so can pass to artifacts.
        # 0 means success, 1 means failure.
        my $result = Test::More::isnt_orig(@_);
        my $exitcode = ($result?0:1);

        # Wriet test case summary to the artifacts file.
        displayTestCaseResult($msg, $exitcode) if defined($ENV{ARTIFACTS_DIR});
        return $result;
    }

    sub my_is_deeply {
        my ($got, $expected, $msg) = @_;
        # The following is needed so that the correct line number is printed
        # when the test fails.
        local $Test::Builder::Level = $Test::Builder::Level + 1;

        # call Test::More::is and retrieve exit code so can pass to artifacts.
        # 0 means success, 1 means failure.
        my $result = Test::More::is_deeply_orig(@_);
        my $exitcode = ($result?0:1);

        # Wriet test case summary to the artifacts file.
        displayTestCaseResult($msg, $exitcode) if defined($ENV{ARTIFACTS_DIR});
        return $result;
    }

    sub my_like ($$;$) {
        my ($got, $expected, $msg) = @_;
        # The following is needed so that the correct line number is printed
        # when the test fails.
        local $Test::Builder::Level = $Test::Builder::Level + 1;

        # call Test::More::isnt and retrieve exit code so can pass to artifacts.
        # 0 means success, 1 means failure.
        my $result = Test::More::like_orig(@_);
        my $exitcode = ($result?0:1);

        # Wriet test case summary to the artifacts file.
        displayTestCaseResult($msg, $exitcode) if defined($ENV{ARTIFACTS_DIR});
        return $result;
    }

    *Test::More::ok = *my_ok;
    *Test::More::is = *my_is;
    *Test::More::isnt = *my_isnt;
    *Test::More::is_deeply = *my_is_deeply;
    *Test::More::like = *my_like;
    Test::More->import;

    *CORE::GLOBAL::die = sub {
        displayTestCaseResult(@_, 1);
        die(@_);
    };

}

END {
    require Test::More;
    print_test_summary();
}

my $Lib_path = "$FindBin::Bin/../../../../lib/Hadoop";
my $GetConfig_job = 'GetConfig2'; # GetConfig or GetConfig2
my $Config = Hadoop::Config->new;
sub config { $Config }

# Define Base Node
sub new {
    my ($class) = @_;
    my $self = {
        config => $Config,
    };
    bless $self, $class;
};


# Used primarily for debugging: prints a sorted hash, filtered
# by $regex if specified
sub print_sorted_hash {
    my ($self, $hash_ref, $regex) = @_;
    foreach my $key (sort keys %$hash_ref) {
        note("$key => $hash_ref->{$key}") if (!defined($regex) || ($key =~ /$regex/));
    }
}


=head2 get_host_shortname($self, $hostname)

get the shortname for the given hostname

=over 2

=item I<Parameters>:

   * hostname

=item I<Returns>:

   * host shortname

=back

=cut
sub get_host_shortname {
    my ($self, $hostname) = @_;
    my @tokens = split('\.', $hostname);
    return $tokens[0];
}


#################################################################################
# SUB TYPE: SHELL BASED FRAMEWORK REALTED SUBROUTINES
#################################################################################

=head2 invoke_shell_lib($self, $function_call)

Invoke the shell library.sh function.

=over 2

=item I<Parameters>:

   * Function call
   * Optional Library file

=item I<Returns>:

   * Function return value

=back

=cut
sub invoke_shell_lib {
    my ($self, $function_call, $library_file) = @_;
    my $lib_path = (-d "$Bin/../../../../lib/") ?
        "$Bin/../../../../lib/" : "$Bin/../../lib/";
    note("WARNING: WORKSPACE is not defined and is needed by library.sh") unless $Config->{WORKSPACE};
    $library_file = 'library.sh' unless defined($library_file);
    my @command = ( "source $lib_path/$library_file;",
                    "$function_call" );
    chomp(my $result = `@command`);
    note($result);
    return $result;
}


=head2 print_test_summary

Print to STDOUT a summary of the test results. This is to satisfy the shell base
test framework (driver.sh) output resquirement.

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * Null

=back

=cut
sub print_test_summary {
    my $tb = Test::More->builder;
    my @summary = $tb->summary;
    my $num_expected_tests = $tb->{Expected_Tests};
    my $num_executed_tests = scalar(@summary);
    my $num_passed = grep {$_} @summary;
    my $num_failed = $num_executed_tests - $num_passed;
    # Report if the number of tests planned is greater than the number executed
    my $num_notrun = $num_expected_tests - $num_executed_tests;

    print "\n\n######################################  SUMMARY  ##############################\n";
    print "##  TOTAL TESTS   : $num_expected_tests\n";
    print "##  PASSED TESTS  : $num_passed\n";
    print "##  FAILED TESTS  : $num_failed\n";
    print "##  NOT RUN TESTS : $num_notrun\n";
    print "####################################  END SUMMARY  ############################\n";

    # MAINTENANCE NOTE: if the number of tests executed is different than
    # expected, it should be handled not only here in the perl framework, but
    # also upstream in the shell based framework. When the shell based framework
    # has been modified accordingly, the following line should then be uncommented. 
    #
    # Revision 53488: 
    # Fix issue with exit codes for perl framework that was causing tests that failed to run to still indicate success
    # http://svn.corp.yahoo.com/viewvc/yahoo/platform/grid/projects/trunk/hudson/internal/HadoopQEAutomation/common/Test.pm?r1=53365&r2=53488
    #
    # my $exit_code = ($num_executed_tests == $num_expected_tests) ? $num_failed : $num_expected_tests - $num_passed;
    my $exit_code = $num_failed;

    print "\nSCRIPT_EXIT_CODE=$exit_code\n";
    return $exit_code;
}


#################################################################################
# SUB TYPE: HEADLESS / SECURITY REALTED SUBROUTINES
#################################################################################
=head2 is_headless_user()

Determine if the specified user is a headless user

=over 2

=item I<Parameters>:

   * user id
     If an user id is not passed in, it will default to the current user running
     the test.

=item I<Returns>:

   * Integer value 1 if true, and integer value 0 if false.

=back

=cut
sub is_headless_user {
    my ($self, $user) = @_;
    $user = getpwuid($>) unless (defined($user));
    return $user =~ /hadoop|hdfs|hdfsqa|mapred|mapredqa/;
}


=head2 get_sudoer($self, $component)

Get the sudoer name for controlling a particular component

=over 2

=item I<Parameters>:

   * component

=item I<Returns>:

   * Sudoer name for controlling a particular component

=back

=cut
sub get_sudoer {
    my ($self, $component) = @_;
    my $sudoer;
    if ($component eq 'namenode') {
        $sudoer = 'hdfs';
    } elsif ($component eq 'datanode') {
        $sudoer = 'root';
    } elsif (($component eq 'jobtracker') || ($component eq 'tasktracker')) {
        $sudoer = 'mapred';
    } elsif ($component eq 'resourcemanager') {
        $sudoer = 'mapredqa';
    }
    return $sudoer;
}


=head2 setup_kerberos($self, user)

Setup the kerberos ticket for the headless user

=over 2

=item I<Parameters>:

   * user
   * verbose

=item I<Returns>:

   * Integer value 0 if cluster is fully up and integer value 1 otherwise.

=back

=cut
sub setup_kerberos {
    my ($self, $user, $verbose) = @_;
    my @command = ();

    # Calling the user_kerb_lib.sh shell library will not work since the
    # export of KRB5CCNAME will not occur in the same shell that the test
    # test is running on.
    # $self->invoke_shell_lib("getKerberosTicketForUser $user",
    #                         'user_kerb_lib.sh') if ($user);
    note("getKerberosTicketForUser $user") if ($verbose);

    my $whoami = getpwuid($>);
    my $kerberos_tickets_dir =
        "/tmp/$whoami/$Config->{CLUSTER}/kerberosTickets";
    system('mkdir', '-p', $kerberos_tickets_dir) unless (-d $kerberos_tickets_dir);
    my $cache_name = "$kerberos_tickets_dir/$user.kerberos.ticket";

    # /usr/kerberos/bin deprecated in RHEL6, but in hadoopqa PATH. So remove.
    my $kinit='kinit';
    # e.g. kinit -c /tmp/hadoopqe/kerberosTickets/hadoop1.kerberos.ticket
    # -k -t /homes/hdfsqa/etc/keytabs/hadoop1.dev.headless.keytab hadoop1
    my $kinit_user = $user;
    $kinit_user = "$user/dev.ygrid.yahoo.com\@DEV.YGRID.YAHOO.COM" if ($user eq 'hdfs');
    my $keytab_files_dir =
        ($user eq 'hadoopqa') ? "/homes/$user" : '/homes/hdfsqa/etc/keytabs';
    @command = ("$kinit", '-c',
                $cache_name,
                '-k', '-t',
                "$keytab_files_dir/$user.dev.headless.keytab",
                $kinit_user);
    note("command=@command") if ($verbose);
    system("@command");

    # e.g. export KRB5CCNAME=/tmp/hadoopqe/kerberosTickets/hadoop1.kerberos.ticket
    $ENV{KRB5CCNAME} = $cache_name;
    note("export KRB5CCNAME=$cache_name") if ($verbose);
}


#################################################################################
# SUB TYPE: HADOOP COMMANDS REALTED SUBROUTINES
#################################################################################

=head2 run_hadoop_command($self, $hash, $type, $verbose)

Run the specified hadoop command with specified arguments.

=over 2

=item I<Parameters>:

   * A hash reference of:
     - user (optional)
     - command (string or array reference)
     - host (optional)
     - queue (optional)
     - args (optional array reference)
   * type
     - {hadoop, mapred} default is hadoop
   * verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_hadoop_command {
    my ($self, $hash, $type, $verbose) = @_;

    $verbose = 1 unless defined($verbose);
    # If user is not defined, check if user running the test is a headless user.
    # If so, define the user variable so that it can properly auto-initialize
    # the kerberos ticket.
    my $user = $hash->{user} if defined($hash->{user});
    unless (defined($user)) {
        my $whoami = getpwuid($>);
        $user = $whoami if ($self->is_headless_user($whoami));
    }
    my $user_str = '';
    if ($user && $self->is_headless_user($user)) {
        $self->setup_kerberos($user, 0);
        $user_str = "as user '$user'";
    }
    my $args = $self->_process_args($hash->{args});

    # Types of valid hadoop binaries are {hadoop, hdfs, mapred, yarn}
    $type = 'HADOOP' unless $type;
    my $main_cmd = $Config->{uc($type)};
    my @command = ($main_cmd);
    push(@command, '--config', $self->{tmp_conf_dir}) if defined($self->{gateway_conf});
    push(@command, ref($hash->{command}) eq 'ARRAY' ? @{$hash->{command}} : $hash->{command});
    push(@command, "-Dmapred.job.queue.name=$hash->{queue}") if defined($hash->{queue});
    push(@command, @$args) if defined($args);
    unshift(@command, 'ssh', $hash->{host}) if defined($hash->{host});

    my $header = "Running hadoop command";
    $header .= " $user_str" if $user_str;
    note("$header (@command)") if ($verbose);

    if ((defined($hash->{mode})) && ($hash->{mode} eq 'system')) {
        return system( "@command" );
    }
    else {
        return capture_exec( "@command" );
    }
}

=head2 run_jar_command($self, $hash, $verbose)

Run the specified command from the jar, with specified arguments.
If 'jar' is undefined, it uses the hadoop examples/test jar.

=over 2

=item I<Parameters>:

   * A hash reference of:
     - user
     - jar
     - command (string or array reference)
     - args (array reference)
   * Verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_jar_command {
    my ($self, $hash, $verbose) = @_;

    note("run_jar_command: ", explain($hash)) if $verbose;
    local $hash->{jar} =
        (($Config->{VERSION} eq '0.20') || ($Config->{VERSION} =~ '^1\.'))
            ? $Config->{HADOOP_EX_JAR}
            : $Config->{HADOOP_TEST_JAR}
        unless ( defined($hash->{jar} ) );
    # Streaming jobs don't have a 'command'
    local $hash->{command} = '' unless defined($hash->{command});
    local $hash->{command} = ['jar', $hash->{jar}, $hash->{command}];
    delete($hash->{jar});

    note("run_jar_command", explain($hash)) if $verbose;
    return $self->run_hadoop_command($hash);
}


=head2 _process_args($self, $args)

Converts the args array ref to a valid array of args for passing to a hadoop command
Places the -D options first in the array

=over 2

=item I<Parameters>:

   * array reference of args (supports (-D, arg) & -Darg)

=item I<Returns>:

   * Array ref of valid arguments for hadoop commands

=back

=cut
sub _process_args {
    my ($self, $args) = @_;
    return unless defined($args);

    my @arg_copy = @$args;
    # Make sure the -D options appear first, or they don't always work
    my @clean_args;
    while (@arg_copy) {
        my $arg = shift(@arg_copy);
        if ($arg eq '-D') {
            unshift(@clean_args, $arg, shift(@arg_copy));
        } elsif ($arg =~ /^-D/) {
            unshift(@clean_args, $arg);
        } else {
            push(@clean_args, $arg);
        }
    }
    return \@clean_args;
}


=head2 jmxGet

Call 'hdfs jmxget' with the specified commands and return the key and value
that were returned.

=over 2

=item I<Parameters>:

   * service: e.g., NameNode, DataNode, JobTracker, etc.
   * class: Class must be a member of 'service'. For e.g., the NameNode service
            contains the NameNodeInfo class.
   * attribute: an mbean for the specified class. For e.g., servicd = NameNode,
                class = NameNodeInfo, attribute = Version returns the version
                of the current instance of Hadoop.

=item I<Returns>:

   * attr: The attributed that was requested. If the attribute 'Version' is
           specified, Version should be returned.
   * val:  The value of the mbean. If the attribute 'Version' is specified, val
           will equal the version of the current instance of Hadoop.

=back

=cut
sub jmxGet{
    my ($self, $service, $class, $attribute, $node, $port) = @_;

    my @command;
    push (@command, "$Config->{HADOOP_HDFS_HOME}/bin/hdfs jmxget");
    push (@command, "-server $node");
    push (@command, "-port $port");
    push (@command, "-service ${service},name=${class} $attribute");

    my ($mbean, $stderr, $success, $exit_code) = capture_exec( "@command" );

    if ($success) {
        chomp($mbean);
        # Format of output of 'hdfs jmxget' command should be '<key>=<value>'.
        my ($attr,$val) = $mbean =~ /^($attribute)=(.*)$/;
        return ($attr,$val);
    }
    note("stderr == $stderr");
    return ('', '');
}


#################################################################################
# SUB TYPE: CLUSTER DAEMON REALTED SUBROUTINES
#################################################################################

=head2 get_daemon_status($component, $daemon_hosts, $verbose)

Get the component daemon status for daemon host(s).

=over 2

=item I<Parameters>:

   * Component      : jobtracker, namenode, datanode, tasktracker, etc
   * Daemon host(s) : Optional hostnames (e.g. foo or foo[])
                      If not specified, default is to use all hosts
   * verbose

=item I<Returns>:

   * the return value is integer value
      1 if the component is fully up
      0 if the component is fully down
     -1 if the component is partially down
   * hash reference of the status for each host(s)

=back

=cut
sub get_daemon_status {
    my ($self, $component, $daemon_hosts, $verbose) = @_;
    my $return_status = 0;
    my $hosts_status = {};
    my $DAEMON_ALL_UP = 1;
    my $DAEMON_ALL_DOWN = 0;
    my $DAEMON_PARTIAL_DOWN = -1;
    $daemon_hosts = $self->get_daemon_hosts($component) unless $daemon_hosts;

    if (ref($daemon_hosts) eq 'ARRAY') {
        my $num_hosts = scalar(@$daemon_hosts);
        my $num_up = 0;
        my $num_down = 0;
        my $result;
        foreach my $host (@$daemon_hosts) {
            $result = $self->get_single_daemon_status($component, $host, $verbose);
            note("$component daemon on $host is ", ($result) ? "up":"down")
                if $verbose;
            if ($result == 1) {
                $num_up++;
            } else {
                $num_down++;
            }
            $hosts_status->{$host} = $result;
        }
        if ($verbose) {
            note("WARN: a total of $num_down host(s) are down.") if ($num_down);
        }
        if ($num_up == 0) {
            $return_status = $DAEMON_ALL_DOWN;
        } elsif ($num_down == 0) {
            $return_status = $DAEMON_ALL_UP;
        } else {
            $return_status = $DAEMON_PARTIAL_DOWN;
        }
        return ($return_status, $hosts_status);
    } else {
        if ($daemon_hosts =~ /,/) {
            my @hosts=split(',',$daemon_hosts);
            return $self->get_cluster_status([$component], \@hosts, $verbose);
        }
        $return_status =
            $self->get_single_daemon_status($component, $daemon_hosts, $verbose);
        $hosts_status->{$daemon_hosts} = int($return_status);
        return ($return_status, $hosts_status);
    }
}


=head2 get_cluster_status()

Get the cluster_status

=over 2

=item I<Parameters>:

   * component
   * component_host
   * verbose

=item I<Returns>:

   * Integer value 1 if cluster is fully up and integer value 0 otherwise.
   * hash reference of the status for each host(s)

=back

=cut
sub get_cluster_status {
    my ($self, $components, $component_host, $verbose) = @_;
    unless ($components) {
        $components = [
                       lc($Config->{RESOURCEMANAGER_NAME}),
                       lc($Config->{NODEMANAGER_NAME}),
                       'namenode',
                       'datanode',
                       ];
    }
    my $overall_status = 1;
    my $status = {};
    foreach my $component (@$components) {
        $component = lc($component);
        my ($component_status, $hosts_status) = 
            $self->get_daemon_status($component, $component_host, $verbose);
        $status->{uc($component)} = $hosts_status;
        note("--> $component daemon is ",
             ($component_status == 1) ? "up" : "down");
        $overall_status = 0 unless ($component_status == 1);
    }
    return ($overall_status, $status);
}

# Get the component daemon status for a single host. This is meant to be called
# internally only. 
# Return value is integer value 
# 1 if the daemon is up
# 0 if the daemon is down.
sub get_single_daemon_status {
    my ($self, $component, $daemon_host, $verbose) = @_;
    my $admin_host = $Config->{ADMIN_HOST}[0];
    # my $sudoer     = $self->get_sudoer($component);
    my $sudoer = 'hadoopqa';

    $daemon_host = $self->get_daemon_hosts($component) unless $daemon_host;

    my $prog = ($component eq 'datanode') ? 'jsvc.exec' : 'java';
    my @command = ( 'ssh', $admin_host, 'pdsh', '-w', "$daemon_host",
        'ps auxww', '|', 'grep',
        shell_quote("$prog -Dproc_$component"),
         '|','grep -v grep -c');
    @command = ('sudo', 'su', "$sudoer", '-c', shell_quote("@command"))
        unless $self->is_headless_user();

    note("@command") if ($verbose);
    chomp(my $job = `@command`);
    # For data node, there should be two jobs per host.
    # One is started by root, and the other by hdfs.
    if ($component eq 'datanode') {
        return $job eq '2';
    }
    return $job;
}


=head2 get_daemon_hosts($self, $component)

Get the hostname(s) for the specified component (e.g. jobtracker, namenode, datanode, etc.)

=over 2

=item I<Parameters>:

   * component

=item I<Returns>:

   * Array of hostnames for the specified component

=back

=cut
sub get_daemon_hosts {
    my ($self, $component) = @_;
    my $hostnames = $Config->{NODES}->{uc($component)}->{HOST};
    diag("ERROR: Found no hostname(s) for the component '$component'",
         explain($Config->{NODES}->{uc($component)})) unless $hostnames;
    return $hostnames;
}


=head2 control_daemon($operation, $component, $daemon_host, $config_dir, $verbose)

Control (stop/start) the component daemon
(e.g jobtracker, namenode, datanode, etc.)

=over 2

=item I<Parameters>:

   * Opereation     : string 'stop', 'start'
   * Component      : jobtracker, namenode, datanode, tasktracker, etc
   * Daemon host(s) : Optional hostnames (e.g. foo[0-10], or foo0,foo1,...)
                      If not specified, default is to use all hosts
   * Config Dir     : Optional hadoop configuration directory
   * Verbose        : 0 for off, 1 for on

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub control_daemon {
    my ($self, $operation, $component, $daemon_hosts, $config_dir, $verbose) = @_;
    my $admin_host = $Config->{ADMIN_HOST}[0];
    my $sudoer     = $self->get_sudoer($component);
    $daemon_hosts   = $self->get_daemon_hosts($component) unless $daemon_hosts;
    my $return = 0;

    my @command;

    # This alternative way of calling the hadoop-daemon.sh will not work when
    # running as hadoopqa user.
    #
    # @command = ( "$Config->{HADOOP_BIN}/hadoop-daemon.sh", '--config',
    #              $Config->{HADOOP_CONF_DIR}, $operation, $component);
    # @command = ( 'ssh', $admin_host, 'pdsh', '-w', $daemon_host, shell_quote("@command"));
    # @command = ( 'sudo', 'su', "$sudoer", '-c', shell_quote("@command") );

    # Set the yinst config variable
    # if (($component ne 'nodemanager') && ($operation ne 'stop'))
    if ($operation ne 'stop')
    {
        if (!defined($config_dir)) {
            # Setting the config_dir to the tmp_conf_dir value can be
            # potentially dangerous if the config dir is not changed on
            # all the components hosts. This could cause some component to fail
            # to start if the tmp config dir does not exists on some hosts. 
            #
            # For example, if connf dir is changed only on the RM host, and this
            # subroutine is then called without explicitly passing in the
            # config_dir to start the NM, then it would attempt to restart the
            # NM with the tmp_conf_dir which does not exist on the NM hosts.
            #
            # The safter thing to do is to default to config_dir to
            # $Config->{HADOOP_CONF_DIR}. Howeveer, because some tests are
            # already relying on mechanism. Change this behavior without also
            # changing the tests will causes tests to fail.
            $config_dir = defined($self->{tmp_conf_dir}) ? $self->{tmp_conf_dir} : 'undefined';
        }
        else {
            $config_dir = $Config->{HADOOP_CONF_DIR} if ($config_dir eq 'default');
        }
    }
    else {
            $config_dir = 'undefined';
    }

    my $hosts = (ref($daemon_hosts) eq 'ARRAY') ?
        join(',', @$daemon_hosts) : $daemon_hosts;

    if ($operation eq 'kill') {
        @command = ('ps','-ef','|','grep java','|','grep -v grep');
        @command = ('/home/y/bin/pdsh', '-w', $hosts, shell_quote("@command"), '2> /dev/null');
        `stty -echo`;
        my $result = `@command`;
        note($result) if $verbose;
        `stty echo`;
        
        my $target = {};
        foreach my $ps_line (split("\n", $result)) {
            my @tokens = split(' ',$ps_line);
            my $host = $tokens[0];
            $host =~ s/://g;
            my $pid = $tokens[2];
            my $pids = ();
            $pids = $target->{$host} if defined($target->{$host});
            push(@$pids, $pid);                                    
            $target->{$host} = $pids;
        }
        
        foreach my $host (sort keys %$target) {
            note("ssh -t $host sudo kill -9 ".join(" ", @{$target->{$host}}));
        }
        return 0;
    }
    
    @command = ('sudo', '/usr/local/bin/yinst', 'set', '-root',
                "$Config->{HADOOP_QA_ROOT}/gs/gridre/yroot.$Config->{CLUSTER}",
                "hadoop_qa_restart_config.HADOOP_CONF_DIR=$config_dir",";",
                'sudo', '/usr/local/bin/yinst', $operation, '-root',
                "$Config->{HADOOP_QA_ROOT}/gs/gridre/yroot.$Config->{CLUSTER}",
                $component);
    @command = ('/home/y/bin/pdsh', '-w', $hosts, shell_quote("@command"));
    note("@command");

    # This is a workaround because pdsh always return 0 even on failure.
    `stty -echo`;
    my $result = `@command`;
    `stty echo`;
    note($result) if $verbose;
    $return += (($result =~ /.*.\Q$operation.*.$component\E/.*.) && ($result !~ /.*.exit code.*./)) ? 0 : 1;

    # When running as hadoopqa and using the yinst stop command to stop the
    # jobtracker instead of calling hadoop-daemon.sh directly, there can be a
    # delay before the job tracker is actually stopped. This is not ideal as it
    # poses potential timing issue. Should investigate why yinst stop is existing
    # before the job pid goes away.
    $return += $self->wait_for_daemon_state($operation, $component, $daemon_hosts);

    return $return;
}


=head2 check_control_daemon($action, $component, $daemon_hosts, $config_dir, $verbose)

Check Stop and Start the component daemon
(e.g jobtracker, namenode, datanode, etc.)

=over 2

=item I<Parameters>:

   * Action         : Action to perform {'stop', 'start'}
   * Component      : jobtracker, namenode, datanode, tasktracker, etc
   * Daemon host(s) : Optional hostnames (e.g. foo[0-10], or foo0,foo1,...)
                      If not specified, default is to use all hosts
   * Config Dir     : Optional hadoop configuration directory
   * Verbose        : 0 for off, 1 for on

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub check_control_daemon {
    my ($self, $action, $component, $daemon_hosts, $config_dir, $verbose) = @_;
    my $result;

    my ($component_status, $hosts_status) =
        $self->get_daemon_status($component, $daemon_hosts, $verbose);
    note("$component daemon is ", ($component_status == 1) ? "up" : "down");

    $result =
        $self->control_daemon($action, $component, $daemon_hosts, $config_dir, $verbose);
    is($result, 0, "Check $action $component daemon");

    ($component_status, $hosts_status) =
        $self->get_daemon_status($component, $daemon_hosts, $verbose);
    my $exp_state = ($action eq 'start') ? 'up' : 'down';
    is($component_status, 
       ($action eq 'start') ? 1 : 0,
       "Check $component daemon is $exp_state");
}


=head2 wait_for_daemon_state($state)

Wait for the daemon to either stop or be started

=over 2

=item I<Parameters>:

   * action
   * component
   * daemon_hosts
   * wait_time
   * max_wait
   * verbose

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub wait_for_daemon_state {
    my ($self, $action, $component, $daemon_hosts, $wait_time, $max_wait, $verbose) = @_;

    my $exp_state = ($action eq 'stop') ? 'stopped' : 'started';
    my $exp_state_i = ($action eq 'stop') ? 0 : 1;

    $wait_time = 3 unless defined($wait_time);
    $max_wait = 10 unless defined($max_wait);
    my $count = 1;
    my $component_status;
    my $hosts_status;
    while ($count <= $max_wait) {
        ($component_status, $hosts_status) =
            $self->get_daemon_status($component, $daemon_hosts, $verbose);
        if ($component_status == $exp_state_i) {
            note("$component daemon process is $exp_state:");
            last;
        }
        
        my $hosts_str = (ref($daemon_hosts) eq 'ARRAY') ?
            join(',',@$daemon_hosts) : $daemon_hosts;
        print_wait_for_msg($count, $wait_time, $max_wait,
                           "'$component' daemon on '$hosts_str' host(s) to be $exp_state");
        sleep $wait_time;
        $count++;
    }
    if ($count > $max_wait) {
        note("WARN: Wait time expired before daemon can be $exp_state:", explain($hosts_status));
        return 1;
    }
    return 0;
}


=head2 restart_resourcemanager($self, $rm_config_dir, $nm_config_dir, $verbose)

Stop and Start the resourcemananager. This method will first stop the
nodemanagers, the resource manager, then start the resource manager and the
nodemanagers.

=over 2

=item I<Parameters>:

   * Resourcemanager Config Dir     : Optional hadoop configuration directory
   * Nodemanager Config Dir     : Optional hadoop configuration directory
   * Verbose        : 0 for off, 1 for on

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub restart_resourcemanager {
    my ($self, $rm_config_dir, $nm_config_dir, $verbose) = @_;
    my $component     = 'resourcemanager';
    my $dep_component = 'nodemanager';
    $rm_config_dir ||= $Config->{HADOOP_CONF_DIR};
    $nm_config_dir ||= $Config->{HADOOP_CONF_DIR};
    $self->control_daemon('stop',  $dep_component, undef, undef,       $verbose);
    $self->control_daemon('stop',  $component,     undef, undef,       $verbose);
    $self->control_daemon('start', $component,     undef, $rm_config_dir, $verbose);
    $self->control_daemon('start', $dep_component, undef, $nm_config_dir, $verbose);
}


=head2 check_restart_resourcemanager($self, $rm_config_dir, $nm_config_dir, $verbose)

Check Stop and Start the resourcemananager. This method will first stop the
nodemanagers, the resource manager, then start the resource manager and the
nodemanagers.

=over 2

=item I<Parameters>:

   * Resourcemanager Config Dir     : Optional hadoop configuration directory
   * Nodemanager Config Dir     : Optional hadoop configuration directory
   * Verbose        : 0 for off, 1 for on

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub check_restart_resourcemanager {
    my ($self, $rm_config_dir, $nm_config_dir, $verbose) = @_;
    my $component     = 'resourcemanager';
    my $dep_component = 'nodemanager';
    $rm_config_dir ||= $Config->{HADOOP_CONF_DIR};
    $nm_config_dir ||= $Config->{HADOOP_CONF_DIR};
    $self->check_control_daemon('stop',  $dep_component, undef, undef,       $verbose);
    $self->check_control_daemon('stop',  $component,     undef, undef,       $verbose);
    $self->check_control_daemon('start', $component,     undef, $rm_config_dir, $verbose);
    $self->check_control_daemon('start', $dep_component, undef, $nm_config_dir, $verbose);
}


=head2 check_restart_daemon($component, $config_dir)

Check Stop and Start the component daemon
(e.g jobtracker, namenode, datanode, etc.)

=over 2

=item I<Parameters>:

   * Component      : jobtracker, namenode, datanode, tasktracker, etc
   * Config Dir     : Optional hadoop configuration directory
   * Verbose        : 0 for off, 1 for on

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub check_restart_daemon {
    my ($self, $component, $config_dir, $verbose) = @_;
    $self->check_control_daemon('stop',  $component, undef, undef,       $verbose);
    $self->check_control_daemon('start', $component, undef, $config_dir, $verbose);
}


=head2 get_num_active_trackers($self, $verbose)

Get the number of active tasktrackers running

=over 2

=item I<Parameters>:

   * Self
   * Verbose

=item I<Returns>:

   * Integer number of active trackers running

=back

=cut
sub get_num_active_trackers {
    my ($self, $verbose) = @_;
    my $return = 0;
    my @args = ('-list-active-trackers');
    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command( {
            command => 'job',
            args => \@args,
        } );
    my @output=split('\n', $stdout);
    my @trackers = grep(/^tracker_/, @output); 
    return scalar(@trackers);
}


=head2 get_am_resource($self)

Get the AM memory resource

=over 2

=item I<Parameters>:

   * Self

=item I<Returns>:

   * Memory resource in GB

=back

=cut
sub get_am_resource {
    my ($self) = @_;
    # queueSetting=$(readAFieldInFileFromRemoteHost $JOBTRACKER "${NEW_CONFIG_LOCATION}/capacity-scheduler.xml" "yarn.scheduler.capacity.root.$1.capacity")
    my $component_host=$Config->{NODES}->{RESOURCEMANAGER}->{HOST};
    my $prop_name = 'yarn.app.mapreduce.am.resource.mb';
    my $filename = $Config->{MAPRED_SITE_XML};
    my $am_mb =
        $self->get_xml_prop_value($prop_name, $filename, $component_host);
    my $am_gb = nearest(.01, $am_mb/1024) if ($am_mb);
    # default value
    $am_gb ||= 1.5;
    return $am_gb;
}

#################################################################################
# SUB TYPE: CONFIG RELATED SUBROUTINES
#################################################################################

=head2 get_xml_prop_value($self, $prop_name, $filename, $component_host)

Edit a given configuration file

=over 2

=item I<Parameters>:

   * Property name
   * XML configuration filename
   * Component host where the file resides
   * Start-tag
   * End-tag
   * verbose

=item I<Returns>:

   * Property value

=back

=cut
sub get_xml_prop_value {
    my ($self, $prop_name, $filename, $component_host, $start_tag, $end_tag, $verbose) = @_;

    $verbose = 1 unless defined($verbose);
    my $prop_value;

    $prop_name = $start_tag.$prop_name if defined($start_tag);
    $prop_name = $prop_name.$end_tag if defined($end_tag);

    # Check if the property name exists or not
    # Get a little context so we can at least try to ignore commented-out properties
    my $cmd = "grep -A 2 '$prop_name' " . $self->_check_tmp_conf_file($filename);
    $cmd = "ssh $component_host ".shell_quote("$cmd") if defined($component_host);

    note("$cmd") if $verbose;
    my $property = `$cmd`;

    # If the property name exists, parse the value
    if ($property) {
        $property =~ s/<!--.*?-->//sg; # Remove any commented-out sections
        $prop_value = $1 if ( $property =~ m{<value>(.*?)</value>} );
    }
    return $prop_value;
}


=head2 set_xml_prop_value($self, $prop_name, $prop_value, $filename, $component_host)

Set property value for a specified property name in a configuration file

=over 2

=item I<Parameters>:

   * Property name
   * Property value
   * XML configuration filename
   * Component host where the file resides (optional)
   * Start-tag
   * End-tag

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub set_xml_prop_value {
    my ($self, $prop_name, $prop_value, $filename, $component_host, $start_tag,
        $end_tag) = @_;

    # Fetch the current property value
    my $orig_prop_value =
        $self->get_xml_prop_value($prop_name, $filename, $component_host,
                                  $start_tag, $end_tag);

    if ((defined($orig_prop_value)) && ($prop_value eq $orig_prop_value)) {
        note("Nothing to set: Current property value of ".
             "'$orig_prop_value' is the same as the new value to set.");
        return 0;
    }

    $filename = $self->_check_tmp_conf_file($filename);

    my @command;
    my $regex;
    if (defined($orig_prop_value)) {
        note("Change value of '$prop_name' from '$orig_prop_value' to ".
             "'$prop_value' in '$filename'.");
         $regex =
            "'s|(<name>\Q$prop_name\E</name>\\s*<value>).*?(?=</value>)" .
             "|\${1}$prop_value|g'";
    }
    else {
        note("Append non-existing property '$prop_name' with value ".
             "'$prop_value' to '$filename':");
        my $match_str = '</property>';
        my $new_property_str =
            '  <property>\n'.
            "    <name>$prop_name</name>".'\n'.
            "    <value>$prop_value</value>".'\n'.
            '  </property>\n\n';
        $regex =
            "'s".
            '|(.*'.$match_str.'.*\n)'.
            '|${1}\n'."$new_property_str".
            "|'";
    }
    @command = ( 'perl', '-pi', '-0e', $regex, $filename);
    @command = ( 'sudo', @command) unless $self->is_headless_user();
    @command = ('ssh', $component_host, shell_quote(@command)) if (defined($component_host));
    note("@command");
    `stty -echo`;
    my $result = system("@command");
    `stty echo`;
    return $result;
}

=head2 replace_value($self, $pattern, $new_value, $filename, $component_host)

Replace the pattern in the specified file with the new value on the specified
component host.

=over 2

=item I<Parameters>:

   * Pattern to replace
   * New Value
   * Filename
   * Component host where the file resides (optional)
   * Start-tag
   * End-tag

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub replace_value {
    my ($self, $pattern, $new_value, $filename, $hostname, $start_tag, $end_tag,
        $option) = @_;

    my @command = ();
    my $on_str = defined($hostname) ? "on '$hostname'" : "on localhost";
    note("Change value from '$pattern' to '$new_value' in '$filename' $on_str.");
    my $regex = "'s|$pattern|$new_value|";
    $regex .= 'g' if (defined($option) && ($option eq 'all'));
    $regex .= "'";
    @command = ('perl', '-pi', '-0e', $regex, $filename);
    @command = ('ssh', $hostname, shell_quote(@command)) if $hostname;
    note("@command");
    `stty -echo`;
    my $result = system("@command");
    `stty echo`;
    return $result;
}


=head2 append_string($self, $value, $filename, $component_host)

Append the value to the specified file the specified component host.

=over 2

=item I<Parameters>:

   * Value
   * Filename
      * Full path to config filename is expected
      * If HADOOP_CONF_DIR has been chaned due to a call to $self->backup_*,
        the full path will be modified to point to the temp hadoop conf dir.
   * Component host where the file resides (optional)

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub append_string {
    my ($self, $value, $filename, $hostname) = @_;

    $filename = $self->_check_tmp_conf_file($filename);
    my @command = ();
    note("Append '$value' to '$filename' on ". "'$hostname'.");
    @command = ('ssh', $hostname, "\"echo \'$value\' >> $filename\"");
    note("@command");
    `stty -echo`;
    my $result = system("@command");
    `stty echo`;
    return $result;
}


sub get_jobtracker_config_dir {
    my ($self) = @_;
    return $self->get_component_config_dir('jobtracker');
}


sub get_component_config_dir {
    my ($self, $component) = @_;

    unless ($component) {
        note("WARN: Missing component name");
        return;
    }

    note("Get component config dir for component '$component'");
    my $host = $Config->{NODES}->{uc($component)}->{HOST};
    $host = $host->[0] if (ref($host) eq 'ARRAY');
    my $prog = 'java';

    my $sudoer = $self->get_sudoer($component);

    my @command = ( 'ps', 'auxww', '|', 'grep',
                    shell_quote("$prog -Dproc_$component"),
                    '|', 'grep', '-v', 'grep');

    @command = ('ssh', $host, @command);

    note("@command");
    chomp(my $job = `@command`);
    # For data node, there should be two jobs per host.
    # One is started by root, and the other by hdfs.

    # Get the conf dir
    # -classpath /tmp/hadoopqa_1299709580:
    my $conf_dir;
    if ($job =~ m/(.*.)(.*.classpath\s)(.*.)/) {
        $conf_dir = $3;
    }
    my @dir = split(":", $conf_dir);
    note("conf dir='$dir[0]'");
    return $dir[0];
}


# Modifies the filename/path if the tmp_conf_dir is set
sub _check_tmp_conf_file {
    my ($self, $file) = @_;
    if (defined($self->{tmp_conf_dir})) {
        $file =~ m{^.*/(.*)$};
        $file = "$self->{tmp_conf_dir}/$1";
    }
    return $file;
}


=head2 copy($self, $source, $destination, $hostname)

Copy file or directory from specified source location to the specified target
location. This is done on the specified host.

=over 2

=item I<Parameters>:

   * Source directory or file
   * Target directory or file
   * Hostname to run on
   * Copy arguments

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub copy {
    my ($self, $source, $destination, $hostname, $args) = @_;
    $args ||= '-rf';
    my @command = ('cp', $args, $source, $destination);
    @command = ('ssh', $hostname, @command) if ($hostname);
    note("@command");
    return system("@command");
}


=head2 remote_copy($self, $source, $target, $target_host, $from_localhost, $args)

Copy directory or files (e.g. canned configuration files) from localhost to
remote host.

=over 2

=item I<Parameters>:

   * Source directory or file
   * Target directory or file
   * Target host
   * from_localhost : 1 for true, 0 for false
   * Copy arguments

=item I<Returns>:

   * None

=back

=cut
sub remote_copy {
    my ($self, $source, $target, $target_host, $from_localhost, $args) = @_;

    $source="$source/*" if (-d $source);
    $args ||= '-f';
    $from_localhost ||= 1;

    my @command;
    if ($from_localhost) {
        @command = ('scp', "$source",
                    "$target_host:$target");
    }
    else {
        @command = ('cp', $args, $source, $target);
        @command = ('ssh', $target_host, @command);
    }

    note("@command");
    `@command`;
    die ("Failed to copy from '$source' on localhost to ".
        "'$target_host:$target'") if ($? != 0);
}


=head2 copy_conf_files_to_config_dir($self, $source_config_dir, $current_config_dir, $target_host)

Copy canned configuration files from the source configuration directory to the
current configuration directory on the target host.

=over 2

=item I<Parameters>:

   * Source configuration directory
   * Current configuration directory
   * Target host

=item I<Returns>:

   * None

=back

=cut
sub copy_conf_files_to_config_dir {
    my ($self, $source_config_dir, $current_config_dir, $target_host) = @_;
    my @command = ('scp', "$source_config_dir/*",
                   "$target_host:$current_config_dir");
    note("@command");
    `@command`;
    die ("Failed to copy test configuration from '$source_config_dir' to ".
        "'$target_host:$current_config_dir'") if ($? != 0);
}


=head2 create_new_conf_dir($self)

Create and setup a new temporary hadoop conf dir so that it can be customized for
testing when test is being run by the hadoopqa headless user who does not
have sudo permission on the jobtracker.

=over 2

=item I<Parameters>:

   * Target host

=item I<Returns>:

   * Hash containing the 'root_dir' and the 'local_cap_sched_conf'

=back

=cut
sub create_new_conf_dir {
    my ($self, $target_host, $source_conf_dir) = @_;

    # follow or dereference the symlinks
    my $cp_args = '-rfL';
    my $whoami = getpwuid($>);
    my $new_conf_dir = "/tmp/$whoami".'_'.time;
    my $result = $self->copy($Config->{HADOOP_CONF_DIR},
                             $new_conf_dir,
                             $target_host,
                             $cp_args);

    # Some of the symlink files points to non existent files
    # is($result, 0, "Copy conf dir to tmp dir '$new_conf_dir'");

    # Change all the xml config files to have the proper permission so that
    # they can be changed by the tests later.
    my @command = ('ssh', $target_host, 'chmod', '644',
                   "$new_conf_dir/*.xml");
    note("@command");
    `@command`;

    if ($source_conf_dir) {
        $self->copy_conf_files_to_config_dir($source_conf_dir,
                                             $new_conf_dir, $target_host);
    } else {
        # This could have unpredictable settings based on the cluster used.
        # Copy the local config files
        $result = $self->copy($Config->{LOCAL_MAPRED_SITE_XML},
                              $new_conf_dir,
                              $target_host,
                              '-rfL');
        is($result,
           0,
           "Copy local mapred site xml file to tmp dir '$new_conf_dir'");

        $result = $self->copy($Config->{LOCAL_CAPACITY_SCHEDULER_XML},
                              $new_conf_dir,
                              $target_host);
        is($result,
           0,
           "Copy local capacity scheduler xml file to tmp dir '$new_conf_dir'");
    }

    my ($filename, $include_file, $old_include, $new_include);

    # Replace capacity-scheduler.xml with a template vanilla one that does not
    # contain any properties. So that test specific configurations can be
    # funneled to the local-capacity-scheduler.xml file.
    $filename = "$new_conf_dir/capacity-scheduler.xml";
    my $conf_path = "$FindBin::Bin/../../../../lib/Hadoop/conf";
    $result = $self->copy("$conf_path/capacity-scheduler.xml",
                          $new_conf_dir,
                          $target_host);

    # Replace the string in the tmp capacity-scheduler.xml file:
    # <xi:include href="/grid/0/gs/conf/local/local-capacity-scheduler.xml"/>
    $filename     = "$new_conf_dir/capacity-scheduler.xml";
    $include_file = "local-capacity-scheduler.xml";
    $old_include  = "$Config->{LOCAL_CONF_DIR}/$include_file";
    $new_include  = "$new_conf_dir/$include_file";
    $result = $self->replace_value($old_include,
                                   $new_include,
                                   $filename,
                                   $target_host);
    my $new_local_cap_sched_conf = $new_include;

    # Replace the string in the tmp mapred-site.xml file:
    # <xi:include href="/grid/0/gs/conf/local/local-mapred-site.xml"/>
    $filename = "$new_conf_dir/mapred-site.xml";
    $include_file = "local-mapred-site.xml";
    $old_include = "$Config->{LOCAL_CONF_DIR}/$include_file";
    $new_include = "$new_conf_dir/$include_file";
    $result = $self->replace_value($old_include,
                                   $new_include,
                                   $filename,
                                   $target_host);
    my $new_local_mapred_site_conf = $new_include;


    my $tmp_conf = { 'root_dir'               => $new_conf_dir,
                     'local_cap_sched_conf'   => $new_local_cap_sched_conf,
                     'local_mapred_site_conf' => $new_local_mapred_site_conf};
    return $tmp_conf;
}


=head2 backup_$component_conf($self) <backup_namenode_conf, backup_secondary_namenode_conf, backup_datanode_conf, backup_tasktracker_conf, backup_jobtracker_conf, backup_gateway_conf>

Looks up the specified component hosts and backs up the config directories on them

=over 2

=item I<Parameters>:

=item I<Returns>:

   * Integer value 0 for SUCCESS and >0 for FAILURE

=back
=cut

=head2 reset_$component_conf(@conf_files) <reset_namenode_conf, reset_secondary_namenode_conf, reset_datanode_conf, reset_tasktracker_conf, reset_jobtracker_conf, reset_gateway_conf>

Looks up the specified component hosts and resets the specified config files by
copying them over from the original config directory.

=over 2

=item I<Parameters>:

   * List of files (with full-paths) to the config files to reset

=item I<Returns>:

   * Integer value 0 for SUCCESS and >0 for FAILURE

=back
=cut

=head2 cleanup_$component_conf(@conf_files) <cleanup_namenode_conf, cleanup_secondarynamenode_conf, cleanup_datanode_conf, cleanup_tasktracker_conf, cleanup_jobtracker_conf, cleanup_gateway_conf>

Looks up the specified component hosts and deletes the temporary config folder created there

=over 2

=item I<Parameters>:

   * List of files (with full-paths) to the config files to reset

=item I<Returns>:

   * Integer value 0 for SUCCESS and >0 for FAILURE

=back
=cut

=head2 set_[gateway_]$component_xml_prop_value($self, $prop_name, $prop_value)

Set property value for a specified property name in a configuration file

=over 2

=item I<Parameters>:

   * Property name
   * Property value

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut

=head2 get_[gateway_]$component_xml_prop_value($self, $prop_name)

Get property value for a specified property name in

=over 2

=item I<Parameters>:

   * Property name

=item I<Returns>:

   * String with property value (undef if not found)

=back

=cut
# This foreach loop is used to define component-specific functions for ease of use.  The functions defined
# below will be expanded to support any of the following components:
# (Also note that the perldoc sections are listed above for each of the functions below)
foreach my $component ('namenode', 'secondary_namenode', 'datanode',
                       'jobtracker', 'tasktracker',
                       'resourcemanager', 'nodemanager', 'gateway') {
    no strict 'refs';
    my $comp_method = "get_$component";
    my $conf_file_method = "get_${component}_conf";

    # sub backup_namenode_conf, sub backup_secondary_namenode_conf, sub backup_datanode_conf, sub backup_tasktracker_conf, sub backup_jobtracker_conf, sub backup_gateway_conf
    *{"backup_${component}_conf"} = sub {
        my $self = shift;
        if ($component eq 'gateway') {
            $self->{gateway_conf} = 1;
            return $self->backup_conf_dir;
        } else {
            my $hosts = $Config->$comp_method();
            return $self->backup_conf_dir(ref($hosts) ? $hosts : [$hosts]);
        }
    };

    # sub cleanup_namenode_conf, sub cleanup_secondary_namenode_conf, sub cleanup_datanode_conf, sub cleanup_tasktracker_conf, sub cleanup_jobtracker_conf, sub cleanup_gateway_conf
    *{"cleanup_${component}_conf"} = sub {
        my $self = shift;
        if ($component eq 'gateway') {
            return $self->cleanup_conf_dir;
        } else {
            my $hosts = $Config->$comp_method();
            return $self->cleanup_conf_dir(ref($hosts) ? $hosts : [$hosts]);
        }
    };

    # sub reset_namenode_conf, sub reset_secondary_namenode_conf, sub reset_datanode_conf, sub reset_tasktracker_conf, sub reset_jobtracker_conf, sub reset_gateway_conf
    *{"reset_${component}_conf"} = sub {
        my ($self, @conf_files) = @_;
        my $hosts;
        if ($component ne 'gateway') {
            $hosts = $Config->$comp_method();
            $hosts = [$hosts] unless ref($hosts);
        }
        return $self->reset_conf_file($hosts, \@conf_files);
    };

    # sub set_namenode_xml_prop_value, sub set_secondary_namenode_xml_prop_value, sub set_datanode_xml_prop_value, sub set_tasktracker_xml_prop_value, sub set_jobtracker_xml_prop_value, sub set_gateway_xml_prop_value
    *{"set_${component}_xml_prop_value"} = sub {
        my ($self, $prop_name, $prop_value, $file) = @_;
        my $hosts = $Config->$comp_method();
        $hosts = [$hosts] unless ref($hosts);
        $file ||= $Config->$conf_file_method();
        foreach my $host (@$hosts) {
            $self->set_xml_prop_value($prop_name, $prop_value, $file, $host);
        }
    };

    # sub get_namenode_xml_prop_value, sub get_secondary_namenode_xml_prop_value, sub get_datanode_xml_prop_value, sub get_tasktracker_xml_prop_value, sub get_jobtracker_xml_prop_value, sub get_gateway_xml_prop_value
    *{"get_${component}_xml_prop_value"} = sub {
        my ($self, $prop_name, $file) = @_;
        my $host = $Config->$comp_method();
        $host = $host->[0] if ref($host); # If we have multiple hosts, just pick the first one
        $file ||= $Config->$conf_file_method();
        $self->get_xml_prop_value($prop_name, $file, $host);
    };

    # sub set_gateway_namenode_xml_prop_value, sub set_gateway_secondary_namenode_xml_prop_value, sub set_gateway_datanode_xml_prop_value, sub set_gateway_tasktracker_xml_prop_value, sub set_gateway_jobtracker_xml_prop_value, sub set_gateway_gateway_xml_prop_value
    *{"set_gateway_${component}_xml_prop_value"} = sub {
        my ($self, $prop_name, $prop_value) = @_;
        $self->set_xml_prop_value($prop_name, $prop_value, $Config->$conf_file_method());
    };

    # sub get_gateway_namenode_xml_prop_value, sub get_gateway_secondary_namenode_xml_prop_value, sub get_gateway_datanode_xml_prop_value, sub get_gateway_tasktracker_xml_prop_value, sub get_gateway_jobtracker_xml_prop_value, sub get_gateway_gateway_xml_prop_value
    *{"get_gateway_${component}_xml_prop_value"} = sub {
        my ($self, $prop_name) = @_;
        $self->get_xml_prop_value($prop_name, $Config->$conf_file_method());
    };


}


=head2 backup_conf_dir($self, $hosts)

Creates a temporary directory and saves it as $self->{temp_conf_dir}
Copies the config directory on all @hosts to this temporary directory (on the hosts)
Future sets/gets should occur from this temp directory.
Component restarts should also occur using this temp directory.
If the gateway is backed up, all hadoop commands should use this temp directory too.

=over 2

=item I<Parameters>:

   * Reference to list of hosts if any (otherwise backups up the local config directory on the gateway)

=item I<Returns>:

   * Integer value 0 for SUCCESS and >0 for FAILURE

=back

=cut
sub backup_conf_dir {
    my ($self, $hosts) = @_;
    $self->{tmp_conf_dir} = "/tmp/hadoop-conf-".time.'-'.$$ unless defined($self->{tmp_conf_dir});
    my @command = ('cp', '-rf', $Config->{HADOOP_CONF_DIR}, $self->{tmp_conf_dir});
    my $result;
    if (defined($hosts)) {
        foreach my $host (@$hosts) {
            note("Backing up $host: ssh $host @command");
            $result += system('ssh', $host, @command);
        }
    } else {
        note("Backing up on localhost: @command");
        $result = system(@command);
    }
    return $result;
}


=head2 reset_conf_file($self, $hosts, $files)

Can only be called after backup_conf_dir has been called.
Copies the specified files from the original config directory to the previously-created
temporary one.

=over 2

=item I<Parameters>:

   * Reference to list of hosts if any (otherwise backups up the local config directory on the gateway)
   * Reference to list of config files to reset (if undefined, resets the whole folder)

=item I<Returns>:

   * Integer value 0 for SUCCESS and >0 for FAILURE

=back

=cut
sub reset_conf_file {
    my ($self, $hosts, $files) = @_;
    $self->backup_conf_dir($self, $hosts) unless defined($files);

    die "You must call backup_conf_dir before calling reset_conf_file!\n" unless defined($self->{tmp_conf_dir});

    my @command = ('cp', '-rf', @$files, $self->{tmp_conf_dir});

    my $result;
    if (defined($hosts)) {
        foreach my $host (@$hosts) {
            note("Resetting config file (@$files) on $host: ssh $host @command");
            $result += system('ssh', $host, @command);
        }
    } else {
        $result = system(@command);
    }

    return $result;
}


=head2 cleanup_conf_dir($self, $hosts)

Removes the temporary config directory created

=over 2

=item I<Parameters>:

   * Reference to list of hosts if any (otherwise cleans up the local config directory on the gateway)

=item I<Returns>:

   * Integer value 0 for SUCCESS and >0 for FAILURE

=back

=cut
sub cleanup_conf_dir {
    my ($self, $hosts, $files) = @_;

    unless (defined($self->{tmp_conf_dir})) {
        note("You must call backup_conf_dir before calling cleanup_conf_file!\n");
        return undef;
    }

    my @command = ('rm', '-rf', $self->{tmp_conf_dir});

    my $result;
    if (defined($hosts)) {
        foreach my $host (@$hosts) {
            note("Removing temporary config folder ($self->{tmp_conf_dir}) on $host: ssh $host @command");
            $result += system('ssh', $host, @command);
        }
    } else {
        $result = system(@command);
    }

    # Do this so caller can restart the daemon with original parameters.
    undef($self->{tmp_conf_dir});

    return $result;
}


=head2 cleanup($self, $dir, $hostname)

Remove specified file or directory recursively on the specified host.

=over 2

=item I<Parameters>:

   * Source directory or file
   * Hostname to run on

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub cleanup {
    my ($self, $dir, $hostname) = @_;
    my @command = ('rm', '-r', $dir);
    @command = ('ssh', $hostname, @command) if ($hostname);
    note("@command");
    return system("@command");
}


=head2 restart_resourcemanager_with_cs_conf($self, $config_file)

Restart resourcemanager with a new capacity scheduler configuration file.

=over 2

=item I<Parameters>:

   * Full path capacity scheduler cofiguration file

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub restart_resourcemanager_with_cs_conf {
    my ($self, $config_file) = @_;
    note("Restart resourcemanager with a new capacity scheduler config file:");
    # Backup all of the xml properties we will be changing
    $self->backup_resourcemanager_conf() unless defined($self->{tmp_conf_dir});
    my $config_dir=$self->{tmp_conf_dir};
    my $result = $self->remote_copy("$config_file",
                                    "$config_dir/capacity-scheduler.xml",
                                    $Config->{NODES}->{RESOURCEMANAGER}->{HOST});
    $self->restart_resourcemanager($config_dir);
}

#################################################################################
# SUB TYPE: JOB RELATED SUBROUTINES
#################################################################################

# Print logging for wait for subroutines
sub print_wait_for_msg {
    my ($count, $wait_time, $max_wait, $msg) = @_;
    $msg ||= 'expected state';
    my $print_msg =
        "Wait #$count of $max_wait for $msg in $wait_time"."(s): ".
        "total wait time=".($count-1)*$wait_time."(s):";
    note("$print_msg");
}

=head2 run_sleep_job($self, $hash, $verbose)

Run the sleep job with the specified or default parameters

=over 2

=item I<Parameters>:

   * A hash reference of:
     - map_task
     - reduce_task
     - map_sleep
     - reduce_sleep
     - user (optional)
     - queue (optional)
     - args: (array reference: should not include -m, -mr, -r, -rt)
   * Verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code, $job_id)

=back

=cut
sub run_sleep_job {
    my ($self, $hash, $verbose) = @_;
    my @args = defined($hash->{args}) ? @{$hash->{args}} : ();
    # Add default values if not passed in

    push(@args,
        '-m', defined($hash->{map_task}) ? $hash->{map_task} : 1,
        '-r', defined($hash->{reduce_task}) ? $hash->{reduce_task} : 1,
        '-mt', defined($hash->{map_sleep}) ? $hash->{map_sleep} : 1000,
        '-rt', defined($hash->{reduce_sleep}) ? $hash->{reduce_sleep} : 1000,
    );

    local $hash->{args} = \@args;
    local $hash->{command} = 'sleep';
    my ($stdout, $stderr, $success, $exit_code) = $self->run_jar_command($hash, $verbose);
    return if ((defined($hash->{mode})) && ($hash->{mode} eq 'system'));

    $stderr =~ /Running job: (job_\d+_\d+)$/m;
    my $job_id = $1 if defined($1);
    diag("ERROR: job_id not found:\n$stderr\n") unless defined($job_id);
    return ($stdout, $stderr, $success, $exit_code, $job_id);
}


=head2 run_wordcount_job($self, $hash, $verbose)

Run the wordcount job with the specified or default parameters

=over 2

=item I<Parameters>:

   * A hash reference of:
     - input dir
     - output dir
     - user
   * Verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_wordcount_job {
    my ($self, $hash, $verbose) = @_;

    $hash->{'input_dir'} = '' unless (defined($hash->{'input_dir'}));
    my $output_dir;
    if (defined($hash->{'output_dir'})) {
        $output_dir = $hash->{'output_dir'};
        delete($hash->{'output_dir'});
    }
    else {
        $output_dir = 'wc_output_dir_' . time;
    };

    $hash->{command} = 'wordcount';
    $hash->{jar} = $Config->{HADOOP_EX_JAR};

    $hash->{args} = [] unless defined($hash->{args});
    my @args = (@{$hash->{args}}, 
                ($hash->{'input_dir'}, $output_dir));
    note('args=',explain(\@args)) if $verbose;
    $hash->{args} = \@args;

    delete($hash->{'input_dir'});
    $hash->{mode} = 'system';
    $self->run_jar_command( $hash, $verbose);
    return $output_dir;
}


=head2 run_teragen_job($self, $hash, $verbose)

Run the teragen job with the specified or default parameters

=over 2

=item I<Parameters>:

   * A hash reference of:
     - input dir
     - output dir
     - user
   * Verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_teragen_job {
    my ($self, $hash, $verbose) = @_;

    my $output_dir;
    if (defined($hash->{'output_dir'})) {
        $output_dir = $hash->{'output_dir'};
        delete($hash->{'output_dir'});
    }
    else {
        $output_dir = 'teragen_output_dir_' . time;
    };

    my $yarn_option = "-Dmapreduce.job.acl-view-job=*";
    my $data_size = 2;

    $hash->{command} = 'teragen';
    $hash->{jar} = $Config->{HADOOP_EX_JAR};
    $hash->{args} = [$yarn_option, $data_size, $output_dir],
    $hash->{mode} = 'system';
    $self->run_jar_command( $hash, $verbose);
    return $output_dir;
}


=head2 run_terasort_job($self, $hash, $verbose)

Run the terasort job with the specified or default parameters

=over 2

=item I<Parameters>:

   * A hash reference of:
     - input dir
     - output dir
     - user
   * Verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_terasort_job {
    my ($self, $hash, $verbose) = @_;

    $hash->{'input_dir'} = '' unless (defined($hash->{'input_dir'}));
    my $output_dir;
    if (defined($hash->{'output_dir'})) {
        $output_dir = $hash->{'output_dir'};
        delete($hash->{'output_dir'});
    }
    else {
        $output_dir = 'terasort_output_dir_' . time;
    };

    $hash->{command} = 'terasort';
    $hash->{jar} = $Config->{HADOOP_EX_JAR};

    my $yarn_option = "-Dmapreduce.job.acl-view-job=*";
    $hash->{args} = [] unless defined($hash->{args});
    my @args = (@{$hash->{args}}, 
                ($yarn_option, "-Dmapreduce.reduce.input.limit=-1",
                 $hash->{'input_dir'}, $output_dir));
    note('args=',explain(\@args)) if $verbose;
    $hash->{args} = \@args;

    delete($hash->{'input_dir'});
    $hash->{mode} = 'system';
    $self->run_jar_command( $hash, $verbose);
    return $output_dir;
}


=head2 run_streaming_job($self, $hash, $verbose)

Run the streaming job with the specified or default parameters

=over 2

=item I<Parameters>:

   * A hash reference of:
     - input dir
     - output dir
     - user
   * Verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_streaming_job {
    my ($self, $hash, $verbose) = @_;

    $hash->{'input_dir'} = '' unless (defined($hash->{'input_dir'}));
    my $output_dir;
    if (defined($hash->{'output_dir'})) {
        $output_dir = $hash->{'output_dir'};
        delete($hash->{'output_dir'});
    }
    else {
        $output_dir = 'stream_output_dir_' . time;
    };


    # $hash->{command} = 'wordcount';
    $hash->{command} = '';
    $hash->{jar} = $Config->{HADOOP_STREAMING_JAR};

    $hash->{args} = [] unless defined($hash->{args});
    # --config /home/gs/gridre/yroot.theoden/conf/hadoop/

    my $mapper = 'cat';
    if (defined($hash->{mapper})) {
        $mapper = $hash->{mapper};
        delete($hash->{mapper});
    }
    my $reducer = 'NONE';
    if (defined($hash->{reducer})) {
        $reducer = $hash->{reducer};
        delete($hash->{reducer});
    }

    my @args = (@{$hash->{args}}, 
                ('-input', $hash->{'input_dir'},
                 '-output', $output_dir,
                 '-mapper' , $mapper,
                 '-reducer', $reducer));
    if (defined($hash->{files})) {
        unshift(@args, ('-files', $hash->{files})) if defined($hash->{files});
        delete($hash->{files});
    }

    note('args=',explain(\@args)) if $verbose;
    $hash->{args} = \@args;

    delete($hash->{'input_dir'});

    $hash->{mode} = 'system';
    $self->run_jar_command( $hash, $verbose);
    return $output_dir;
}


=head2 run_fs($self, $args, $user, $verbose)

Run hadoop fs command

=over 2

=item I<Parameters>:

   * array reference to fs args
   * Verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_fs {
    my ($self, $args, $user, $verbose) = @_;
    my $hash = {'command' => 'fs',
                'args' => $args,
                'mode' => 'system' };
    $hash->{user} = $user if $user;
    $self->run_hadoop_command($hash);
}

=head2 test_run_sleep_job($self, $job_hash, $should_fail, $error_message)

Test run sleep job with the specified or default parameters. This adds two tests.

=over 2

=item I<Parameters>:

   * A hash reference of:
     - map_task: number of map tasks to run
     - reduce_task: number of reduce tasks to run
     - map_sleep: how long each map task will sleep
     - reduce_sleep: how long each reduce task will sleep
   * (Optional) should fail
   * (Optional) error message

=item I<Returns>:

   * None.

=back

=cut
sub test_run_sleep_job {
    my ($self, $job_hash, $should_fail, $error_message) = @_;

    $should_fail || 0;
    my ($stdout, $stderr, $success, $exit_code, $job_id) =
        $self->run_sleep_job($job_hash);

    if ($job_id) {
        note("Job invoked '$job_id' is expected to ",$should_fail ? "fail" : "pass",".");
    }
    else {
        note("Job invoked is expected to ",$should_fail ? "fail" : "pass",".");
    }
    my $msg = "Check MR sleep job ran";
    if ($should_fail) {
        isnt($exit_code, 0, "$msg: exit code should not be 0");
        like($stderr, qr/$error_message/, "$msg: log should have expected error message");
    }
    else {
        is($exit_code, 0, "$msg: exit code is 0");
        if ($Config->{YARN_USED}) {
            # Job job_1308666974385_0005 completed successfully
            like($stderr, qr/Job job_[\d]+_[\d]+ completed successfully/, "$msg: no error in log");
        }
        else {
            like($stderr, qr/Job complete/, "$msg: no error in log");
        }
    }

    return ($stdout, $stderr, $success, $exit_code, $job_id);
}


=head2 run_randomwriter_job($self, $user)

Run the random writer job

=over 2

=item I<Parameters>:

   * user

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_randomwriter_job {
    my ($self, $user) = @_;
    my $input = 'input_' . time;

    return $self->run_jar_command( {
        user => $user,
        command => 'randomwriter',
        [$input],
    } );
}


=head2 run_randomtextwriter_job($self, $hash, $verbose)

Run the random text writer job

=over 2

=item I<Parameters>:

   * A hash reference of:
     - input dir
     - input dir
     - output dir
     - user
   * verbose

=item I<Returns>:

   * ($stdout, $stderr, $success, $exit_code)

=back

=cut
sub run_randomtextwriter_job {
    my ($self, $hash, $verbose) = @_;

    my $output_dir;
    if (defined($hash->{'output_dir'})) {
        $output_dir = $hash->{'output_dir'};
        delete($hash->{'output_dir'});
    }
    else {
        $output_dir = 'rtw_output_dir_' . time;
    };

    $hash->{command} = 'randomtextwriter';
    $hash->{jar} = $Config->{HADOOP_EX_JAR};
    $hash->{args} = [$output_dir],
    $hash->{mode} = 'system';
    $self->run_jar_command( $hash, $verbose);
    return $output_dir;
}


=head2 run_get_config_job($self, @args)

Run the GetConfig job with the specified arguments and return a
hash of the configuration settings.  The MapReduce job simply
writes out all the configuration and environment settings from the child.

=over 2

=item I<Parameters>:

   * comma-separated args

=item I<Returns>:

   * $config_hash: Hash reference of all configuration settings on the child

=back

=cut
sub run_get_config_job {
    my ($self, @args) = @_;
    my $cur_time = time;
    my $input_path = 'HadoopQEInput' . $$ . '-' . $cur_time;
    my $output_path = 'HadoopQEOutput' . $$ . '-' . $cur_time;
    my $input_file = "$input_path/dummy_input";

    # Write the dummy input file
    $self->run_hadoop_command( {
        command => 'fs',
        args => ['-put', "$Lib_path/dummy_input", $input_file],
    } );

    unshift(@args, $input_path, $output_path);
    $self->run_jar_command( {
        jar => "$Lib_path/HadoopQEJobs.jar",
        command => $GetConfig_job,
        args => \@args,
    } );

    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command( {
        command => 'fs',
        args => ['-cat', "$output_path/*"]
        } );

    # $stdout now contains the configuration information, so parse it
    my %config_hash = map { split(/\s+/, $_, 2) } split(/\n/, $stdout);

    # Remove the input and output paths now
    $self->run_hadoop_command( {
        command => 'fs',
        args => ['-rmr', $output_path, $input_path],
    } );

    return \%config_hash;
}


=head2 get_job_status($self, $job_id, $verbose)

Get the job status for a specified job id

=over 2

=item I<Parameters>:

   * Job Id
   * Verbose

=item I<Returns>:

   * Hash containing the job id, state, start_time, username, priority, and
     schedule_info

=back

=cut
sub get_job_status {
    my ($self, $job_id, $verbose) = @_;
    my $all_status = $self->get_all_jobs_status($verbose);
    note("WARN: job status for '$job_id' not found") if (!exists($all_status->{$job_id}));
    return $all_status->{$job_id};

}


=head2 get_jobs_status($self, $option, $verbose)

Get accepted jobs status

=over 2

=item I<Parameters>:

   * Option {'all'}
   * Verbose

=item I<Returns>:

   * Hash containing hashes of job id, state, start_time, username, priority, and
     schedule_info

=back

=cut
sub get_jobs_status {
    my ($self, $option, $verbose) = @_;
    my $status;

    my @args = ('-list');
    push(@args, $option) if defined($option);
    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command( {
            command => 'job',
            args => \@args,
        });
    note("$stdout") if $verbose;
    my @output = split('\n', $stdout);
    foreach my $line (@output) {
        # note("line = $line");
        if ($line =~ m/(job_.*.)/) {
            my @tokens = split(' ', $line);
            my $hash = {};
            $hash->{id}            = shift(@tokens);
            $hash->{state}         = shift(@tokens);
            $hash->{start_time}    = shift(@tokens);
            $hash->{username}      = shift(@tokens);
            $hash->{queue}         = shift(@tokens);
            $hash->{priority}      = shift(@tokens);

            if ($Config->{YARN_USED}) {
                # $hash->{maps}          = shift(@tokens);
                # $hash->{reduces}       = shift(@tokens);
                $hash->{UsedContainers} = shift(@tokens);
                $hash->{RsvdContainers} = shift(@tokens);
                $hash->{UsedMem}        = shift(@tokens);
                $hash->{RsvdMem}        = shift(@tokens);
                $hash->{RsvdMem}        = shift(@tokens);
                $hash->{AM}             = shift(@tokens);
            }
            else {
                $hash->{schedule_info} = "@tokens";
            }
            $status->{$hash->{id}} = $hash;
        }
    }
    # note(explain($status)) if ($status && $verbose);
    return $status;
}


=head2 get_all_jobs_status($self, $verbose)

Get all job status

=over 2

=item I<Parameters>:

   * Verbose

=item I<Returns>:

   * Hash containing hashes of job id, state, start_time, username, priority, and
     schedule_info

=back

=cut
sub get_all_jobs_status {
    my ($self, $verbose) = @_;
    my $status = $self->get_jobs_status('all', $verbose);
    return $status;
}


# Get the task id
sub get_task_id {
    my ($self, $job_id, $task_type, $task_index, $verbose) = @_;
    my $task_id = $job_id;
    $task_id =~ s/job_/attempt_/;    
    $task_id .= '_';
    # task type is all caps: MAP or REDUCE
    $task_id .= substr($task_type, 0, 1);
    $task_id .= '_';
    $task_id .= sprintf("%06d", $task_index);
    $task_id .= "_0";
    note("task_id='$task_id'") if ($verbose);
    return $task_id;
}


=head2 get_tasktracker_host_from_jobtracker_log($self, $task_id)

Get the task tracker host from the jobtracker log using the task id

=over 2

=item I<Parameters>:

   * Task ID

=item I<Returns>:

   * Task Tracker Host

=back

=cut
sub get_tasktracker_host_from_jobtracker_log {
    my ($self, $task_id) = @_;
    unless ($task_id) {
        diag("Task id parameter is missing: unable to lookup jobtracker log:");
        return;
    }

    my $str = "'$task_id' to tip task";
    my $log = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{LOG};
    my $host = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{HOST};
    my @command = ('grep', '-i', shell_quote("$str"), $log);
    @command = ('ssh', $host, shell_quote("@command"));
    note("@command");
    my $result = `@command`;
    note("result='$result'");

    my $tracker = $3 if ($result =~ m/(.*.)(for tracker)(.*.)/);
    $tracker =~ s/\n//;
    $tracker =~ s/tracker_//;
    $tracker =~ s/\'//g;
    $tracker =~ s/ //g;
    my @tokens = split(":", $tracker);
    $tracker = $tokens[0];
    return $tracker;
}


=head2 get_last_tasktracker_host_from_jobtracker_log($self)

Get the last task tracker host from the jobtracker log

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * Task Tracker Host

=back

=cut
sub get_last_tasktracker_host_from_jobtracker_log {
    my ($self) = @_;

    my $str = "Adding task";
    my $log = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{LOG};
    my $host = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{HOST};
    my @command = ('grep', '-i', shell_quote("$str"), $log, "|", "tail", "-1");
    @command = ('ssh', $host, shell_quote("@command"));
    note("@command");
    my $result = `@command`;
    note("result='$result'");

    my $task_id;
    my $tracker;
    if ($result =~ m/(.*.)(to tip\s+)(.*.)(,\s+for tracker)(.*.)/) {
        $task_id = $3;
        $tracker = $5;
    }
    $tracker =~ s/\n//;
    $tracker =~ s/tracker_//;
    $tracker =~ s/\'//g;
    $tracker =~ s/ //g;
    my @tokens = split(":", $tracker);
    $tracker = $tokens[0];
    # note("tracker host = '$tracker', task id = '$task_id'");
    return $tracker;
}


=head2 submit_sleep_job_wo_wait($self, $job_hash)

Submit a sleep job and return immediately without any waiting

=over 2

=item I<Parameters>:

   * A hash reference of:
     - map_task
     - reduce_task
     - map_sleep
     - reduce_sleep
     - user

=item I<Returns>:

   * Job Id

=back

=cut
sub submit_sleep_job_wo_wait {
    my ($self, $hash) = @_;

    my $default_task = 0;

    use Time::HiRes qw(time);
    my $log_file = '/tmp/job.'.time;

    my @args = defined($hash->{args}) ? @{$hash->{args}} : ();
    push(@args,
         '-m',  defined($hash->{map_task})     ? $hash->{map_task}     : $default_task,
         '-r',  defined($hash->{reduce_task})  ? $hash->{reduce_task}  : $default_task,
         '-mt', defined($hash->{map_sleep})    ? $hash->{map_sleep}    : 1000,
         '-rt', defined($hash->{reduce_sleep}) ? $hash->{reduce_sleep} : 1000,
         "> $log_file", '2>&1', '&' );

    local $hash->{args} = \@args;
    local $hash->{command} = 'sleep';
    $self->run_jar_command($hash);

    return $log_file;
}


=head2 submit_sleep_job($self, $job_hash, $wait_time, $max_wait)

Submit a sleep job and return immediately

=over 2

=item I<Parameters>:

   * A hash reference of:
     - map_task
     - reduce_task
     - map_sleep
     - reduce_sleep
     - user
   * wait time
   * maximum wait

=item I<Returns>:

   * Job Id

=back

=cut
sub submit_sleep_job {
    my ($self, $hash, $wait_time, $max_wait) = @_;

    my $default_task = 0;
    my $log_file = '/tmp/job.'.time;    
    my @args = defined($hash->{args}) ? @{$hash->{args}} : ();

    # Check for memory allocation for tasks
    if (defined($hash->{mapper_mem})) {
        my $mapper_mem  = $hash->{mapper_mem};
        push(@args, "-Dmapreduce.map.memory.mb=$mapper_mem");
        delete($hash->{mapper_mem});
    }
    if (defined($hash->{reducer_mem})) {
        my $reducer_mem  = $hash->{reducer_mem};
        push(@args, "-Dmapreduce.reduce.memory.mb=$reducer_mem");
        delete($hash->{reducer_mem});
    }
    if (defined($hash->{am_mem})) {
        my $am_mem  = $hash->{am_mem};
        push(@args, "-Dyarn.app.mapreduce.am.resource.mb=$am_mem");
        delete($hash->{am_mem});
    }

    push(@args,
         '-m',  defined($hash->{map_task})     ? $hash->{map_task}     : $default_task,
         '-r',  defined($hash->{reduce_task})  ? $hash->{reduce_task}  : $default_task,
         '-mt', defined($hash->{map_sleep})    ? $hash->{map_sleep}    : 1000,
         '-rt', defined($hash->{reduce_sleep}) ? $hash->{reduce_sleep} : 1000,
         "> $log_file", '2>&1', '&' );

    local $hash->{args} = \@args;
    local $hash->{command} = 'sleep';
    $self->run_jar_command($hash);

    # Wait for the job id
    my ($app_id, $job_id);
    if ($Config->{YARN_USED}) {
        # For jobs that (is expected) to fail, only the application id will be
        # shown in the log file.
        $app_id = $self->wait_for_app_id($log_file, $wait_time, $max_wait);
        $job_id = $self->get_job_id($app_id);
    }
    else {
        $job_id = $self->wait_for_job_id($log_file, $wait_time, $max_wait);
    }
    note("Job submit failed!!!") unless ($job_id);
    return $job_id;
}


=head2 submit_batch_jobs($self, $jobs, $sleep, $verbose)

Submit a batch of sleep job

=over 2

=item I<Parameters>:

   * A hash reference of sleep jobs
   * Sleep duration between job submitions
   * Verbose

=item I<Returns>:

   * Job Ids

=back

=cut
sub submit_batch_jobs {
    my ($self, $jobs, $sleep, $verbose) = @_;
    my $num_jobs = scalar(@$jobs);
    note("--> Submit ($num_jobs) batch jobs: ");
    note("jobs = ", explain($jobs)) if ($verbose);

    # SUBMIT JOBS
    $sleep ||= 0;
    my $job_ids = [];
    foreach my $job (@$jobs) {
        my $job_id = $self->submit_sleep_job($job);
        sleep $sleep if ($sleep);
        note("Submitted job id '$job_id'");
        push(@$job_ids, $job_id);

    }
    my $num_jobs_submitted = scalar(@$job_ids);
    note("Submitted ($num_jobs_submitted) batch jobs: ".join(',',@$job_ids));
    return $job_ids;
}


=head2 submit_batch_job_wo_wait($self, $jobs, $sleep, $verbose)

Submit a batch of sleep jobs and return immediately

=over 2

=item I<Parameters>:

   * A hash reference of sleep jobs
   * Sleep duration between job submitions
   * Verbose

=item I<Returns>:

   * Job Ids

=back

=cut
sub submit_batch_jobs_wo_wait {
    my ($self, $jobs, $sleep, $verbose) = @_;    
    my $num_jobs = scalar(@$jobs);
    note("--> Submit ($num_jobs) batch jobs: ");
    note("jobs = ", explain($jobs)) if ($verbose);

    # SUBMIT JOBS
    $sleep ||= 3;
    my $job_ids = [];
    my $log_files = [];
    foreach my $job (@$jobs) {
        my $log_file = $self->submit_sleep_job_wo_wait($job);
        sleep $sleep if ($sleep);
        push(@$log_files, $log_file);
    }
    @$job_ids = $self->parse_job_ids($log_files);

    my $num_jobs_submitted = scalar(@$job_ids);
    note("Submitted ($num_jobs_submitted) batch jobs: ".join(',',@$job_ids));
    return $job_ids;
}


=head2 parse_job_ids($self, $log_files)

Parse the job ids give an array reference of log files

=over 2

=item I<Parameters>:

   * Array reference of log files containing job ids

=item I<Returns>:

   * Array reference of job ids

=back

=cut
sub parse_job_ids {
    my ($self, $log_files) = @_;

    note(explain($log_files));
    my @job_ids;

    foreach my $log_file (@$log_files) {
        my $job_id = $self->wait_for_job_id($log_file);

        if ($job_id) {
            note("Job id='$job_id'");
        }
        else {
            note("Job submit failed for log file '$log_file'!!!");
        }
        push(@job_ids, $job_id);
    }
    note(explain(\@job_ids));

    # sort the job ids
    @job_ids = sort(@job_ids);
    note(explain(\@job_ids));

    return @job_ids;
}


=head2 wait_for_app_id($self, $log_file, $wait_time, $max_wait)

Wait for the app id for the submitted app

=over 2

=item I<Parameters>:

   * Log file
   * (Optional) wait interval time
   * (Optional) max wait time

=item I<Returns>:

   * App ID

=back

=cut
sub wait_for_app_id {
    my ($self, $log_file, $wait_time, $max_wait) = @_;

    # Wait for the app id
    $wait_time = 1 unless defined($wait_time);
    $max_wait = 60 unless defined($max_wait);
    my $count = 1;
    my ($app_str, $app_id);
    while ($count <= $max_wait) {
        print_wait_for_msg($count, $wait_time, $max_wait, 
                           "app to start running");
        sleep $wait_time;
        my @command = ('grep' ,"'Submitted application'", $log_file);
        $app_str = `@command`;
        $app_id = $1 if ($app_str =~ m/(?:.*.)(application_\d+_\d+)(.*.)/);
        last if ($app_id);
        $count++;
    }
    if ($count > $max_wait) {
        note("WARN: Wait time expired before app was able to start");
        note("cat logfile '$log_file'");
        system("/bin/cat $log_file");
    }
    return $app_id;
}


=head2 wait_for_job_id($self, $log_file, $wait_time, $max_wait)

Wait for the job id for the submitted job

=over 2

=item I<Parameters>:

   * Log file
   * (Optional) wait interval time
   * (Optional) max wait time

=item I<Returns>:

   * Job ID

=back

=cut
sub wait_for_job_id {
    my ($self, $log_file, $wait_time, $max_wait) = @_;

    # Wait for the job id
    $wait_time = 1 unless defined($wait_time);
    $max_wait = 60 unless defined($max_wait);
    my $count = 1;
    my ($job_str, $job_id);
    while ($count <= $max_wait) {
        print_wait_for_msg($count, $wait_time, $max_wait, 
                           "job to start running");
        sleep $wait_time;
        my @command = ('grep' ,"'Running job:'", $log_file);
        $job_str = `@command`;
        $job_id = $1 if ($job_str =~ m/(?:.*.)(job_*.*$)/);
        last if ($job_id);
        $count++;
    }
    if ($count > $max_wait) {
        note("WARN: Wait time expired before job was able to start");
        note("cat logfile '$log_file'");
        system("/bin/cat $log_file");
    }
    return $job_id;
}


=head2 check_jobs_succeeded($self, $job_ids, $verbose)

Check jobs ran successfully

=over 2

=item I<Parameters>:

   * Array of Job IDs
   * Verbose

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub check_jobs_succeeded {
    my ($self, $job_ids, $verbose) = @_;
    my $all_status = $self->get_all_jobs_status();
    my $result = 0;
    my $status;
    foreach my $job_id (@$job_ids) {
        note("job id='$job_id'");
        $status = $all_status->{$job_id};
        note(explain($status)) if ($verbose);
        $result++ if ($status->{state} != 2);
    }
    return $result;
}


=head2 is_job_running($self, $job_id)

Check if the job is running

=over 2

=item I<Parameters>:

   * Job Id

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub is_job_running {
    my ($self, $job_id, $verbose) = @_;
    my $status = $self->get_job_status($job_id, $verbose);
    my $current_state = $status->{state};
    return (($current_state eq 'PREP') || ($current_state eq 'RUNNING')) ? 1 : 0;
}


=head2 is_any_job_running($self)

Check if any job is running

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub is_any_job_running {
    my ($self, $verbose) = @_;
    my $status = $self->get_jobs_status(undef, 0);
    return ($status) ? 1 : 0;
}


=head2 wait_for_job_state($self, $job_id, $job_state)

Wait for job to be in a specified state

=over 2

=item I<Parameters>:

   * Job Id
   * Job State: Running (1), Succeeded (2), Failed (3), Prep (4)
     Can be one or more state. Hash is expected for multiple matching states.
   * Wait interval
   * Max wait time

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub wait_for_job_state {
    my ($self, $job_id, $job_state, $wait_time, $max_wait, $verbose) = @_;
    $verbose ||= 1;
    my ($status, $result);    

    # Initialized the expected job states array reference
    my $exp_job_states = (ref($job_state) eq 'ARRAY') ? $job_state : [ $job_state ];
    # Convert interger job states (e.g [1-4]) to string job states
    # (e.g. 'RUNNING', 'SUCCEEDED', etc.)
    my $exp_job_states_str = [];
    my $states_str = [];
    unless ($Config->{YARN_USED}) {
        $states_str = [ 'RUNNING',
                        'SUCCEEDED',
                        'FAILED',
                        'PREP' ];
        foreach $job_state (@$exp_job_states) {
            push(@$exp_job_states_str, $states_str->[$job_state-1]);
        }        
    }
    # Initialized the expected job states hash reference
    my $exp_job_states_hash = {};
    foreach $job_state (@$exp_job_states) {
        $exp_job_states_hash->{uc($job_state)} = 1;
    }        
    note("Job States: RUNNING (1), SUCCEEDED (2), FAILED (3), PREP (4)");

    my $count = 1;
    $wait_time = 5 unless defined($wait_time);
    $max_wait = 36 unless defined($max_wait);
    my $is_found = 0;
    my $current_state = "";
    while ($count <= $max_wait) {
        $result = 0;
        $status = $self->get_job_status($job_id, $verbose);
        $current_state = $status->{state};
        if (exists $exp_job_states_hash->{$current_state}) {
            $is_found = 1;
            note("Found job '$job_id' in state '$current_state': ".
                 "total wait=".($count-1)*$wait_time.":");
            last;
        }
        $count++;
        my $msg = "job to be in expected state(s)";
        $msg .= " '".join(" or ", @$exp_job_states)."'";
        $msg .= " (".join(" or ", @$exp_job_states_str).")" unless ($Config->{YARN_USED});
        $msg .= ", current state='$current_state'";
        $msg .= " ($states_str->[$current_state-1])" unless ($Config->{YARN_USED});
        print_wait_for_msg($count, $wait_time, $max_wait, "$msg");
        sleep $wait_time;
    }
    note("WARN: Wait time expired before job(s) was able to complete")
        if ($count > $max_wait);
    return ($is_found) ? 0 : 1;
}


=head2 wait_for_tasks_state($self, $job_id, $task_type, $task_state, $num_tasks,
$wait_time, $max_wait, $verbose)

Wait for task to be in a specified state

=over 2

=item I<Parameters>:

   * Job Id
   * Job Type
   * Job State: Running (1), Succeeded (2), Failed (3), Prep (4)
     Can be one or more state. Hash is expected for multiple matching states.
   * Num Tasks: Number of tasks
   * Wait interval
   * Max wait time
   * Verbose

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub wait_for_tasks_state {
    my ($self, $job_id, $task_type, $task_state, $num_tasks, $wait_time, $max_wait, $verbose) = @_;

    my $return = 0;
    my $count = 1;
    $wait_time = 1 unless defined($wait_time);
    $max_wait = 60 unless defined($max_wait);

    my $task_id = ($num_tasks) ? $self->get_task_id($job_id, $task_type, $num_tasks-1, $verbose) : '';
    my $msg = ($task_id) ? "task id='$task_id'" : '';
    while ($count <= $max_wait) {
        print_wait_for_msg($count, $wait_time, $max_wait, $msg);
        my @args = ('-list-attempt-ids', $job_id, $task_type, $task_state);
        my ($stdout, $stderr, $success, $exit_code) =
            $self->run_hadoop_command( {
                command => 'job',
                args => \@args,
            } );

        # note("$stdout");        
        if ($task_id) {
            # Don't match case because formats are different between reeleases.
            # E.g. 
            # attempt_1329522202425_0001_M_000099_0
            # attempt_1329522202425_0001_m_000099_0
            last if ($stdout =~ /$task_id/i);
        }
        else {
            last if ($stdout);
        }
        sleep $wait_time;
        $count++;
    }
    if ($count > $max_wait) {
        note("WARN: Wait time expired before all $task_type tasks were $task_state");
        $return = 1;
    }
    return $return;
}


=head2 get_num_tasks($self, $job_id, $task_type, $task_state, $verbose)

Get the number of tasks

=over 2

=item I<Parameters>:

   * Job Id
   * Job Type
   * Job State: Running (1), Succeeded (2), Failed (3), Prep (4)
     Can be one or more state. Hash is expected for multiple matching states.
   * Verbose

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub get_num_tasks {
    my ($self, $job_id, $task_type, $task_state, $verbose) = @_;
    my $return = 0;
    my @args = ('-list-attempt-ids', $job_id, $task_type, $task_state);
    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command( {
            command => 'job',
            args => \@args,
        } );
    my @tasks=split('\n', $stdout);
    note("tasks = $stdout");
    my $num_tasks_found = scalar(@tasks);
    note("num tasks found = '$num_tasks_found'");
    return $num_tasks_found;
}


=head2 get_num_job_tasks($self, $job_id, $task_state, $verbose)

Get the number of tasks for the job

=over 2

=item I<Parameters>:

   * Single Job Id or an Array of Job Id
   * Job State: Running (1), Succeeded (2), Failed (3), Prep (4)
     Can be one or more state. Hash is expected for multiple matching states.
   * Verbose

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub get_num_jobs_tasks {
    my ($self, $job_ids, $task_state, $verbose) = @_;
    my $job_ids_ref = (ref($job_ids) eq 'ARRAY') ? $job_ids : [ $job_ids ];
    use Time::HiRes qw(time);
    my $log_file = '/tmp/tasks_'.time;
    my @task_types=('MAP','REDUCE');
    my (@args, @tasks, $result, $num_tasks_found, $file);
    my ($stdout, $stderr, $success, $exit_code);
    foreach my $job_id (@$job_ids_ref) {
        foreach my $task_type (@task_types) {
            $file=$log_file."_".$job_id."_".$task_type;
            @args = ('-list-attempt-ids', $job_id, $task_type, $task_state);
            push(@args, "> $file", '&' );
            ($stdout, $stderr, $success, $exit_code) =
                $self->run_hadoop_command( {
                    command => 'job',
                    args => \@args,
                },
                undef,
                $verbose);
        }
    }
    sleep 5;
    my $num_jobs_tasks;
    foreach my $job_id (@$job_ids_ref) {
        my $num_job_tasks = {};
        foreach my $task_type (@task_types) {
            $file=$log_file."_".$job_id."_".$task_type;
            $result=`/bin/cat $file`;
            note("$job_id $task_type result='$result'") if ($verbose);
            @tasks=split('\n', $result);
            $num_tasks_found = scalar(@tasks);
            $num_job_tasks->{$task_type}=$num_tasks_found;
        }
        $num_jobs_tasks->{$job_id}=$num_job_tasks;
    }
    note(explain($num_jobs_tasks)) if ($verbose);
    unlink glob "$log_file.*";
    return $num_jobs_tasks;
}


=head2 wait_for_jobs_to_complete($self, $wait_time, $max_wait, $verbose)

Wait for jobs to complete

=over 2

=item I<Parameters>:

   * None
   * Wait interval
   * Max wait time
   * Verbose

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub wait_for_jobs_to_complete {
    my ($self, $wait_time, $max_wait, $verbose) = @_;
    my $status;
    my $count = 1;
    $wait_time = 20 unless defined($wait_time);
    $max_wait = 15 unless defined($max_wait);
    my $is_found = 0;
    my $current_state = "";
    while ($count <= $max_wait) {
        $status = $self->get_jobs_status(undef, 0);
        if ($status) {
            note(explain($status));
        }
        else {
            $is_found = 1;
            last;
        }
        note("Wait ($count) for jobs to complete in $wait_time second(s): ".
             "total wait=".($count-1)*$wait_time.":");
        sleep $wait_time;
        $count++;
    }
    note("WARN: Wait time expired before jobs were able to complete")
        if ($count > $max_wait);
    return ($is_found) ? 0 : 1;
}


=head2 wait_for_job_to_succeed($self, $job_id)

Wait for jobs to succeed

=over 2

=item I<Parameters>:

   * Job IDs
   * wait time
   * maximum wait
   * verbose

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub wait_for_job_to_succeed {
    my ($self, $job_id, $wait_time, $max_wait, $verbose) = @_;
    return $self->wait_for_jobs_to_succeed([ $job_id ],
                                           $wait_time,
                                           $max_wait,
                                           $verbose);
}


=head2 convert_state($self, $state)

Conver state string value to equivalent numeric value

=over 2

=item I<Parameters>:

   * State string

=item I<Returns>:

   * State numeric number

=back

=cut
sub convert_state {
    my ($self, $state) = @_;

    if ($state eq 'RUNNING') {
        return 1;
    }
    elsif ($state eq 'SUCCEEDED') {
        return 2;
    }
    elsif ($state eq 'FAILED') {
        return 3;
    }
    elsif ($state eq 'PREP') {
        return 4;
    }
    else {
        return -1;
    }
}


=head2 get_job_id($self, $app_id)

get the job id using the given app id

=over 2

=item I<Parameters>:

   * App ID

=item I<Returns>:

   * Job ID

=back

=cut
sub get_job_id {
    my ($self, $app_id) = @_;
    $app_id =~ s/application/job/;
    my @tokens = split("_", $app_id);
    return join("_",@tokens);
}


=head2 get_app_id($self, $job_id)

get the app id using the given job id

=over 2

=item I<Parameters>:

   * Job ID

=item I<Returns>:

   * App ID

=back

=cut
sub get_app_id {
    my ($self, $job_id) = @_;
    $job_id =~ s/job/app/;
    my @tokens = split("_", $job_id);
    $tokens[-1] =~ s/^00//;
    return join("_",@tokens);
}


=head2 get_jobhistory_job_id($self, $job_id)

get the job history job id using the given job id

=over 2

=item I<Parameters>:

   * Job ID

=item I<Returns>:

   * Job History Job ID

=back

=cut
sub get_jobhistory_job_id {
    my ($self, $job_id) = @_;
    my @tokens = split("_", $job_id);
    $tokens[-1] =~ s/^00//;
    return join("_",@tokens)."_$tokens[-1]";
}


=head2 wait_for_jobs_to_succeed($self, $job_ids)

Wait for jobs to succeed

=over 2

=item I<Parameters>:

   * Array of Job IDs
   * wait time
   * maximum wait
   * verbose

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub wait_for_jobs_to_succeed {
    my ($self, $job_ids, $wait_time, $max_wait, $verbose) = @_;
    my ($all_status, $status, $result);
    my $count = 1;
    my $msg = "all jobs (@$job_ids) to succeed";
    # States are: Running : 1, Succeeded : 2, Failed : 3, Prep : 4
    my $pass_state = 2;
    $wait_time = 5 unless defined($wait_time);
    $max_wait = 36 unless defined($max_wait);
    while ($count <= $max_wait) {
        print_wait_for_msg($count, $wait_time, $max_wait, $msg);
        sleep $wait_time;
        $result = 0;
        $all_status = $self->get_all_jobs_status();
        # note(explain($all_status));
        foreach my $job_id (@$job_ids) {
            # note("job_id = '$job_id'");
            $status = $all_status->{$job_id};
            if (defined($status)) {
                note(explain($status)) if ($verbose);
                $status->{state} = $self->convert_state($status->{state})
                    if ($Config->{YARN_USED});
                $result++ if ($status->{state} != $pass_state);
            }
            else {
                diag("Did not find job '$job_id' in job list.");
            }
        }
        last if ($result == 0);
        $count++;
    }
    note("WARN: Wait time expired before all jobs were able to complete")
        if ($count > $max_wait);
    return $result;
}


=head2 kill_job($self, $job_id)

Kill jobs

=over 2

=item I<Parameters>:

   * Job ID

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE

=back

=cut
sub kill_job {
    my ($self, $job_id) = @_;
    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command( {
            command => 'job',
            args => ['-kill', $job_id],
        } );
    return $success;
}

# Get the used memory (including the AM memory allocation)
sub get_used_memory {
    my ($self, $job_status, $verbose) = @_;
    my $used_memory = $job_status->{UsedMem};
    note("raw allocated/used memory = '$used_memory'");
    $used_memory =~ s/M$//g;
    $used_memory = nearest(.01, $used_memory/1024);
    note("allocated/used memory = '$used_memory' GB");
    return $used_memory;
}

# Get the used containers (including the AM container)
sub get_used_containers {
    my ($self, $job_status, $verbose) = @_;
    my $used_containers = $job_status->{UsedContainers};
    note("allocated/used containers = '$used_containers'");
    return $used_containers;
}

# This need to round up by factor of the min memory allocation
sub calc_slot_mem_size {
    my ($self, $configured_mem, $min_memory_allocation) = @_;
    return (ceil($configured_mem/$min_memory_allocation) *
            $min_memory_allocation)/1024;
}

#################################################################################
# SUB TYPE: CAPACITY SCHEDULER RELATED SUBROUTINES
#################################################################################

# Get the Total Cluster Capacity
sub get_cluster_capacity { 
    my ($self, $gb_ram_per_host) = @_;
    my $component_host=$Config->{NODES}->{RESOURCEMANAGER}->{HOST};
    my $prop_name="yarn.nodemanager.resource.memory-mb";
    my $filename=$Config->{YARN_SITE_XML};
    my $node_mb=$self->get_xml_prop_value($prop_name, $filename, $component_host);
    my $node_gb=int($node_mb/1024);
    $gb_ram_per_host = $node_gb;
    note("nodemanager resource per node = '$gb_ram_per_host' GB");

    # TODO: We could have timing issues if the trackers are still coming online
    # after a reboot, so the call only gets partial number of trackers?
    my $hostnames = $Config->{NODES}->{NODEMANAGER}->{HOST};
    my $num_exp_trackers = scalar(@$hostnames);
    note("expected number of nodemanager = '$num_exp_trackers'");

    my $num_active_trackers = $self->get_num_active_trackers();
    my $attempt = 0;
    my $max_attempts = 6;
    my $sleep = 5;
    while (($num_active_trackers != $num_exp_trackers) && 
           ($attempt < $max_attempts)) {
        note("number of active trackers does not match configured number of trackers... ".
             "wait $sleep seconds and try again...");
        sleep $sleep;
        $num_active_trackers = $self->get_num_active_trackers();
        $attempt++;
    }
    if ($num_active_trackers == $num_exp_trackers) {
        note("Found active trackers for all '$num_exp_trackers' ".
             "expected nodes");
    }
    else {
        note("Found '$num_active_trackers' active trackers for ".
             "'$num_exp_trackers' expected nodes");
    }

    my $total_cluster_capacity=$num_active_trackers*$gb_ram_per_host;
    note("total cluster capacity = '$total_cluster_capacity'");
    return $total_cluster_capacity;
}

sub get_maximum_queue_capacity { 
    my ($self, $queue, $total_cluster_capacity) = @_;
    my $component_host=$Config->{NODES}->{RESOURCEMANAGER}->{HOST};
    my $prop_name="yarn.scheduler.capacity.root.$queue.maximum-capacity";
    my $filename=$Config->{CAPACITY_SCHEDULER_XML};
    my $queue_setting=$self->get_xml_prop_value($prop_name, $filename, $component_host);
    $queue_setting ||= 100;

    my $maximum_queue_capacity=ceil(($queue_setting*$total_cluster_capacity)/100);
    note("queue maximum capacity of '$queue_setting'% of total capacity of ".
         "'$total_cluster_capacity' = '$maximum_queue_capacity'");
    return $maximum_queue_capacity;
}

sub get_queue_capacity { 
    my ($self, $queue, $total_cluster_capacity) = @_;
    my $component_host=$Config->{NODES}->{RESOURCEMANAGER}->{HOST};
    my $prop_name="yarn.scheduler.capacity.root.$queue.capacity";
    my $filename=$Config->{CAPACITY_SCHEDULER_XML};
    my $queue_setting=$self->get_xml_prop_value($prop_name, $filename, $component_host);

    my $queue_capacity=ceil(($queue_setting*$total_cluster_capacity)/100);
    note("queue capacity of '$queue_setting'% of total capacity of ".
         "'$total_cluster_capacity' = '$queue_capacity'");
    return $queue_capacity;
}

sub get_min_user_limit_percent { 
    my ($self, $queue, $total_cluster_capacity) = @_;
    my $component_host=$Config->{NODES}->{RESOURCEMANAGER}->{HOST};
    my $prop_name="yarn.scheduler.capacity.root.$queue.minimum-user-limit-percent";
    my $filename=$Config->{CAPACITY_SCHEDULER_XML};
    my $setting=$self->get_xml_prop_value($prop_name, $filename, $component_host);
    return $setting;
}

sub get_user_limit_factor { 
    my ($self, $queue, $total_cluster_capacity) = @_;
    my $component_host=$Config->{NODES}->{RESOURCEMANAGER}->{HOST};
    my $prop_name="yarn.scheduler.capacity.root.$queue.user-limit-factor";
    my $filename=$Config->{CAPACITY_SCHEDULER_XML};
    my $setting=$self->get_xml_prop_value($prop_name, $filename, $component_host);
    $setting ||= 1;
    return $setting;
}

sub get_queues { 
    my ($self) = @_;
    my $component_host=$Config->{NODES}->{RESOURCEMANAGER}->{HOST};
    my $prop_name="yarn.scheduler.capacity.root.queues";
    my $filename=$Config->{CAPACITY_SCHEDULER_XML};
    my $setting=$self->get_xml_prop_value($prop_name, $filename, $component_host);
    note("queues = '$setting'");
    my @queues=split(',', $setting);
    return \@queues;
}

sub get_min_memory_allocation { 
    my ($self) = @_;
    my $prop_name  = "yarn.scheduler.minimum-allocation-mb";
    my $config_dir = (defined($self->{tmp_conf_dir})) ?
        $self->{tmp_conf_dir} : $Config->{HADOOP_CONF_DIR};
    my $prop_file  = "$config_dir/capacity-scheduler.xml";
    my $prop_value = $self->get_resourcemanager_xml_prop_value($prop_name, $prop_file);
    return $prop_value;
}


=head2 get_queue_limits_init($self)

Get the queue limits initialization from the jobtracker/resourcemanager log

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * Hash containing the capacity scheduler configured limits from the log

=back

=cut
sub get_queue_limits_init {
    my ($self) = @_;

    my $component = $Config->{RESOURCEMANAGER_NAME};
    my $log = $Config->{NODES}->{$component}->{LOG};
    my $host = $Config->{NODES}->{$component}->{HOST};

    my $str;
    my $hash = {};
    my @command = ();

    my $queues = $self->get_queues();
    if ($Config->{YARN_USED}) {
        $str = "'INFO org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.[P|L].*Queue: Initializing'";
        @command = ('grep', shell_quote($str), $log);
        @command = ('ssh', $host, @command,);
        note("@command");
        my $result = `@command`;
        note("$result");
        my @kvp;
        foreach my $line (split("\n", $result)) {
            my $queue_type;
            my $substr;
            if ($line =~ m/(.*.)(INFO .*capacity.)(.*.Queue)(: Initializing\s+)(.*.)/) {
                $queue_type = $3;
                $substr = $5;
            }
            # note("log substr = '$substr'");

            my @tokens = split(',', $substr);
            my $queue_name = trim(shift(@tokens));
            my $queue_hash = {};
            foreach my $attribute (@tokens) {
                if ($attribute =~ "=") {
                    # note("attribute = '$attribute'");
                    @kvp = split("=", $attribute);
                    $queue_hash->{trim($kvp[0])} = trim($kvp[1]);
                }
                else {
                    # $queue_name = trim($attribute);
                    # note("queue_name = $queue_name");
                    note("att = $attribute");
                }
            }            
            # note("queue_name = '$queue_name'");
            $queue_hash->{queue_type} = $queue_type;
            $hash->{$queue_name} = $queue_hash;
        }
    }
    else {
        $str = "\\\"Initializing \\'default\\' queue\\\"";
        @command = ('grep', $str, $log);
        @command = ('ssh', $host, @command, '|', 'tail', '-1');
        note("@command");
        my $result = `@command`;
        note("$result");
        
        if ($result =~
            m/(\d\d\d\d-\d\d-\d\d\s\d\d:\d\d:\d\d)(.*.)(Initializing \'default\' queue with\s+)(.*.)/) {
            
            $hash->{date} = $1;
            my $values = {};
            my @kvps = split(",", $4);
            foreach my $kvp (@kvps) {
                my @tokens = split("=", $kvp);
                $values->{trim($tokens[0])} = $tokens[1];
            }
            $hash->{values} = $values;
            
            note("date='$hash->{date}'");
            note(explain($values));
        }
    }
    note("logged limits = ", explain($hash));
    return $hash;
}


sub get_capacity {
    my ($self, $verbose) = @_;
    my $capacity = { 'cluster' => $self->get_cluster_capacity() };
    my $queues = $self->get_queues();
    foreach my $queue (@$queues) {
        my $queue_capacity = {};
        $queue_capacity->{queue} = $self->get_queue_capacity($queue,
                                                             $capacity->{cluster});
        $queue_capacity->{maximum}  = $self->get_maximum_queue_capacity($queue,
                                                                        $capacity->{cluster});
        $queue_capacity->{ulf}      = $self->get_user_limit_factor($queue);
        $queue_capacity->{mulp}     = $self->get_min_user_limit_percent($queue);
        use List::Util qw(min max);

        $queue_capacity->{user}     = 
            min(ceil($queue_capacity->{queue} * $queue_capacity->{ulf}), 
                $capacity->{cluster});

        $queue_capacity->{minimum_user_limit} =
            min(ceil(($queue_capacity->{queue}*$queue_capacity->{mulp})/100),
                $queue_capacity->{queue});

        $capacity->{queues}->{$queue} = $queue_capacity;
    }
    note(explain($capacity)) if ($verbose);
    return $capacity;
}


=head2 expand_jobs($self, $job_hash)

Submit a sleep job and return immediately

=over 2

=item I<Parameters>:

   * A hash reference of:
     - user
     - reduce_task
     - map_sleep
     - reduce_sleep
     - user

=item I<Returns>:

   * Array reference of job hashes

=back

=cut
sub expand_jobs {
    my ($self, $args, $verbose) = @_;
    my $def_user        = 'hadoopqa';
    my $def_queue       = 'default';
    my $def_tasks       = 1;
    my $def_task_sleep  = 2000;
    my $user            = defined($args->{user})         ? $args->{user}         : $def_user;
    my $queue           = defined($args->{queue})        ? $args->{queue}        : $def_queue;
    my $num_tasks       = defined($args->{num_tasks})    ? $args->{num_tasks}    : $def_tasks;
    my $task_sleep      = defined($args->{task_sleep})   ? $args->{task_sleep}   : $def_task_sleep;
    my $m_task_sleep    = defined($args->{m_task_sleep}) ? $args->{m_task_sleep} : $task_sleep;
    my $r_task_sleep    = defined($args->{r_task_sleep}) ? $args->{r_task_sleep} : $task_sleep;
    my $am_mem          = defined($args->{am_mem})       ? $args->{am_mem}       : 2048;
    my $mapper_mem      = defined($args->{mapper_mem})   ? $args->{mapper_mem}   : 1024;
    my $reducer_mem     = defined($args->{reducer_mem})  ? $args->{reducer_mem}  : 1024;
    my $properties      = defined($args->{properties})   ? $args->{properties}   : '';
    my $capacity        = defined($args->{capacity})     ? $args->{capacity}     : 0;
    my $factor          = defined($args->{factor})       ? $args->{factor}       : 1;
    my $num_jobs;
    my $jobs = [];
    note("---> Expand jobs: ", explain($args)) if $verbose;

    # Determine the number of jobs to run.
    if (defined($args->{jobs})) {
        $jobs = $args->{jobs};
        $num_jobs = scalar(@$jobs);
    } elsif (defined($args->{num_jobs})) {
        $num_jobs = $args->{num_jobs};
    } else {
        $num_jobs=1;
    }
    note("num jobs = $num_jobs") if $verbose;

    # Set the tasks sleep duration
    if (ref($task_sleep) eq 'ARRAY') {
        $m_task_sleep=$task_sleep->[0];
        $r_task_sleep=$task_sleep->[1];
    }

    my $default_job = { 'map_task'     => $num_tasks,
                        'reduce_task'  => $num_tasks,
                        'map_sleep'    => $m_task_sleep,
                        'reduce_sleep' => $r_task_sleep,
                        'user'         => $user,
                        'queue'        => $queue };

    $properties = [ '-Dmapreduce.job.acl-view-job=*' ]
        unless (defined($args->{properties}));
    unless (defined($args->{jobs})) {
        my $mem_properties = [ "-Dyarn.app.mapreduce.am.resource.mb=$am_mem",
                               "-Dmapreduce.map.memory.mb=$mapper_mem",
                               "-Dmapreduce.reduce.memory.mb=$reducer_mem" ];
        push(@$properties, @$mem_properties);
    }
    $default_job->{args} = $properties;

    # Fill out jobs array
    if (defined($args->{jobs})) {
        foreach my $job_index (1..$num_jobs) {
            my $job = $jobs->[($job_index-1)];

            # Set User
            unless (defined($job->{user})) {
                if (defined($args->{user})) {
                    $job->{user} = $args->{user};
                } else {
                    $job->{user} = 'hadoop'.((($job_index-1) % 20)+1); 
                }
            }

            # Values from %hash2 (defined values) replace values from
            # %hash1 (default values) when they have keys in common.
            my %hash1 = %{$default_job};
            my %hash2 = %{$job};
            @hash1{ keys %hash2 } = values %hash2;
            $job = \%hash1;

            # Set Memory Allocation - 
            # Job level specification if defined will override the default or
            # higher level arguments.
            $am_mem      = $job->{am_mem}      if (defined($job->{am_mem}));
            $mapper_mem  = $job->{mapper_mem}  if (defined($job->{mapper_mem}));
            $reducer_mem = $job->{reducer_mem} if (defined($job->{reducer_mem}));
            my $mem_properties = [ "-Dyarn.app.mapreduce.am.resource.mb=$am_mem",
                                   "-Dmapreduce.map.memory.mb=$mapper_mem",
                                   "-Dmapreduce.reduce.memory.mb=$reducer_mem" ];
            push(@{$job->{args}}, @$mem_properties);
            note("job args = ",explain($job->{args})) if ($verbose);

            # Set map and reduce tasks
            my $queue = $job->{queue};
            # If capacity is passed in, check to redefine the num tasks
            if (defined($capacity)) {
                my $queue_capacity=0;
                if (defined($capacity->{queues}->{$queue}->{queue})) {
                    $queue_capacity=$capacity->{queues}->{$queue}->{queue};
                } else {
                    warn("Capcity for queue '$queue' is not defined!!!")
                        unless defined($capacity->{queues}->{$queue}->{queue});
                    $jobs = [];
                    return $jobs;
                }
                my $tasks_factor =
                    (defined($job->{factor})) ? $job->{factor} : $factor;
                $num_tasks = int($queue_capacity*$tasks_factor);
            }
            $job->{'map_task'} = $num_tasks;
            $job->{'reduce_task'} = $num_tasks;

            $jobs->[($job_index-1)] = $job;
        }
    } else {        
        unless (defined($args->{num_tasks})) {
            my $queue_capacity=$capacity->{queues}->{$queue}->{queue};
            $num_tasks = int($queue_capacity*$factor);
            $default_job->{map_task} = $num_tasks;
            $default_job->{reduce_task} = $num_tasks;
        }
        foreach my $job_index (1..$num_jobs) {
            $jobs->[($job_index-1)] = { %$default_job };
        }
    }

    foreach my $job (@$jobs) {
        delete($job->{factor})      if (defined($job->{factor}));
        delete($job->{am_mem})      if (defined($job->{am_mem}));
        delete($job->{mapper_mem})  if (defined($job->{mapper_mem}));
        delete($job->{reducer_mem}) if (defined($job->{reducer_mem}));
    }
    note("jobs=", explain($jobs)) if $verbose;
    return $jobs;
}

#################################################################################
# High RAM Test Functions
#################################################################################

sub setup_mem_test_conf {
    my ($self, $args, $verbose) = @_;
    $verbose     ||= 0;
    my $min_memory_allocation =
        defined($args->{min_memory_allocation}) ?
        $args->{min_memory_allocation} : 1024;
    my $config_file =
        defined($args->{config}) ?
        $args->{config} : 'capacity-scheduler100-multiplequeues.xml';
    my $conf_dir="$Bin/../../../../data/conf/";
    $self->restart_resourcemanager_with_cs_conf("$conf_dir/$config_file");
    # my $capacity = $self->get_capacity();

    $self->backup_resourcemanager_conf() unless defined($self->{tmp_conf_dir});
    my $config_dir=$self->{tmp_conf_dir};

    my ($prop_name, $prop_value, $prop_file, $new_value, $rm_host);
    
    # Modify the jmx metrics refresh intreval
    $rm_host = $Config->get_resourcemanager();
    $prop_file  = "$config_dir/hadoop-metrics2.properties";
    $self->replace_value('\*\.period=10',
                         '\*\.period=1',
                         $prop_file,
                         $rm_host);
    my $period = `ssh $rm_host "cat $config_dir/hadoop-metrics2.properties | grep period="`;
    note("New $config_dir/hadoop-metrics2.properties: $period");
    
    # Modify mapreduce.job.reduce.slowstart.completedmaps value to 1 in mapred-site.xml file 
    $prop_name = "mapreduce.job.reduce.slowstart.completedmaps";
    $prop_value = "1.0";
    $prop_file  = "$config_dir/mapred-site.xml";
    $self->set_resourcemanager_xml_prop_value($prop_name, $prop_value, $prop_file);
    $new_value = $self->get_resourcemanager_xml_prop_value($prop_name, $prop_file);
    note("value for '$prop_name' is set to '$new_value'");
    
    $prop_name  = "yarn.scheduler.minimum-allocation-mb";
    $prop_value = $min_memory_allocation;
    $prop_file  = "$config_dir/capacity-scheduler.xml";
    $self->set_resourcemanager_xml_prop_value($prop_name, $prop_value, $prop_file);
    $new_value = $self->get_resourcemanager_xml_prop_value($prop_name, $prop_file);
    note("value for '$prop_name' is set to '$new_value'");
    
    $prop_name  = "yarn.mapreduce.job.reduce.rampup.limit";
    $prop_value = "0";
    $self->set_resourcemanager_xml_prop_value($prop_name, $prop_value, $prop_file);
    $new_value = $self->get_resourcemanager_xml_prop_value($prop_name, $prop_file);
    note("value for '$prop_name' is set to '$new_value'");
    
    note("Moving both the jmxterm and Fetchmetrics info to hadoop homes dir /homes/hadoopqa/");
    my $source_dir=$Config->{WORKSPACE}."/data/metrics";
    my $dest_dir="/homes/hadoopqa";
    system("cp $source_dir/jmxterm-1.0-SNAPSHOT-uber.jar $dest_dir");
    system("cp $source_dir/FetchMetricsInfo.sh $dest_dir");

    # restart it
    $self->restart_resourcemanager($config_dir);
    return;
}

sub check_tasks_status {
    my ($self, $job_id, $job_status, $found_status, $slot_mem_size,
        $verbose) = @_;

    my $am_container    = 1;
    my $am_mem          = $slot_mem_size->{am};

    # Tasks counts via separate calls to job -list-attempt-ids for each
    # job are not guaranteed to be accurate due to timing issues.
    # So use tasks counts from job -list instead. 

    my $task_containers = $self->get_used_containers($job_status) - $am_container;
    my $tasks_mem       = $self->get_used_memory($job_status) - $am_mem;

    my $running_job_tasks = $self->get_num_jobs_tasks($job_id, 'running', $verbose);
    my $completed_job_tasks = $self->get_num_jobs_tasks($job_id, 'completed', $verbose);

    note("--------------------------------------------------------------------------------");
    note("Validate memory allocation: Job '$job_id':");
    note("Job List Output: '$task_containers' M/R task containers of $tasks_mem' GB ".
         "+ '$am_container' AM container of '$am_mem' GB:");
    note("job list output used mem = '$tasks_mem'");

    note("--> Check running tasks status=", explain($running_job_tasks));
    note("--> Check completed tasks status=", explain($completed_job_tasks));

    my $running_m_tasks    = $running_job_tasks->{$job_id}->{MAP};
    my $running_r_tasks    = $running_job_tasks->{$job_id}->{REDUCE};
    my $num_running_tasks  = $running_m_tasks + $running_r_tasks;

    my $completed_m_tasks  = $completed_job_tasks->{$job_id}->{MAP};
    my $completed_r_tasks  = $completed_job_tasks->{$job_id}->{REDUCE};

    # No reducers should be running while mappers are still running
    if (($running_m_tasks > 0) && ($running_r_tasks > 0)) {
        note("ERROR: Reducer tasks are running before all Mapper tasks are completed when ".
             "mapreduce.job.reduce.slowstart.completedmaps is set to 1.0");
        $found_status->{no_reducer_while_mapper_running} = 0;
    }

    my $m_task_slot_size = $slot_mem_size->{map};
    my $r_task_slot_size = $slot_mem_size->{reduce};

    my $running_m_tasks_mem = $running_m_tasks * $m_task_slot_size;
    my $running_r_tasks_mem = $running_r_tasks * $r_task_slot_size;
    my $running_tasks_mem   = $running_m_tasks_mem + $running_r_tasks_mem;

    note("running mapper tasks mem = '$running_m_tasks_mem' (".
         "running mapper tasks '$running_m_tasks' * ".
         "mapper slots size '$m_task_slot_size')");
    note("running reducer tasks mem = '$running_r_tasks_mem' (".
         "running reducer tasks '$running_r_tasks' * ".
         "reducer slots size '$r_task_slot_size')");
    note("running tasks total mem = '$running_tasks_mem' (".
         "running mapper tasks mem '$running_m_tasks_mem' + ".
         "running reducer tasks mem '$running_r_tasks_mem')");
    note("Validate memory allocation: Job '$job_id': ".
         "job list output used mem '$tasks_mem' ?= running tasks total mem '$running_tasks_mem'");

    if ($tasks_mem == $running_tasks_mem) {
        note("Mapper and Reducer slots are properly allocated.");
    }
    else {
        if ($task_containers == $num_running_tasks) {
            note("Number of mapper and reducer slots are not properly used.");
            $found_status->{found_expected_slots} = 0;
        }
        else {
            # TODO: possible timing issue when transitioning from mapper tasks
            # to reducer tasks. 
            if (($running_m_tasks == 0) &&
                ($completed_m_tasks  > 0) && 
                ($completed_r_tasks == 0)) {
                note("task containers from job list output ($task_containers) != ".
                     "task containers from job list attempt ids running ($num_running_tasks): ".
                     "possible test timing issue switching from map to reduce tasks");
            }
            else {
                if ($task_containers > $num_running_tasks) {
                    note("Found Bug 4624829: AM releases extra containers: ".
                         "task containers from job list output ($task_containers) != ".
                         "task containers from job list attempt ids running ($num_running_tasks)");
                    $found_status->{no_extra_containers} = 0;
                }
                else {
                    note("task containers from job list output ($task_containers) < ".
                         "task containers from job list attempt ids running ($num_running_tasks): ".
                         "possible test timing issue");
                }
            }
        }
    }
    return $found_status;
}

sub check_jobs_status {
    my ($self, $job_ids, $mem_settings, $sleep, $max_iter, $verbose) = @_;
    $sleep    ||= 5;
    $max_iter ||= 1000;
    $verbose  ||= 0;

    # Init expected mapper and reducer slots
    my $job_id;
    my $slot_mem_size = {};
    my $job_index = 0;
    my $min_memory_allocation = $self->get_min_memory_allocation();
    foreach my $job_mem_settings (@$mem_settings) {
        my $m_task_slot_size  =
            $self->calc_slot_mem_size($job_mem_settings->{map},
                                      $min_memory_allocation);
        my $r_task_slot_size =
            $self->calc_slot_mem_size($job_mem_settings->{reduce},
                                      $min_memory_allocation);
        my $am_slot_size      =
            $self->calc_slot_mem_size($job_mem_settings->{am},
                                      $min_memory_allocation);
        $job_id = $job_ids->[$job_index];
        $slot_mem_size->{$job_id} = { 'map'    => $m_task_slot_size,
                                      'reduce' => $r_task_slot_size,
                                      'am'     => $am_slot_size };
        $job_index++;
    }
    note("Expected slot memory size = ", explain($slot_mem_size));

    # Init variables for verifications
    my $found = {};
    foreach my $job_id (@$job_ids) {
        my $found_status = { 'no_reducer_while_mapper_running' => 1,
                             'no_extra_containers'             => 1,
                             'found_expected_slots'            => 1 };
        $found->{$job_id} = $found_status;
    }

    my $index = 0;
    # Get status while jobs are still running, or until max iteration limit
    while ($self->is_any_job_running($verbose)) {
        $index++;
        last if ($index > $max_iter);
        note("--> loop # $index:");
        note("--> Check jobs status:");
        my $jobs_status = $self->get_jobs_status(undef, 1);
        note("jobs status = ", explain($jobs_status)) if ($verbose);

        foreach $job_id (@$job_ids) {
            my $job_status = $jobs_status->{$job_id};
            note("job status = ", explain($job_status)) if ($verbose);
            next unless ($job_status);
            $found->{$job_id} =
                $self->check_tasks_status($job_id,
                                          $job_status,
                                          $found->{$job_id},
                                          $slot_mem_size->{$job_id});
            sleep $sleep;
        }
    }
    note("Found = ",explain($found));

    $job_index = 0;
    my $found_no_reducer_while_mapper_running = 1;
    my $found_no_extra_containers = 1;
    foreach $job_id (@$job_ids) {
        $job_index++;
        my $found_status = $found->{$job_id};
        ok($found_status->{found_expected_slots},
           "Number of slots for mapper and reducer for job $job_index are properly used.");

        if ($found_status->{no_extra_containers} == 0) {
            $found_no_extra_containers = 0;
            note("Found Bug 4624829: AM should not release extra containers: job $job_index");
        }
           
        # If mapper was not run before any reducer 
        # If a reduce task was started before all mappers were completed
        # reduce_before_all_mapper_completed
        if ($found_status->{no_reducer_while_mapper_running} == 0) {
            $found_no_reducer_while_mapper_running = 0;
            note("Found Bug 5564075: Reducer tasks should not be started until ".
                 "all mapper tasks are completed when mapreduce.job.reduce.".
                 "slowstart.completedmaps is set to 1.0: job $job_index");
        }
    }

    # This is technically not a defect. Consider commenting it out.
    if ($found_no_extra_containers == 1) {
        note("Did not find AM releasing extra containers. ".
             "But test is expected to fail, set the boolean value from true to false.");
        $found_no_extra_containers = 0;
    }
    ok($found_no_extra_containers == 1,
       "Bug 4624829: AM should not release extra containers");

    ok($found_no_reducer_while_mapper_running == 1,
       "Bug 5564075: Reducer tasks should not be started until ".
       "all mapper tasks are completed when mapreduce.job.reduce.".
       "slowstart.completedmaps is set to 1.0");

    $job_index = 0;
    foreach $job_id (@$job_ids) {
        $job_index++;
        is($self->wait_for_job_to_succeed($job_id, 5, 72, $verbose),
           0,
           "Job $job_index '$job_id' should completed successfully.");
    }
    return;
}

sub test_high_ram_jobs {
    my ($self, $args, $verbose) = @_;
    $verbose     ||= 0;
    my $default_queues = ['default', 'grideng', 'gridops', 'search'];
    my $mem_settings = defined($args->{mem_settings}) ? $args->{mem_settings} : '';
    my $queues       = defined($args->{queues})       ? $args->{queues}       : $default_queues;
    my $task_sleep   = defined($args->{task_sleep})   ? $args->{task_sleep}   : 20000;
    my $num_tasks    = defined($args->{num_tasks})    ? $args->{num_tasks}    : 10;

    note("1. Submit job(s) with high RAM memory allocations of: ",
         explain($mem_settings));
    note("2. Verify the high RAM job ran properly and sucessfully");

    my $properties = [ "-Dmapreduce.job.reduce.slowstart.completedmaps=1.0",
                       '-Dmapreduce.job.acl-view-job=*' ];
    my $index = 0;
    my $jobs = [];
    foreach my $mem_setting (@$mem_settings) {
        my $job = { 'mapper_mem'   => $mem_setting->{map},
                    'reducer_mem'  => $mem_setting->{reduce},
                    'am_mem'       => $mem_setting->{am},
                    'queue'        => $queues->[$index],
                    'map_task'     => $num_tasks,
                    'reduce_task'  => $num_tasks,
                    'map_sleep'    => $task_sleep,
                    'reduce_sleep' => $task_sleep,
                    'args'         => $properties };    
        push(@{$jobs}, $job);
        $index++;
    }

    my $job_ids = $self->submit_batch_jobs($jobs);
    $self->check_jobs_status($job_ids, $mem_settings);
    return;
}


1;
