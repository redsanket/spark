#!/usr/local/bin/perl

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Test::More tests => 9;
use Time::HiRes qw(usleep);

use Hadoop::JobTracker;

my $self = __PACKAGE__->new;
my $Config = $self->config;

use Data::Dumper;
print(Dumper($Config));

$ENV{OWNER} = 'jeagles';

note("Hadoop version = '$Config->{VERSION}'");
note("JT = '$Config->{NODES}->{JOBTRACKER}->{HOST}'");
note("NN = '$Config->{NODES}->{NAMENODE}->{HOST}'");
note("TT = @{$Config->{NODES}->{TASKTRACKER}->{HOST}}");
note("DN = @{$Config->{NODES}->{DATANODE}->{HOST}}");
note("DN number = ", scalar @{$Config->{NODES}->{DATANODE}->{HOST}});

my $jobtracker = Hadoop::JobTracker->new($self);
my $jobtracker_startup_time = 30;

note("Creating jobtracker working config dir");
$self->backup_jobtracker_conf();
$self->backup_tasktracker_conf();
$self->set_jobtracker_xml_prop_value($Hadoop::JobTracker::HEARTBEATS_SCALING_FACTOR_PROPERTY, 10);

sub heartbeat_test {
	my ($num_heartbeats_in_second) = @_;

	# set the new property on the jobtracker
	my $heartbeat_interval_settling_time = 2 * $jobtracker->expected_heartbeat_interval();
	my $result = $self->set_jobtracker_xml_prop_value($Hadoop::JobTracker::NUM_HEARTBEATS_IN_SECOND_PROPERTY, $num_heartbeats_in_second);

	# restart the host so the new property values are used
	$self->control_daemon('stop', 'jobtracker');
	$self->control_daemon('stop', 'tasktracker');
	$self->control_daemon('start', 'jobtracker');
	sleep($jobtracker_startup_time);
	$self->control_daemon('start', 'tasktracker');

	# need to wait until the next heartbeat is sent to make sure
	# the task trackers are following the new heartbeat interval rules
	note("Waiting $heartbeat_interval_settling_time seconds for new heartbeat interval to take effect");
	sleep($heartbeat_interval_settling_time);

	# measure the actual heartbeat interval
	note("Starting heartbeat measurement");
	note("Expected heartbeat interval: ", $jobtracker->expected_heartbeat_interval());
	my $heartbeat = $jobtracker->measure_heartbeat_interval();
	note("Measured heartbeat interval: $heartbeat->{measured}");
	for my $host (sort keys %{$heartbeat->{data}}) {
		my $heartbeats = join(', ', @{$heartbeat->{data}{$host}});
		note("$host: $heartbeats");
	}

	my $ok = abs($heartbeat->{measured} - $heartbeat->{expected}) <= 1;
	ok($ok, "Heartbeat measurement for $Hadoop::JobTracker::NUM_HEARTBEATS_IN_SECOND_PROPERTY = $num_heartbeats_in_second: Expecting $heartbeat->{expected} seconds, Measured $heartbeat->{measured} seconds");
}

my $old_value = $self->get_jobtracker_xml_prop_value($Hadoop::JobTracker::NUM_HEARTBEATS_IN_SECOND_PROPERTY);

#Source of Test Plan
# http://twiki.corp.yahoo.com/pub/Grid/MapreduceTestPlan_0_20_100/HADOOP_5784.html

#Testcase_1: Not Automated
# Run all the benchmark jobs
# There should be no performance degradation

#Testcase_2: Not Automated
# Run gridmix in a loop several times
# Jobs should complete successfully

#Testcase_3: Not Automated
# Run TestMapredHeartbeat
# The unit test must pass

#Testcase_4
# Set mapred.heartbeats.in.second to negative value (set to -100)
# The heartbeat cycle is 3 secs

heartbeat_test(-100);

#Testcase_5
# Set mapred.heartbeats.in.second to negative value (set to 0)
# The heartbeat cycle is 3 secs

heartbeat_test(0);

#Testcase_6
# Set mapred.heartbeats.in.second to negative value (set to abc)
# The heartbeat cycle is 3 secs

heartbeat_test("abc");

#Testcase_7
# Set mapred.heartbeats.in.second to 1
# The heartbeat cycle is 10 secs

heartbeat_test(1);

#Testcase_8
# Set mapred.heartbeats.in.second to 2
# The heartbeat cycle is 5 secs

heartbeat_test(2);

#Testcase_9
# Set mapred.heartbeats.in.second to >=3 <=9 (decimal values included)
# The heartbeat cycle fluctuates between 3-4 secs

heartbeat_test(6);

#Testcase_10
# Set mapred.heartbeats.in.second to 10
# The heartbeat cycle is set to 3

heartbeat_test(10);

#Testcase_11
# Set mapred.heartbeats.in.second to > 10
# The heartbeat cycle is set to 3

heartbeat_test(200);

#Testcase_12
# Set mapred.heartbeats.in.second to negative value=1(18TTs were used, 8 were decommissioned)
# The heartbeat cycle is initially set to 18 .After decommissioning 8 TTs, heartbeat cycle reduces to 10

heartbeat_test(1);

$self->cleanup_jobtracker_conf();
$self->cleanup_tasktracker_conf();
