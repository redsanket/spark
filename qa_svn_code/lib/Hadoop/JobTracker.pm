package Hadoop::JobTracker;

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";

use HTML::Parser ();
use List::Util qw(max);
use LWP::Simple;
use POSIX;
use Scalar::Util qw(looks_like_number);
use Time::HiRes qw(usleep);
use Test::More;

# global constants
our $NUM_HEARTBEATS_IN_SECOND_PROPERTY_20 = "mapred.heartbeats.in.second";
our $NUM_HEARTBEATS_IN_SECOND_PROPERTY_22 = "mapreduce.jobtracker.heartbeats.in.second";
our $NUM_HEARTBEATS_IN_SECOND_PROPERTY = $NUM_HEARTBEATS_IN_SECOND_PROPERTY_22;
our $HEARTBEATS_SCALING_FACTOR_PROPERTY = "mapreduce.jobtracker.heartbeats.scaling.factor";
our $MIN_NUM_HEARTBEATS_IN_SECOND = 1;
our $DEFAULT_NUM_HEARTBEATS_IN_SECOND = 100;
our $MIN_HEARTBEATS_SCALING_FACTOR = 0.01;
our $DEFAULT_HEARTBEATS_SCALING_FACTOR = 1.0;
our $HEARTBEAT_INTERVAL_MIN = 3;

my $machine_list_table_header = 'Task Trackers';
my @machine_list_table_fields_20 = ('Name', 'Host', '# running tasks', 'Max Map Tasks', 'Max Reduce Tasks', 'Failures', 'Node Health Status', 'Seconds Since Node Last Healthy', 'Total Tasks Since Start', 'Succeeded Tasks Since Start', 'Total Tasks Last Day', 'Succeeded Tasks Last Day', 'Total Tasks Last Hour', 'Succeeded Tasks Last Hour', 'Seconds since heartbeat');
my @machine_list_table_fields_22 = ('Name', 'Host', '# running tasks', 'Max Map Tasks', 'Max Reduce Tasks', 'Failures', 'Node Health Status', 'Seconds Since Node Last Healthy', 'Total Tasks Since Start', 'Succeeded Tasks Since Start', 'Failed Health Checks Since Start', 'Total Tasks Last Day', 'Succeeded Tasks Last Day', 'Failed Health Checks Last Day', 'Total Tasks Last Hour', 'Succeeded Tasks Last Hour', 'Failed Health Checks Last Hour', 'Seconds since heartbeat');
my @machine_list_table_fields = @machine_list_table_fields_22;
my $seconds_since_heartbeat_property = "Seconds since heartbeat";

sub new {
    my ($class, $test) = @_;
    my $self = {test => $test};
    bless ($self,$class);
	if (($test->config->{VERSION} eq '0.20') || ($test->config->{VERSION} =~ '^1\.')) {
		$NUM_HEARTBEATS_IN_SECOND_PROPERTY = $NUM_HEARTBEATS_IN_SECOND_PROPERTY_20;
		@machine_list_table_fields = @machine_list_table_fields_20;
	}
	return $self;
};

sub hadoop_machine_list_uri {
    my ($self) = @_;
    my $host = $self->{test}->config->{NODES}->{JOBTRACKER}->{HOST};
    my $port = $self->{test}->config->{NODES}->{JOBTRACKER}->{PORT};
    return "http://$host:$port/machines.jsp?type=active";
}

my @_columns = ();
my $_machine_list_table = {};
my $_num_rows = 0;

sub _text {
	my $p = shift;
	my $text = shift;
	chomp $text;
	push (@_columns, $text);
}

sub _start {
	my $p = shift;
	my $tagname = shift;

	if('table' eq $tagname) {
		$p->report_tags(qw(table th td tr));
		$p->handler('text' => \&_text, 'self, text' );
	}
}

sub _end {
	my $p = shift;
	my $tagname = shift;

	if ('tr' eq $tagname) {
		if (scalar @_columns == scalar @machine_list_table_fields) {
			if ($_num_rows > 0) {
				@{$_machine_list_table->{$_columns[1]}}{@machine_list_table_fields} = @_columns;
			}

			$_num_rows++;
		}

		@_columns = ();
	}
}

sub get_jobtracker_machine_list {
	my ($self) = @_;

	@_columns = ();
	$_machine_list_table = {};
	$_num_rows = 0;

	my $html = get($self->hadoop_machine_list_uri());
	$html =~ s/\n//g;

	my $p = HTML::Parser->new(
		api_version => 3,
		unbroken_text => 1,
		start_h => [\&_start, 'self, tagname, attr'],
		end_h => [\&_end,   'self, tagname, attr']);
	$p->report_tags(qw(table));
	$p->handler('text' => '');
	$p->parse($html);
	$p->eof;

	return $_machine_list_table;
}

sub expected_heartbeat_interval {
	my ($self) = @_;

	# keys are a list of all machines in the cluster
	my $expected_cluster_size = @{$self->{test}->config->get_tasktracker()};
	my $cluster_size = keys %{$self->get_jobtracker_machine_list()};
	note("Expected cluster size: $expected_cluster_size");
	note("Measure cluster size: $cluster_size");
	my $num_heartbeats_in_second = $self->{test}->get_jobtracker_xml_prop_value($NUM_HEARTBEATS_IN_SECOND_PROPERTY);
	my $heartbeats_scaling_factor = $self->{test}->get_jobtracker_xml_prop_value($HEARTBEATS_SCALING_FACTOR_PROPERTY);

	if (!looks_like_number($num_heartbeats_in_second) ||
		($num_heartbeats_in_second < $MIN_NUM_HEARTBEATS_IN_SECOND)) {
		note("Using default num heartbeats in second");
		$num_heartbeats_in_second = $DEFAULT_NUM_HEARTBEATS_IN_SECOND;
	}

	if (!looks_like_number($heartbeats_scaling_factor) ||
		($heartbeats_scaling_factor < $MIN_HEARTBEATS_SCALING_FACTOR)) {
		note("Using default heartbeats scaling factor");
		$heartbeats_scaling_factor = $DEFAULT_HEARTBEATS_SCALING_FACTOR;
	}

	# perl port of the heartbeat interval algorithm from JobTracker.java
	my $heartbeat_interval = max(
		int($heartbeats_scaling_factor * ceil($cluster_size / $num_heartbeats_in_second)),
		$HEARTBEAT_INTERVAL_MIN);

	return $heartbeat_interval;
}

sub measure_heartbeat_interval {
	my ($self) = @_;

	# sample data from the 
	# wait twice the expected heartbeat interval to ensure current
	# interval value is being used by the task trackers
	my $expected_heartbeat_interval = $self->expected_heartbeat_interval();
	my $endtime = time + 2 * $expected_heartbeat_interval;
	my $heartbeat_data = {};
	while (time < $endtime) {
		my $machine_list = $self->get_jobtracker_machine_list();
		usleep(500_000);
		for my $machine (keys %$machine_list) {
			push(@{$heartbeat_data->{$machine}}, $machine_list->{$machine}{$seconds_since_heartbeat_property})
		}
	}

	my $measured_heartbeat_interval = 0;
	my $measured_heartbeat_machine = '';
	while (my ($machine, $data)  = each %$heartbeat_data) {

		# find the heartbeat
		# use data from a host that can be trusted to have the correct heartbeat interval
		# - data must contain a zero: a full cycle has finished
		my @rdata = reverse @$data;

		for my $i (0 .. $#rdata - 1) {
			if (($rdata[$i] == 0) && ($rdata[$i + 1] > 0)) {
				$measured_heartbeat_interval = $rdata[$i + 1] + 1;
				$measured_heartbeat_machine = $machine;
				note("Measured heartbeat interval $machine: $measured_heartbeat_interval");
				last;
			}
		}

		note("Using measured heartbeat interval $machine: $measured_heartbeat_interval");
	}

	return {measured => $measured_heartbeat_interval, expected => $expected_heartbeat_interval, data => $heartbeat_data};
}

1;
