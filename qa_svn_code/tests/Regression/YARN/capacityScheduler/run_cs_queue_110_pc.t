#!/usr/local/bin/perl

#################################################################################
# Test single queue user limit of 110%
# Runtime: About 2 minutes
#################################################################################

use strict;
use warnings;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop23::CapacityScheduler;
use Test::More tests => 6;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;
$self->{cs} = Hadoop23::CapacityScheduler->new($self);

my $conf_dir="$Bin/../../../../data/conf/";
$self->restart_resourcemanager_with_cs_conf("$conf_dir/capacity-scheduler110.xml");
my $capacity = $self->get_capacity();

my @queues=('default', 'grideng', 'gridops', 'search');
my $queue = $queues[0];

$self->test_cs_single_queue_110_percent_1($queue, $capacity);

# CLEANUP
$self->cleanup_resourcemanager_conf;


#################################################################################
# Test Functions
#################################################################################

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit_GreaterThan100
sub test_cs_single_queue_110_percent_1 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $user = 'hadoopqa';
    my $task_sleep = 2000;
    my $wait_time = 0;

    note  "1. Set user Limit for queue default equals to 110%";
    note  "2. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note  "3. Submit a first normal job which have number of map and reduce tasks equal 6 times queue capacity";
    note  "4. Submit a second normal job which have number of map and reduce tasks equal 4 times queue capacity";
    note  "5. Verify map tasks from second job starts when last set of maps from first job starts finishing ***";
    note  "6. Verify reduce tasks from second job starts when last set of reduces from first job starts finishing ***";
    note  "7. Verify the 2 jobs ran sucessfully";

    # $task_sleep = 30000;
    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'user'       => $user,
                                'jobs'       => [ { 'factor' => 7 },
                                                  { 'factor' => 4 }, ],
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'            => $jobs,
                                          'job_ids'         => $job_ids,
                                          'capacity'        => $capacity,
                                          'wait_time'       => $wait_time});
    return;
}



