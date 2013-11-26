#!/usr/local/bin/perl

#################################################################################
# Test single queue user limit of 100% (bug 4588509)
# Runtime: About 5 minutes
#################################################################################

use strict;
use warnings;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop23::CapacityScheduler;
use Test::More tests => 15;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;
$self->{cs} = Hadoop23::CapacityScheduler->new($self);

my $conf_dir="$Bin/../../../../data/conf/";
$self->restart_resourcemanager_with_cs_conf("$conf_dir/capacity-scheduler100.xml");
my $capacity = $self->get_capacity();

my @queues=('default', 'grideng', 'gridops', 'search');
my $queue = $queues[0];

$self->test_cs_single_queue_100_percent_1($queue, $capacity);
$self->test_cs_single_queue_100_percent_2($queue, $capacity);
$self->test_cs_single_queue_100_percent_3($queue, $capacity);

# CLEANUP
$self->cleanup_resourcemanager_conf;


#################################################################################
# Test Functions
#################################################################################

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit100_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes (QueueCapacity)
# Function to check user limit (100%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_100_percent_1 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $user = 'hadoopqa';
    my $task_sleep = 2000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. Submit a normal job which have number of map and reduce tasks equal 2 times queue capacity";
    note "3. Verify the number of tasks do not exceed queue capacity limit * user limit factor for a single user";
    note "4. Verify the normal job can run sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'user'       => $user,
                                'num_jobs'   => 1,
                                'factor'     => 3,
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'      => $jobs,
                                          'job_ids'   => $job_ids,
                                          'capacity'  => $capacity,
                                          'wait_time' => $wait_time});
    return;
}

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit100_2normalJobsWithMapReduceTasksEqualNumNodes (QueueCapacity)
# Function to check user limit (100%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_100_percent_2 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $user = 'hadoopqa';
    my $task_sleep = 2000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. Submit a first normal job which have number of map and reduce tasks equal half queue capacity";
    note "3. Submit a second normal job which have number of map and reduce tasks equal half queue capacity";
    note "4. Verify the number of tasks do not exceed queue capacity limit * user limit factor for a single user";
    note "5. Verify the 2 normal jobs can run in parallel with number of tasks not exceeded queue capacity ***";
    note "6. Verify the 2 jobs ran sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'user'       => $user,
                                'num_jobs'   => 2,
                                'factor'     => 0.5,
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'      => $jobs,
                                          'job_ids'   => $job_ids,
                                          'capacity'  => $capacity,
                                          'wait_time' => $wait_time,
                                          'min_concur_jobs' => 2});
    return;
}

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit100_3normalJobsQueueUp (QueueCapacity)
# Function to check user limit (100%) for normal job with map and reduce tasks
# equal multiple times number of nodes
sub test_cs_single_queue_100_percent_3 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $user = 'hadoopqa';
    my $task_sleep = 2000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. Submit a first normal job which have 3 times queue capacity map tasks and queue capacity reduce tasks";
    note "3. Submit a second normal job which have 2 times queue capacity map tasks and queue capacity reduce tasks";
    note "4. Submit a second normal job which have queue capacity map tasks and half queue capacity reduce tasks";
    note "5. Verify the number of tasks do not exceed queue capacity limit * user limit factor for a single user";
    note "6. Verify the first normal job takes up all queue capacity to run and the other 2 queue up ***";
    note "7. Verify the second job runs when first job map tasks completed ***";
    note "8. Verify the third job runs when second job tasks completed ***";
    note "9. Verify all the jobs completed sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'user'       => $user,
                                'jobs'       => [ { 'factor' => 3 },
                                                  { 'factor' => 2 },
                                                  { 'factor' => 1 }, ],
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'      => $jobs,
                                          'job_ids'   => $job_ids,
                                          'capacity'  => $capacity,
                                          'wait_time' => $wait_time});
    return;
}

