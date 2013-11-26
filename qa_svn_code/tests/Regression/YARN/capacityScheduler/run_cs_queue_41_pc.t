#!/usr/local/bin/perl

#################################################################################
# Test single queue user limit of 41%.
# Runtime: About 4 minutes
#################################################################################

use strict;
use warnings;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop23::CapacityScheduler;
use Test::More tests => 12;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;
$self->{cs} = Hadoop23::CapacityScheduler->new($self);

my $conf_dir="$Bin/../../../../data/conf/";
$self->restart_resourcemanager_with_cs_conf("$conf_dir/capacity-scheduler41.xml");
my $capacity = $self->get_capacity();

my @queues=('default', 'grideng', 'gridops', 'search');
my $queue = $queues[0];

$self->test_cs_single_queue_41_percent_1($queue, $capacity);
$self->test_cs_single_queue_41_percent_2($queue, $capacity);

# CLEANUP
$self->cleanup_resourcemanager_conf;


#################################################################################
# Test Functions
#################################################################################

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit25_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes (QueueCapacity)
# Function to check user limit (25%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_41_percent_1 {
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
                                'factor'     => 4,
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'      => $jobs,
                                          'job_ids'   => $job_ids,
                                          'capacity'  => $capacity,
                                          'wait_time' => $wait_time});
    return;
}

# This test is ported from run_capacityScheduler.sh from the two functions:
# 1) run_capacityScheduler.sh: test_userLimit41_4normalJobsQueueUp and
# 2) run_capacityScheduler.sh: test_userLimit41_4normalJobsRunningSequencially.
# These two tests are essentially the same.  The only difference is that the
# sequential test submits a job only after the previou one has started running.
sub test_cs_single_queue_41_percent_2 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 2000;
    my $wait_time = 5;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity.";
    note "2. User1 submits job J1 which has 6 times queue capacity map and reduce tasks.";
    note "3. User2 submits job J2 which has 6 times queue capacity map and reduce tasks.";
    note "4. User3 submits job J3 which has 3 times queue capacity map and reduce tasks.";
    note "5. User4 submits job J4 which has 4 times queue capacity map and reduce tasks.";
    note "6. Verify J1 and J2 jobs take up about 82% queue capacity to run, J3 takes about 18% queue capacity, and J4 remains in queue. ***";
    note "7. Verify when J1 and J2 map tasks completed, the slots are given to J3 first and then J4. ***";
    note "8. Verify all the jobs completed sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'jobs'       => [ { 'factor' => 6 },
                                                  { 'factor' => 6 },
                                                  { 'factor' => 3 },
                                                  { 'factor' => 4 }, ],
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'            => $jobs,
                                          'job_ids'         => $job_ids,
                                          'capacity'        => $capacity,
                                          'wait_time'       => $wait_time,
                                          'min_concur_jobs' => 3});
    return;
}
