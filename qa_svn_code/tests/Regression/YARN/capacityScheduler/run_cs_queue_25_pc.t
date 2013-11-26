#!/usr/local/bin/perl

#################################################################################
# Test single queue user limit of 25%
# Runtime: About 6 minutes
#################################################################################

use strict;
use warnings;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop23::CapacityScheduler;
use Test::More tests => 62;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;
$self->{cs} = Hadoop23::CapacityScheduler->new($self);

my $conf_dir="$Bin/../../../../data/conf/";
$self->restart_resourcemanager_with_cs_conf("$conf_dir/capacity-scheduler25.xml");
my $capacity = $self->get_capacity();

my @queues=('default', 'grideng', 'gridops', 'search');
my $queue = $queues[0];

$self->test_cs_single_queue_25_percent_1($queue, $capacity);
$self->test_cs_single_queue_25_percent_2($queue, $capacity);
$self->test_cs_single_queue_25_percent_3($queue, $capacity);
$self->test_cs_single_queue_25_percent_4($queue, $capacity);
$self->test_cs_single_queue_25_percent_5($queue, $capacity);
$self->test_cs_single_queue_25_percent_6($queue, $capacity);

# CLEANUP
$self->cleanup_resourcemanager_conf;


#################################################################################
# Test Functions
#################################################################################

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit25_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes (QueueCapacity)
# Function to check user limit (25%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_25_percent_1 {
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
                                'factor'     => 2,
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'      => $jobs,
                                          'job_ids'   => $job_ids,
                                          'capacity'  => $capacity,
                                          'wait_time' => $wait_time});
    return;
}

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit25_2normalJobsWithMapReduceTasksEqualNumNodes (QueueCapacity)
# Function to check user limit (25%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_25_percent_2 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 2000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity, queue capacity, and user limit";
    note "2. Submit a first normal job as hadoop3 user which have number of map and reduce tasks equal half queue capacity";
    note "3. Submit a second normal job as hadoopqa user which have number of map and reduce tasks equal half queue capacity";
    note "4. Verify the 2 normal jobs can run in parallel each takes up 50% of queue capacity";
    note "5. Verify the 2 jobs ran sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'num_jobs'   => 2,
                                'factor'     => 0.5,
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'            => $jobs,
                                          'job_ids'         => $job_ids,
                                          'capacity'        => $capacity,
                                          'wait_time'       => $wait_time,
                                          'min_concur_jobs' => 2});
    return;
}

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit25_3normalJobsQueueUp (QueueCapacity)
# Function to check user limit (25%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_25_percent_3 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 2000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. Submit a first normal job which have 8 times queue capacity map tasks and queue capacity reduce tasks";
    note "3. After (2 times number of Queue capacity) map tasks of first job completed, submit a second normal job which have 6 times queue capacity map tasks and queue capacity reduce tasks ***";
    note "4. Verify the first and second normal jobs take up equal queue capacity to run ***";
    note "5. After all map tasks of first job completed, submit a third normal job which have 4 times queue capacity map tasks and half queue capacity reduce tasks ***";
    note "6. Verify all the jobs runs with equal share of queue capacity ***";
    note "7. Verify all the jobs completed sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'jobs'       => [ { 'factor' => 8 },
                                                  { 'factor' => 6 },
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

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit25_4normalJobsQueueUp (QueueCapacity)
# Function to check user limit (25%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_25_percent_4 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 2000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. Submit first normal job which has 4 times queue capacity for map and reduce tasks";
    note "3. Submit second normal job which has 3 times queue capacity for map and reduce tasks";
    note "4. Submit third normal job which has 2 times queue capacity for map and reduce tasks";
    note "5. Submit fourth normal job which has equal queue capacity for map and reduce tasks";
    note "6. Verify all the jobs run, each take 25% of queue capacity ***";
    note "7. Verify all the jobs completed sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'jobs'       => [ { 'factor' => 4 },
                                                  { 'factor' => 3 },
                                                  { 'factor' => 2 },
                                                  { 'factor' => 1 }, ],
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'            => $jobs,
                                          'job_ids'         => $job_ids,
                                          'capacity'        => $capacity,
                                          'wait_time'       => $wait_time,
                                          'min_concur_jobs' => 4});
    return;
}

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit25_6normalJobsQueueUp (QueueCapacity)
# Function to check user limit (25%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_25_percent_5 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 2000;
    # $task_sleep = 30000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    #note "2. Submit a first normal job which have 3 times queue capacity map tasks and queue capacity reduce tasks";
    #note "3. Submit a second normal job which have 2 times queue capacity map tasks and queue capacity reduce tasks";
    #note "4. Submit a second normal job which have queue capacity map tasks and half queue capacity reduce tasks";
    #note "5. Verify the first normal job takes up all queue capacity to run and the other 2 queue up";
    #note "6. Verify the second job runs when first job map tasks completed ***";
    #note "7. Verify the third job runs when second job tasks completed ***";
    note "8. Verify all the jobs completed sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'jobs'       => [ { 'factor' => 4 },
                                                  { 'factor' => 3 },
                                                  { 'factor' => 2 },
                                                  { 'factor' => 1 },
                                                  { 'factor' => 1 },
                                                  { 'factor' => 1 }, ],
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'            => $jobs,
                                          'job_ids'         => $job_ids,
                                          'capacity'        => $capacity,
                                          'wait_time'       => $wait_time,
                                          'min_concur_jobs' => 4});
    return;
}

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit25_7normalJobsAddingAndQueueUp 
# Function to check user limit (25%) for normal job with map and reduce tasks
# equal multiple times number of nodes.
sub test_cs_single_queue_25_percent_6 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 2000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. User1 submits a J1 job which have 6 times queue capacity map tasks and queue capacity reduce tasks";
    note "3. User2 submits a J2 job which have 5 times queue capacity map tasks and queue capacity reduce tasks";
    note "4. User3 submits a J3 job which have 4 times queue capacity map tasks and half queue capacity reduce tasks";
    note "5. User4 submits a J4 job which have 3 times queue capacity map tasks and queue capacity reduce tasks";
    note "6. Verify all 4 jobs running with equal number of tasks ***";
    note "7. User2 submits a J5 job which have 2 times queue capacity map tasks and queue capacity reduce tasks";
    note "8. User5 submits a J6 job which have 2 times queue capacity map tasks and half queue capacity reduce tasks";
    note "9. User6 submits a J7 job which have 2 times queue capacity map tasks and half queue capacity reduce tasks";
    note "10. Verify the last 3 jobs queued up ***";
    note "11. Verify when all maps of J4 finishes then maps of J6 starts. And when maps of J3 finishes J7 maps starts. J5 starts after last set of maps of J2 finishes. ***";
    note "12. Verify when maps J5 and J1 remaining goes first to j7 then J5 as J7 started first. And J7 finishes all map slots are given to J5. Similarly when reducers of J4 finish, reducers of J6 starts. When reducer of J3 finishes (J3 finishes), reducers of J7 starts. ***";
    note "13. Verify when reducers of j6 finish (J6 finish). Now there are 3 users User1 (J1), User2 (J2, J5), User6 (J7). Limit re-distribution occurs and as J2 is running J5 will not considered until last set reducers from J2 starts finishing. So J1=J2=J7=capacity/3. And as last set reducers starts finishing reducers from J5 starts keeping limit = (33%). When J7 finishes user limit eventually again re-distributed to 50%. J1=J5=50%.When J1 finishes J5 get 100% slots ***";
    note "14. Verify all the jobs completed sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'jobs'       => [ { 'user' => 'hadoop1', 'factor' => 5 },
                                                  { 'user' => 'hadoop2', 'factor' => 4 },
                                                  { 'user' => 'hadoop3', 'factor' => 3 },
                                                  { 'user' => 'hadoop4', 'factor' => 2 },
                                                  { 'user' => 'hadoop2', 'factor' => 2 },
                                                  { 'user' => 'hadoop5', 'factor' => 2 },
                                                  { 'user' => 'hadoop6', 'factor' => 2 }, ],
                                'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);

    # We can't guarantee that J6 and J7 starts before J5.
    # 'start_order'  => [0, 1, 2, 3, 5, 6, 4],

    $self->{cs}->test_capacity_scheduler({'jobs'            => $jobs,
                                          'job_ids'         => $job_ids,
                                          'capacity'        => $capacity,
                                          'wait_time'       => $wait_time,
                                          'min_concur_jobs' => 4});
    return;
}



# function test_userLimit100_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes {
# function test_userLimit100_2normalJobsWithMapReduceTasksEqualNumNodes {
# function test_userLimit100_3normalJobsQueueUp {
# function test_userLimit25_normalJobWithMapReduceTasksEqualMultipleTimesNumNodes {
# function test_userLimit25_3normalJobsQueueUp {
#    <property>
#       <name>yarn.app.mapreduce.am.resource.mb</name>
#       <value>2048</value>
#    </property>

