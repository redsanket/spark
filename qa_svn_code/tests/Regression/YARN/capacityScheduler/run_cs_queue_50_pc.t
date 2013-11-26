#!/usr/local/bin/perl

#################################################################################
# Test single queue user limit of 50%.
# Runtime: About 5 minutes
#################################################################################

use strict;
use warnings;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop23::CapacityScheduler;
use Test::More tests => 8;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;
$self->{cs} = Hadoop23::CapacityScheduler->new($self);

my $conf_dir="$Bin/../../../../data/conf/";
$self->restart_resourcemanager_with_cs_conf("$conf_dir/capacity-scheduler50.xml");
my $capacity = $self->get_capacity();

my @queues=('default', 'grideng', 'gridops', 'search');
my $queue = $queues[0];

$self->test_cs_single_queue_50_percent_1($queue, $capacity);

# CLEANUP
$self->cleanup_resourcemanager_conf;


#################################################################################
# Test Functions
#################################################################################

# This test is ported from run_capacityScheduler.sh from the two functions:
# 1) run_capacityScheduler.sh: test_userLimit41_4normalJobsQueueUp and
# 2) run_capacityScheduler.sh: test_userLimit41_4normalJobsRunningSequencially.
# These two tests are essentially the same.  The only difference is that the
# sequential test submits a job only after the previou one has started running.
sub test_cs_single_queue_50_percent_1 {
    my ($self, $queue, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 20000;
    # my $task_sleep = 2000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity.";
    note "2. User1 submits job J1 which has 2 times queue capacity map and reduce tasks.";
    note "3. User2 submits job J2 which has 2 times queue capacity map and reduce tasks.";
    note "4. User3 submits job J3 which has 2 times queue capacity map and reduce tasks.";
    note "5. Verify J1 and J2 jobs take up about 100% of the cluster capacity, while J3 waits in pending state. ***";
    note "6. Verify when J1 and J2 map tasks completed, the slots are given to J3 first and then J4. ***";
    note "7. Verify all the jobs completed sucessfully";

    $jobs = $self->expand_jobs({'queue'      => $queue,
                                'capacity'   => $capacity,
                                'jobs'       => [ { 'user' => 'hadoop1', 'factor' => 2 },
                                                  { 'user' => 'hadoop2', 'factor' => 2 },
                                                  { 'user' => 'hadoop3', 'factor' => 1 } ],
                                'task_sleep' => $task_sleep});
    my $sleep_bt_jobs = 10;
    $job_ids = $self->submit_batch_jobs($jobs, $sleep_bt_jobs);
    $self->{cs}->test_capacity_scheduler({'jobs'            => $jobs,
                                          'job_ids'         => $job_ids,
                                          'capacity'        => $capacity,
                                          'wait_time'       => $wait_time,
                                          'min_concur_jobs' => 2});
    return;
}
