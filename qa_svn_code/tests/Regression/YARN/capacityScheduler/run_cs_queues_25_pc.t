#!/usr/local/bin/perl

#################################################################################
# Use customer capacity scheduler where user limit is 25%
# Runtime: About 5 minutes
#################################################################################

use strict;
use warnings;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop23::CapacityScheduler;
use Test::More tests => 41;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;
$self->{cs} = Hadoop23::CapacityScheduler->new($self);

my $conf_dir="$Bin/../../../../data/conf/";
$self->restart_resourcemanager_with_cs_conf("$conf_dir/capacity-scheduler25.xml");
my $capacity = $self->get_capacity();

my $queues=['default', 'grideng', 'gridops', 'search'];

$self->test_cs_multiple_queues_25_percent($queues, $capacity);

# CLEANUP
$self->cleanup_resourcemanager_conf;


#################################################################################
# Test Functions
#################################################################################

# This test is ported from run_capacityScheduler.sh from the function:
# test_guaranteeCapacity_differentUsersOnDifferentQueues_userLimit25
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by
# different users (User limit=25%)
sub test_cs_multiple_queues_25_percent {
    my ($self, $queues, $capacity) = @_;
    my ($jobs, $job_ids);
    my $user = 'hadoopqa';
    # my $task_sleep = 2000;
    my $task_sleep = 30000;
    my $wait_time = 0;

    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. User1 submits a J1 job to default queue which has map and reduce tasks=0.5 queue capacity";
    note "3. User2 submits a J2 job to default queue which has map and reduce tasks=0.4 queue capacity";
    note "4. User3 submits a J3 job to default queue which has map and reduce tasks=0.3 queue capacity";
    note "5. User4 submits a J4 job to default queue which has map and reduce tasks=0.2 queue capacity";
    note "6. User5 submits a J5 job to default queue which has map and reduce tasks=0.1 queue capacity";
    note "7. User6 submits a J6 job to grideng queue which has map and reduce tasks=0.7 queue capacity";
    note "8. User7 submits a J7 job to grideng queue which has map and reduce tasks=0.4 queue capacity";
    note "9. User8 submits a J8 job to grideng queue which has map and reduce tasks=0.4 queue capacity";
    note "10. Again User6 submits a J9 job to grideng queue which has map and reduce tasks=0.3 queue capacity";
    note "11. User9 submits a J10 job to gridops queue which has map and reduce tasks=1.0 queue capacity";
    note "12. User10 submits a J11 job to gridops queue which has map and reduce tasks=0.9 queue capacity";
    note "13. User11 submits a J12 job to search queue which has map and reduce tasks=1.0 queue capacity";
    note "14. User12 submits a J13 job to search queue which has map and reduce tasks=1.0 queue capacity";
    note "15. Again User1 submits a J14 job to default queue which has map and reduce tasks=0.6 cluster capacity";
    note "16. User13 submits a J15 job to default queue which has map and reduce tasks=0.3 queue capacity";
    note "17. User14 submits a J16 job to grideng queue which has map and reduce tasks=0.4 queue capacity";
    note "18. User15 submits a J17 job to grideng queue which has map and reduce tasks=0.05 queue capacity";
    note "19. User16 submits a J18 job to gridops queue which has map and reduce tasks=0.4 queue capacity";
    note "20. Verify all the jobs completed sucessfully";

    $jobs =
        $self->expand_jobs({'capacity'   => $capacity,
                            'jobs'       => [ { 'user' => 'hadoop1',  'queue' => $queues->[0], 'factor' => 0.5 },
                                              { 'user' => 'hadoop2',  'queue' => $queues->[0], 'factor' => 0.4 },
                                              { 'user' => 'hadoop3',  'queue' => $queues->[0], 'factor' => 0.3 },
                                              { 'user' => 'hadoop4',  'queue' => $queues->[0], 'factor' => 0.2 },
                                              { 'user' => 'hadoop5',  'queue' => $queues->[0], 'factor' => 0.1 },
                                              { 'user' => 'hadoop6',  'queue' => $queues->[1], 'factor' => 0.7 },
                                              { 'user' => 'hadoop7',  'queue' => $queues->[1], 'factor' => 0.4 },
                                              { 'user' => 'hadoop8',  'queue' => $queues->[1], 'factor' => 0.4 },
                                              { 'user' => 'hadoop6',  'queue' => $queues->[1], 'factor' => 0.3 },
                                              { 'user' => 'hadoop9',  'queue' => $queues->[2], 'factor' => 1   },
                                              { 'user' => 'hadoop10', 'queue' => $queues->[2], 'factor' => 0.9 },
                                              { 'user' => 'hadoop11', 'queue' => $queues->[3], 'factor' => 1   },
                                              { 'user' => 'hadoop12', 'queue' => $queues->[3], 'factor' => 1   },
                                              { 'user' => 'hadoop1',  'queue' => $queues->[0], 'factor' => 0.6 },
                                              { 'user' => 'hadoop13', 'queue' => $queues->[0], 'factor' => 0.3 },
                                              { 'user' => 'hadoop14', 'queue' => $queues->[1], 'factor' => 0.4 },
                                              { 'user' => 'hadoop15', 'queue' => $queues->[1], 'factor' => 0.5 },
                                              { 'user' => 'hadoopqa', 'queue' => $queues->[2], 'factor' => 0.4 }
                                              ],
                            'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    # 'start_order'  => [0, 1, 2, 3, 5, 4, 6],
    $self->{cs}->test_capacity_scheduler({'jobs'            => $jobs,
                                          'job_ids'         => $job_ids,
                                          'capacity'        => $capacity,
                                          'wait_time'       => $wait_time,
                                          'num_concur_jobs' => 4});
    return;
}



