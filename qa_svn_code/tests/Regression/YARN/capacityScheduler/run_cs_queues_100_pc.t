#!/usr/local/bin/perl

#################################################################################
# Use customer capacity scheduler where user limit is 100%
# Runtime: About 8 minutes
#################################################################################

use strict;
use warnings;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop23::CapacityScheduler;
use Test::More tests => 69;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;
$self->{cs} = Hadoop23::CapacityScheduler->new($self);

my $conf_dir="$Bin/../../../../data/conf/";
$self->restart_resourcemanager_with_cs_conf("$conf_dir/capacity-scheduler100-multiplequeues.xml");
my $capacity = $self->get_capacity();

my $queues=['default', 'grideng', 'gridops', 'search'];

$self->test_cs_multiple_queues_100_percent_1($queues, $capacity);
$self->test_cs_multiple_queues_100_percent_2($queues, $capacity);
$self->test_cs_multiple_queues_100_percent_3($queues, $capacity);

# CLEANUP
$self->cleanup_resourcemanager_conf;


#################################################################################
# Test Functions
#################################################################################

# This test is ported from run_capacityScheduler.sh from the two functions:
# test_guaranteeCapacity_differentUsersOnDifferentQueuesInParallel_userLimit100 
# test_guaranteeCapacity_differentUsersOnDifferentQueuesInSequence_userLimit100
# Always has one job failed since running as same user.
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by
# different users (User limit=100%).
sub test_cs_multiple_queues_100_percent_1 {
    my ($self, $queues, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 10000;
    my $wait_time = 0;
    
    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. User1 submits a J1 job to default queue which has map and reduce tasks=0.45 queue capacity";
    note "3. User2 submits a J2 job to grideng queue which has map and reduce tasks=0.32 queue capacity";
    note "4. User3 submits a J3 job to gridops queue which has map and reduce tasks=0.23 queue capacity";
    note "5. Wait for all 3 jobs running";
    note "6. User4 submits a J4 job to search queue which has map and reduce tasks=0.03 queue capacity";
    note "7. User5 submits a J5 job to gridops queue which has map and reduce tasks=0.02 queue capacity";
    note "8. Again User4 submits a J6 job to search queue which has map and reduce tasks=0.01 queue capacity";
    note "9. User6 submits a J7 job to search queue which has map and reduce tasks=0.1 queue capacity";
    note "10. Verify all the jobs completed sucessfully";

    $jobs =
        $self->expand_jobs({'capacity'   => $capacity,
                            'jobs'       => [ { 'user' => 'hadoop1',  'queue' => $queues->[0], 'factor' => 0.45 },
                                              { 'user' => 'hadoop2',  'queue' => $queues->[1], 'factor' => 0.32 },
                                              { 'user' => 'hadoop3',  'queue' => $queues->[2], 'factor' => 0.23 },
                                              { 'user' => 'hadoop4',  'queue' => $queues->[3], 'factor' => 0.9  },
                                              { 'user' => 'hadoop5',  'queue' => $queues->[3], 'factor' => 0.8  },
                                              { 'user' => 'hadoop4',  'queue' => $queues->[3], 'factor' => 0.1  },
                                              { 'user' => 'hadoop6',  'queue' => $queues->[3], 'factor' => 0.2  },
                                              ],
                            'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    # 'start_order'  => [0, 1, 2, 3, 5, 4, 6],
    # 'num_concur_jobs' => 4
    $self->{cs}->test_capacity_scheduler({'jobs'      => $jobs,
                                          'job_ids'   => $job_ids,
                                          'capacity'  => $capacity,
                                          'wait_time' => $wait_time});
    return;
}


# This test is ported from run_capacityScheduler.sh from the function:
# test_guaranteeCapacity_differentUsersOnDifferentQueuesResourceReclaimed_userLimit100
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by
# different users (User limit=100%).
sub test_cs_multiple_queues_100_percent_2 {
    my ($self, $queues, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 30000;
    my $wait_time = 0;
    
    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. User1 submits a J1 job to default queue which has map and reduce tasks=0.5 queue capacity";
    note "3. User2 submits a J2 job to default queue which has map and reduce tasks=0.4 queue capacity";
    note "4. User3 submits a J3 job to default queue which has map and reduce tasks=0.3 queue capacity";
    note "5. User4 submits a J4 job to grideng queue which has map and reduce tasks=0.2 queue capacity";
    note "6. User5 submits a J5 job to grideng queue which has map and reduce tasks=0.1 queue capacity";
    note "7. User6 submits a J6 job to grideng queue which has map and reduce tasks=0.7 queue capacity";
    note "8. User7 submits a J7 job to gridops queue which has map and reduce tasks=0.4 queue capacity";
    note "9. User8 submits a J8 job to gridops queue which has map and reduce tasks=0.4 queue capacity";
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
                            'jobs'       => [ { 'user' => 'hadoop1',  'queue' => $queues->[0], 'factor' => 0.3  },
                                              { 'user' => 'hadoop2',  'queue' => $queues->[0], 'factor' => 0.2  },
                                              { 'user' => 'hadoop3',  'queue' => $queues->[0], 'factor' => 0.15 },
                                              { 'user' => 'hadoop4',  'queue' => $queues->[1], 'factor' => 0.2  },
                                              { 'user' => 'hadoop5',  'queue' => $queues->[1], 'factor' => 0.05 },
                                              { 'user' => 'hadoop6',  'queue' => $queues->[1], 'factor' => 0.4  },
                                              { 'user' => 'hadoop7',  'queue' => $queues->[2], 'factor' => 0.1  },
                                              { 'user' => 'hadoop8',  'queue' => $queues->[2], 'factor' => 0.1  },
                                              { 'user' => 'hadoop9',  'queue' => $queues->[2], 'factor' => 0.25 },
                                              { 'user' => 'hadoop4',  'queue' => $queues->[3], 'factor' => 0.09 },
                                              { 'user' => 'hadoop5',  'queue' => $queues->[3], 'factor' => 0.08 },
                                              { 'user' => 'hadoop4',  'queue' => $queues->[3], 'factor' => 0.1  },
                                              { 'user' => 'hadoopqa', 'queue' => $queues->[3], 'factor' => 0.2  },
                                              ],
                            'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    # 'start_order'  => [0, 1, 2, 3, 5, 4, 6],
    # 'num_concur_jobs' => 4
    $self->{cs}->test_capacity_scheduler({'jobs'      => $jobs,
                                          'job_ids'   => $job_ids,
                                          'capacity'  => $capacity,
                                          'wait_time' => $wait_time});
    return;
}


# This test is ported from run_capacityScheduler.sh from the function:
# test_guaranteeCapacity_differentUsersOnDifferentQueuesResourceFreedUp_userLimit100 
# Function to test guarantee capacity on different queues (QC=40,30,20,10) by
# different users (User limit=100%).
sub test_cs_multiple_queues_100_percent_3 {
    my ($self, $queues, $capacity) = @_;
    my ($jobs, $job_ids);
    my $task_sleep = 10000;
    my $wait_time = 0;
    
    note "1. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "2. User1 submits a J1 job to default queue which has map and reduce tasks=0.4 queue capacity";
    note "3. User2 submits a J2 job to default queue which has map and reduce tasks=0.25 queue capacity";
    note "4. User3 submits a J3 job to grideng queue which has map and reduce tasks=0.32 queue capacity";
    note "5. User4 submits a J4 job to grideng queue which has map and reduce tasks=0.1 queue capacity";
    note "6. User5 submits a J5 job to gridops queue which has map and reduce tasks=0.18 queue capacity";
    note "7. User6 submits a J6 job to gridops queue which has map and reduce tasks=0.02 queue capacity";
    note "8. Once all of the above jobs are running, User7 submits a J7 job to search queue which has map and reduce tasks=0.1 queue capacity";
    note "9. Verify J7 job completed before all other jobs";
    note "10. Verify all the jobs completed sucessfully";

    $jobs =
        $self->expand_jobs({'capacity'   => $capacity,
                            'jobs'       => [ { 'user' => 'hadoop1',  'queue' => $queues->[0], 'factor' => 0.40 },
                                              { 'user' => 'hadoop2',  'queue' => $queues->[0], 'factor' => 0.25 },
                                              { 'user' => 'hadoop3',  'queue' => $queues->[1], 'factor' => 0.32 },
                                              { 'user' => 'hadoop4',  'queue' => $queues->[1], 'factor' => 0.10 },
                                              { 'user' => 'hadoop5',  'queue' => $queues->[2], 'factor' => 0.05 },
                                              { 'user' => 'hadoop6',  'queue' => $queues->[2], 'factor' => 0.04 },
                                              { 'user' => 'hadoop7',  'queue' => $queues->[3], 'factor' => 0.10 }
                                              ],
                            'task_sleep' => $task_sleep});
    $job_ids = $self->submit_batch_jobs($jobs);
    # 'start_order'  => [0, 1, 2, 3, 5, 4, 6],
    # 'num_concur_jobs' => 4
    $self->{cs}->test_capacity_scheduler({'jobs'      => $jobs,
                                          'job_ids'   => $job_ids,
                                          'capacity'  => $capacity,
                                          'wait_time' => $wait_time});
    return;
}




