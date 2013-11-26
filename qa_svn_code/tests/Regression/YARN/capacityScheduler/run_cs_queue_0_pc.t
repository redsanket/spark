#!/usr/local/bin/perl

#################################################################################
# Test single queue user limit of 0%.
# This should fail according to Bug 4592235.
# Runtime: About 1 minutes
#################################################################################

use strict;
use warnings;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Test::More tests => 1;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;

my $conf_dir="$Bin/../../../../data/conf/";
$self->restart_resourcemanager_with_cs_conf("$conf_dir/capacity-scheduler0.xml");
my $capacity = $self->get_capacity();

my @queues=('default', 'grideng', 'gridops', 'search');
my $queue = $queues[0];

$self->test_cs_single_queue_0_percent_1($queue, $capacity);

# CLEANUP
$self->cleanup_resourcemanager_conf;


#################################################################################
# Test Functions
#################################################################################

# This test is ported from run_capacityScheduler.sh from the function:
# test_userLimit_Zero
sub test_cs_single_queue_0_percent_1 {
    my ($self, $queue, $capacity) = @_;
    my $user = 'hadoopqa';
    
    note "1. Set user Limit for queue default equals to 0%";
    note "2. Get number of nodes on the cluster, and calculate cluster capacity and queue capacity";
    note "3. Submit six normal jobs by 6 different users which have number of map and reduce tasks equal 2 times queue capacity";
    note "4. Verify task slots are distributed equally among first 5 and the last one get the rest";
    note "5. Verify all jobs ran sucessfully";

    my $job_id = $self->submit_sleep_job({'queue' => $queue,
                                          'user'  => $user});
    is($self->wait_for_job_state($job_id, 'FAILED'),
       0,
       "Job '$job_id' should fail as expected.");
    return;
}
