#!/usr/local/bin/perl

#################################################################################
# Test High RAM jobs for a single queue
# Runtime: About 8 minutes
#################################################################################

use strict;
use warnings;
use POSIX;
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Test::More tests => 12;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self = __PACKAGE__->new;
my $Config = $self->config;

# NOTE: make sure all the queues have sufficiently large capacity so that they
# are able to complete the jobs, and in a timely manner.
my $min_memory_allocation = 1024;
$self->setup_mem_test_conf({'min_memory_allocation' => $min_memory_allocation,
                            'config'                => 'capacity-scheduler100-multiplequeues.xml'});

# The following tests are ported from run_capacityScheduler.sh from the
# corresponding function:
my $mem_settings;

# test_highRAM_slotUtilization
# Function for checking slot utilization for normal and high RAM jobs
$mem_settings = [ {'map' => 1024, 'reduce' => 1024, 'am' => 2048},
                  {'map' => 2048, 'reduce' => 2048, 'am' => 2048},
                  {'map' => 2048, 'reduce' => 2048, 'am' => 2048},
                  {'map' => 1024, 'reduce' => 1024, 'am' => 2048} ];
$self->test_high_ram_jobs( {'mem_settings' => $mem_settings} );


# test_highRAM_userLimitSupportHighRAMJob
# Function for high RAM job with user limit
$mem_settings = [ {'map' => 2048, 'reduce' => 2048, 'am' => 2048},
                  {'map' => 2048, 'reduce' => 2048, 'am' => 2048},
                  {'map' => 2048, 'reduce' => 2048, 'am' => 2048},
                  {'map' => 2048, 'reduce' => 2048, 'am' => 2048} ];
$self->test_high_ram_jobs( {'mem_settings' => $mem_settings} );

# CLEANUP
$self->cleanup_resourcemanager_conf;
$self->restart_resourcemanager();


