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

my $min_memory_allocation = 1024;
$self->setup_mem_test_conf({'min_memory_allocation' => $min_memory_allocation,
                            'config'                => 'capacity-scheduler100.xml'});

# The following tests are ported from run_capacityScheduler.sh from the
# corresponding function:
my $mem_settings;

# test_highRAM_hiRAMMapOnlyExceedMemoryLimit 
# Function for high RAM job with high RAM map requirement exceed cluster max memory limit
$mem_settings = [ {'map'    => 10240,
                   'reduce' => 2048,
                   'am'     => 2048} ];
$self->test_high_ram_jobs( {'mem_settings' => $mem_settings} );

# test_highRAM_hiRAMReduceOnlyExceedMemoryLimit
# Function for high RAM job with high RAM reduce requirement exceed cluster max memory limit
$mem_settings = [ {'map'    => 2048,
                   'reduce' => 10240,
                   'am'     => 2048} ];
$self->test_high_ram_jobs( {'mem_settings' => $mem_settings} );

# test_highRAM_hiRAMMapAndReduceExceedMemoryLimit
# Function for high RAM job with high RAM map and reduce requirement exceed cluster max memory limit
$mem_settings = [ {'map'    => 10240,
                   'reduce' => 10240,
                   'am'     => 2048} ];
$self->test_high_ram_jobs( {'mem_settings' => $mem_settings} );

# CLEANUP
$self->cleanup_resourcemanager_conf;
$self->restart_resourcemanager();


