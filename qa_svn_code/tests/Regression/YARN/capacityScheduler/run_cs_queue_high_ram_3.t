#!/usr/local/bin/perl

#################################################################################
# Test High RAM jobs for a single queue
# Runtime: About 6 minutes
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
my ($map_mem, $reduce_mem);

# test_highRAM_hiRAMMapAndReduceLowMemory
# Function for high RAM job with high RAM map and reduce memory requirement below memory limit
$map_mem = 0.5;
$reduce_mem = 0.5;
$mem_settings = [ {'map'    => ($map_mem*1024),
                   'reduce' => ($reduce_mem*1024),
                   'am'     => 2048} ];
$self->test_high_ram_jobs( {'mem_settings' => $mem_settings,
                            'num_tasks' => 20,
                            'task_sleep' => 20000} );

# MISC

$map_mem = 2;
$reduce_mem = 3.5;
$mem_settings = [ {'map'    => ($map_mem*1024),
                   'reduce' => ($reduce_mem*1024),
                   'am'     => 2048} ];
$self->test_high_ram_jobs( {'mem_settings' => $mem_settings,
                            'num_tasks' => 20,
                            'task_sleep' => 30000} );

$map_mem = 3.5;
$reduce_mem = 4.5;
$mem_settings = [ {'map'    => ($map_mem*1024),
                   'reduce' => ($reduce_mem*1024),
                   'am'     => 2048} ];
$self->test_high_ram_jobs( {'mem_settings' => $mem_settings,
                            'num_tasks' => 20,
                            'task_sleep' => 30000} );

# CLEANUP
$self->cleanup_resourcemanager_conf;
$self->restart_resourcemanager();


