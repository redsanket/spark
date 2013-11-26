#!/usr/local/bin/perl

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use List::Util qw(min max);
use base qw(Hadoop::Test);
use Hadoop23::CapacityScheduler;
use Getopt::Long;
use Test::More;

$ENV{OWNER} = 'philips';

my %options             = ();
my $level               = 'all';
my $type                = 'all';
my $def_queue           = "root.default";
my $def_source_conf_dir = "$Bin/config/baseline";
my $queue               = $def_queue;
my $source_conf_dir     = $def_source_conf_dir;
my $result = 
    GetOptions ( \%options,
                 "level=s"     => \$level,
                 "type=s"      => \$type,
                 "queue=s"     => \$queue,
                 "conf=s"      => \$source_conf_dir,
                 "help|h|?");

usage(1) if (!$result) or (@ARGV);
usage(0) if $options{help};

sub usage
{
    my($exit_code) = @_;
    diag << "EOF";
Usage: $Script
   [-type  <type>                       ] : default all. {'user','queue','all'}
   [-level <limit level>                ] : default all. {'below','at','above','all'}
   [-queue <fully qualified queue name> ] : default $def_queue
   [-conf  <source config dir>          ] : default $def_source_conf_dir
   [-help|h|?                                ] : Display usage

EOF
    exit $exit_code;
}

my $self = __PACKAGE__->new;
my $cs = Hadoop23::CapacityScheduler->new($self);
my $Config = $self->config;

my $num_tests;
if ($level eq 'below') {
    $num_tests = 6;
}
elsif ($level eq 'at') {
    $num_tests = 7;
}
elsif ($level eq 'above') {
    $num_tests = 9;
}
else {
    $num_tests = 61;
}
plan tests => $num_tests;

die "Error: source config dir '$source_conf_dir' not found"
    unless (-d $source_conf_dir);

# Setup
my ($conf, $config_limits, $limits, $logged_limits) =
    $cs->set_config2($queue, $source_conf_dir);
note("Setup: 1) Config Limits:");
note(explain($config_limits));
note("Setup: 2) Derived Limits:");
note(explain($limits));
note("Setup: 3) Log Limits:");
note(explain($logged_limits));

# Test the jobs per user or per queue at the levels of
# a) below the max, b) at the max, and c) above the max
my @levels = ($level ne 'all') ? ($level) : ('below', 'at', 'above');
my @types = ($type ne 'all') ? ($type) : ('user', 'queue');

foreach my $type (@types) {
    my $hash = {'limits' => $limits,
                'queue'  => $queue,
                'entity' => 'jobs',
                'type'   => $type,
                'state'  => 'init'};
    my $max_jobs =
        ($type eq 'user') ? 
        $limits->{queues}->{$queue}->{maxApplicationsPerUser} :
        $limits->{queues}->{$queue}->{maxApplications};
    
    my $should_fail = 0;
    my $error_message;
    foreach my $level (@levels) {
        $hash->{level} = $level;
        if ($level eq 'above') {
            $should_fail = 1;
            $error_message = 
                ".*.Queue $queue already has ".$max_jobs.
                " applications". 
                (($type eq 'user') ? ' from user hadoopqa' : ',').
                " cannot accept submission of ".
                "application.*.";
        }
        my $num_jobs = $cs->test_limits($hash, $should_fail, $error_message);
    }
}
