#!/usr/local/bin/perl

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use List::Util qw(min max);
use base qw(Hadoop::Test);
use Hadoop22::CapacityScheduler;
use Getopt::Long;
use Test::More;

$ENV{OWNER} = 'philips';

my %options           = ();
my $level             = 'all';
my $should_fail       = 0;
my $error_message;
my $result = 
    GetOptions ( \%options,
                 "level=s"     => \$level,
                 "help|h|?");

usage(1) if (!$result) or (@ARGV);
usage(0) if $options{help};

sub usage
{
    my($exit_code) = @_;
    diag << "EOF";
Usage: $Script
   [-level       <limit level> ] : default all. {'below','at','above', 'all'}
   [-help|h|?                  ] : Display usage

EOF
    exit $exit_code;
}

my $self = __PACKAGE__->new;
my $cs = Hadoop22::CapacityScheduler->new($self);
my $Config = $self->config;
note(explain($Config));

my $num_tests;
if (($level eq 'below') || ($level eq 'at')) {
    $num_tests = 12;
}
elsif ($level eq 'above') {
    $num_tests = 27;
}
else {
    $num_tests = 43;
}
plan tests => $num_tests;

# Determine the config dir in effect
my ($conf, $config_limits, $limits, $logged_limits) = $cs->set_config2();

$limits = $limits->{$Config->{DEFAULT_QUEUE}} if ($Config->{YARN_USED});
note(explain($limits));

# Test the tasks per queue to run limits for values 
# a) below the max, b) at the max, and c) above the max
my @levels = ($level ne 'all') ? ($level) : ('below', 'at', 'above');
my $hash = {'limits' => $limits,
            'entity' => 'tasks',
            'type'   => 'queue',
            'state'  => 'init'};
foreach my $level (@levels) {
    $hash->{level} = $level;
#    my $num_jobs_ran = $cs->test_limits($hash);

    if ($level eq 'above') {
        $hash->{option} = 'same-user';
        $cs->test_limits($hash);
 
        $hash->{option} = 'multiple-queues';
#        $cs->test_limits($hash);
 
        delete($hash->{option});
    }
}
