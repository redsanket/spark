#!/usr/local/bin/perl

# To run the scripts related with stopping and starting the cluster, or
# individual components, they must be run as hadoopqa.
#
# Examples:
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o stop -c namenode
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o start -c namenode
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o stop -o start -c namenode
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o stop -o start -c namenode -c jobtracker
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o stop -c resourcemanager
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o stop -c resourcemanager -config <config dir>
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o start -c nodemanager -host <hostname>
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o kill -c nodemanager
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o stop -o start -c tasktracker
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o stop -o start -c nodemanager
# $ CLUSTER=<cluster name> hadoop_daemon.pl -o stop -o start -c historyserver

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../lib";
use base qw(Hadoop::Test);
use Getopt::Long;
use Test::More;

my %options = ();
my @components;
my @operations;
my $config_dir;
my $daemon_host;
my $result  = 
    GetOptions ( \%options,
                 "operation|o=s{,}" => \@operations,
                 "component|c=s{,}" => \@components,
                 "host=s"           => \$daemon_host,
                 "config=s"         => \$config_dir,
                 "help|h|?");

usage(0) if $options{help};

unless (@operations) {
    warn("Error: mising required operation");
    $result=0;
}

unless (@components) {
    warn("Error: mising required component");
    $result=0;
}

# Check user is hadoopqa
my $user = getpwuid($>);
unless ($user eq 'hadoopqa') {
    warn("Error: This script can only be run by the user 'hadoopqa'");
    $result=0;
}

usage(1) if (!$result) or (@ARGV);

# Instead of calling the overwriten method from Test.pm
# CORE::die("Error: mising required operation") unless (@operations);
# CORE::die("Error: mising required component") unless (@components);

sub usage
{
    my($exit_code) = @_;
    print STDERR << "EOF";

Usage: $Script
   -operation|o <operation> ...       : One or more operations: E.g. start, stop, kill
   -component|c <component name> ...  : one or more components: E.g. jobtracker, namenode, etc.
   [ -config <config dir>  ]          : optional configuration directory
   [ -host   <daemon host> ]          : daemon hostname
   [-help|h|?              ]          : Display usage

EOF
    exit $exit_code;
}

my $self = __PACKAGE__->new;
my $Config = $self->config;

plan tests => scalar(@operations) * scalar(@components) * 2;

# note(explain($Config));

foreach my $operation (@operations) {
    foreach my $component (@components) {

        $component = lc($component);
        $component = lc($Config->{RESOURCEMANAGER_NAME})
            if (($component eq 'jobtracker') || ($component eq 'resroucemanager'));
        $component = lc($Config->{NODEMANAGER_NAME})
            if (($component eq 'tasktracker') || ($component eq 'nodemanager'));

        note("--> ".uc($operation)." component '$component'");
        $self->check_control_daemon($operation, $component, $daemon_host, $config_dir);
    }
}
