#!/usr/local/bin/perl

# This script is used to facilitate changing or adding hadoop config property
# to a target clsuter. This script must be run as hadoopqa.
#
# Examples:
# $ CLUSTER=<cluster name> configure_cluster.pl -name <config property name> -file <config filename>
# $ CLUSTER=<cluster name> configure_cluster.pl -name <config property name> -file <config filename> -config_dir <config dir>
# $ CLUSTER=<cluster name> configure_cluster.pl -name <config property name> -file <config filename> -config_dir <config dir> -component <hadoop component>
# $ CLUSTER=theoden configure_cluster.pl -name yarn.log-aggregation-enable -file yarn-site.xml
# $ CLUSTER=theoden configure_cluster.pl -name yarn.log-aggregation-enable -file yarn-site.xml -comp namenode -comp resourcemanager
# $ CLUSTER=theoden configure_cluster.pl -name yarn.log-aggregation-enable -file yarn-site.xml -operation fetch  -config_dir /home/gs/gridre/yroot.theoden/conf/hadoop/ 
# $ CLUSTER=<cluster name> configure_cluster.pl -operation set -name <config property name> -file <config filename>
#


use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../lib";
use base qw(Hadoop::Test);
use Getopt::Long;
use Test::More;

my %options = ();
my $prop_name;
my $prop_value;
my $config_file;

my @components;
my $operation='fetch';
my $config_dir;
my $daemon_host;

my $result  = 
    GetOptions ( \%options,
                 "name=s"         => \$prop_name,
                 "value=s"        => \$prop_value,
                 "file=s"         => \$config_file,
                 "config_dir=s"   => \$config_dir,
                 "operation=s"    => \$operation,
                 "component|c=s{,}" => \@components,
                 "host=s"           => \$daemon_host,
                 "help|h|?");

usage(0) if $options{help};

unless ($prop_name) {
    warn("Error: mising required property name");
    $result=0;
}

unless ($prop_value) {
    if ($operation eq 'set') {
        warn("Error: mising required property value");
        $result=0;
    }
}

unless ($config_file) {
    warn("Error: mising required configuration file");
    $result=0;
}

# Check user is hadoopqa
my $user = getpwuid($>);
unless ($user eq 'hadoopqa') {
#    warn("Error: This script can only be run by the user 'hadoopqa'");
#    $result=0;
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
   -name <property name>                 : hadoop property name
   -value <property value>               : hadoop property value
   -file <config file>                   : hadoop configuration filename
   [ -operation|o <operation> ]          : {fetch, set} default is 'fetch'
   [ -config <config dir>  ]             : optional configuration directory
   [ -component|c <component name> ... ] : one or more components: E.g. jobtracker, namenode, etc.
   [ -host   <daemon host> ]             : daemon hostname
   [-help|h|?              ]             : Display usage

EOF
    exit $exit_code;
}

my $self = __PACKAGE__->new;
my $Config = $self->config;

# plan tests => scalar(@operations) * scalar(@components) * 2;

# note(explain($Config));

$config_dir = "/home/gs/gridre/yroot.".$Config->{CLUSTER}."/conf/hadoop/"
    unless $config_dir;


my $hosts = [ ];
if (@components) {
    foreach my $component (@components) {

        my $component = lc($component);
        $component = lc($Config->{RESOURCEMANAGER_NAME})
            if (($component eq 'jobtracker') || ($component eq 'resroucemanager'));
        $component = lc($Config->{NODEMANAGER_NAME})
            if (($component eq 'tasktracker') || ($component eq 'nodemanager'));
        my $hosts_ = $Config->{NODES}->{uc($component)}->{HOST};

        if (ref($hosts_) eq 'ARRAY') {
            push(@$hosts, @{$hosts_});
        }
        else {
            push(@{$hosts}, $hosts_);
        }
    }
}
else {
    $hosts = $Config->{NODES}->{HOST};
}

my $current_value;
foreach my $host (@$hosts) {
    if ($operation eq 'fetch') {
        $current_value =
            $self->get_xml_prop_value($prop_name, "$config_dir/$config_file", $host,
                                      undef, undef, 0);
        note("$host: property (name, value) = ('$prop_name', '$current_value')");
    }
    elsif ($operation eq 'set') {
        $current_value =
            $self->set_xml_prop_value($prop_name, $prop_value,
                                      "$config_dir/$config_file", $host,
                                      undef, undef, 1);
    }
    else {
        note("ERROR: unsupported operation '$operation'");
        usage(1);
    }
}
