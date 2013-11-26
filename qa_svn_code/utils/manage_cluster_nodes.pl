#!/usr/local/bin/perl

# This script is used to facilitate changing or adding hadoop config property
# to a target clsuter. This script must be run as hadoopqa.
#
# Examples:
#
# $ CLUSTER=<cluster name> manage_cluster.pl -name <config property name> -file <config filename>
# $ CLUSTER=<cluster name> manage_cluster.pl -name <config property name> -file <config filename> -config_dir <config dir>
# $ CLUSTER=<cluster name> manage_cluster.pl -name <config property name> -file <config filename> -config_dir <config dir> -component <hadoop component>
# $ CLUSTER=<cluster name> manage_cluster.pl -operation set -name <config property name> -file <config filename>
#
# $ CLUSTER=theoden manage_cluster.pl -name yarn.log-aggregation-enable -file yarn-site.xml
# $ CLUSTER=theoden manage_cluster.pl -name yarn.log-aggregation-enable -file yarn-site.xml -comp namenode -comp resourcemanager
# $ CLUSTER=theoden manage_cluster.pl -name yarn.log-aggregation-enable -file yarn-site.xml -operation fetch  -config_dir /home/gs/gridre/yroot.theoden/conf/hadoop/ 
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
my $exclude_file;

my @components;
my @hosts;
my $operation='fetch';
my $config_dir;

my $result  = 
    GetOptions ( \%options,
                 "name=s"           => \$prop_name,
                 "value=s"          => \$prop_value,
                 "exclude_file=s"   => \$exclude_file,
                 "file=s"           => \$config_file,
                 "config_dir=s"     => \$config_dir,
                 "operation=s"      => \$operation,
                 "component|c=s{,}" => \@components,
                 "hosts=s{,}"       => \@hosts,
                 "help|h|?");

usage(0) if $options{help};

# Check user is hadoopqa
my $user = getpwuid($>);
if (($operation ne 'fetch') && ($user ne 'hadoopqa'))
{
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
   -name <property name>                 : hadoop property name
   -value <property value>               : hadoop property value
   -file <config file>                   : hadoop configuration filename
   [ -operation|o <operation> ]          : {fetch, set} default is 'fetch'
   [ -config <config dir>  ]             : optional configuration directory
   [ -component|c <component name> ... ] : one or more components: E.g. jobtracker, namenode, etc.
   [ -host <host> ... ]                  : hostname
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

# insert
# remove
note("manager cluster nodes:");
note("hosts = ",join(', ',@hosts));

sub show_current {
    # Current excluded nodes
    my $exclude_nodes = $Config->{NODES}->{EXCLUDE_NODES};
    note("Current exclude nodes = ", explain($exclude_nodes));

    my $dn = $Config->{NODES}->{DATANODE}->{HOST};
    note("dn = ", explain($dn));
}

my @command;
if ($operation eq 'fetch') {
    show_current();
}
elsif ($operation eq 'exclude') {    
    # if file does not exist, touch it
    note("exclude file = '$exclude_file'");
    @command = ('/bin/cat', $exclude_file);
    note("Run: @command");
    system(@command);
}
elsif ($operation eq 'commission') {
    
}
elsif ($operation eq 'decommission') {
    
}
else {
    note("ERROR: unsupported operation '$operation'");
    usage(1);
}

exit;

# /home/gs/gridre/yroot.theoden/share/hadoop/bin/yarn rmadmin -refreshNodes
my @args = ('-refreshNodes');
$self->run_hadoop_command( {
    type => 'yarn',
    command => 'rmadmin',
    args => \@args,
} );
