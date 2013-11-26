#!/usr/local/bin/perl

##
# Test Feature HADOOP-3741.
# Test to make sure that secondary name node is running and that it contains
# the expected fields.

use strict;
use warnings;

# Pull in the automated test framework perl modules.
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Test::More tests => 9;

# Use "get()" method from LWP::Simple to access the web UI for the Secondary
# name node.
use LWP::Simple;
use HTML::Parser ();

# Initialize test framework environment.
my $test_self = __PACKAGE__->new;
my $Config = $test_self->config;

# Display the environment.
use Data::Dumper;
print(Dumper($Config));

$ENV{OWNER} = 'ericp';

# Access the top-level page on the secondary name node Web UI.
my $host = $Config->{NODES}->{SECONDARYNAMENODE}->{HOST};
my $port = $Config->{NODES}->{SECONDARYNAMENODE}->{PORT};
my $html = get("http://$host:$port/status.jsp");

# $content will hold the list of fields and their values from the web UI.
my $content = "";

# This is the list of expected fields on the web UI.
my @expected_fields = (
"Start Time",
"Checkpoint Dirs",
"Checkpoint Period",
"Checkpoint Edits Dirs",
"Checkpoint Size",
"Name Node Address",
"Last Checkpoint Time"
);

# The HTML::Parser will parse the data contained in the $html variable and
# call the start_handler method to store all of the text between the
# <pre> ... </pre> tags in the $content variable.

# Below, start_handler() is registered w/ the $p parser object. If a 'start'
# event is encountered during the call to $p->parse($html), start_handler()
# is called with the following signature: start_handler($tagname,$p) .
# Then, once the <pre> tag is encountered, the start_handler() will register
# a 'text' event subroutine and an 'end' event subroutine. As the call to
# $p->parse($html) continues, it will hit the 'text' event and call the embedded
# subroutine listed below, which will store the $dtext in $content. Then, when
# the 'end' event for the </pre> section is encountered, the shift-eof will be
# called and all parsing will stop (??).
sub start_handler
{
  return if shift ne "pre";
  my $self = shift;
  $self->handler(text => sub { $content = shift }, "dtext");
  $self->handler(end  => sub { shift->eof if shift eq "pre"; },
                         "tagname,self");
}

# HTML::Parser magic.
my $p = HTML::Parser->new(api_version => 3);
$p->handler( start => \&start_handler, "tagname,self");
$p->parse($html);

my %fields;

# Get the content and store values in a hash.
foreach my $line (split("\n",$content)) {
  chomp($line);
  my ($field,$val) = $line =~ /^(.*?):(.*)/;
  $field =~ s/\s+$//g if defined($field); # Take off trailing whitespace
  $field =~ s/^\s+//g if defined($field); # Take off leading whitespace
  $val =~ s/\s+$//g if defined($val); # Take off trailing whitespace
  $val =~ s/^\s+//g if defined($val); # Take off leading whitespace
  $fields{$field} = $val if defined($field);
}

##############################################################################
# Verify that the secondary name node is running.
ok(defined($html), "Check that Secondary Namenode is running");

##############################################################################
# Verify that all expected fields exist on Secondary namenode web UI.
for my $ef (@expected_fields) {
  ok(exists($fields{$ef}) == 1,
     "Bugzilla Ticket 4416527: Check that \"$ef\" status is displayed on Secondary Namenode web service.");
}

##############################################################################
# Verify that values of expected fields have expected format
$fields{'Name Node Address'} =~ m:^(.*)/.*$:;
my $testvar = $1;
$host = $Config->{NODES}->{NAMENODE}->{HOST1}
is($testvar, $host,
   "Bugzilla Ticket 4416527: Check that \"Name Node Address\" contains name node: $host");
