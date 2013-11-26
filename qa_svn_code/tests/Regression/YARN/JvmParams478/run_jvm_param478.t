#!/usr/local/bin/perl

###############################################################################
# This tests MAPREDUCE-478: separate jvm param for mapper and reducer
#
# These tests should be run on Hadoop-0.20, Hadoop-0.22 and later
# Tests run in approximately 14 minutes
###############################################################################

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop::Streaming;

use Test::More tests => 56;
$ENV{OWNER} = 'jnaisbit';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;
# note(explain($Config));

# This issue only exists in 0.22, but to keep the files consistent,
# we parameterize it.
my $known_issue_msg =
    (($Config->{VERSION} eq '0.20') || ($Config->{VERSION} =~ '^1\.')) ?
    '' : '(Known issue: bugs 5312711)';

# Define version-dependent variables:
my %settings_hash;
if (($Config->{VERSION} eq '0.20') || ($Config->{VERSION} =~ '^1\.')) {
    %settings_hash = (
        'old.java.opts'    => 'mapred.child.java.opts',
        'old.env'          => 'mapred.child.env',
        'old.ulimit'       => 'mapred.child.ulimit',
        'map.java.opts'    => 'mapred.map.child.java.opts',
        'map.env'          => 'mapred.map.child.env',
        'map.ulimit'       => 'mapred.map.child.ulimit',
        'reduce.java.opts' => 'mapred.reduce.child.java.opts',
        'reduce.env'       => 'mapred.reduce.child.env',
        'reduce.ulimit'    => 'mapred.reduce.child.ulimit',
    );
} else {
    %settings_hash = (
        'old.java.opts'    => 'mapred.child.java.opts',
        'old.env'          => 'mapred.child.env',
        'old.ulimit'       => 'mapred.child.ulimit',
        'map.java.opts'    => 'mapreduce.map.java.opts',
        'map.env'          => 'mapreduce.map.env',
        'map.ulimit'       => 'mapreduce.map.ulimit',
        'reduce.java.opts' => 'mapreduce.reduce.java.opts',
        'reduce.env'       => 'mapreduce.reduce.env',
        'reduce.ulimit'    => 'mapreduce.reduce.ulimit',
    );
}

sub set_property {
    my ($self, $property_name, $new_value) = @_;
    $self->set_gateway_jobtracker_xml_prop_value($property_name, $new_value);
    return $new_value;
}


# Necessary since property values set to '' are not seen at the child
# (i.e. undef)
sub check_config_value {
    my ($self, $original_key, $got_hash, $expected_hash, $msg) = @_;
    # For test failures, point to the caller of this function
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    my $config_key = $original_key;
    $config_key =~ s/\./_/g;
    my $child_value = $got_hash->{$config_key};
    my $expected_value = $expected_hash->{$original_key};

    if ($expected_value) {
        is($child_value, $expected_value, $msg);
    } else {
        # expected_value is undef or '', so just check that it matches
        # the boolean result
        ok(!defined($child_value) || $child_value eq '', $msg);
    }
}


###############################################################################
# Set $settings_hash{'old.java.opts'}, $settings_hash{'old.env'} and
# $settings_hash{'old.ulimit'} in the config file.  For the tests below,
# pass in the options per job.
###############################################################################
my ($key, $config_hash, %global_settings, %mapper_settings, %reducer_settings);
my $hadoop_streaming = Hadoop::Streaming->new($self);

# Backup all of the xml properties we will be changing
$self->backup_gateway_conf;
 
# First, verify that the xml values are picked up in the mapper and reducer
# for the deprecated settings.
note("1. Verify xml values are picked up in the mapper and reducer for the ".
     "deprecated settings");
$global_settings{$settings_hash{'old.env'}} =
    $self->set_property($settings_hash{'old.env'}, 'childenv1=childenv');
$global_settings{$settings_hash{'old.java.opts'}} =
    $self->set_property($settings_hash{'old.java.opts'},
                        '-server -Xmx1156m -Djava.net.preferIPv4Stack=true');
$global_settings{$settings_hash{'old.ulimit'}} =
    $self->set_property($settings_hash{'old.ulimit'}, '8000007');
note(explain(\%settings_hash));

$config_hash = $hadoop_streaming->run_env_mapper();
foreach $key (keys %global_settings) {
    $self->check_config_value($key, $config_hash, \%global_settings,
                              "$key in mapper matches xml value");
}

is($config_hash->{'childenv1'}, 'childenv',
   "The mapper child environment correctly uses the $settings_hash{'old.env'} ".
   'from the xml file');
 
$config_hash = $hadoop_streaming->run_env_reducer();
foreach $key (keys %global_settings) {
     $self->check_config_value($key, $config_hash, \%global_settings, 
                               "$key in reducer matches xml value ".
                               "$known_issue_msg");
}
is($config_hash->{'childenv1'}, 'childenv',
   "The reducer child environment correctly uses the ".
   "$settings_hash{'old.env'} from the xml file $known_issue_msg");

 
# Verify that invalid settings actually cause the jobs to fail:
# Set an invalid java.opts and verify that the job fails
# (meaning it picked up that setting)
note("2. Verify invalid settings cause the jobs to fail");
$global_settings{$settings_hash{'old.java.opts'}} =
    $self->set_property($settings_hash{'old.java.opts'}, '-youshoulddie');

$config_hash = $hadoop_streaming->run_env_mapper();
ok(!defined($config_hash),
   "The mapper picked up the invalid java.opts xml setting, ".
   "and the job failed as expected");

$config_hash = $hadoop_streaming->run_env_reducer();
ok(!defined($config_hash),
   "The reducer picked up the invalid java.opts xml setting, ".
   "and the job failed as expected");


# Now, set an invalid ulimit and verify that the job fails
$self->reset_gateway_conf($Config->get_jobtracker_conf());

# TODO: {
#    local $TODO = "$known_issue_msg" if ($known_issue_msg);

# 11
# $global_settings{$settings_hash{'old.ulimit'}} =
#    $self->set_property($settings_hash{'old.ulimit'}, '10');
# $global_settings{$settings_hash{'old.ulimit'}} =
#    $self->set_property($settings_hash{'old.ulimit'}, '-10');
$global_settings{$settings_hash{'old.ulimit'}} =
    $self->set_property($settings_hash{'old.ulimit'}, 'invalid');
$global_settings{$settings_hash{'map.ulimit'}} =
    $self->set_property($settings_hash{'map.ulimit'}, 'invalid');
$config_hash = $hadoop_streaming->run_env_mapper();
ok(!defined($config_hash),
   "The mapper picked up the invalid ulimit xml setting, ".
   "and the job failed as expected");

# 12
$global_settings{$settings_hash{'reduce.ulimit'}} =
    $self->set_property($settings_hash{'map.ulimit'}, 'invalid');
$config_hash = $hadoop_streaming->run_env_reducer();
ok(!defined($config_hash),
   "The reducer picked up the invalid ulimit xml setting, ".
   "and the job failed as expected");

# }

$global_settings{$settings_hash{'old.java.opts'}} =
    $self->set_property($settings_hash{'old.java.opts'},'-youshoulddie');

# At this point, the java.opts and ulimit values are still invalid and will 
# cause job failure. Now, pass in these settings on the command-line and verify
# that they are picked up and not the xml settings. To do this, set invalid 
# settings for the java.opts and ulimit, and then pass in valid ones on the
# command-line. The job will fail unless the command-line settings are picked up.
# The $settings_hash{'old.env'} settings can be checked through the environment.

# Test 4: Submit a job by setting $settings_hash{'old.java.opts'}
#   Both map and reduce tasks should take the value of
#   $settings_hash{'old.java.opts'}

# Test 5: Submit a job by setting $settings_hash{'old.env'}
#   Both map and reduce tasks should take the value of
#   $settings_hash{'old.env'}

# Test 6: Submit a job by setting $settings_hash{'old.ulimit'}
#   Both map and reduce tasks should take the value of
#   $settings_hash{'old.ulimit'}

my %new_global_settings = %global_settings;
$new_global_settings{$settings_hash{'old.env'}} =
    'childenv1=commandlinechildenv';
$new_global_settings{$settings_hash{'old.java.opts'}} =
    '-server -Xmx1156m -Djava.net.preferIPv4Stack=true';
$new_global_settings{$settings_hash{'old.ulimit'}} = '8000008';

$config_hash = $hadoop_streaming->run_env_mapper(\%new_global_settings);
ok(defined($config_hash),
   'The mapper used the ulimit and java.opts settings from the command-line');

foreach $key (keys %new_global_settings) {
    $self->check_config_value($key, $config_hash, \%new_global_settings,
                              "$key in mapper matches command-line value");
}
is($config_hash->{'childenv1'}, 'commandlinechildenv',
   "The mapper child environment correctly uses the $settings_hash{'old.env'} ".
   'from the command-line');
 
 
$config_hash = $hadoop_streaming->run_env_reducer(\%new_global_settings);
ok(defined($config_hash),
   'The reducer used the ulimit and java.opts settings from the command-line');

foreach $key (keys %new_global_settings) {
    $self->check_config_value($key, $config_hash,
                              \%new_global_settings,
                              "$key in reducer matches command-line value ".
                              "$known_issue_msg");
}

is($config_hash->{'childenv1'}, 'commandlinechildenv',
   "The reducer child environment correctly uses the $settings_hash{'old.env'} ".
   "from the command-line $known_issue_msg");

# Test 1: Submit a job by setting $settings_hash{'map.java.opts'} and
#         $settings_hash{'reduce.java.opts'}.  The job should complete by
#         picking up the new variables

# Test 2: Submit a job by setting $settings_hash{'map.env'} and
#         $settings_hash{'reduce.env'}.  The job should complete by
#         picking up the new variables

# Test 3: Submit a job by setting  $settings_hash{'map.ulimit'} and
#         $settings_hash{'reduce.ulimit'}.  The job should complete by
#         picking up the new variables

# NOTE: we still have invalid $settings_hash{'old.java.opts'} and
# $settings_hash{'old.ulimit'} values, so we will verify that the below
# child values are used

$mapper_settings{$settings_hash{'map.java.opts'}} =
    '-server -Xmx1156m -Djava.net.preferIPv4Stack=true';
$mapper_settings{$settings_hash{'map.env'}} = 'childenv1=mapchildenv';
$mapper_settings{$settings_hash{'map.ulimit'}} = '8000010';

$reducer_settings{$settings_hash{'reduce.java.opts'}} =
    '-server -Xmx1156m -Djava.net.preferIPv4Stack=true';
$reducer_settings{$settings_hash{'reduce.env'}} = 'childenv1=redchildenv';
$reducer_settings{$settings_hash{'reduce.ulimit'}} = '8000010';
my %mapreduce_settings = (%mapper_settings, %reducer_settings);

# Check all the settings in the mapper
$config_hash = $hadoop_streaming->run_env_mapper(\%mapper_settings);
ok(defined($config_hash),
   'On the mapper, the command-line map-specific settings overrode '.
   "the invalid deprecated settings from the xml file $known_issue_msg");
foreach $key (keys %mapper_settings) {
    $self->check_config_value($key, $config_hash, \%mapper_settings,
                              "$key in mapper matches command-line setting ".
                              "$known_issue_msg");
}

is($config_hash->{'childenv1'}, 'mapchildenv',
   "The mapper child environment correctly uses the $settings_hash{'map.env'} ".
   "value passed in $known_issue_msg");

# Now check all the settings in the reducer
# NOTE!!: since the run_env_reducer also runs 'cat' on the mapper,
# we must override the invalid mapper settings as well!!!
$config_hash = $hadoop_streaming->run_env_reducer(\%mapreduce_settings);
ok(defined($config_hash),
   'On the reducer, the command-line reduce-specific settings overrode ' .
   "the invalid deprecated settings from the xml file $known_issue_msg");
foreach $key (keys %reducer_settings) {
    $self->check_config_value($key, $config_hash, \%reducer_settings,
        "$key in reducer matches command-line setting $known_issue_msg");
}

is($config_hash->{'childenv1'}, 'redchildenv',
   "The reducer child environment correctly uses the $settings_hash{'map.env'} ".
   "value passed in $known_issue_msg");

# Reset the values that have been set in the xml file
$self->reset_gateway_conf($Config->get_jobtracker_conf());


###############################################################################
# Set $settings_hash{'map.java.opts'}, $settings_hash{'reduce.java.opts'},
#   $settings_hash{'map.env'}, $settings_hash{'reduce.env'},
#   $settings_hash{'map.ulimit'},
#   $settings_hash{'reduce.ulimit'}i
###############################################################################

%global_settings = ();
%mapper_settings = ();
%reducer_settings = ();

$mapper_settings{$settings_hash{'map.java.opts'}} =
    $self->set_property($settings_hash{'map.java.opts'},
                        '-server -Xmx1156m -Djava.net.preferIPv4Stack=true');
$mapper_settings{$settings_hash{'map.env'}} =
    $self->set_property($settings_hash{'map.env'}, 'childenv3=mapchildvalue');
$mapper_settings{$settings_hash{'map.ulimit'}} =
    $self->set_property($settings_hash{'map.ulimit'}, '8000800');

$reducer_settings{$settings_hash{'reduce.java.opts'}} =
    $self->set_property($settings_hash{'reduce.java.opts'},
                        '-server -Xmx1156m -Djava.net.preferIPv4Stack=true');
$reducer_settings{$settings_hash{'reduce.env'}} =
    $self->set_property($settings_hash{'reduce.env'}, 'childenv3=redchildvalue');
$reducer_settings{$settings_hash{'reduce.ulimit'}} =
    $self->set_property($settings_hash{'reduce.ulimit'}, '8000801');

# Check that the child xml settings are picked up in the mapper
$config_hash = $hadoop_streaming->run_env_mapper();
foreach $key (keys %mapper_settings) {
    $self->check_config_value($key, $config_hash, \%mapper_settings,
                              "$key matches xml property");
}

is($config_hash->{'childenv3'}, 'mapchildvalue',
   'The mapper child environment correctly uses the global ' .
   "$settings_hash{'map.env'} xml value $known_issue_msg");


# Check that the child xml settings are picked up in the reducer
$config_hash = $hadoop_streaming->run_env_reducer();
foreach $key (keys %reducer_settings) {
    $self->check_config_value($key, $config_hash, \%reducer_settings,
                              "$key matches xml property $known_issue_msg");
}

is($config_hash->{'childenv3'}, 'redchildvalue',
   'The reducer child environment correctly uses the global ' .
   "$settings_hash{'reduce.env'} xml value $known_issue_msg");


# Test 7: Submit a job by setting $settings_hash{'old.java.opts'}
#   The job should complete by picking up the new variables and
#   the old config variables should be ignored

# Test 8: Submit a job by setting $settings_hash{'old.env'}
#   The job should complete by picking up the new variables and
#   the old config variables should be ignored

# Test 9: Submit a job by setting $settings_hash{'old.ulimit'}
#   The job should complete by picking up the new variables and
#   the old config variables should be ignored

$global_settings{$settings_hash{'old.java.opts'}} =
    '-server -Xmx1156m -Djava.net.preferIPv4Stack=true';
$global_settings{$settings_hash{'old.env'}} = 'childenv3=childenvvalue';
$global_settings{$settings_hash{'old.ulimit'}} = '8000888';

# Check that the map/reduce child xml settings are ignored in the mapper
$config_hash = $hadoop_streaming->run_env_mapper(\%global_settings);
foreach $key (keys %global_settings) {
    $self->check_config_value($key, $config_hash, \%global_settings,
                              "$key in mapper matches xml property");
}

is($config_hash->{'childenv3'}, 'mapchildvalue',
   'The mapper child environment correctly uses the ' .
   "new $settings_hash{'map.env'} even when the deprecated ".
   "$settings_hash{'old.env'} value is passed in $known_issue_msg");
 
# Check that the map/reduce child xml settings are ignored in the reducer
$config_hash = $hadoop_streaming->run_env_reducer(\%global_settings);
foreach $key (keys %global_settings) {
    $self->check_config_value($key, $config_hash, \%global_settings,
                              "$key in reducer matches xml property ".
                              "$known_issue_msg");
}

is($config_hash->{'childenv3'}, 'redchildvalue',
   'The reducer child environment correctly uses the ' .
   "new $settings_hash{'reduce.env'} even when the deprecated ".
   "$settings_hash{'old.env'} value is passed in $known_issue_msg");

# Now, to verify that the mapper and reducer use the new java.opts & ulimit
# settings even if the deprecated ones are passed in on the command-line, change
# the command-line,  We pass in invalid settings for each (one at a time) - the
# job should succeed.
$global_settings{$settings_hash{'old.java.opts'}} = '-youshoulddie';

$config_hash = $hadoop_streaming->run_env_mapper(\%global_settings);
ok(defined($config_hash),
   "The mapper picked up the valid $settings_hash{'map.java.opts'} ".
   "from the xml file even with deprecated value passed in $known_issue_msg");

$config_hash = $hadoop_streaming->run_env_reducer(\%global_settings);
ok(defined($config_hash),
   "The reducer picked up the valid $settings_hash{'reduce.java.opts'} ".
   "from the xml file even with deprecated value passed in $known_issue_msg");


# Now do the same for the ulimit setting:
$global_settings{$settings_hash{'old.java.opts'}} =
    '-server -Xmx1156m -Djava.net.preferIPv4Stack=true';
$global_settings{$settings_hash{'old.ulimit'}} = '10';

$config_hash = $hadoop_streaming->run_env_mapper(\%global_settings);
ok(defined($config_hash),
   "The mapper picked up the valid $settings_hash{'map.ulimit'} ".
   "from the xml file even with deprecated value passed in $known_issue_msg");

$config_hash = $hadoop_streaming->run_env_reducer(\%global_settings);
ok(defined($config_hash),
   "The reducer picked up the valid $settings_hash{'reduce.ulimit'} ".
   "from the xml file even with deprecated value passed in $known_issue_msg");

$self->cleanup_gateway_conf;
