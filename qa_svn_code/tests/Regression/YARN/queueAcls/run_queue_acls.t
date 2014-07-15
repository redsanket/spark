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

use Test::More tests => 60;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;

my ($key, $config_hash, %global_settings, %mapper_settings, %reducer_settings);

my ($queue, $should_fail, $error_message);
my ($stdout, $stderr, $success, $exit_code);
my ($result, $user, $type);
my @command;

# Bugzilla Ticket 5368787 - mapred showacls expected output
# . If acl is undefined for a root queue, it defaults to * which implies everyone
# . If acl is undefined for a non-root queue, it inherit from the parent queue
# . acl value of ' ' implied no users are added
# . acl permissions for a queue can only be added and not subtracted
# . showacls output should show the effective permissions for the user at all levels, whether it's a parent or leaf queue 

sub check_acls {
    my ($user, $hash) = @_;
    my $type = 'mapred';
    my @command = ("queue", "-showacls");
    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command( { command => "@command",
                                     user => $user,
                                     mode => 'qx'
                                     },
                                   $type );
    like($stdout,
         qr/Queue acls for user :\s+$user/, 
         "Check for expected queue acls for user '$user'");

    foreach my $key (sort keys %$hash) {
        like($stdout,
             qr/$key\s+$hash->{$key}/,
             "Check for expected queue acls for user '$user' queue '$key'");
    }
}

#################################################################################
# Use custom capacity scheduler file #1
#################################################################################
$self->restart_resourcemanager_with_cs_conf("$Bin/data/capacity-scheduler_queue_acls_1.xml");
check_acls('hadoopqa', 
           { 'root' => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a1'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a2'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'b'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c1'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c2'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS'});
check_acls('hadoop1', 
           { 'root' => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a1'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a2'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'b'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c1'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c2'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS'});
check_acls('hadoop5', 
           { 'root' => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a1'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a2'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'b'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c1'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c2'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS'});

#################################################################################
# Use custom capacity scheduler file #2
#################################################################################
$self->restart_resourcemanager_with_cs_conf("$Bin/data/capacity-scheduler_queue_acls_2.xml");
check_acls('hadoopqa', 
           { 'root' => 'ADMINISTER_QUEUE',
             'a'    => '',
             'a1'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'a2'   => 'ADMINISTER_QUEUE',
             'b'    => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS',
             'c'    => '',
             'c1'   => 'ADMINISTER_QUEUE',
             'c2'   => 'ADMINISTER_QUEUE'});

note("---> Test queue acls inheritance (bug 4620788)");
$user='hadoopqa';
$queue="a1";
$self->test_run_sleep_job({'user'  => $user,
                           'queue' => $queue });

note("---> Test queue acls inheritance (bug 4620788)");
$user='hadoopqa';
$queue="a2";
# Bug 4620788 closed as invalid, but passes on 2.0.
# Still fails on 0.23, dded to 0.23 known failures.
# $should_fail=1;
# $error_message=
#    "org.apache.hadoop.security.AccessControlException: ".
#    "User $user cannot submit applications to queue root.a.a2";
$self->test_run_sleep_job({'user'  => $user,
                           'queue' => $queue });
#                          $should_fail, $error_message);

#################################################################################
# Use custom capacity scheduler file #3
#################################################################################
$self->restart_resourcemanager_with_cs_conf("$Bin/data/capacity-scheduler_queue_acls_3.xml");
check_acls('hadoopqa', 
           { 'root' => '',
             'a'    => '',
             'a1'   => '',
             'a2'   => 'SUBMIT_APPLICATIONS',
             'b'    => 'ADMINISTER_QUEUE',
             'c'    => 'ADMINISTER_QUEUE',
             'c1'   => 'ADMINISTER_QUEUE',
             'c2'   => 'ADMINISTER_QUEUE,SUBMIT_APPLICATIONS'});
check_acls('hadoop2', 
           { 'root' => '',
             'a'    => '',
             'a1'   => 'ADMINISTER_QUEUE',
             'a2'   => '',
             'b'    => 'ADMINISTER_QUEUE',
             'c'    => 'SUBMIT_APPLICATIONS',
             'c1'   => 'SUBMIT_APPLICATIONS',
             'c2'   => 'SUBMIT_APPLICATIONS'});

note("---> Test queue acls inheritance (bug 4620788)");
$user='hadoopqa';
$queue="b";
# Bug 4620788 closed as invalid, but passes on 2.0.
# Still fails on 0.23, dded to 0.23 known failures.
# $should_fail=1;
# $error_message=
#     "org.apache.hadoop.security.AccessControlException: ".
#    "User $user cannot submit applications to queue root.b";
$self->test_run_sleep_job({'user'  => $user,
                           'queue' => $queue });
#                          $should_fail, $error_message);

# CLEANUP
$self->cleanup_resourcemanager_conf;

$self->restart_resourcemanager($Config->{HADOOP_CONF_DIR});

