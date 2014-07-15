#!/usr/local/bin/perl

# This test is part of the Queue Limits test suite. It specifically tests
# refreshing queue capacity and adding queues. The actual test plan can be found
# at: http://twiki.corp.yahoo.com/view/Grid/
# MRFredFeatureTestScenarios#Refreshing_Queue_capacity_and_ad

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
my $type                = 'all';
my $def_queue           = "root.default";
my $def_source_conf_dir = "$Bin/config/baseline";
my $queue               = $def_queue;
my $source_conf_dir     = $def_source_conf_dir;
my $result = 
    GetOptions ( \%options,
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
   [-type  <test type>                  ] : default all. {'limit','capacity','queue','all'}
   [-queue <fully qualified queue name> ] : default $def_queue
   [-conf  <source config dir>          ] : default $def_source_conf_dir
   [-help|h|?                           ] : Display usage

EOF
    exit $exit_code;
}

my $self = __PACKAGE__->new;
my $cs = Hadoop23::CapacityScheduler->new($self);
my $Config = $self->config;

my $num_tests;
if ($type eq 'limit') {
    $num_tests = 17;
}
elsif ($type eq 'capacity') {
    $num_tests = 13;
}
elsif ($type eq 'queue') {
    $num_tests = 65;
}
else {
    $num_tests = 29;
}
plan tests => $num_tests;

die "Error: source config dir '$source_conf_dir' not found"
    unless (-d $source_conf_dir);

my ($orig_config_limits, $orig_limits, $orig_logged_limits);
my $default_limits;

# Setup
# Set a baseline for the capacity limits and restart the ResourceManager to tie in the
# new config dir that will be necessary for the refresh queue to work.
# Note: If defined, the minimum-user-limit-percent should be less than 100.
# This is because the defined values will be incremented later in the test,
# which could exceed the valid limits of 100.
note("---> Setup a baseline for the capacity limits:");
my ($conf, $config_limits, $limits, $logged_limits) =
    $cs->set_config2($queue, $source_conf_dir);
note("Setup: 1) Config Limits:");
note(explain($config_limits));
note("Setup: 2) Derived Limits:");
note(explain($limits));
note("Setup: 3) Log Limits:");
note(explain($logged_limits));

# /home/gs/gridre/yroot.theoden/share/hadoop/bin/yarn rmadmin -refreshQueues
my $refresh_queues = {
    command => 'rmadmin',
    args => ['-refreshQueues'],
};

# test_refresh_limits($orig_limits->{'root.default'});

# cmdline
# /grid/0/gs/gridre/yroot.boromir/share/hadoopcommon/bin/hadoop jar /grid/0/gs/gridre/yroot.boromir/share/hadoopmapred/hadoop-test.jar sleep -libjars /grid/0/gs/gridre/yroot.boromir/share/yarn/modules/yarn-mapreduce-client-0.2.1104040202.jar 
# /grid/0/gs/gridre/yroot.boromir/share/hadoopcommon/bin/hadoop jar /grid/0/gs/gridre/yroot.boromir/share/hadoopmapred/hadoop-test.jar sleep -libjars /grid/0/gs/gridre/yroot.boromir/share/yarn/modules/yarn-mapreduce-client-0.2.1104040202.jar
#                                      -Dmapreduce.job.user.name=philips    -Dmapred.job.queue.name=b 
# -Dhadoop.http.filter.initializers="" -Dmapreduce.job.user.name= 
# -m 2 -r 2 -mt 10 -rt 10
# -m 0 -r -1 -mt 1000 -rt 1000

$queue = 'root.a.a3';
if (($type eq 'all') || ($type eq 'limit')) {
    # Test the new tasks per job to accept limit will result in failure.
    $limits = test_unrefreshed_limits($default_limits);

    # Test new limits have taken effect after queues have been refreshed
    test_refresh_limits($limits);
}

if (($type eq 'all') || ($type eq 'capacity')) {
    # Test invalid queue capacity
    test_invalid_queue_capacity($conf, $queue);
}

if (($type eq 'all') || ($type eq 'queue')) {
    # Test new queue with invalid aggregate capacity
    # Test new queue in stopped state cannot process job
    # Test new queue in running state should process job successfully
    test_new_queues($conf, $queue);
}

# Tear down: 1) restart the ResourceManager with the default config dir, and 2)
# remove the temp config dir.
note("---> Tear down: restart the ResourceManager with the default config dir:");
$self->check_restart_daemon('resourcemanager');
note("---> Tear down: remove the temp config dir:");
$result = $self->cleanup($conf->{root_dir},
                         $Config->{NODES}->{RESOURCEMANAGER}->{HOST});
is($result, 0, "Clean up tmp dir: '$conf->{root_dir}'");





# Test the new tasks per job to accept limit will result in failure.
sub test_unrefreshed_limits {
    my ($default_limits) = @_;
    note("---> Test unrefreshed limits:");

    # Set new limits (baseline values+1) in the temp config dir
    note("---> Set the capacity limits to test in the new config dir:");
    my $diff = 1;
    my $target_limits = {};

    foreach my $prop_name (sort(keys %$default_limits)) {
        note("prop name = $prop_name");
        $target_limits->{$prop_name} = $default_limits->{$prop_name} + $diff;
    }
    my $queue_name = "root.default" if ($Config->{YARN_USED});
    my $queue_config = {
        "maximum-applications" => 30,
        "minimum-allocation-mb" => 1024,
        "maximum-am-resource-percent" => 0.1,
    };
    my $target_limit = $cs->construct_target_limits($queue_config,
                                                    $queue);
    $target_limit->{option} = 'no-restart';
    my ($conf, $config_limits, $limits, $logged_limits) =
        $cs->test_set_config_limits($target_limit, 
                                    $source_conf_dir);
    note("Setup: 1) Config Limits:");
    note(explain($config_limits));
    note("Setup: 2) Derived Limits:");
    note(explain($limits));
    note("Setup: 3) Log Limits:");
    note(explain($logged_limits));

    $limits = $limits->{$Config->{DEFAULT_QUEUE}} if ($Config->{YARN_USED});
    note(explain($limits));

    # TODO ##########################
    my $hash = {'limits' => $limits,
                'queue'  => $queue_name,
                'entity' => 'jobs',
                'type'   => 'user',
                'state'  => 'init'};

    my $entity = $hash->{entity} if defined($hash->{entity});
    my $state  = $hash->{state}  if defined($hash->{state});
    my $level  = $hash->{level}  if defined($hash->{level});

    my $num_jobs_over_limit = 1;
    my ($true_limit, $test_limit) =
        $cs->get_test_limits($limits, $entity, $type, $state, $level,
                             $num_jobs_over_limit, undef, $queue_name);

    note("true limit = ",explain($true_limit));
    note("test limit = ",explain($test_limit));

    return;
    ###########################

    my $max_tasks_per_job_to_accept = $limits->{max_tasks_per_job_to_accept};
    note("max tasks per job to accept = $max_tasks_per_job_to_accept");
    my $user            = $self->{DEFAULT_USER};
    my $num_map_task    = 0;
    my $num_reduce_task = $max_tasks_per_job_to_accept;

    # bug workaround
    # my $num_map_task    = 1;
    # my $num_reduce_task = 1;

    my $map_sleep       = 1000;
    my $reduce_sleep    = 1000;
    my $should_fail     = 1;
    my $MAX_TASKS_PROP  = "maximum-initialized-active-tasks-per-user";
    my $DEFAULT_MAX_TASKS_PER_JOB_TO_ACCEPT = 100000;
    my $error_message =
        ($max_tasks_per_job_to_accept >= $DEFAULT_MAX_TASKS_PER_JOB_TO_ACCEPT) ?
        'The number of tasks for this job \d+ exceeds the configured limit \d+' :
        'Job .*. from user .*. rejected since it has '.$num_reduce_task.
        ' tasks which exceeds the limit of '.
        $default_limits->{"$MAX_TASKS_PROP"}.' tasks per-user';

    note("---> Test job with tasks equal to the unrefreshed new max_tasks ".
         "per job to accept should fail:");
    $self->test_run_sleep_job({'map_task'     => $num_map_task,
                               'reduce_task'  => $num_reduce_task,
                               'map_sleep'    => $map_sleep,
                               'reduce_sleep' => $reduce_sleep,
                               'user'         => $user},
                              $should_fail, $error_message);


    note("---> Test job with tasks equal to current max_tasks per job to accept ".
         "should pass:");
    $num_reduce_task = $max_tasks_per_job_to_accept - $diff;
    $self->test_run_sleep_job({'map_task'     => $num_map_task,
                               'reduce_task'  => $num_reduce_task,
                               'map_sleep'    => $map_sleep,
                               'reduce_sleep' => $reduce_sleep,
                               'user'         => $user});

    return $limits;
}

# Test new limits have taken effect after queues have been refreshed
sub test_refresh_limits {
    my ($limits) = @_;
    note("---> Test refreshed limits:");

    note("---> Refresh the queue on the ResourceManager via the gateway:");
     $self->run_hadoop_command($refresh_queues, 'YARN');
    
    note("---> Test job with tasks at new max_tasks per job to accept should pass:");
    my $max_tasks_per_job_to_accept = $limits->{max_tasks_per_job_to_accept};
    my $user            = $self->{DEFAULT_USER};
    my $num_map_task    = 1;
    my $num_reduce_task = $max_tasks_per_job_to_accept;
    my $map_sleep       = 1000;
    my $reduce_sleep    = 1000;
    $self->test_run_sleep_job({'map_task'     => $num_map_task,
                               'reduce_task'  => $num_reduce_task,
                               'map_sleep'    => $map_sleep,
                               'reduce_sleep' => $reduce_sleep,
                               'user'         => $user});
}

# Test invalid queue capacity
sub test_invalid_queue_capacity {
    my ($conf, $queue_name) = @_;
    $queue_name = "default" unless defined($queue_name);

    note("---> Test invalid queue capacity:");
    my $base_prop_name = 'capacity';
    my $orig_prop_value =
        $self->get_xml_prop_value( $cs->get_full_prop_name($base_prop_name, $queue_name),
                                   $conf->{local_cap_sched_conf},
                                   $Config->{NODES}->{RESOURCEMANAGER}->{HOST},
                                   undef,
                                   '</name>' );
    my $diff = 1;
    my $new_prop_value = $orig_prop_value + $diff;
    my $aggregate_capacity = 100 + $diff;

    note("---> Set capacity of '$queue_name' queue to plus 1 value of ".
         "'$new_prop_value':");
    $cs->set_config_limits(
        $cs->construct_target_limits({"capacity" => $new_prop_value}, $queue_name),
        $conf);

    $new_prop_value =
        $self->get_xml_prop_value( $cs->get_full_prop_name($base_prop_name, $queue_name),
                                   $conf->{local_cap_sched_conf},
                                   $Config->{NODES}->{RESOURCEMANAGER}->{HOST},
                                   undef,
                                   '</name>' );
    note("new config value = '$new_prop_value'");

    note("---> Refresh the queue on the ResourceManager via the gateway should ".
         "result in error:");
    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command($refresh_queues, 'YARN');
    note("stdout = '$stdout'");
    note("stderr = '$stderr'");
    my $error_message =
        "refreshQueues: java.io.IOException: ".
        "java.lang.IllegalArgumentException: ".
        "Sum of queue capacities not 100% at $aggregate_capacity.0";
    like($stderr, qr/$error_message/,
         "Check refresh queues should fail when total capacity is not 100");

    note("---> Set capacity of default queue back to the original value of ".
         "'$orig_prop_value'");
    $cs->set_config_limits(
        $cs->construct_target_limits({"capacity" => $orig_prop_value}, $queue_name),
        $conf);

    $new_prop_value =
        $self->get_xml_prop_value( $cs->get_full_prop_name($base_prop_name, $queue_name),
                                   $conf->{local_cap_sched_conf},
                                   $Config->{NODES}->{RESOURCEMANAGER}->{HOST},
                                   undef,
                                   '</name>' );
    note("new config value = '$new_prop_value'");

    note("---> Refresh the queue on the ResourceManager via the gateway should ".
         "run successfully:");
    ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command($refresh_queues, 'YARN');
    note("stdout = '$stdout'");
    note("stderr = '$stderr'");
    is($exit_code, 0,
       "Check refresh queues should succeed when total capacity is 100.");
}

# Test new queue with invalid aggregate capacity
# Test new queue in stopped state cannot process job
# Test new queue in running state should process job successfully
sub test_new_queues {
    my ($conf, $queue) = @_;
    $self->copy_conf_files_to_config_dir("$Bin/config/new_queues",
                                         $conf->{root_dir},
                                         $Config->{NODES}->{RESOURCEMANAGER}->{HOST});

    # Bugzilla Ticket 5701254 - refreshQueues gives a vague error meesage when
    # queue capacity is misconfigured

    # Test new queue with invalid aggregate capacity will fail on refresh.
    note("---> Test invalid new queue: $queue");
    test_invalid_queue_capacity($conf, $queue);
  
    # Bug:5701942 Error recovery for new queue refresh is broken and causes
    # subsequent retries to always fail

    # Test new queue in stopped state should NOT process job successfully.
    note("---> Test stopped new queue: $queue");
    test_stopped_new_queues($conf);

    # Test new queue in running state should process job successfully.
    note("---> Test submit job to new queue: $queue");
    test_running_new_queues($conf);
}


# Test new queue in stopped state should NOT process job successfully.
# This test relies on test_new_queues for setup and cannot be run independently.
sub test_stopped_new_queues {
    my ($conf) = @_;
    note("---> Test stopped new queue:");
    my $old_state = 'RUNNING';
    my $new_state = 'STOPPED';
    $self->replace_value($old_state,
                         $new_state,
                         $conf->{local_cap_sched_conf},
                         $Config->{NODES}->{RESOURCEMANAGER}->{HOST},
                         undef, undef, 'all');

    note("---> Refresh the queue on the ResourceManager via the gateway:");
    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command($refresh_queues, 'YARN');
    note("stdout = '$stdout'");
    note("stderr = '$stderr'");
    is($exit_code, 0, "Check refresh queues ran without error");

    my $queue_name      = 'a3';
    my $user            = $self->{DEFAULT_USER};
    my $num_map_task    = 0;
    my $num_reduce_task = 0;
    my $map_sleep       = 1000;
    my $reduce_sleep    = 1000;
    my $should_fail     = 1;
    my $error_message = 
        "org.apache.hadoop.security.AccessControlException: ".
        "Queue root.a.$queue_name is STOPPED. ".
        "Cannot accept submission of application:";

    note("---> Test submitting job to a new queue '$queue_name' in a stopped ".
         "state should FAIL:");
    $self->test_run_sleep_job({'map_task'     => $num_map_task,
                               'reduce_task'  => $num_reduce_task,
                               'map_sleep'    => $map_sleep,
                               'reduce_sleep' => $reduce_sleep,
                               'user'         => $user,
                               'queue'        => $queue_name},
                              $should_fail, $error_message);
    $queue_name = 'default';
    note("---> Test submitting job to an existing queue '$queue_name' should ".
         "SUCCEED:");
    $self->test_run_sleep_job({'map_task'     => $num_map_task,
                               'reduce_task'  => $num_reduce_task,
                               'map_sleep'    => $map_sleep,
                               'reduce_sleep' => $reduce_sleep,
                               'user'         => $user,
                               'queue'        => $queue_name});
}

# Test new queue in running state should process job successfully.
# This test relies on test_new_queues for setup and cannot be run independently.
sub test_running_new_queues {
    my ($conf) = @_;
    note("---> Test running new queue:");
    my $old_state = 'STOPPED';
    my $new_state = 'RUNNING';
    $self->replace_value($old_state,
                         $new_state,
                         $conf->{local_cap_sched_conf},
                         $Config->{NODES}->{RESOURCEMANAGER}->{HOST},
                         undef, undef, 'all');
    note("---> Refresh the queue on the ResourceManager via the gateway:");

    my ($stdout, $stderr, $success, $exit_code) =
        $self->run_hadoop_command($refresh_queues, 'YARN');
    note("stdout = '$stdout'");
    note("stderr = '$stderr'");
    is($exit_code, 0, "Check refresh queues ran without error ");

    my @queue_names     = ('a3','a4');
    my $user            = $self->{DEFAULT_USER};
    my $num_map_task    = 0;
    my $num_reduce_task = 0;
    my $map_sleep       = 1000;
    my $reduce_sleep    = 1000;
    foreach my $queue_name (@queue_names) {
        note("---> Test submitting job to a new queue '$queue_name' in a ".
             "running state should SUCCEED:");
        $self->test_run_sleep_job({'map_task'     => $num_map_task,
                                   'reduce_task'  => $num_reduce_task,
                                   'map_sleep'    => $map_sleep,
                                   'reduce_sleep' => $reduce_sleep,
                                   'user'         => $user,
                                   'queue'        => $queue_name});
    }
}
