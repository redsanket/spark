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
use POSIX;

use Test::More tests => 60;
$ENV{OWNER} = 'philips';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;

my ($key, $config_hash, %global_settings, %mapper_settings, %reducer_settings);

my ($queue, $cluster_capacity, $queue_capacity, $mulp, $user_queue_capacity);
my ($should_fail, $error_message);
my ($stdout, $stderr, $success, $exit_code);
my ($result, $user, $type, $exp_str, $new_cs_config);
my @command;


# # verify cluster_capacity and queue_capacity against log ???
# # user limit factor
# #my $init_limits = $self->get_queue_limits_init();
# #note(explain($init_limits));

$queue='default';

# #################################################################################
# # Use customer capacity scheduler where user limit is 100%
# #################################################################################
$new_cs_config="$Bin/../../../../data/conf/capacity-scheduler100.xml";
$self->restart_resourcemanager_with_new_cs_conf($new_cs_config);
$cluster_capacity=$self->get_cluster_capacity();
$queue_capacity=$self->get_queue_capacity('default', $cluster_capacity);
$mulp=$self->get_min_user_limit_percent($queue);
note("user limit percent = '$mulp'");
$user_queue_capacity=ceil(($queue_capacity*$mulp)/100);
note("user queue capacity = '$user_queue_capacity'");

# TODO 
# we should test that it is below the max caapcity and above the capacity...

# test_capacity_scheduler({'queue'        => $queue,
#                          'capacity'     => $user_queue_capacity,
#                          'num_jobs'     => 1,
#                          'tasks_factor' => 2,
#                          'wait_time'    => 0,
#                          'num_tests'    => 5});
 
# test_capacity_scheduler({'queue'        => $queue,
#                          'capacity'     => $user_queue_capacity,
#                          'num_jobs'     => 2,
#                          'tasks_factor' => 0.5,
#                          'wait_time'    => 0,
#                          'num_tests'    => 5});
# 
test_capacity_scheduler({'queue'        => $queue,
                         'capacity'     => $user_queue_capacity,
                         'num_jobs'     => 2,
                         'tasks_factor' => 2,
                         'wait_time'    => 0,
                         'num_tests'    => 5});
# 
# 
# #################################################################################
# # Use customer capacity scheduler where user limit is 25%
# #################################################################################
# $new_cs_config="$Bin/../../../../data/conf/capacity-scheduler25.xml";
# $self->restart_resourcemanager_with_new_cs_conf($new_cs_config);
# $cluster_capacity=$self->get_cluster_capacity();
# $queue_capacity=$self->get_queue_capacity('default', $cluster_capacity);
# $mulp=$self->get_min_user_limit_percent($queue);
# note("user limit percent = '$mulp'");
# $user_queue_capacity=ceil(($queue_capacity*$mulp)/100);
# note("user queue capacity = '$user_queue_capacity'");
# 
# test_capacity_scheduler({'queue'        => $queue,
#                          'capacity'     => $user_queue_capacity,
#                          'num_jobs'     => 2,
#                          'tasks_factor' => 2,
#                          'wait_time'    => 0,
#                          'num_tests'    => 5});

# test_capacity_scheduler({'queue'        => $queue,
#                          'capacity'     => $queue_capacity,
#                          'num_jobs'     => 2,
#                          'tasks_factor' => 2,
#                          'tasks_sleep'  => 2000,
#                          'wait_time'    => 0,
#                          'num_tests'    => 5,
#                          'multi_users'  => 1});

# test_capacity_scheduler({'queue'        => $queue,
#                          'capacity'     => $queue_capacity,
#                          'num_jobs'     => 4,
#                          'tasks_factor' => 3,
#                          'tasks_sleep'  => 2000,
#                          'wait_time'    => 0,
#                          'num_tests'    => 10,
#                          'num_parallel' => 4,
#                          'multi_users'  => 1});

# test_capacity_scheduler({'queue'        => $queue,
#                          'capacity'     => $queue_capacity,
#                          'num_jobs'     => 6,
#                          'tasks_factor' => 3,
#                          'tasks_sleep'  => 2000,
#                          'wait_time'    => 0,
#                          'num_tests'    => 10,
#                          'num_parallel' => 4,
#                          'multi_users'  => 1});

# NOT PASSING
# # if job has small # of tasks, the wait time should be longer, else it will be gone completely 
# my $users=['hadoop1', 'hadoop2', 'hadoop3', 'hadoop4','hadoop2', 'hadoop5', 'hadoop6'];
# my $start_order=
#     test_capacity_scheduler({'queue'        => $queue,
#                              'capacity'     => $queue_capacity,
#                              'num_jobs'     => 7,
#                              'users'        => $users,
#                              'tasks_factor' => 4,
#                              'tasks_sleep'  => 1000,
#                              'wait_time'    => 0,
#                              'num_tests'    => 15,
#                              'num_parallel' => 4,
#                              'check_order'  => 1,
#                              'multi_users'  => 1});

# Test start order


# sub submit_sleep_job_wo_wait {


# #################################################################################
# # Use customer capacity scheduler where user limit is 25%
# #################################################################################
# $new_cs_config="$Bin/../../../../data/conf/capacity-scheduler41.xml";
# $self->restart_resourcemanager_with_new_cs_conf($new_cs_config);
# $cluster_capacity=$self->get_cluster_capacity();
# $queue_capacity=$self->get_queue_capacity('default', $cluster_capacity);
# $mulp=$self->get_min_user_limit_percent($queue);
# note("user limit percent = '$mulp'");
# $user_queue_capacity=ceil(($queue_capacity*$mulp)/100);
# note("user queue capacity = '$user_queue_capacity'");
# 
# test_capacity_scheduler({'queue'        => $queue,
#                          'capacity'     => $queue_capacity,
#                          'num_jobs'     => 4,
#                          'tasks_factor' => 4,
#                          'tasks_sleep'  => 2000,
#                          'wait_time'    => 0,
#                          'num_tests'    => 10,
#                          'num_parallel' => 3,
#                          'multi_users'  => 1});

#################################################################################
# Use customer capacity scheduler where user limit is 110%
#################################################################################
# $new_cs_config="$Bin/../../../../data/conf/capacity-scheduler110.xml";
# $self->restart_resourcemanager_with_new_cs_conf($new_cs_config);
# $cluster_capacity=$self->get_cluster_capacity();
# $queue_capacity=$self->get_queue_capacity('default', $cluster_capacity);
# $mulp=$self->get_min_user_limit_percent($queue);
# note("user limit percent = '$mulp'");
# $user_queue_capacity=ceil(($queue_capacity*$mulp)/100);
# note("user queue capacity = '$user_queue_capacity'");
# 
# test_capacity_scheduler({'queue'        => $queue,
#                          'capacity'     => $queue_capacity,
#                          'num_jobs'     => 2,
#                          'tasks_factor' => 4,
#                          'tasks_sleep'  => 2000,
#                          'wait_time'    => 0,
#                          'num_tests'    => 10,
#                          'num_parallel' => 3,
#                          'multi_users'  => 0});

# TODO: Test with user limit factor set to be greater than 1.


#################################################################################
# Use customer capacity scheduler where user limit is 0%
#################################################################################
# User limit at 0% - commented out due to invalid bug #4592235
#setupNewCapacitySchedulerConfFile "$WORKSPACE/data/conf/capacity-scheduler0.xml"
#test_userLimit_Zero

# CLEANUP
$self->cleanup_resourcemanager_conf;



#################################################################################
# Verify the number of tasks run do not exceeded the queue limit
#################################################################################
sub test_capacity_scheduler {
    my ($args) = @_;    

    my $queue          = defined($args->{queue})        ? $args->{queue}        : 'default';
    my $capacity       = defined($args->{capacity})     ? $args->{capacity}     : 0;
    my $num_jobs       = defined($args->{num_jobs})     ? $args->{num_jobs}     : 1;
    my $factor         = defined($args->{tasks_factor}) ? $args->{tasks_factor} : 1;
    my $task_sleep     = defined($args->{tasks_sleep})  ? $args->{tasks_sleep}  : 2000;
    my $num_tests      = defined($args->{num_tests})    ? $args->{num_tests}    : 10;
    my $multi_users    = defined($args->{multi_users})  ? $args->{multi_users}  : 0;
    my $users          = defined($args->{users})        ? $args->{users}        : 0;
    my $wait_time      = defined($args->{wait_time})    ? $args->{wait_time}    : int($task_sleep/1000)+int($num_jobs*5);
    my $num_parallel   = defined($args->{num_parallel}) ? $args->{num_parallel} : 0;

    my $check_order    = defined($args->{check_order} ) ? $args->{check_order}  : 0;

    # note("---> Test Capacity Scheduler: queue='$queue', ".
    #      "qeue capacity='$capacity', num_jobs='$num_jobs', ".
    #      "factor='$factor': ");
    note("---> Test Capacity Scheduler: ", explain($args));
    note("Verify the number of tasks run do not exceeded the queue limit:");
    my (@job_ids, @num_map_tasks, @num_reduce_tasks, @total_job_tasks, $total_queue_tasks);
    my @test_job_process_order;
    my @starting_order;
    my $num_jobs_tasks;
    my $jobs_running_in_parallel=0;
    # my $buffer=0.98;
    my $buffer=1;
    my $default_user='hadoopqa';
    my $num_tasks=int($capacity*$factor);
    my $map_tasks=$num_tasks;
    my $reduce_tasks=$num_tasks;
    my $total_tasks_per_job = ($map_tasks+$reduce_tasks);
    my @args=("-Dmapreduce.map.memory.mb=1024",
              "-Dmapreduce.reduce.memory.mb=1024",
              "-Dyarn.app.mapreduce.am.resource.mb=2048");
    push(@args, '-Dmapreduce.job.acl-view-job=*');
    my $job_hash = { 'map_task'     => $map_tasks,
                     'reduce_task'  => $reduce_tasks,
                     'map_sleep'    => $task_sleep,
                     'reduce_sleep' => $task_sleep,
                     'user'         => $default_user,
                     'queue'        => $queue,
                     'args'         => \@args };

    # SUBMIT JOBS
    my @log_files;
    foreach my $job_count (1..$num_jobs) {
        if ($multi_users) {
            if ($users) {
                $job_hash->{user} = $users->[($job_count-1)];
            }
            else {
                $job_hash->{user} = 'hadoop'.((($job_count-1) % 20)+1);
            }
        }
        note("User=$job_hash->{user}");

        push(@job_ids, $self->submit_sleep_job($job_hash));
        note("Submitted job id '".$job_ids[($job_count-1)]."'");
        #my $log_file = $self->submit_sleep_job_wo_wait($job_hash);
        #push(@log_files, $log_file);
    }
    #@job_ids = $self->parse_job_ids(\@log_files);
    note("Submitted jobs: ".join(',',@job_ids));
    
    $self->wait_for_tasks_state($job_ids[0], 'MAP', 'running', undef, undef, undef, 0);
    foreach my $index (1..$num_tests) {

        note("**************************************************");
        note("Check jobs status=");
        my ($stdout, $stderr, $success, $exit_code) =
            $self->run_hadoop_command( {
                command => 'job',
                args => ['-list'],
            } );
        note("$stdout");

        my $jobs_status = $self->get_jobs_status();
        note("Check jobs status=",explain($jobs_status));

        $total_queue_tasks=0;

        # my $num_jobs_tasks = $self->get_num_jobs_tasks(\@job_ids, 'running', 0);
        # note("**************************************************");
        # note("Check running tasks status=",explain($num_jobs_tasks));

        foreach my $j_index (0..$#job_ids) {
            #$num_reduce_tasks[$j_index] = $num_jobs_tasks->{$job_ids[$j_index]}->{REDUCE};
            #$num_map_tasks[$j_index]    = $num_jobs_tasks->{$job_ids[$j_index]}->{MAP};

            #$total_job_tasks[$j_index]  =($num_map_tasks[$j_index]+$num_reduce_tasks[$j_index]);
            $total_job_tasks[$j_index] =
                defined($jobs_status->{$job_ids[$j_index]}->{UsedContainers}) ?
                $jobs_status->{$job_ids[$j_index]}->{UsedContainers} : 0;
            
            # note("Number of Job ".($j_index+1)." '$job_ids[$j_index]' tasks: ".
            #      "m tasks='$num_map_tasks[$j_index]', ".
            #      "r tasks='$num_reduce_tasks[$j_index]', ".
            #      "total job tasks='$total_job_tasks[$j_index]'");
            ok(($total_job_tasks[$j_index] <= $capacity),
               "Number of Job ".($j_index+1).
               " tasks of '$total_job_tasks[$j_index]' should be ".
               "<= queue capacity of '$capacity'");
            $total_queue_tasks+=$total_job_tasks[$j_index];

            # Track job start order
            unless (grep {$_ == $j_index} @starting_order) {
                # push(@starting_order, $j_index) if ($num_map_tasks[$j_index] > 0);
                push(@starting_order, $j_index) if ($total_job_tasks[$j_index] > 0);
            }
        }

        note("**************************************************");
        note("Check total running tasks status=",join(',',@total_job_tasks));

        note("starting order=".join(",",@starting_order));
        my $buffer_total=int($total_queue_tasks*$buffer);
        ok(($buffer_total <= $capacity),
           "Number of total queue tasks of '$buffer_total' should be ".
           "<= queue capacity of '$capacity': tasks per job=".join(',',@total_job_tasks));

        # TEST JOBS RUNNING IN PARALLEL
        if (($num_jobs > 1) && ($multi_users) && ($num_parallel) && ($total_job_tasks[0])) {
            my $all_para_inst=1;
            my $num_jobs_running=1;
            foreach my $j_index (1..$#job_ids) {
                $num_jobs_running++ if ($total_job_tasks[$j_index] > 0);
            }
            note("number of jobs running concurrently: $num_jobs_running");
            $all_para_inst=0 if ($num_jobs_running < $num_parallel);

            # First instance where all jobs are running, set flag value to 1.
            $jobs_running_in_parallel=1 if
                (($all_para_inst) && ($jobs_running_in_parallel < 1));
            # Any instance where number of jobs running are greater than the
            # expected number of concurrent jobs, set flag value to 2.
            if ($num_jobs_running > $num_parallel) {
                note("ERROR: Number of concurrent jobs running ".
                     "'$num_jobs_running' is greater than the expected number ".
                     "concurrent jobs ($num_parallel)to be running.");
                $jobs_running_in_parallel=2;
            }
        }

        $num_jobs_tasks = $self->get_num_jobs_tasks(\@job_ids, 'completed', 0);
        note("**************************************************");
        note("Check completed tasks status=",explain($num_jobs_tasks));

        # If tasks of a job exceeds the queue capacity, verify subsequent jobs
        # are queue up and have 0 tasks running.
        if (($num_jobs > 1) && ($total_tasks_per_job >= $capacity) && ($multi_users==0)) {
            # my $num_jobs_tasks = $self->get_num_jobs_tasks(\@job_ids, 'completed', 1);
            # Test for each job except for the last one.
            foreach my $j_index (0..($num_jobs-2)) {
                my $completed_reduce_tasks = $num_jobs_tasks->{$job_ids[$j_index]}->{REDUCE};
                my $completed_map_tasks    = $num_jobs_tasks->{$job_ids[$j_index]}->{MAP};
                my $completed_tasks = $completed_map_tasks+$completed_reduce_tasks;
                note("Number of completed tasks for Job ".($j_index+1).
                     ": m tasks '$completed_map_tasks', r tasks ".
                     "'$completed_reduce_tasks', total '$completed_tasks'");
                my $incomplete_tasks=($total_tasks_per_job - $completed_tasks);
                my $incomplete_map_tasks=($map_tasks-$completed_tasks);

                # if ($incomplete_tasks >= $capacity) {
                if ($incomplete_map_tasks >= $capacity) {
                    note("total_task_per_job=$total_tasks_per_job");
                    note("queue capacity=$capacity");
                    foreach my $j_index2 ($j_index..($num_jobs-2)) {
                        if (($total_job_tasks[$j_index2+1]) > 0) {
                            note("ERROR: Job tasks for subsequent Job ".
                                 ($j_index2+2)." is greater than zero: ".
                                 "$total_job_tasks[$j_index2+1]: while current ".
                                 "job has incomplete map tasks of ".
                                 "'$incomplete_map_tasks' which is >= the ".
                                 "queue capacity '$capacity'");
                            $test_job_process_order[$j_index2]=1;
                        }
                    }
                }
            }
        }
        note("sleep $wait_time");
        sleep $wait_time;
    }
    
    # ADDITIONAL VERIFICATION STEPS

    note("starting order=".join(",",@starting_order));

    # VERIFY THE CORRECT NUMBER OF CONCURRENT JOBS ARE ACHIEVED
    if (($num_jobs > 1) && ($multi_users) && ($num_parallel)) {
        is($jobs_running_in_parallel,
           1,
           "Jobs submitted by different users should run in parallel with ".
           "$num_parallel concurrent jobs.");
    }

    # VERIFY SOME BASIC TASKS PROCESSING ORDERS BETWEEN JOBS
    if (($num_jobs > 1) && ($total_tasks_per_job >= $capacity)) {
        foreach my $j_index (0..($num_jobs-2)) {
            my $is_valid = defined($test_job_process_order[$j_index]) ? 
                $test_job_process_order[$j_index] : 0;
            is($is_valid,
               0,
               "Tasks for subsequent jobs while Job ".($j_index+1).
               " is taking up the entire queue capacity should be 0.");
        }
    }

    # VERIFY THE JOBS COMPLETED SUCCESSFULLY
    foreach my $j_index (0..$#job_ids) {
        is($self->wait_for_job_to_succeed($job_ids[$j_index],5,60),
           0,
           "Job ".($j_index+1)." '$job_ids[$j_index]' should completed successfully.");
    }
    return \@starting_order;
}
