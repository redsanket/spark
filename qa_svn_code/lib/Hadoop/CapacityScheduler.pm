package Hadoop::CapacityScheduler;

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use POSIX qw(ceil floor);
use List::Util qw(min max);
use Test::More;
use Text::Trim; # ypan/perl-Text-Trim
use String::ShellQuote; # perl-String-ShellQuote

# global constants
our $MRCS = "mapred.capacity-scheduler";
our $JOB_RUNNING_STATE = 1;
our $JOB_SUCCEEDED_STATE = 2;
our $JOB_FAILED_STATE = 3;
our $JOB_PREP_STATE = 4;
our $WAIT_INTERVAL_ALL_JOBS = 20;

my $Config = Hadoop::Config->new;


sub new {
    my ($class, $test) = @_;
    my $self = {test => $test};
    bless ($self,$class);
    return $self;
};


#################################################################################
# Get Methods
#################################################################################

=head2 get_full_prop_name($self, $prop_name, $queue_name)

Get the fully extended property name

=over 2

=item I<Parameters>:

   * Property name
   * Queue name

=item I<Returns>:

   * The full extended property name

=back

=cut
sub get_full_prop_name {
    my ($self, $prop_name, $queue_name) = @_;
    $queue_name = "default" unless defined($queue_name);
    my $prop_prefix = "mapred.capacity-scheduler";
    my $queue_prop_prefix = "$prop_prefix.queue.$queue_name";
    my $prefix = ($prop_name eq 'maximum-system-jobs') ?
        $prop_prefix :
        $queue_prop_prefix;
    return "$prefix.$prop_name";
}


=head2 get_default_limits_prop_name()

Get the default capacity scheduler limit property names

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * The default capacity scheduler limit property names

=back

=cut
sub get_default_limits_prop_names {
    my ($self) = @_;
    return [
        "maximum-system-jobs",
        "maximum-initialized-active-tasks-per-user",
        "maximum-initialized-active-tasks",
        "minimum-user-limit-percent",
        "init-accept-jobs-factor",
        "user-limit-factor",
        "capacity",
        "maximum-capacity"
    ];
}


=head2 get_default_prop_value($self, $prop_name)

Get the default property value for the specified propperty name

=over 2

=item I<Parameters>:

   * Property name

=item I<Returns>:

   * The default value for the specified propperty name

=back

=cut
sub get_default_prop_value {
    my ($self, $prop_name) = @_;
    my $default_value = {
        'maximum-initialized-active-tasks-per-user' => 100000,
        'maximum-initialized-active-tasks'          => 200000,
        'maximum-system-jobs'                       => 5000,
        'minimum-user-limit-percent'                => 100,
        'init-accept-jobs-factor'                   => 10,
        'user-limit-factor'                         => 1,
        'maximum-capacity'                          => 25,  # Default is the cluster capacity or -1, use 25
        };
    my @tokens = split('\.', $prop_name);
    return $default_value->{"$tokens[-1]"};
}


=head2 get_limits($self, $config_file, $queue_name)

Get the capacity scheduler config limits, derived limits, and last limits entry
from jobtracker log

=over 2

=item I<Parameters>:

   * Config Filename
   * Queue Name

=item I<Returns>:

   * 3 Hashes:
     1) hash containing limits defined in the capacity scheduler config file,
     2) hash containing the derived limits, and
     3) hash containing the last limits entry from the jobtracker log

=back

=cut
sub get_limits {
    my ($self, $config_file, $queue_name) = @_;
    $queue_name = "default" unless defined($queue_name);

    # If config file is not passed in, use the one that is currently being used
    # by the JobTracker.
    $config_file =
        $self->{test}->get_jobtracker_config_dir().
        '/local-capacity-scheduler.xml' unless ($config_file);

    note("---> Get the defined capacity limits:"); 
    my $config_limits = $self->get_config_limits($config_file, $queue_name);

    note("---> Get the derived capacity limits:"); 
    my $derived_limits = $self->get_derived_limits($config_limits, $queue_name);

    note("---> Get the current capacity limits from the JobTracker Log:"); 
    my $logged_limits = $self->get_limits_from_jobtracker_log();

    return ($config_limits, $derived_limits, $logged_limits);
}


=head2 get_limits_from_jobtracker_log($self)

Get the capacity scheduler limits from the jobtracker log

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * Hash containing the capacity scheduler configured limits form the
     jobtracker log

=back

=cut
sub get_limits_from_jobtracker_log {
    my ($self) = @_;
    my $str = "\\\"Initializing \\'default\\' queue\\\"";
    my @command = ('grep', $str, $Config->{NODES}->{JOBTRACKER}->{LOG});
    @command = ('ssh', $Config->{NODES}->{JOBTRACKER}->{HOST}, @command,
                '|', 'tail', '-1');
    note("@command");
    my $result = `@command`;
    note("$result");

    my $hash = {};
    if ($result =~
        m/(\d\d\d\d-\d\d-\d\d\s\d\d:\d\d:\d\d)(.*.)(Initializing \'default\' queue with\s+)(.*.)/) {

        $hash->{date} = $1;
        my $values = {};
        my @kvps = split(",", $4);
        foreach my $kvp (@kvps) {
            my @tokens = split("=", $kvp);
            $values->{trim($tokens[0])} = $tokens[1];
        }
        $hash->{values} = $values;

        note("date='$hash->{date}'");
        note(explain($values));
    }
    return $hash;
}


=head2 get_job_error_from_jobtracker_log($self, $job_id)

Get the job status from the jobtracker log

=over 2

=item I<Parameters>:

   * Job ID

=item I<Returns>:

   * Hash containing the capacity scheduler configured limits form the
     jobtracker log

=back

=cut
sub get_job_error_from_jobtracker_log {
    my ($self, $job_id) = @_;
    unless ($job_id) {
        diag("Job id parameter is missing: unable to lookup jobtracker log:");
        return;
    }
    my $str = "$job_id.*.excee";
    my @command = ('grep', '-i', shell_quote("$str"),
                   $Config->{NODES}->{JOBTRACKER}->{LOG});
    @command = ('ssh', $Config->{NODES}->{JOBTRACKER}->{HOST}, @command);
    note("@command");
    my $result = `@command`;
    note("result='$result'");
    return $result;
}


=head2 get_config_limits($self, $config_file, $queue_name, $prop_names)

Get the capacity scheduler configuration limits

=over 2

=item I<Parameters>:

   * Configuration filename
   * Queue name
   * Array containing property names

=item I<Returns>:

   * Hash containing the capacity scheduler configuration limits

=back

=cut
sub get_config_limits {
    my ($self, $config_file, $queue_name, $prop_names) = @_;
    $queue_name = "default" unless defined($queue_name);
    $prop_names = $self->get_default_limits_prop_names() unless ($prop_names);
    $config_file = $Config->{LOCAL_CAPACITY_SCHEDULER_XML} unless ($config_file);
    my $component_host = $Config->{NODES}->{uc('jobtracker')}->{HOST};
    my $config = {};
    foreach my $base_prop_name (@$prop_names) {
        my $prop_name =
            $self->get_full_prop_name($base_prop_name, $queue_name);
        note("Fetch property value for '$prop_name' in '$config_file'");
        my $prop_value = $self->{test}->get_xml_prop_value( $prop_name,
                                                            $config_file,
                                                            $component_host,
                                                            undef,
                                                            '</name>' );
        unless (defined($prop_value)) {
            $prop_value = $self->get_default_prop_value($base_prop_name);
            note("Set undefined property value for '$prop_name' to default ".
                 "value of '$prop_value'");
        }
        $config->{$prop_name} = $prop_value;
    }
    note(explain($config));
    return $config;
}


=head2 get_derived_limits($self, $config, $queue_name)

Get the derived capacity scheduler limits

=over 2

=item I<Parameters>:

   * Hash containing the parsed capacity scheduler configuration file limits
   * Queue name

=item I<Returns>:

   * Hash containing the derived capacity scheduler configuration limits

=back

=cut
sub get_derived_limits {
    my ($self, $config, $queue_name) = @_;

    ################################################################################
    # Actual limits are derived uses the following formula:
    # See $HADOOP_HOME/src/contrib/capacity-scheduler/src/java/org/apache/hadoop/
    # mapred/CapacitySchedulerConf.java
    ################################################################################

    # max_jobs_per_user_to_init     = <max system jobs> * <queue capacity percent> *
    #                                 <min user limit percent>
    # max_jobs_per_queue_to_init    = <max system jobs> * <queue capacity percent>

    # max_jobs_per_user_to_accept   = <max system jobs> * <queue capacity percent> *
    #                                 <job init to accept factor> * <min user limit percent>
    # max_jobs_per_queue_to_accept  = <max system jobs> * <queue capacity percent> *
    #                                 <job init to accept factor>

    # max_tasks_per_job_to_init     = min value of (
    #                                     ceil(<queue hard limit> * <min user limit percent>) and
    #                                     ceil(<queue capacity> * <user limit factor>)
    # max_tasks_per_user_to_init    = min value of (
    #                                     ceil(<queue hard limit> * <min user limit percent>) and
    #                                     ceil(<queue capacity> * <user limit factor>)
    # max_tasks_per_queue_to_init   = <queue hard limit>

    # max_tasks_per_job_to_accept   = $queue_prop_prefix.maximum-initialized-active-tasks-per-user
    #                                 or defaults to 100000
    # max_tasks_per_user_to_accept  = $queue_prop_prefix.maximum-initialized-active-tasks-per-user
    #                                 or defaults to 100000
    # max_tasks_per_queue_to_accept = $queue_prop_prefix.maximum-initialized-active-tasks
    #                                 or defaults to 200000

    ################################################################################
    # Maping to the values in the Code / JobTracker Log / Config File
    # The value in parenthesis are the equivalent names in the JobTracker log.
    # The value in bracket are the equivalent names in the capacity scheduler xml
    ################################################################################
    # max_jobs_per_user_to_init     = (maxJobsPerUserToInit)
    # max_jobs_per_queue_to_init    = (maxJobsToInit)

    # max_jobs_per_user_to_accept   = (maxJobsPerUserToAccept)
    # max_jobs_per_queue_to_accept  = (maxJobsToAccept)

    # max_tasks_per_job_to_init     = 
    # max_tasks_per_user_to_init    = 
    # max_tasks_per_queue_to_init   = [maximum-capacity]

    # max_tasks_per_job_to_accept   = (maxActiveTasksPerUser)
    # max_tasks_per_user_to_accept  = (maxActiveTasksPerUser)
    # max_tasks_per_queue_to_accept = (maxActiveTasks)

    $queue_name = "default" unless defined($queue_name);

    my $max_system_jobs           = $config->{$self->get_full_prop_name("maximum-system-jobs")};
    my $max_active_tasks          = $config->{$self->get_full_prop_name("maximum-initialized-active-tasks")};
    my $max_active_tasks_per_user = $config->{$self->get_full_prop_name("maximum-initialized-active-tasks-per-user")};
    my $queue_capacity_percent    = $config->{$self->get_full_prop_name("capacity")};
    my $min_user_limit_percent    = $config->{$self->get_full_prop_name("minimum-user-limit-percent")};
    my $job_init_to_accept_factor = $config->{$self->get_full_prop_name("init-accept-jobs-factor")};
    my $user_limit_factor         = $config->{$self->get_full_prop_name("user-limit-factor")};
    my $queue_hard_limit          = $config->{$self->get_full_prop_name("maximum-capacity")};

    my $limits = {};
    $limits->{'max_tasks_per_job_to_init'} =
        $self->get_max_tasks_per_job_to_init($queue_hard_limit,
                                             $min_user_limit_percent,
                                             $queue_capacity_percent,
                                             $user_limit_factor);

    $limits->{'max_tasks_per_job_to_accept'} = $max_active_tasks_per_user;

    $limits->{'max_tasks_per_user_to_init'} =
        $self->get_max_tasks_per_user_to_init($queue_hard_limit,
                                              $min_user_limit_percent,
                                              $queue_capacity_percent,
                                              $user_limit_factor);

    $limits->{'max_tasks_per_user_to_accept'} = $max_active_tasks_per_user;

    $limits->{'max_tasks_per_queue_to_init'} = $queue_hard_limit;

    $limits->{'max_tasks_per_queue_to_accept'} = $max_active_tasks;

    $limits->{'max_jobs_per_user_to_init'} =
        $self->get_max_jobs_per_user_to_init($max_system_jobs,
                                             $queue_capacity_percent,
                                             $min_user_limit_percent);

    $limits->{'max_jobs_per_user_to_accept'} =
        $self->get_max_jobs_per_user_to_accept($max_system_jobs,
                                               $queue_capacity_percent,
                                               $min_user_limit_percent,
                                               $job_init_to_accept_factor);

    $limits->{'max_jobs_per_queue_to_init'} =
        $self->get_max_jobs_per_queue_to_init($max_system_jobs,
                                              $queue_capacity_percent);

    $limits->{'max_jobs_per_queue_to_accept'} =
        $self->get_max_jobs_per_queue_to_accept($max_system_jobs,
                                                $queue_capacity_percent,
                                                $job_init_to_accept_factor);

    note(explain($limits));
    note("hard limit=max_tasks_per_queue_to_init");
    return $limits;
}


# Get task limits per job

# . Get the maximum number of tasks per job that can be initialized to run at
#   any given time. (?) Subsequent tasks are queued on disk.
# . The value of max tasks per user to init is equal to the minimum between:
#   ceil(<queue hard limit> * <min user limit percent>) and
#   ceil(<queue capacity> * <user limit factor>
# . This is the same as the maximum number of tasks per user that can be
#   initialized.
sub get_max_tasks_per_job_to_init {
    my ($self, $queue_hard_limit, $min_user_limit_percent, $queue_capacity_percent,
        $user_limit_factor);
    return get_max_tasks_per_user_to_init(@_);
}

# . Get the maximum number of tasks per job that can be initialized to run at
#   any given time. Job with tasks greater than the maximum will fail.
# . See get_derived_limits
# sub get_max_tasks_per_job_to_accept {
# }

# Get task limits per user

# . Get the maximum number of tasks per user that can be initialized to run at
#   any given time. (?) Subsequent tasks are queued on disk.
# . The value of max tasks per user to init is equal to the minimum between:
#   ceil(<queue hard limit> * <min user limit percent>) and
#   ceil(<queue capacity> * <user limit factor>
sub get_max_tasks_per_user_to_init {
    my ($self, $queue_hard_limit, $min_user_limit_percent, $queue_capacity_percent,
        $user_limit_factor) = @_;
    return min( ceil($queue_hard_limit * $min_user_limit_percent/100),
                ceil($user_limit_factor * $queue_capacity_percent/100) );
}

# . Get the maximum number of tasks per user that can be accepted at any given
#   time. (?) Subsequent tasks are queued on disk.
# . See get_derived_limits
# sub get_max_tasks_per_user_to_accept {
# }

# Get task limits per queue

# . Get the maximum number of tasks per queue that can be initialized to run at
#   any given time. (?) Subsequent tasks are queued on disk.
# . See get_derived_limits
# sub get_max_tasks_per_queue_to_init {
# }

# . Get the maximum number of tasks per queue that can be accepted at any given
#   time. (?) Subsequent jobs are queued on disk.
# . See get_derived_limits
# sub get_max_tasks_per_queue_to_accept {
# }

# Get job limits per user

# . Get the maximum number of jobs per user that can be initialized to run
#   at any give time. Subsequent jobs will be queued on disk.
# . The value of max jobs per user to init is equal to the ceiling of:
#   <max system jobs> * <queue capacity percent> * <min user limit percent>
sub get_max_jobs_per_user_to_init {
    my ($self, $max_system_jobs, $queue_capacity_percent,
        $min_user_limit_percent) = @_;
    return ceil($self->get_max_jobs_per_queue_to_init($max_system_jobs,
                                                      $queue_capacity_percent)
                 * $min_user_limit_percent/100);
}

# . Get the maximum number of jobs per user that can be accepted at any give time.
#   Subsequent jobs submitted will be rejected.
# . The value of max jobs per user to accept is equal to the ceiling of:
#   <max system jobs> * <queue capacity percent> * <min user limit percent> *
#   <job init to accept factor>
sub get_max_jobs_per_user_to_accept {
    my ($self, $max_system_jobs, $queue_capacity_percent,
        $min_user_limit_percent, $job_init_to_accept_factor) = @_;
    return ceil($self->get_max_jobs_per_user_to_init($max_system_jobs,
                                                     $queue_capacity_percent,
                                                     $min_user_limit_percent)
                * $job_init_to_accept_factor);
}

# Get job limits per queue

# . Get the the maximum number of jobs per queue that can be initialized to run
#   at any give time. Subsequent jobs will be queued on disk.
# . The value of max jobs per queue to init is equal to the ceiling of:
#   <max system jobs> * <queue capacity percent>
sub get_max_jobs_per_queue_to_init {
    my ($self, $max_system_jobs, $queue_capacity_percent) = @_;
    return ceil($max_system_jobs * $queue_capacity_percent/100);
}

# . Get the maximum number of jobs per queue that can be accepted at any give time.
#   Subsequent jobs submitted will be rejected.
# . The value of max jobs per queue to accept is equal to the ceiling of:
#   <max system jobs> * <queue capacity percent> * <job init to accept factor>
sub get_max_jobs_per_queue_to_accept {
    my ($self, $max_system_jobs, $queue_capacity_percent,
        $job_init_to_accept_factor) = @_;
    return ceil($self->get_max_jobs_per_queue_to_init($max_system_jobs,
                                                      $queue_capacity_percent)
                * $job_init_to_accept_factor);
}


#################################################################################
# Set Methods
#################################################################################

# Base on the settings and formula above, the derived limits will be:
# -------------------------------------------------------------------------------
# max_jobs_per_user_to_init     = 1   (maxJobsPerUserToInit) (i.e. maximum-system-jobs*queue-capacity*minimum-user-limit-percent)
# max_jobs_per_queue_to_init    = 2   (maxJobsToInit)        (i.e. maximum-system-jobs*queue-capacity)
# -------------------------------------------------------------------------------
# max_jobs_per_user_to_accept   = 2   (maxJobsPerUserToAccept)
# max_jobs_per_queue_to_accept  = 4   (maxJobsToAccept)
# -------------------------------------------------------------------------------
# max_tasks_per_job_to_init     = 5
# max_tasks_per_user_to_init    = 5
# max_tasks_per_queue_to_init   = 25  [maximum-capacity]
# -------------------------------------------------------------------------------
# max_tasks_per_job_to_accept   = 10  (maxActiveTasksPerUser)
# max_tasks_per_user_to_accept  = 10  (maxActiveTasksPerUser)
# max_tasks_per_queue_to_accept = 20  (maxActiveTasks)
# -------------------------------------------------------------------------------
sub set_config1 {
    my ($self) = @_;
    return $self->test_set_config_limits($self->construct_target_limits(
        {
            "maximum-system-jobs"                       => 8,  # default = 5000
            "capacity"                                  => 25, # no default value
            "minimum-user-limit-percent"                => 50, # default = 100
            "init-accept-jobs-factor"                   => 2,  # default = 10
            "user-limit-factor"                         => 20, # default = 1
            "maximum-capacity"                          => 25, # default = -1 (i.e.cluster limit)
            "maximum-initialized-active-tasks-per-user" => 10, # Default = 100000
            "maximum-initialized-active-tasks"          => 20  # Defautl = 200000
        }));
}

# Base on the settings and formula above, the derived limits will be:
# -------------------------------------------------------------------------------
# max_jobs_per_user_to_init     = 2   (maxJobsPerUserToInit) (i.e. maximum-system-jobs*queue-capacity*minimum-user-limit-percent)
# max_jobs_per_queue_to_init    = 4   (maxJobsToInit)        (i.e. maximum-system-jobs*queue-capacity)
# -------------------------------------------------------------------------------
# max_jobs_per_user_to_accept   = 4   (maxJobsPerUserToAccept)
# max_jobs_per_queue_to_accept  = 6   (maxJobsToAccept)
# -------------------------------------------------------------------------------
# max_tasks_per_job_to_init     = 10  capacity * user-limit-factor
# max_tasks_per_user_to_init    = 10  capacity * user-limit-factor
# max_tasks_per_queue_to_init   = 25  [maximum-capacity] (i.e. hard limit)
# -------------------------------------------------------------------------------
# max_tasks_per_job_to_accept   = 15  (maxActiveTasksPerUser)
# max_tasks_per_user_to_accept  = 15  (maxActiveTasksPerUser)
# max_tasks_per_queue_to_accept = 40  (maxActiveTasks)
# -------------------------------------------------------------------------------
sub set_config2 {
    my ($self) = @_;
    return $self->test_set_config_limits($self->construct_target_limits(
        {                                                                
            "maximum-system-jobs"                       => 14, # default = 5000
            "capacity"                                  => 25, # no default value
            "minimum-user-limit-percent"                => 50, # default = 100
            "init-accept-jobs-factor"                   => 2,  # default = 10
            "user-limit-factor"                         => 40, # default = 1
            "maximum-capacity"                          => 25, # default = -1 (i.e.cluster limit)
            "maximum-initialized-active-tasks-per-user" => 15, # Default = 100000
            "maximum-initialized-active-tasks"          => 40  # Defautl = 200000
        }));
}

# Base on the settings and formula above, the derived limits will be:
# -------------------------------------------------------------------------------
# max_jobs_per_user_to_init     = 2   (maxJobsPerUserToInit) (i.e. maximum-system-jobs*queue-capacity*minimum-user-limit-percent)
# max_jobs_per_queue_to_init    = 3   (maxJobsToInit)        (i.e. maximum-system-jobs*queue-capacity)
# -------------------------------------------------------------------------------
# max_jobs_per_user_to_accept   = 4   (maxJobsPerUserToAccept)
# max_jobs_per_queue_to_accept  = 6   (maxJobsToAccept)
# -------------------------------------------------------------------------------
# max_tasks_per_job_to_init     = 5   capacity * user-limit-factor
# max_tasks_per_user_to_init    = 5   capacity * user-limit-factor
# max_tasks_per_queue_to_init   = 25  [maximum-capacity]
# -------------------------------------------------------------------------------
# max_tasks_per_job_to_accept   = 10  (maxActiveTasksPerUser)
# max_tasks_per_user_to_accept  = 10  (maxActiveTasksPerUser)
# max_tasks_per_queue_to_accept = 20  (maxActiveTasks)
# -------------------------------------------------------------------------------
sub set_config3 {
    my ($self) = @_;
    return $self->test_set_config_limits($self->construct_target_limits(
        {                                                                
            "maximum-system-jobs"                       => 12, # default = 5000
            "capacity"                                  => 25, # no default value
            "minimum-user-limit-percent"                => 50, # default = 100
            "init-accept-jobs-factor"                   => 2,  # default = 10
            "user-limit-factor"                         => 20, # default = 1
            "maximum-capacity"                          => 25, # default = -1 (i.e.cluster limit)
            "maximum-initialized-active-tasks-per-user" => 10, # Default = 100000
            "maximum-initialized-active-tasks"          => 20  # Defautl = 200000
        }));
}

=head2 construct_target_limits($self, $target_limits, $queue_name)

Construct fully named target limits base on a partially named target limits

=over 2

=item I<Parameters>:

   * Hash containing the partially named target limits
   * Queue name

=item I<Returns>:

   * Hash containing the fully named target limits

=back

=cut
sub construct_target_limits {
    my ($self, $target_limits, $queue_name) = @_;

    $queue_name = "default" unless defined($queue_name);
    my $prop_prefix = "mapred.capacity-scheduler";
    my $queue_prop_prefix = "mapred.capacity-scheduler.queue.$queue_name";

    my $new_target_limits;
    foreach my $prop_name (sort(keys %$target_limits)) {
        my $full_prop_name =
            $self->get_full_prop_name($prop_name, $queue_name);
        $new_target_limits->{$full_prop_name} = $target_limits->{$prop_name};
    }
    return $new_target_limits;
}


=head2 set_config_limits($self, $targeted_limits, $config_file)

Setup the capacity scheduler limits.

=over 2

=item I<Parameters>:

   * Hash reference containing the targeted limits to be set
   * Hash reference containing the config dir and the capacity scheduler xml
     where the settings need to be changed

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE, 
   * A hash containing the configuration directory and the location of the
     local capacity scheduler xml file.

=back

=cut
sub set_config_limits {
    my ($self, $target_limits, $conf) = @_;

    my $component = 'jobtracker';
    my $component_host = $Config->{NODES}->{uc($component)}->{HOST};

    unless ($conf) {
        note("---> Setup a config dir where capacity limits can be set:");
        $conf = $self->{test}->create_new_conf_dir($Config->{NODES}->{JOBTRACKER}->{HOST},
                                                   "$Bin/config/baseline");
    }

    my $result = 0;
    my $config_file = $conf->{local_cap_sched_conf};
    note("---> Set the target capacity limits:"); 
    foreach my $prop_name (sort(keys %$target_limits)) {
        my $prop_value = $target_limits->{$prop_name};
        note("Set ('$prop_name', '$prop_value') in ".
             "('$component_host', '$config_file')");
        $result += $self->{test}->set_xml_prop_value($prop_name,
                                                     $prop_value,
                                                     $config_file,
                                                     $component_host,
                                                     undef,
                                                     '</name>');
    }
    return ($result, $conf);
}

#################################################################################
# Test Methods
#################################################################################

=head2 test_set_config_limits($self, $limits, $type, $level)

Test setting the capacity scheduler configuration limits

=over 2

=item I<Parameters>:

   * Limits hash containing the limits values
   or 
   * Hash containing:
     - limits: Limits hash containing the limits values
     - queue: queue name
     - conf: config hash
     - option: 'no-restart'

=item I<Returns>:

   * 4 Hashes:
     1) hash containing the configuration directory and the location of the
        local capacity scheduler xml file.
     2) hash containing limits defined in the capacity scheduler config file,
     3) hash containing the derived limits, and
     4) hash containing the last limits entry from the jobtracker log

=back

=cut
sub test_set_config_limits {
    my ($self, $target_limits) = @_;

    my $hash = {};
    if (defined($target_limits->{limits})) {
        $hash = $target_limits;
    }
    else {
        $hash = { 'limits' => $target_limits };
    }    

    # Set the capacity scheduler limits
    my ($result, $conf) =
        $self->set_config_limits($hash->{limits}, $hash->{conf});

    # Restarting the JobTracker and tie the new config dir to the JobTracker.
    # The running JobTracker will shutdown if this config dir is removed.
    unless (defined($hash->{option}) && $hash->{option} eq 'no-restart') {
        note("---> Restart the JobTracker:");
        $self->{test}->check_restart_daemon('jobtracker', $conf->{root_dir});
    }

    note("---> Get the capacity limits:");
    my ($config_limits, $limits, $logged_limits) =
        $self->get_limits($conf->{local_cap_sched_conf}, $hash->{queue});

    return ($conf, $config_limits, $limits, $logged_limits);
}


=head2 test_log_shows_job_queued($self, $job_id, $is_true)

Test tasks limits

=over 2

=item I<Parameters>:

   * Job ID
   * is_true

=item I<Returns>:

   * None

=back

=cut
sub test_log_shows_job_queued {
    my ($self, $job_id, $is_true) = @_;

    my $error = $self->get_job_error_from_jobtracker_log($job_id);
    if ($is_true) {
        isnt($error, 
             '',
             "Check jobtracker log shows job '$job_id' has been queued ".
             "due to limit being exceeded.");
    }
    else {
        is($error, 
           '',
           "Check jobtracker log does not show job '$job_id' has been queued ".
           "due to limit being exceeded.");
    }
}


=head2 get_test_limits($self, $limits, $entity, $type, $state, $level)

Test tasks limits

=over 2

=item I<Parameters>:

   * Limits
   * Entity {'tasks', 'jobs'}
   * Type {'job', 'user', 'qeue'}
   * State {'accept', 'init'}
   * Level {'below', 'at', 'above'}

=item I<Returns>:

   * None

=back

=cut
sub get_test_limits {
    my ($self, $limits, $entity, $type, $state, $level, $plus, $minus) = @_;

    $plus ||= 1;
    $minus ||= 1;
    my $limit_prop_name = 'max_'.$entity.'_per_'.$type.'_to_'.$state;
    my $true_limit = $limits->{$limit_prop_name};
    my $test_limit = $true_limit;
    $test_limit-=$minus if (($level eq 'below') && ($test_limit > 1));
    $test_limit+=$plus if ($level eq 'above');
    return ($true_limit, $test_limit);
}


=head2 test_limits($self, $hash, $should_fail, $error_message)

Test jobs limits

=over 2

=item I<Parameters>:

   * A hash reference of:
     - limits: Limits hash containing the limits values
     - entity: Entity {'jobs','tasks'}
     - type:   Type limits, for example per user, per queue. {'job', 'user', 'queue'}
     - state:  State limits, for example to accept, to run. {'accept', 'init'}
     - level:  Relative to the limit to test. {'below', 'at', 'above'}
       <Level> the <Entity> per <Type> to <State> limit
       E.g. above the jobs per user to accept limit
            below the tasks per queue to init limit
            at the tasks per user to accept limit
     - option: Optional. {'multiple-queues'}. 
   * Should fail: 1 for true, 0 for false
   * Error message if failure is expected

=item I<Returns>:

   * Number of jobs ran

=back

=cut
sub test_limits {
    my ($self, $hash, $should_fail, $error_message) = @_;

    unless ($self->{test}->wait_for_jobs_to_complete() == 0) {
        die("No jobs should be running prior to running tests for limits");
    }
    # note("Show no jobs are currently running:");
    # my $all_status = $self->{test}->get_jobs_status(undef, 0);
    # if ($all_status) {
    #     note(explain($all_status));
    #     die("No jobs should be running prior to running tests for limits");
    # }

    if ($hash->{entity} eq 'jobs') {
        return test_jobs_limits(@_);
    }
    elsif ($hash->{entity} eq 'tasks') {
        return test_tasks_limits(@_);
    }
}

=head2 test_tasks_limits($self, $limits, $type, $level)

Test tasks limits

=over 2

=item I<Parameters>:

   * A hash reference of:
     - limits: Limits hash containing the limits values
     - entity: Entity {'tasks'}
     - type:   Type limits, for example per user, per queue. {'job', 'user', 'queue'}
     - state:  State limits, for example to accept, to run. {'accept', 'init'}
     - level:  Relative to the limit to test. {'below', 'at', 'above'}
       <Level> the <Entity> per <Type> to <State> limit
       E.g. above the jobs per user to accept limit
            below the tasks per queue to init limit
            at the tasks per user to accept limit
     - option: Optional. {'different-user', 'same-user', 'across-queues',
                          'multiple-jobs'}. 
   * Should fail: 1 for true, 0 for false
   * Error message if failure is expected

=item I<Returns>:

   * Number of jobs ran

=back

=cut
sub test_tasks_limits{
    my ($self, $hash, $should_fail, $error_message) = @_;
    my $limits = $hash->{limits} if defined($hash->{limits});
    my $entity = $hash->{entity} if defined($hash->{entity});
    my $type   = $hash->{type}   if defined($hash->{type});
    my $state  = $hash->{state}  if defined($hash->{state});
    my $level  = $hash->{level}  if defined($hash->{level});
    my $option = defined($hash->{option}) ? $hash->{option} : '';
    die ("Error: Missing required parameters")
        unless (defined($limits) && defined($entity) && defined($type)
                && defined($state) && defined($level));
    # Setup the true and test limit. True limit is the real limit that we want to
    # test. Test limit is the relative limits below, at, or above the true limit
    # that we want to individually test each time this function is called.
    my ($true_limit, $test_limit) =
        $self->get_test_limits($limits, $entity, $type, $state, $level, 5);
    my $queue;    
    my $user            = 'hadoopqa';
    my $job_count       = 1;
    my @job_ids         = ();
    my $wait_interval   = 1;
    my $verbose         = 0;
    my @queues          = ('default', 'grideng', 'grid-dev', 'gridops')
        if ($option eq 'multiple-queues');
    my $job_hash        = { 'map_task'     => 0,
                            'reduce_task'  => $test_limit,
                            'map_sleep'    => 1000,
                            'reduce_sleep' => 1000,
                            'user'         => $user,
                            'queue'        => $queue };

    note("---> Test submitting ($test_limit) $entity per $type ".uc($level).
         " the ($true_limit) max $entity per $type limit to $state ",
         ($option) ? "($option) " : "", "should ",
         ($should_fail) ? 'FAIL' : 'SUCCEED', ':');

    if ($should_fail) {
        if ($type eq 'job' && $state eq 'accept') {
            $error_message = ($true_limit == 100000) ?
                'The number of tasks for this job \d+ exceeds the configured limit \d+' :
                "Job .*. from user '$user' rejected since it has $test_limit tasks ".
                "which exceeds the limit of $true_limit tasks per-user";
        }
        $self->{test}->test_run_sleep_job($job_hash,
                                          $should_fail,
                                          $error_message);
        return;
    }

    if ($type eq 'job' && $state eq 'init' && $level eq 'above') {
        my $job_id = $self->{test}->submit_sleep_job($job_hash);
        push(@job_ids, $job_id);
        $self->{test}->wait_for_job_state($job_id,
                                          [$JOB_RUNNING_STATE, $JOB_SUCCEEDED_STATE],
                                          $wait_interval, undef,
                                          $verbose);
        is($self->{test}->wait_for_jobs_to_succeed(\@job_ids),
           0,
           "Check previous (".scalar(@job_ids).") submitted background jobs succeeded.");
        return;
    }

    my $num_reduce_task;
    my $total_tasks_submitted = 0;
    my $total_tasks_submitted_in_last_job = 0;
    my $max_tasks_per_user_to_state = $limits->{'max_tasks_per_user_to_'.$state};
    while ($total_tasks_submitted < $test_limit) {
        note("total tasks submitted='$total_tasks_submitted', ".
             "tts last job='$total_tasks_submitted_in_last_job', tasks limit to test='$test_limit'");

        $job_hash->{user} = 'hadoop'.((($job_count-1) % 20)+1)
            if ((($type eq 'queue') || ($option eq 'multiple-users')) &&
                (($type ne 'user') && ($option ne 'same-user')));
        $job_hash->{queue} = $queues[(($job_count-1) % 4)]
            if ($option eq 'multiple-queues');
        note("Submit job ($job_count) as user '$job_hash->{user}': ".
             "(max tasks per job to init='$limits->{max_tasks_per_job_to_init}', ".
             "max tasks per user to init='$limits->{max_tasks_per_user_to_init}', ".
             "max tasks per queue to init='$limits->{max_tasks_per_queue_to_init}', ".
             "max tasks per job to accept='$limits->{max_tasks_per_job_to_accept}', ".
             "max tasks per user to accept='$limits->{max_tasks_per_user_to_accept}', ".
             "max tasks per queue to accept='$limits->{max_tasks_per_queue_to_accept}', ".
             "max jobs per user to init='$limits->{max_jobs_per_user_to_init}', ".
             "max jobs per queue to init='$limits->{max_jobs_per_queue_to_init}', ".
             "max jobs per user to accept='$limits->{max_jobs_per_user_to_accept}', ".
             "max jobs per queue to accept='$limits->{max_jobs_per_queue_to_accept}', ".
             "):");
        my $tasks_remaining = $test_limit - $total_tasks_submitted;
        if ($tasks_remaining > $max_tasks_per_user_to_state) {
            note("Submit job in the background: ".
                 "Using max tasks per user to $state value of ".
                 "'$max_tasks_per_user_to_state':");
            $num_reduce_task = $max_tasks_per_user_to_state;
            $job_hash->{'reduce_task'} = $num_reduce_task;
            my $job_id = $self->{test}->submit_sleep_job($job_hash);
            note("Job ($job_count) job id='$job_id'");
            push(@job_ids, $job_id);
        }
        elsif (($option =~ 'multiple') && ($job_count == 1)) {
            if ($limits->{max_jobs_per_user_to_init} == 1) {
                diag("WARNING: Max jobs per user to init is 1. Submitting ".
                     "multiple jobs will be queued.");
            }
            note("Submit job in the background:");
            $num_reduce_task = ceil($max_tasks_per_user_to_state/2);
            $job_hash->{'reduce_task'} = $num_reduce_task;
            note("Submit job using num tasks $num_reduce_task: a part of the".
                 "remainig tasks per $type to $state value of ".
                 "'$tasks_remaining':");
            my $job_id = $self->{test}->submit_sleep_job($job_hash);
            note("Job ($job_count) job id='$job_id'");
            push(@job_ids, $job_id);
        }
        else {
            note("Submit job in the background: ".
                 "Using remainig tasks per $type to $state value of ".
                 "'$tasks_remaining':");
            $num_reduce_task = $tasks_remaining;
            $job_hash->{'reduce_task'} = $num_reduce_task;
            my $job_id = $self->{test}->submit_sleep_job($job_hash);
            note("Job ($job_count) job id='$job_id'");
            push(@job_ids, $job_id);

            # if tasks is:
            # > tasks per job to init     -> tasks are queued
            # > tasks per user to init    -> tasks are queued
            # > tasks per queue to init   -> tasks are queued
            # > tasks per job to accept   -> job fails
            # > tasks per user to accept  -> subsequent jobs are queued on disk
            # > tasks per queue to accept -> subsequent jobs are queued on disk

            # Some delay before jobs transitions from Prep (4) to Running (1)
            # If test limit is greater than the true limit, wait until the
            # previous to last job state to become 1 or running. The last
            # job since it is over the (init) limit, should be in
            # waiting/queue/prep.
            my $lookup_job_id =
                (($test_limit > $true_limit) &&
                 ($option ne 'multiple-jobs') &&
                 ($option ne 'multiple-queues')) ?
                 $job_ids[-2] : $job_ids[-1];
            $self->{test}->wait_for_job_state($lookup_job_id,
                                              [$JOB_RUNNING_STATE, $JOB_SUCCEEDED_STATE],
                                              $wait_interval, undef,
                                              $verbose);
            
            my $status = $self->{test}->get_all_jobs_status();
            
            # Check the status of the last job. If test limit is greater than
            # the true limit, and jobs are not distributed across the queues or
            # across jobs, then the last job should be queued up and not running.
            if (($test_limit > $true_limit) && ($option ne 'multiple-jobs') &&
                ($option ne 'multiple-queues')) {

                # NOTE: for tasks per queque to run above limit, it is not
                # working properly.
                if (($state eq 'init') && ($level eq 'above') &&
                    ($option ne 'same-user')) {
                    note("job state='$status->{$job_id}->{state}'");
                    $self->test_log_shows_job_queued($job_ids[-1], 0);
                }
                else {
                    is($status->{$job_ids[-1]}->{state}, $JOB_PREP_STATE,
                       "Job '$job_id' is queued up.");
                    $self->test_log_shows_job_queued($job_ids[-1], 1);
                }
                
            } else {
                ok( (($status->{$job_id}->{state} == $JOB_RUNNING_STATE) ||
                     ($status->{$job_id}->{state} == $JOB_SUCCEEDED_STATE)),
                    "Job '$job_id' is initialized and running or ran ".
                    "successfully." );                
                $self->test_log_shows_job_queued($job_ids[-1], 0);
            }

            foreach my $job_id (@job_ids) {
                ok(defined($status->{$job_id}), "Check job '$job_id' is accepted");                                    

                # Jobs execpt for the last should be running/completed.
                if ($job_id ne $job_ids[-1]) {
                    note("job id '$job_id' status='$status->{$job_id}->{state}'");
                    
                    # The following unless block is added to catch and protect
                    # against sporadic occurrences where even though the last job
                    # submitted is in the state running, the job submitted prior
                    # is still in the prep state. This is still currently being
                    # investigated.
                    unless (($status->{$job_id}->{state} == $JOB_RUNNING_STATE) ||
                            ($status->{$job_id}->{state} == $JOB_SUCCEEDED_STATE))
                    {
                        note("WARN: job id '$job_id' has state ".
                             "'$status->{$job_id}->{state}': ".
                             "expected state 1 or 2");
                        my $result =
                            $self->{test}->wait_for_job_state($job_id,
                                                              [$JOB_RUNNING_STATE, $JOB_SUCCEEDED_STATE],
                                                              $wait_interval, undef,
                                                              $verbose);
                        is($result,
                           0,
                           "Job '$job_id' is initialized and running or ran ".
                           "successfully." );                          
                    }
                    else {                    
                        ok( (($status->{$job_id}->{state} == $JOB_RUNNING_STATE) ||
                             ($status->{$job_id}->{state} == $JOB_SUCCEEDED_STATE)),
                            "Job '$job_id' is initialized and running or ran ".
                            "successfully." );
                    }
                }
            }
        }
        $job_count++;
        $total_tasks_submitted_in_last_job = $total_tasks_submitted;
        $total_tasks_submitted += $num_reduce_task;
        note("limit to test='$test_limit': ".
             "total number of tasks submitted='$total_tasks_submitted', ".
             "number of tasks remaining to submit='".
             ($test_limit - $total_tasks_submitted)."':");
    }
    
    # Check job ids for jobs expected to pass have passed.
    is($self->{test}->wait_for_jobs_to_succeed(\@job_ids, $WAIT_INTERVAL_ALL_JOBS,
                                               undef, $verbose),
       0,
       "Check previous (".scalar(@job_ids).") submitted background jobs succeeded.");
    return $job_count-1;
}


=head2 test_jobs_limits($self, $hash, $should_fail, $error_message)

Test jobs limits

=over 2

=item I<Parameters>:

   * A hash reference of:
     - limits: Limits hash containing the limits values
     - entity: Entity {'jobs'}
     - type:   Type limits, for example per user, per queue. {'user', 'queue'}
     - state:  State limits, for example to accept, to run. {'accept', 'init'}
     - level:  Relative to the limit to test. {'below', 'at', 'above'}
       <Level> the <Entity> per <Type> to <State> limit
       E.g. above the jobs per user to accept limit
            below the tasks per queue to init limit
            at the tasks per user to accept limit
     - option: Optional. {'multiple-queues'}. 
   * Should fail: 1 for true, 0 for false
   * Error message if failure is expected

=item I<Returns>:

   * Number of jobs ran

=back

=cut
sub test_jobs_limits {
    my ($self, $hash, $should_fail, $error_message) = @_;
    my $limits = $hash->{limits} if defined($hash->{limits});
    my $entity = $hash->{entity} if defined($hash->{entity});
    my $type   = $hash->{type}   if defined($hash->{type});
    my $state  = $hash->{state}  if defined($hash->{state});
    my $level  = $hash->{level}  if defined($hash->{level});
    my $option = defined($hash->{option}) ? $hash->{option} : '';
    die ("Error: Missing required parameters")
        unless (defined($limits) && defined($entity) && defined($type)
                && defined($state) && defined($level));
    # Setup the true and test limit. True limit is the real limit that we want to
    # test. Test limit is the relative limits below, at, or above the true limit
    # that we want to individually test each time this function is called.
    my ($true_limit, $test_limit) =
        $self->get_test_limits($limits, $entity, $type, $state, $level);
    my $queue;    
    my $user            = 'hadoopqa';
    my $job_count       = 1;
    my @job_ids         = ();
    my $wait_interval   = 1;
    my $verbose         = 0;
    my @queues          = ('default', 'grideng', 'grid-dev', 'gridops')
        if ($option eq 'multiple-queues');
    my $job_hash        = { 'map_task'     => 0,
                            'reduce_task'  => 0,
                            'map_sleep'    => 1000,
                            'reduce_sleep' => 1000,
                            'user'         => $user,
                            'queue'        => $queue };

    # For testing jobs are queued when limit is exceeded, we need to make sure
    # the tasks are sufficiently large such that there will be large enough of
    # a delay to check that the job exceeding the limit is queued up.
    $job_hash->{reduce_task} = ($level eq 'above') ?
        $limits->{max_tasks_per_job_to_accept} :
        min($limits->{max_tasks_per_job_to_accept}, 5);

    note("---> Test submitting ($test_limit) $entity per $type ".uc($level).
         " the ($true_limit) max $entity per $type limit to $state ",
         ($option) ? "($option) " : "", "should ",
         ($should_fail) ? 'FAIL' : 'SUCCEED', ':');

    while ($job_count <= $test_limit) {
        $job_hash->{user} = 'hadoop'.((($job_count-1) % 20)+1)
            if ($type eq 'queue');
        $job_hash->{queue} = $queues[(($job_count-1) % 4)]
            if ($option eq 'multiple-queues');
        note("Submit job ($job_count) as user '$job_hash->{user}': ".
             "(max $entity per $type to $state='$true_limit'):");

        if ($job_count < $test_limit) {
            note("Submit job in the background: ".
                 "job count '$job_count' < test limit '$test_limit'");
            my $job_id = $self->{test}->submit_sleep_job($job_hash);
            note("Job ($job_count): job id='$job_id'");
            push(@job_ids, $job_id);
        }
        elsif ($job_count == $test_limit) {
            if ($should_fail) {
                note("Submit job $level the max $entity per $type to $state ".
                     "limit in the foreground and ".
                     "expect error:");
                $self->{test}->test_run_sleep_job($job_hash,
                                                  $should_fail,
                                                  $error_message);
            }
            elsif (($state eq 'accept') || ($option eq 'multiple-queues')) {
                note("Spread $entity across queues")
                    if ($option eq 'multiple-queues');
                note("Submit job $level the max $entity per $type to $state ".
                     "limit in the foreground and ".
                     "expect passing:");           
                $self->{test}->test_run_sleep_job($job_hash);
            }
            else {
                note("Submit job $level the max $entity per $type to accept ".
                     "limit in the background:");
                my $job_id = $self->{test}->submit_sleep_job($job_hash);
                note("Job ($job_count): job id='$job_id'");
                push(@job_ids, $job_id);
                
                if (($test_limit > $true_limit) && ($option ne 'multiple-queues')) {
                    # Wait for the job submitted before the limit is exceeded to
                    # change to the running state.
                    $self->{test}->wait_for_job_state($job_ids[-2],
                                                      [$JOB_RUNNING_STATE, $JOB_SUCCEEDED_STATE],
                                                      $wait_interval, undef,
                                                      $verbose);
                    # Get all job status since some jobs may be completed.
                    my $status = $self->{test}->get_all_jobs_status();                

                    # The last job should be in the queued state.
                    is($status->{$job_ids[-1]}->{state}, $JOB_PREP_STATE,
                       "Job '$job_id' is queued up.");

                    $self->test_log_shows_job_queued($job_ids[-1], 1);

                    # Jobs execpt for the last should be running/completed.
                    foreach my $job_id (@job_ids) {
                        if ($job_id ne $job_ids[-1]) {                            
                            ok( ($status->{$job_id}->{state} == $JOB_RUNNING_STATE) ||
                                ($status->{$job_id}->{state} == $JOB_SUCCEEDED_STATE),
                                "Job '$job_id' is initialized and running or ran ".
                                "successfully." );
                        }
                    }
                }
                else {
                    $self->test_log_shows_job_queued($job_ids[-1], 0);
                }
            }
        }
        $job_count ++;
    }

    # Check job ids for jobs expected to pass have passed.
    is($self->{test}->wait_for_jobs_to_succeed(\@job_ids, $WAIT_INTERVAL_ALL_JOBS,
                                               undef, $verbose),
       0,
       "Check previous (".scalar(@job_ids).") submitted background jobs succeeded.");

    return $job_count-1;
}


1;
