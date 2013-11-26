package Hadoop23::CapacityScheduler;

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use POSIX qw(ceil floor isdigit);
use List::Util qw(min max);
use Test::More;
use Math::Round;
use Text::Trim; # ypan/perl-Text-Trim
use String::ShellQuote; # perl-String-ShellQuote
use XML::Simple;


#################################################################################
# SUBMITTED JOB STATES:
#################################################################################
# job -list: PREP       -> RUNNING             -> SUCCEEDED
# Web UI   : ALLOCATING -> LAUNCHED -> RUNNING -> COMPLETED
#            <-queued up-> <----initialized----->
#################################################################################

# global constants
our $MRCS = "mapred.capacity-scheduler";

# JOB STATES are integer value 1-4 for Hadoop 20, and 
# 'RUNNING', 'SUCCEEDED', 'FAILED', 'PREP' for 23
our $JOB_RUNNING_STATE = 'RUNNING';
our $JOB_SUCCEEDED_STATE = 'SUCCEEDED';
our $JOB_FAILED_STATE = 'FAILED';
our $JOB_PREP_STATE = 'PREP';

our $ROOT_QUEUE = 'root';
our $WAIT_INTERVAL_ALL_JOBS = 20;

# The capacity scheduler configuration prefix in the capacity-scheduler.xml file
# has changed from: "yarn.capacity-scheduler.$queue.queues" to
# "yarn.scheduler.capacity.$queue.queues".
our $CS_PREFIX = "yarn.scheduler.capacity";

# Some capacity scheduler configuration prefix in the capacity-scheduler.xml file
# has changed from: "yarn.scheduler.capacity.<property name> to
# "yarn.scheduler.<property name>".
our $SCHED_PREFIX = "yarn.scheduler";

our $def_source_conf_dir = "$Bin/config/baseline";
our $def_queue = 'root.default';

my $Config = Hadoop::Config->new;

sub new {
    my ($class, $test) = @_;
    my $self = {test => $test};
    bless($self, $class);
    return $self;
};


#################################################################################
# Get Methods
#################################################################################

# Parse the cluster level capacity configuraitons
sub parse_cluster_config {
    my ($self, $verbose) = @_;
    my $property = $self->{property};
    my $hash = {};
    my $key;

    $key = "yarn.scheduler.capacity.maximum-applications";
    $hash->{"maximum-applications"} =
        defined($property->{$key}) ? $property->{$key} : 10000;

    $key = "yarn.scheduler.maximum-am-resource-percent";
    $hash->{"maximum-am-resource-percent"} =
        defined($property->{$key}) ? $property->{$key} : 0.1;

    $key = "yarn.scheduler.minimum-allocation-mb";
    $hash->{"minimum-allocation"} =
        defined($property->{$key}) ? $property->{$key} : 1024;

    $hash->{"total_cluster_mem"} = ($self->{test}->get_cluster_capacity()*1024);
    return $hash;
}

# Parse the queue specific capacity configuraitons
# e.g. <name>yarn.scheduler.capacity.root.a.capacity</name>
sub parse_queue_config {
    my ($self, $queue, $verbose) = @_;

    note("Parse queue config for queue '$queue'") if $verbose;
    my $cs_prefix = "$CS_PREFIX.$queue";
    my $property = $self->{property};
    my $hash = {};
    $hash->{"user-limit-factor"} = 1;
    $hash->{"minimum-user-limit-percent"} = 100;

    if ($queue eq $ROOT_QUEUE) {
        $hash->{"absoluteCapacity"} = 100;
        $hash->{"absoluteMaxCapacity"} = 100;
    }

    foreach my $key (sort(keys %$property)) {
        note("property key = $key") if $verbose;
        next unless ($key =~ "$cs_prefix.");
        my @tokens = split('\.', $key);
        my $short_key = $tokens[-1];

        if (($key eq "$cs_prefix.capacity") ||
            ($key eq "$cs_prefix.maximum-capacity") ||
            ($key eq "$cs_prefix.minimum-user-limit-percent") ||
            ($key eq "$cs_prefix.user-limit-factor")) {

            $hash->{"$short_key"} = $property->{$key};
        }
        else {
            note("unknown property key: '$key', value: '".$property->{$key}."'")
                if $verbose;
        }
    } 

    my @parent_queues = ();
    my @queue_tokens = split ('\.', $queue);
    my $queue_shortname = pop(@queue_tokens);
    my $parent_queue;
    foreach my $queue_token (@queue_tokens) {
        $parent_queue .= ($parent_queue) ? ".$queue_token" : $queue_token;
        push(@parent_queues, $parent_queue);
    }
    @parent_queues = reverse(@parent_queues); 

    $hash->{parents} = join(" ",@parent_queues);
    note(explain($hash)) if ($verbose);

    return $hash;
}

sub parse_sub_queues {
    my ($self, $queue, $cap, $verbose) = @_;
    my $property = $self->{property};
    my $query_str = "$CS_PREFIX.$queue.queues";
    my $sub_queue;
    if (exists($property->{$query_str}))
    {
        $cap->{$queue}->{queue_type} = 'ParentQueue';
        delete($cap->{$queue}->{min_user_limit_percent});
        my @sub_queues= split(",", $property->{$query_str});
        note("Sub queues for '$queue'='@sub_queues'") if $verbose;
        foreach my $sub_queue_part (@sub_queues){
            note("Get sub queue '$sub_queue_part' of queue '$queue':")
                if $verbose;
            $sub_queue = $queue.".".$sub_queue_part;            
            if($sub_queue ne "root") {
                $cap->{$sub_queue} = $self->parse_queue_config($sub_queue);
            }
            $self->parse_sub_queues($sub_queue, $cap);
        }
    }
    else {
        note("sub queues for '$queue' = <none>") if $verbose;
        $cap->{$queue}->{queue_type} = 'LeafQueue';
    }
    return $cap;
}


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
    my ($prop_prefix, $queue_prop_prefix, $prefix);
    if ($Config->{YARN_USED}) {
        if (($prop_name eq 'maximum-applications') || 
            ($prop_name eq 'maximum-am-resource-percent')) {
            $prefix = $CS_PREFIX;
        }
        elsif ($prop_name eq 'minimum-allocation-mb') {
            $prefix = $SCHED_PREFIX;
        }
        else {
            $prefix = "$CS_PREFIX.$queue_name";
        }
    }
    else {
        $prop_prefix = "mapred.capacity-scheduler";
        $queue_prop_prefix = "$prop_prefix.queue.$queue_name";
        $prefix = ($prop_name eq 'maximum-system-jobs') ?
            $prop_prefix :
            $queue_prop_prefix;
    }
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
        "minimum-user-limit-percent",
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
        'minimum-user-limit-percent'                => 100,
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
    my ($self, $config_file, $queue_name, $verbose) = @_;

    unless (defined($queue_name)) {
        $queue_name = ($Config->{YARN_USED}) ? $def_queue : 'default';
    }

    # If config file is not passed in, get the one that is currently being used.
    unless ($config_file) {
        $config_file =
            $self->{test}->get_component_config_dir($Config->{RESOURCEMANAGER_NAME}).
            '/local-capacity-scheduler.xml' 
    }

    note("---> Get the defined capacity limits:"); 
    my $config_limits =
        $self->get_config_limits($config_file, undef, undef, $verbose);

    note("---> Get the derived capacity limits:"); 
    my $derived_limits = {};
    if ($Config->{YARN_USED}) {
        my $queue_config_limits = $config_limits->{queues};
        # E.g. 'root','root.a', 'root.a.a1', 'root.a.a2', 'root.b', ...
        foreach my $queue_full_name (sort(keys %$queue_config_limits)) {
            next if ($queue_full_name eq 'root');
            note("---> Get the derived capacity limits for queue '$queue_full_name':"); 
            my $derived_queue_limits =
                $self->get_derived_limits($config_limits, $queue_full_name,
                                          $verbose);
            $derived_limits = $derived_queue_limits;
        }
    }
    else {
        $derived_limits = $self->get_derived_limits($config_limits, $queue_name,
                                                    $verbose);
    }

    note("---> Get the current capacity limits from the ",
         ($Config->{YARN_USED}) ? 'Resource Manager' : 'JobTracker' ," Log:"); 
    my $logged_limits = $self->get_limits_from_jobtracker_log($verbose);

    return ($config_limits, $derived_limits, $logged_limits);
}


sub compare_limits {
    my ($self, $derived_limits, $logged_limits, $verbose) = @_;

    note("---> Compare the configured limits against the logged limits:");
    note(explain($derived_limits)) if $verbose;
    note(explain($logged_limits)) if $verbose;

    # For each expected queue (hash $exp_queue) in the $derived_limits hash,
    # compare the content to the initialized queue (hash $init_queue) in the
    # $logged_limits hash parsed from the resource manager log file.
    my $queues_config_limits = $derived_limits->{queues};
    foreach my $queue_full_name (sort(keys %$queues_config_limits)) {
        my $queue_config_limits = $queues_config_limits->{$queue_full_name};

        if ($queue_config_limits->{queue_type} eq 'ParentQueue') {
            note("Skip comparing ParentQueue '$queue_full_name'");
            next;
        }

        my @tokens = split('\.', $queue_full_name);
        my $queue_name = $tokens[-1];
        my $init_queue = $logged_limits->{$queue_name}
            if (exists($logged_limits->{$queue_name}));
        is(exists($logged_limits->{$queue_name}), 1,
           "check resource manager log shows queue '$queue_name'");
        my $queue_msg = "('$queue_name' queue)";

        note(explain($queue_config_limits)) if $verbose;
        note(explain($init_queue)) if $verbose;
        foreach my $key (sort(keys %$queue_config_limits)) {
            if (($key eq 'capacity') ||
                ($key eq 'absoluteCapacity') ||
                ($key eq 'absoluteMaxCapacity')) {
                is($init_queue->{$key}*100,
                   $queue_config_limits->{$key},
                   "Check key value for '$key' $queue_msg");
            }
            # ($key eq 'maxActiveApplications') ||
            # ($key eq 'maxActiveApplicationsPerUser')) {
            elsif (($key eq 'maxApplications') ||
                   ($key eq 'maxApplicationsPerUser')) {
                is($init_queue->{$key},
                   $queue_config_limits->{$key},
                   "Check key value for '$key' $queue_msg");
            }
            elsif ($key eq 'maximum-capacity') {
                ok( ($init_queue->{maxCapacity} ==
                     sprintf("%.2f", ($queue_config_limits->{$key}/100))),
                    "Check key value for '$key' $queue_msg");
            }
            elsif ($key eq 'minimum-user-limit-percent') {
                unless ($queue_config_limits->{queue_type} eq 'ParentQueue') {
                    is($init_queue->{userLimit},
                       $queue_config_limits->{$key},
                       "Check key value for '$key' $queue_msg");
                }
            }
            elsif ($key eq 'user-limit-factor') {
                unless ($queue_config_limits->{queue_type} eq 'ParentQueue') {
                    ok( ($init_queue->{userLimitFactor} ==
                         sprintf("%.1f", $queue_config_limits->{$key})),
                        "Check key value for '$key' $queue_msg");
                }
            }
            elsif ($key eq 'queue_type') {
                is($init_queue->{queue_type},
                   $queue_config_limits->{queue_type},
                   "Check queue type for '$queue_name' is ".
                   "'$queue_config_limits->{queue_type}'");
            }
            elsif ($key eq 'name') {
                is($queue_name,
                   $queue_config_limits->{name},
                   "Check queue name is '$queue_name'");
            }
            else {
                note("limits key '$key' not checked");
                
            }
        }
    }
}
    
=head2 rotate_jobtracker_log($self)

Rotate the resource manager log

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE, 

=back

=cut
sub rotate_resourcemanager_log {
    my ($self) = @_;

    my $log = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{LOG};
    my $host = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{HOST};
    my $sudoer = $self->{test}->get_sudoer('resourcemanager');

    use POSIX qw(strftime);
    my $timestamp = strftime("%Y-%b-%e_%H-%M", localtime);

    my @command = ('ssh', $host, '/bin/cp', $log, "$log.$timestamp");
    @command = ('sudo', 'su', "$sudoer", '-c', shell_quote("@command"));
    note("@command");
    my $result = `@command`;
    note("result='$result'");

    @command = ('/bin/cat', '/dev/null', '>', $log);
    @command = ('ssh', $host, shell_quote("@command"));
    @command = ('sudo', 'su', "$sudoer", '-c', shell_quote("@command"));
    note("@command");
    $result = `@command`;
    note("result='$result'");

    return $result;
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
    my ($self, $verbose) = @_;

    my $component = $Config->{RESOURCEMANAGER_NAME};
    my $log = $Config->{NODES}->{$component}->{LOG};
    my $host = $Config->{NODES}->{$component}->{HOST};
    note("get limits from JT/RM log: host='$host', log='$log'");

    my $match;
    my $hash = {};
    my @command = ();

    if ($Config->{YARN_USED}) {

        # Get the starting tag where the last RM restart occurred
        $match = "'ResourceManager: STARTUP_MSG:'";
        @command = ('grep', shell_quote($match), $log, '|', '/usr/bin/tail', '-1');
        @command = ('ssh', $host, @command,);
        note("@command") if $verbose;
        my $result = `@command`;
        note("$result") if $verbose;

        # Capture the pertinent log to a separate temp file
        $match = trim($result);
        $match =~ s/\[/\\[/g;
        $match =~ s/\]/\\]/g;
        my $logfile = "/tmp/rm_start.log";
        @command = ('grep', '-A', '120', shell_quote($match), $log, '>', $logfile);
        @command = ('ssh', $host, shell_quote(@command));
        note("@command");
        $result = `@command`;

        # Get the one line summary showing the 'initialized queue: <queue>'
        $match = 'initialized queue';
        @command = ('grep', '-i', shell_quote($match), $logfile);
        @command = ('ssh', $host, shell_quote(@command));
        note("@command");
        $result = `@command`;
        note("$result") if $verbose;

        my @kvp;
        foreach my $line (split("\n", $result)) {
            my $queue_type = "LeafQueue";
            my $substr;
            my $queue_name;
            if ($line =~ m/(.*.)(INFO .*capacity.CapacityScheduler:\s+)(Initialized queue:\s+)(.*.)(:\s+)(.*.)/) {
                $queue_name = $4;
                $substr = $6;
            }
            note("queue = '$queue_name'") if $verbose;
            note("log substr = '$substr'") if $verbose;

            my @tokens = split(',', $substr);
            my $queue_hash = {};

            # aggregate other attributes based on the queue name
            # E.G. $ grep -A 18 "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue: Initializing default" /tmp/rm_start.log
            $match = "capacity.LeafQueue: Initializing $queue_name";
            @command = ('grep', '-A', '18', shell_quote($match), $logfile, '|', 'cut', '-d','" "','-f1-3');
            @command = ('ssh', $host, shell_quote(@command));
            note("@command");
            my $result = `@command`;
            my @tokens2 = split("\n", trim($result));
            push(@tokens, @tokens2);

            foreach my $attribute (@tokens) {
                note("parse attribute = '$attribute'") if $verbose;
                if ($attribute =~ "=") {
                    @kvp = split("=", $attribute);
                    $queue_hash->{trim($kvp[0])} = trim($kvp[1]);
                }
                else {
                    note("not an attribute to parse: '$attribute'") if $verbose;
                }
            }
            $queue_hash->{"queue_type"} = 'LeafQueue';
            $hash->{$queue_name} = $queue_hash;
        }
    }
    else {
        $match = "\\\"Initializing \\'default\\' queue\\\"";
        @command = ('grep', $match, $log);
        @command = ('ssh', $host, @command, '|', 'tail', '-1');
        note("@command");
        my $result = `@command`;
        note("$result");
        
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
    }
    note("logged limits = ", explain($hash)) if ($verbose);
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
    my $app_id = $self->{test}->get_app_id($job_id);
    my $str =
        "already has.*.applications from user.*.cannot accept submission of ".
        "application: $app_id";
    my $log = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{LOG};
    my $host = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{HOST};

    my @command = ('grep', '-i', shell_quote("$str"), $log);
    @command = ('ssh', $host, shell_quote(@command));
    note("@command");
    my $result = `@command`;
    note("result='$result'");

    return $result;
}


=head2 get_config_limits($self, $config_file, $queue_name, $prop_names, $verbose)

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
    my ($self, $config_file, $queue_name, $prop_names, $verbose) = @_;

    if ($Config->{YARN_USED}) {

        my $time = time;
        my $file = "/tmp/local-capacity-scheduler-$time.xml";
        my $component_host = $Config->{NODES}->{RESOURCEMANAGER}->{HOST};
        my @command = ('scp', "$component_host:$config_file", $file);
        note("@command");
        `@command`;
        die ("Failed to copy config file '$config_file' from host ".
             "'$component_host' to '$file'") if ($? != 0);

        my $property = {};
        my $xml = new XML::Simple(KeyAttr=>[]);
        my $data = $xml->XMLin($file);
        foreach (@{$data->{property}}){
            $property->{$_->{name}} = $_->{value};
        }
        note("config properties = '", explain($property), "'") if $verbose;
        
        $self->{data} = $data;
        $self->{property} = $property;

        my $cluster_cap = {};
        $cluster_cap = $self->parse_cluster_config($verbose);

        my $queues_cap = {};
        $queues_cap->{$ROOT_QUEUE} =
            $self->parse_queue_config($ROOT_QUEUE, $verbose);
        $self->parse_sub_queues($ROOT_QUEUE, $queues_cap, $verbose);
        $cluster_cap->{queues} = $queues_cap;

        $self->{capacity_config} = $cluster_cap;
        note("capacity config = '", explain($self->{capacity_config}), "'")
            if $verbose;

        return $self->{capacity_config};
    }

    $queue_name = "default" unless defined($queue_name);
    $prop_names = $self->get_default_limits_prop_names() unless ($prop_names);
    $config_file = $Config->{LOCAL_CAPACITY_SCHEDULER_XML} unless ($config_file);
    my $component_host = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{HOST};
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
    note(explain($config)) if $verbose;
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
    my ($self, $cluster_config, $queue_name, $verbose) = @_;
    my $queues_config = $cluster_config->{queues};
    my $queue_config = $queues_config->{$queue_name};
    note("Get derived limits for queue '$queue_name':", explain($queue_config))
        if $verbose;

    my ($max_system_jobs, $max_active_tasks, $max_active_tasks_per_user,
        $queue_capacity_percent, $min_user_limit_percent,
        $job_init_to_accept_factor, $user_limit_factor, $queue_hard_limit);
    $queue_name = "default" unless defined($queue_name);

    if ($Config->{YARN_USED}) {
        my $cluster_max_apps = $cluster_config->{"maximum-applications"};
        my $parent_queue_name = (split(" ", $queue_config->{parents}))[0];
        my $parent_absolute_cap =
            $queues_config->{$parent_queue_name}->{absoluteCapacity};
        my $parent_absolute_max_cap =
            $queues_config->{$parent_queue_name}->{absoluteMaxCapacity};
        
        note("get derived limits: parent queue '$parent_queue_name', ".
             "parent abs cap = '$parent_absolute_cap'") if $verbose;
        my $abs_cap = $queue_config->{capacity} * $parent_absolute_cap / 100;
        my $abs_max_cap =
            $queue_config->{"maximum-capacity"} * $parent_absolute_max_cap / 100;
        note("get derived limits: queue abs cap = '$abs_cap', ".
             "queue abs max cap = '$abs_max_cap'") if $verbose;
        $cluster_config->{queues}->{$queue_name}->{absoluteCapacity} = $abs_cap;
        $cluster_config->{queues}->{$queue_name}->{absoluteMaxCapacity} = $abs_max_cap;

        # Get the max apps
        my $queue_max_apps = ($cluster_max_apps * $abs_cap / 100);
        $cluster_config->{queues}->{$queue_name}->{maxApplications} = $queue_max_apps;
        $cluster_config->{queues}->{$queue_name}->{maxApplicationsPerUser} =
            $queue_max_apps * 
            ($cluster_config->{queues}->{$queue_name}->{"minimum-user-limit-percent"} / 100) *
            $cluster_config->{queues}->{$queue_name}->{"user-limit-factor"};

        # Get the max active apps
        my $total_cluster_mem = $cluster_config->{total_cluster_mem};
        note("total cluster mem = '$total_cluster_mem'") if $verbose;
        my $maxAMResourcePercent = $cluster_config->{"maximum-am-resource-percent"};
        note("max am resource % = '$maxAMResourcePercent'") if $verbose;
        my $minAllocation = $cluster_config->{"minimum-allocation"};
        note("min allocation = '$minAllocation'") if $verbose;
        my $maxActiveApps = 
            max( ceil(($total_cluster_mem/$minAllocation) *
                       $maxAMResourcePercent *
                       $abs_max_cap/100), 1);
        my $maxActiveAppsAbsCap = 
            max( ceil(($total_cluster_mem/$minAllocation) *
                       $maxAMResourcePercent *
                       $abs_cap/100), 1);
        my $maxActiveAppsPerUser =
            max( ceil( $maxActiveAppsAbsCap * 
                       ($cluster_config->{queues}->{$queue_name}->{"minimum-user-limit-percent"} / 100) *
                       $cluster_config->{queues}->{$queue_name}->{"user-limit-factor"}), 1);
        $cluster_config->{queues}->{$queue_name}->{maxActiveApplications} =
            $maxActiveApps;
        $cluster_config->{queues}->{$queue_name}->{maxActiveApplicationsPerUser} =
            $maxActiveAppsPerUser;

        return $cluster_config;

        $queue_capacity_percent    = $queue_config->{"capacity"};
        $min_user_limit_percent    = $queue_config->{"minimum-user-limit-percent"};
        $user_limit_factor         = $queue_config->{"user-limit-factor"};
        $queue_hard_limit          = $queue_config->{"maximum-capacity"};
    }
    else {
        $queue_capacity_percent    = $queue_config->{$self->get_full_prop_name("capacity", $queue_name)};
        $min_user_limit_percent    = $queue_config->{$self->get_full_prop_name("minimum-user-limit-percent", $queue_name)};
        $user_limit_factor         = $queue_config->{$self->get_full_prop_name("user-limit-factor", $queue_name)};
        $queue_hard_limit          = $queue_config->{$self->get_full_prop_name("maximum-capacity", $queue_name)};
    }

    my $limits = {};
    note(explain($limits)) if $verbose;

    note("TEST CONFIGURATION VALUES FOR QUEUE '$queue_name':");
    printf("%-30s = %-5s = %-30s\n", '$min_user_limit_percent',     "$min_user_limit_percent",    '<minimum-user-limit-percent>');
    printf("%-30s = %-5s = %-30s\n", '$queue_capacity_percent',     "$queue_capacity_percent",    '<capacity>');
    printf("%-30s = %-5s = %-30s\n", '$queue hard limit',           "$queue_hard_limit",          '<maximum-capacity>');
    printf("%-30s = %-5s = %-30s\n", '$user limit factor',          "$user_limit_factor",         '<user-limit-factor>');
    note("TEST DERIVED VALUES FOR QUEUE '$queue_name':");
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
    note("missing value for queue hard limit")           unless ($queue_hard_limit);
    note("missing value for minimum user limit percent") unless ($min_user_limit_percent);
    note("missing value for user limit factor")          unless ($user_limit_factor);
    note("missing value for queue capacity percent")     unless ($queue_capacity_percent);
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
    note("missing value for max system jobs")        unless ($max_system_jobs);
    note("missing value for queue capacity percent") unless ($queue_capacity_percent);
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
    note("missing value for job_init to accept factor") unless ($job_init_to_accept_factor);
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
    note("missing value for max system jobs")        unless ($max_system_jobs);
    note("missing value for queue capacity percent") unless ($queue_capacity_percent);
    return ceil($max_system_jobs * $queue_capacity_percent/100);
}

# . Get the maximum number of jobs per queue that can be accepted at any give time.
#   Subsequent jobs submitted will be rejected.
# . The value of max jobs per queue to accept is equal to the ceiling of:
#   <max system jobs> * <queue capacity percent> * <job init to accept factor>
sub get_max_jobs_per_queue_to_accept {
    my ($self, $max_system_jobs, $queue_capacity_percent,
        $job_init_to_accept_factor) = @_;
    note("missing value for job_init to accept factor") unless ($job_init_to_accept_factor);
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
            "capacity"                                  => 25, # no default value
            "minimum-user-limit-percent"                => 50, # default = 100
            "user-limit-factor"                         => 20, # default = 1
            "maximum-capacity"                          => 25, # default = -1 (i.e.cluster limit)
        }));
}

# Base on the settings and formula above, the derived limits will be:
# -------------------------------------------------------------------------------
# max app per queue          = 4
# max app per queue per user = 2
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
    my ($self, $queue, $source_conf_dir) = @_;
    $queue ||= $def_queue;
    $source_conf_dir ||= $def_source_conf_dir;

    # Using values from svn/HadoopQEAutomation/branch-23/tests/Regression/YARN/
    # CapacitySchedulerLimits/config/baseline/local-capacity-scheduler.xml    

    # root.default: 
    # 'absoluteCapacity' => '20',
    # 'absoluteMaxCapacity' => '25',
    # 'capacity' => 20,
    # 'maxActiveApplications' => '3',
    # 'maxActiveApplicationsPerUser' => 1,
    # 'maxApplications' => '4',
    # 'maxApplicationsPerUser' => 2,
    # 'maximum-capacity' => 25,
    # 'minimum-user-limit-percent' => 50,
    # 'parents' => 'root',
    # 'queue_type' => 'LeafQueue',
    # 'user-limit-factor' => 1
    # 
    # my $default_queue_config = {
    #     "maximum-applications" => 20,
    #     "minimum-allocation-mb" => 1024,
    #     "maximum-am-resource-percent" => 0.1,
    #     "capacity"                                  => 20, # no default value
    #     "maximum-capacity"                          => 25, # default = -1 (i.e.cluster limit)
    #     "minimum-user-limit-percent"                => 50, # default = 100
    #     "user-limit-factor"                         => 1, # default = 1
    #     "maximum-capacity"                          => 50, # default = -1 (i.e.cluster limit)
    # };

    # root.a.a2
    # 'absoluteCapacity' => '30',
    # 'absoluteMaxCapacity' => '40',
    # 'capacity' => 60,
    # 'maxActiveApplications' => '4',
    # 'maxActiveApplicationsPerUser' => '2',
    # 'maxApplications' => '6',
    # 'maxApplicationsPerUser' => 3,
    # 'maximum-capacity' => 50,
    # 'minimum-user-limit-percent' => 50,
    # 'parents' => 'root.a root',
    # 'queue_type' => 'LeafQueue',
    # 'user-limit-factor' => 1
    # 
    # my $a2_queue_config = {
    #     "capacity"                                  => 60, # no default value
    #     "maximum-capacity"                          => 50, # default = -1 (i.e.cluster limit)
    #     "minimum-user-limit-percent"                => 50, # default = 100
    #     "user-limit-factor"                         => 1, # default = 1
    #     "maximum-capacity"                          => 50, # default = -1 (i.e.cluster limit)
    #     "maximum-applications" => 20,
    #     "minimum-allocation-mb" => 1024,
    #     "maximum-am-resource-percent" => 0.1,
    # };

    my $queue_config = {
        "maximum-applications" => 20,
        "minimum-allocation-mb" => 1024,
        "maximum-am-resource-percent" => 0.1,
    };
    return $self->test_set_config_limits(
               $self->construct_target_limits($queue_config,
                                              $queue),
               $source_conf_dir);
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
            "capacity"                                  => 25, # no default value
            "minimum-user-limit-percent"                => 50, # default = 100
            "user-limit-factor"                         => 20, # default = 1
            "maximum-capacity"                          => 25, # default = -1 (i.e.cluster limit)
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
    $queue_name = $def_queue unless defined($queue_name);
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
   * optional source config dir

=item I<Returns>:

   * Integer value 0 for SUCCESS and 1 for FAILURE, 
   * A hash containing the configuration directory and the location of the
     local capacity scheduler xml file.

=back

=cut
sub set_config_limits {
    my ($self, $target_limits, $conf, $source_conf_dir) = @_;

    my $component_host = $Config->{NODES}->{$Config->{RESOURCEMANAGER_NAME}}->{HOST};
    unless ($conf) {
        note("---> Setup a config dir where capacity limits can be set:");
        $source_conf_dir ||= $def_source_conf_dir;
        $conf = $self->{test}->create_new_conf_dir($component_host,
                                                   $source_conf_dir);
    }

    note("---> Set the target capacity limits:"); 
    my $result = 0;
    my $config_file = $conf->{local_cap_sched_conf};
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

=head2 check_set_config_limits($self, $target_limits, $source_conf_dir, $verbose)

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
   * source conf dir
   * Verbose: 0 for off, 1 for on

=item I<Returns>:

   * 4 Hashes:
     1) hash containing the configuration directory and the location of the
        local capacity scheduler xml file.
     2) hash containing limits defined in the capacity scheduler config file,
     3) hash containing the derived limits, and
     4) hash containing the last limits entry from the jobtracker log

=back

=cut
sub check_set_config_limits {
    my ($self, $target_limits, $source_conf_dir, $verbose) = @_;
    $source_conf_dir ||= $def_source_conf_dir;

    my $hash = (defined($target_limits->{limits})) ?
        $target_limits : { 'limits' => $target_limits };

    # Set the capacity scheduler limits
    my ($result, $conf) =
        $self->set_config_limits($hash->{limits},
                                 $hash->{conf},
                                 $source_conf_dir);

    # Restarting the JobTracker and tie the new config dir to the JobTracker.
    # The running JobTracker will shutdown if this config dir is removed.
    unless (defined($hash->{option}) && $hash->{option} eq 'no-restart') {
        my $component = lc($Config->{RESOURCEMANAGER_NAME});
        note("---> Restart the $component:");        

        if ($component eq 'resourcemanager') {
            $self->{test}->check_restart_resourcemanager(
                $conf->{root_dir}, 'default', $verbose);
        }
        else {
            $self->{test}->check_restart_daemon(
                $component, $conf->{root_dir}, 'default', $verbose);
        }
    }

    note("---> Get the capacity limits:");
    my ($config_limits, $limits, $logged_limits) =
        $self->get_limits($conf->{local_cap_sched_conf}, $hash->{queue});

    return ($conf, $config_limits, $limits, $logged_limits);
}


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
    my ($self, $target_limits, $source_conf_dir) = @_;
    $source_conf_dir ||= $def_source_conf_dir;

    my ($conf, $config_limits, $limits, $logged_limits) =
        $self->check_set_config_limits($target_limits, $source_conf_dir);

    unless ($target_limits->{option} eq 'no-restart') {
        # INVESTIGATE: the logging pattern has either changed or is not turned out
        $self->compare_limits($limits, $logged_limits);
    }

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


=head2 get_test_limits($self, $limits, $entity, $type, $state, $level, $amt_over,
$amt_under, $queue)

Test tasks limits

=over 2

=item I<Parameters>:

   * Limits
   * Entity {'tasks', 'jobs'}
   * Type {'job', 'user', 'qeue'}
   * State {'accept', 'init'}
   * Level {'below', 'at', 'above'}
   * amt_over  amount over the limit
   * amt_under amount under the limit
   * queue

=item I<Returns>:

   * None

=back

=cut
sub get_test_limits {
    my ($self, $limits, $entity, $type, $state, $level, $amt_over, $amt_under,
        $queue) = @_;
    $amt_over  ||= 1;
    $amt_under ||= 1;
    $queue     ||= $def_queue;

    # my $limit_prop_name = 'max_'.$entity.'_per_'.$type.'_to_'.$state;
    # my $true_limit = $limits->{$limit_prop_name};
    my $limit_type =
        ($type eq 'user') ? 'maxApplicationsPerUser' : 'maxApplications';
    my $true_limit = $limits->{queues}->{$queue}->{$limit_type};

    my $test_limit = $true_limit;
    $test_limit-=$amt_under if (($level eq 'below') && ($test_limit > 1));
    $test_limit+=$amt_over  if ($level eq 'above');
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
    # my $num_tasks_over_limit = 5;
    my $num_tasks_over_limit = 30;
    my ($true_limit, $test_limit) =
        $self->get_test_limits($limits, $entity, $type, $state, $level,
                               $num_tasks_over_limit);
    my $queue;    
    my $user            = $Config->{DEFAULT_USER};
    my $job_count       = 1;
    my @log_files       = ();
    my @job_ids         = ();
    my $wait_interval   = 1;
    my $verbose         = 0;
    note(explain($Config));
    my @queues          = @{$Config->{QUEUE}} if ($option eq 'multiple-queues');

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
        return unless ($job_id);
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
             "max jobs per queue to accept='$limits->{max_jobs_per_queue_to_accept}'".
             "):");
        my $tasks_remaining = $test_limit - $total_tasks_submitted;
        if ($tasks_remaining > $max_tasks_per_user_to_state) {
            note("Submit job in the background: ".
                 "Using max tasks per user to $state value of ".
                 "'$max_tasks_per_user_to_state':");
            $num_reduce_task = $max_tasks_per_user_to_state;
            $job_hash->{'reduce_task'} = $num_reduce_task;

            # my $job_id = $self->{test}->submit_sleep_job($job_hash);
            # return unless ($job_id);
            # note("Submitted Job ($job_count) job id='$job_id'");
            # push(@job_ids, $job_id);

            my $log_file = $self->{test}->submit_sleep_job_wo_wait($job_hash);
            push(@log_files, $log_file);
        }
        elsif (($option =~ 'multiple') && ($job_count == 1)) {
            if ($limits->{max_jobs_per_user_to_init} == 1) {
                diag("WARNING: Max jobs per user to init is 1. Submitting ".
                     "multiple jobs will be queued.");
            }
            note("Submit job in the background:");
            $num_reduce_task = ceil($max_tasks_per_user_to_state/2);
            $job_hash->{'reduce_task'} = $num_reduce_task;
            note("Submit job using num tasks $num_reduce_task: a part of the ".
                 "remainig tasks per $type to $state value of ".
                 "'$tasks_remaining':");

            # my $job_id = $self->{test}->submit_sleep_job($job_hash);
            # return unless ($job_id);
            # note("Submitted Job ($job_count) job id='$job_id'");
            # push(@job_ids, $job_id);

            my $log_file = $self->{test}->submit_sleep_job_wo_wait();
            push(@log_files, $log_file);
        }
        else {
            note("Submit job in the background: ".
                 "Using remainig tasks per $type to $state value of ".
                 "'$tasks_remaining':");
            $num_reduce_task = $tasks_remaining;
            $job_hash->{'reduce_task'} = $num_reduce_task;

            # my $job_id = $self->{test}->submit_sleep_job($job_hash);
            # return unless ($job_id);
            # note("Submitted Job ($job_count) job id='$job_id'");
            # push(@job_ids, $job_id);

            my $log_file = $self->{test}->submit_sleep_job_wo_wait($job_hash);
            push(@log_files, $log_file);

            @job_ids = $self->{test}->parse_job_ids(\@log_files);
            my $job_id = $job_ids[-1];

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

            # NOTE: potential timming issue because the lookup job id could have
            # finished by this time
            $self->{test}->wait_for_job_state($lookup_job_id,
                                              [$JOB_RUNNING_STATE, $JOB_SUCCEEDED_STATE],
                                              $wait_interval, undef,
                                              $verbose);
            
            my $status = $self->{test}->get_all_jobs_status();
            
            note(explain($status));

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
                ok((($status->{$job_id}->{state} eq $JOB_RUNNING_STATE) || 
                    ($status->{$job_id}->{state} eq $JOB_SUCCEEDED_STATE)),
                   "Job '$job_id' is initialized and running: got state '$status->{$job_id}->{state}'");
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
                    unless (($status->{$job_id}->{state} eq $JOB_RUNNING_STATE) ||
                            ($status->{$job_id}->{state} eq $JOB_SUCCEEDED_STATE))
                    {
                        note("WARN: job id '$job_id' has state ".
                             "'$status->{$job_id}->{state}': ".
                             "expected state '$JOB_RUNNING_STATE' or ".
                             "'$JOB_SUCCEEDED_STATE'");
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
                        ok( (($status->{$job_id}->{state} eq $JOB_RUNNING_STATE) ||
                             ($status->{$job_id}->{state} eq $JOB_SUCCEEDED_STATE)),
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
    my $queue  = $hash->{queue}  if defined($hash->{queue});
    $queue ||= $def_queue;

    my @tokens = split('\.', $queue);
    my $queue_sn = $tokens[-1];

    my $option = defined($hash->{option}) ? $hash->{option} : '';
    die ("Error: Missing required parameters")
        unless (defined($limits) && defined($entity) && defined($type)
                && defined($state) && defined($level));
    # Setup the true and test limit. True limit is the real limit that we want to
    # test. Test limit is the relative limits below, at, or above the true limit
    # that we want to individually test each time this function is called.
    my $num_jobs_over_limit = 1;
    my ($true_limit, $test_limit) =
        $self->get_test_limits($limits, $entity, $type, $state, $level,
                               $num_jobs_over_limit, undef, $queue);
    my $user            = $Config->{DEFAULT_USER};
    my $job_count       = 1;
    my $job_ids         = [];
    my $jobs            = [];
    my $wait_interval   = 1;
    my $verbose         = 0;
    my @queues          = @$Config->{QUEUE}
        if ($option eq 'multiple-queues');
    my $job_hash        = { 'map_task'     => 0,
                            'reduce_task'  => 0,
                            'map_sleep'    => 3000,
                            'reduce_sleep' => 3000,
                            'user'         => $user,
                            'queue'        => $queue_sn };

    # For testing jobs are queued when limit is exceeded, we need to make sure
    # the tasks are sufficiently large such that there will be large enough of
    # a delay to check that the job exceeding the limit is queued up.

    # $job_hash->{reduce_task} = ($level eq 'above') ?
    #     $limits->{max_tasks_per_job_to_accept} :
    #     min($limits->{max_tasks_per_job_to_accept}, 5);
    $job_hash->{map_task} = 30;
    $job_hash->{reduce_task} = 30;

    note("---> Submit $entity of ($test_limit) that is [$level] the max $entity per $type ".
         "limit of ($true_limit) ",
         ($option) ? "($option) " : "", "should ",
         ($should_fail) ? 'FAIL' : 'SUCCEED', ":");

    my $num_jobs_failed = 0;
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
            return unless ($job_id);
            note("Submitted Job ($job_count): job id='$job_id'");
            push(@$job_ids, $job_id);
            push(@$jobs, $job_hash);
        }
        elsif ($job_count == $test_limit) {
            if ($should_fail) {
                note("Submit job $level the max $entity per $type to $state ".
                     "limit in the foreground and ".
                     "expect error:");
                $self->{test}->test_run_sleep_job($job_hash,
                                                  $should_fail,
                                                  $error_message);
                $num_jobs_failed = 1;
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
                note("Submitted Job ($job_count): job id='$job_id'");
                push(@$job_ids, $job_id);
                push(@$jobs, $job_hash);
                
                if (($test_limit > $true_limit) && ($option ne 'multiple-queues')) {
                    # Wait for the job submitted before the limit is exceeded to
                    # change to the running state.
                    # Fetch the index for the last job that should not be queued up.
                    my $index = ($num_jobs_over_limit * -1) - 1;
                    $self->{test}->wait_for_job_state($job_ids->[$index],
                                                      [$JOB_RUNNING_STATE, $JOB_SUCCEEDED_STATE],
                                                      $wait_interval, undef,
                                                      $verbose);
                    # Get all job status since some jobs may be completed.
                    my $status = $self->{test}->get_all_jobs_status();                
                    note(explain($status));

                    # The last job should be in the queued state.
                    is($status->{$job_ids->[-1]}->{state}, $JOB_PREP_STATE,
                       "Job '$job_id' is queued up.");

                    $self->test_log_shows_job_queued($job_ids->[-1], 1);

                    # job -list: PREP       -> RUNNING             -> SUCCEEDED
                    # Web UI   : ALLOCATING -> LAUNCHED -> RUNNING -> COMPLETED
                    #            <queued up>   <initialized        >

                    # Jobs within the max limit should be running or completed.
                    # They should not be queued up. 
                    foreach my $job_id ($job_ids->[0..$true_limit-1]) {
                        ok( ($status->{$job_id}->{state} eq $JOB_RUNNING_STATE) ||
                            ($status->{$job_id}->{state} eq $JOB_SUCCEEDED_STATE),
                            "Job '$job_id' is initialized and running or ran ".
                            "successfully." );
                    }

                    foreach my $job_id ($job_ids->[$true_limit..$#$job_ids]) {
                        my $job_id_state = exists($status->{$job_id}) ?
                            $status->{$job_id}->{state} : 'undefined';
                        is( $job_id_state, $JOB_PREP_STATE,
                            "Job '$job_id' is in the prep (allocating) state." );
                    }

                }
                else {
                    $self->test_log_shows_job_queued($job_ids->[-1], 0);
                }
            }
        }
        $job_count ++;
    }

    note("--> Check jobs status:");
    my $jobs_status = $self->{test}->get_jobs_status(undef, 1);

    my $wait_time       = 0;
    my $num_iter        = 1000;
    my $running_stats =
        $self->capture_run_stats($jobs, $job_ids, $wait_time, $num_iter, $verbose);
    note("running_stats=", explain($running_stats)) if $verbose;

    # Check max active running jobs
    my $active_app_limit_type = 
        ($type eq 'user') ? 'maxActiveApplicationsPerUser' : 'maxActiveApplications';
    my $active_app_limit = $limits->{queues}->{$queue}->{$active_app_limit_type};

    my $real_max_active_apps = max( @{$running_stats->{"concurrent_instances"}} );
    ok((isdigit $real_max_active_apps) && $real_max_active_apps <= $active_app_limit,
       "Check the real max active apps '$real_max_active_apps' is less than or ".
       "equal to the max active apps limit of '$active_app_limit' ($active_app_limit_type)");

    # Check job ids for jobs expected to pass have passed.
    is($self->{test}->wait_for_jobs_to_succeed($job_ids, $WAIT_INTERVAL_ALL_JOBS,
                                               undef, $verbose),
       0,
       "Check previous (".scalar(@$job_ids).") submitted background jobs succeeded.");

    my $num_jobs = {
        'attempted' => $job_count-1,
        'ran' => $job_count-1-$num_jobs_failed,
    };

    ok($num_jobs->{ran} <= $true_limit,
       "Check when ".(($type eq 'user') ? 'an user' : 'users').
       " submit(s) (".$num_jobs->{attempted}.") job(s) which are [$level] the ".
       "max $type limit of $true_limit, the number of jobs ran (".
       $num_jobs->{ran}.") should be less than or eqaul to the max jobs per ".
       "$type limit of ($true_limit).");
    
    return $num_jobs;
}

#################################################################################
# Verify the number of tasks run do not exceeded the queue limit
#################################################################################

sub get_capacity_limit {
    my ($self, $capacity, $queue, $single_user, $num_active_queues,
        $verbose) = @_;    

    my $capacity_limit;

    # If only a single user and a single queue is tested,
    # use the user queue capacity.
    if (($single_user) && ($num_active_queues == 1)) {
        $capacity_limit=$capacity->{queues}->{$queue}->{user};

        note("For single user and a single queue, use the user queue capacity ".
             "(with ulf) as the limit for the queue '$queue': $capacity_limit");

        # WORKAROUND: HADOOP ALLOWS +1 TASK/CONTAINER OVER THE 
        # QUEUE CAPACITY LIMIT: bug 5503048
        my $allowed_overage = 1;
        $capacity_limit += $allowed_overage;
        note("Bug 5503048: Hadoop allows +1 task container over the ".
             "queue capacity limit: '$capacity_limit'");
    } else {
        $capacity_limit=$capacity->{queues}->{$queue}->{maximum};
        note("For multiple users or multiple queues, use the maximum ".
             " queue capacity as the limit for the queue '$queue': ".
             "$capacity_limit");
    }    
    return $capacity_limit;
}

sub parse_job_info {
    my ($self, $jobs) = @_;
    my $user_per_job = [];
    my $queue_per_job = [];
    my $tasks = [];
    foreach my $job (@$jobs) {
        push(@$user_per_job, ($job->{user}||'hadoopqa'));
        push(@$queue_per_job, ($job->{queue}||'default'));
        push(@$tasks, ($job->{map_task}+$job->{reduce_task}));
    }
    my $users_ut = [sort keys %{{map { $_ => 1 } @$user_per_job}}];
    my $queues_ut = [sort keys %{{map { $_ => 1 } @$queue_per_job}}];
    my $single_user = (scalar(@$users_ut) > 1) ? 0 : 1;
    my $single_queue = (scalar(@$queues_ut) > 1) ? 0 : 1;

    # check the system property values first
    my $am_resource;
    foreach my $prop (@{$jobs->[0]->{args}}) {
        next unless ($prop =~ '-Dyarn.app.mapreduce.am.resource.mb');
        my @tokens = split('=', $prop);
        my $am_mb = $tokens[1];
        my $am_gb=nearest(.01, $am_mb/1024) if ($am_mb);
        $am_resource = $am_gb;
        note("AM RESOURCE = '$am_resource'");
    }
    $am_resource ||= $self->{test}->get_am_resource();

    my $jobs_info = { 'users'        => $user_per_job,
                      'queues'       => $queue_per_job,
                      'tasks'        => $tasks, 
                      'users_ut'     => $users_ut,
                      'queues_ut'    => $queues_ut,
                      'single_user'  => $single_user, 
                      'single_queue' => $single_queue,
                      'am_resource'  => $am_resource,
                      'num_jobs'     => scalar(@$jobs) };
    return $jobs_info;
}

sub capture_run_stats {
    my ($self, $jobs, $job_ids, $capacity, $wait_time, $max_iter, $verbose) = @_;
    my $num_jobs_tasks;
    my $active_users = {};
    my ($running_job_tasks, $running_queue_tasks, $running_cluster_tasks);
    my ($num_active_queues, $num_active_users);
    my $actual_start_order;
    my $actual_concurrent_instances;

    my $jobs_info = $self->parse_job_info($jobs);
    my $running_job_tasks_all = {};
    my $running_queue_tasks_all = {};
    my $running_cluster_tasks_all = [];
    my $running_stats = { 'job'     => $running_job_tasks_all,
                          'queue'   => $running_queue_tasks_all,
                          'cluster' => $running_cluster_tasks_all };
    foreach my $job_index (0..$#$job_ids) {
        $running_job_tasks_all->{$job_index} = [];
    }
    foreach my $queue (@{$jobs_info->{queues_ut}}) {
        $running_queue_tasks_all->{$queue} = [];
    }

    $max_iter ||= 1000;
    my $index = 0;
    while ($self->{test}->is_any_job_running($verbose)) {
        $index++;
        last if ($index > $max_iter);
        note("--> loop # $index:");

        foreach my $queue (@{$jobs_info->{queues_ut}}) {
            $running_queue_tasks->{$queue}=0;
        }
        $running_cluster_tasks=0;

        # Tasks counts via separate calls to job -list-attempt-ids for each
        # job are not guaranteed to be accurate due to timing issues.
        # So use tasks counts from job -list instead. 
        # $num_jobs_tasks = $self->{test}->get_num_jobs_tasks($job_ids, 'running', $verbose);
        # note("--> Check running tasks status=",explain($num_jobs_tasks));
        # my $num_map_tasks[$job_index]    = $num_jobs_tasks->{$job_ids->[$job_index]}->{MAP};
        # my $num_reduce_tasks[$job_index] = $num_jobs_tasks->{$job_ids->[$job_index]}->{REDUCE};
        note("--> Check jobs status:");
        my $jobs_status = $self->{test}->get_jobs_status(undef, 1);

        $active_users = {};
        foreach my $job_index (0..$#$job_ids) {
            my $job_id = $job_ids->[$job_index];
            my $job_status = $jobs_status->{$job_id};
            note("job status = ",explain($job_status)) if ($verbose);

            # Clear the job tasks numbers that was stored from the previous poll. 
            $running_job_tasks->[$job_index] = 0;
            next unless ($job_status);

            my $queue                 = $job_status->{queue};
            my $username              = $job_status->{username};
            my $running_tasks_for_job = int($self->{test}->get_used_memory($job_status));

            # This includes the 1 AM task though, which has default value of 1 GB.
            # $running_tasks_for_job = $running_tasks_for_job - 1;

            $active_users->{$username} = 1 if ($username);
            $running_job_tasks->[$job_index] = $running_tasks_for_job;
            $running_cluster_tasks += $running_tasks_for_job;
            note("running_tasks_for_job '$job_id' (memory used) = ".
                 "'$running_tasks_for_job' GB");
            note("running_queue_tasks = ",explain($running_queue_tasks)) if ($verbose);
            $running_queue_tasks->{$queue} += $running_tasks_for_job;
            note("running_queue_tasks = ",explain($running_queue_tasks)) if ($verbose);

            # Track job start order
            unless (grep {$_ == $job_index} @$actual_start_order) {
                push(@$actual_start_order, $job_index)
                    if ($running_tasks_for_job > 0);
            }

            # CAPTURE THE JOB CAPACITY LIMIT
            push(@{$running_job_tasks_all->{$job_index}}, $running_tasks_for_job);
        }

        # CAPTURE QUEUE CAPACITY LIMITS
        note("running_queue_tasks = ",explain($running_queue_tasks));
        foreach my $queue (@{$jobs_info->{queues_ut}}) {
            push(@{$running_queue_tasks_all->{$queue}}, $running_queue_tasks->{$queue});
        }

        # CAPTURE THE CLUSTER CAPACITY LIMIT
        push(@$running_cluster_tasks_all, $running_cluster_tasks);

        note("starting order=".join(",", @$actual_start_order));
        if ($verbose) {
            note("running job tasks all = ", explain($running_job_tasks_all));
            note("running queue tasks all = ", explain($running_queue_tasks_all));
            note("running cluster tasks all = ", explain($running_cluster_tasks_all));
        }

        $num_active_users = keys( %$active_users );
        $num_active_queues = keys( %$running_queue_tasks_all );
        note("num_active_users = '$num_active_users':");
        note("num_active_queues = '$num_active_queues':");
        if ($verbose) {
            note("num_active_users = '$num_active_users':", explain($active_users));
            note("num_active_queues = '$num_active_queues':", explain($running_queue_tasks_all));
        }

        # Track jobs running in parallel
        note("running job tasks=", explain($running_job_tasks)) if $verbose;
        if ($jobs_info->{num_jobs} > 1) {
            my $num_jobs_running=0;
            foreach my $job_index (0..$#$job_ids) {
                $num_jobs_running++ if ($running_job_tasks->[$job_index] > 0);
            }
            note("number of jobs running concurrently: $num_jobs_running");
            push(@$actual_concurrent_instances, $num_jobs_running);
        }

        # if ($wait_time) {
        #     note("sleep $wait_time");
        #     sleep $wait_time;
        # }

        next;
    }
    $running_stats->{start_order} = $actual_start_order;
    $running_stats->{concurrent_instances} = $actual_concurrent_instances;
    $running_stats->{num_active_users} = keys( %$active_users );
    $running_stats->{num_active_queues} = keys( %$running_queue_tasks_all );
    return $running_stats;
}

sub test_capacity_scheduler {
    my ($self, $args, $verbose) = @_;    
    note("--> Test Capacity Scheduler: Called by ", join(' ', caller()));
    note("Verify the number of running tasks do not exceeded the capacity limit:");
    note("args = ", explain($args));
    my $user            = defined($args->{user})            ? $args->{user}            : 'hadoopqa';
    my $queue           = defined($args->{queue})           ? $args->{queue}           : 'default';
    my $capacity        = defined($args->{capacity})        ? $args->{capacity}        : 0;
    my $num_tasks       = defined($args->{num_tasks})       ? $args->{num_tasks}       : 0;
    my $factor          = defined($args->{factor})          ? $args->{factor}          : 1;
    my $num_iter        = defined($args->{num_iter})        ? $args->{num_iter}        : 1000;
    my $wait_time       = defined($args->{wait_time})       ? $args->{wait_time}       : 0;
    my $min_concur_jobs = defined($args->{min_concur_jobs}) ? $args->{min_concur_jobs} : 0;
    my $exp_start_order = defined($args->{start_order} )    ? $args->{start_order}     : 0;
    my $jobs    = $args->{jobs}    if defined($args->{jobs});
    my $job_ids = $args->{job_ids} if defined($args->{job_ids});
    my @is_subseq_jobs_on_hold;

    my $jobs_info = $self->parse_job_info($jobs);
    note("--> Testing capacity scheduler with: '$jobs_info->{num_jobs}' jobs, '".
         scalar(@{$jobs_info->{users_ut}})."' users (".
         join(',', @{$jobs_info->{users_ut}}).") and '".
         scalar(@{$jobs_info->{queues_ut}})."' queues (".
         join(',', @{$jobs_info->{queues_ut}})."):");
    note("Wait for the map task for the first job to start running:");
    $self->{test}->wait_for_tasks_state($job_ids->[0], 'MAP', 'running',
                                        undef, 1, 10, $verbose);
    my $running_stats =
        $self->capture_run_stats($jobs, $job_ids, $wait_time, $num_iter, $verbose);
    my $running_job_tasks_all       = $running_stats->{job};
    my $running_queue_tasks_all     = $running_stats->{queue};
    my $running_cluster_tasks_all   = $running_stats->{cluster};
    my $actual_start_order          = $running_stats->{start_order};
    my $actual_concurrent_instances = $running_stats->{concurrent_instances};
    my $num_active_queues           = $running_stats->{num_active_queues};

    note("capacity = ", explain($capacity));
    note("running job tasks all = ", explain($running_job_tasks_all));
    note("running queue tasks all = ", explain($running_queue_tasks_all));
    note("running cluster tasks all = ", explain($running_cluster_tasks_all));
    
    my $capacity_limits = {};
    foreach my $queue (@{$jobs_info->{queues_ut}}) {
        $capacity_limits->{$queue} =
            $self->get_capacity_limit($capacity, $queue, $jobs_info->{single_user},
                                      $num_active_queues);
    }

    # VERIFY JOB TASKS ARE WITHIN USER/QUEUE/CLUSTER CAPACITY LIMITS

    # TODO:
    # CHECK FOR GUARANTEED CAPACITY
    # if not use maximum capacity
    # if multiple queues, may need to use queue_capacity, minimum_user_limit_percent
    # Effective_capacity

    # TEST THE JOB CAPACITY LIMIT IS NOT EXCEEDED FOR EACH USER QUEUE LIMIT
    # TODO:
    # 1) We should test that job is using capacity below the max capacity and above
    # the user/queue capacity.
    # 2) Verify cluster_capacity and queue_capacity against log
    # 3) Test user limit factor at, below, and above 1
    note("Test the capacity limit for each job(s) submitted:");
    foreach my $job_index (0..$#$job_ids) {
        my $queue = $jobs->[$job_index]->{queue};

        # If only a single user is tested, use the user queue capacity.
        # my $capacity_limit = $capacity_limits->{$queue};
        my $capacity_limit = $capacity->{queues}->{$queue}->{user};
        note("For single user, use the user queue capacity (with ulf) as the ".
             "limit for the queue '$queue': $capacity_limit");

        # WORKAROUND: HADOOP ALLOWS +1 TASK/CONTAINER OVER THE 
        # QUEUE CAPACITY LIMIT: bug 5503048
        my $allowed_overage = 1;
        $capacity_limit += $allowed_overage;
        note("Bug 5503048: Hadoop allows +1 task container over the ".
             "queue capacity limit: '$capacity_limit'");
        
        note("Running tasks for Job ".($job_index+1)." for each check point: (".
             join(',', @{$running_job_tasks_all->{$job_index}}).")");
        my $max_running_tasks_for_job = max( @{$running_job_tasks_all->{$job_index}} );
        $max_running_tasks_for_job ||= 0;
        ok(($max_running_tasks_for_job <= $capacity_limit),
           "Test Job Limit (J".($job_index+1)."): Maximum actual number of tasks of ".
           "'$max_running_tasks_for_job' for Job ".($job_index+1)." for the ".
           "'$queue' queue should be ".
           "<= maximum user capacity limit of '$capacity_limit'");

        # TODO: THIS CANNOT BE RELIABLY TESTED
        # # Should be greater or equal to the minimum user limit if (# of users x mul) > 100
        # # Should be smaller or equal to the minimum user limit if (# of users x mul) < 100
        # # Need to account for the AM
        # $capacity_limit = $capacity->{queues}->{$queue}->{minimum_user_limit};
        # ok(($max_running_tasks_for_job >= $capacity_limit),
        #    "Test Job Limit (J".($job_index+1)."): Maximum actual number of tasks of ".
        #    "'$max_running_tasks_for_job' for Job ".($job_index+1)." for the ".
        #    "'$queue' queue should be ".
        #    ">= minimum user limit of '$capacity_limit'");
    }

    # TEST THE QUEUE CAPACITY LIMIT FOR EACH QUEUE IS NOT EXCEEDED FOR MULTIPLE
    # JOBS WITH MULTIPLE USERS.
    if ($jobs_info->{num_jobs} > 1) {
        note("Test the capacity limit for each queue with job(s) submitted:");
        my $index=1;
        foreach my $queue (@{$jobs_info->{queues_ut}}) {
            my $capacity_limit = $capacity_limits->{$queue};
            note("Running tasks for the '$queue' queue for each check point: (".
                 join(',', @{$running_queue_tasks_all->{$queue}}).")");
            my $max_running_tasks_for_queue = max( @{$running_queue_tasks_all->{$queue}} );
            ok(($max_running_tasks_for_queue <= $capacity_limit),
               "Test Queue Limit: (Q$index): Maximum actual number of total running tasks of ".
               "'$max_running_tasks_for_queue' for the queue '$queue' should be ".
               "<= maximum queue capacity limit of '$capacity_limit':");
            $index++;
        }
    }    

    # TEST THE CLUSTER CAPACITY LIMIT IS NOT EXCEEDED FOR JOBS WITH
    # MULTIPLE QUEUES
    unless ($jobs_info->{single_queue}==1) {
        note("Test the capacity limit for the cluster:");
        note("Running tasks for the entier cluster for each check point: (".
             join(',', @$running_cluster_tasks_all).")");
        my $max_running_tasks_for_cluster = max( @$running_cluster_tasks_all );
        my $cluster_capacity=$capacity->{cluster};
        ok(($max_running_tasks_for_cluster <= $cluster_capacity),
           "Test Cluster Limit: Maximum actual number of total running tasks of ".
           "'$max_running_tasks_for_cluster' in the entire cluster should be ".
           "<= cluster capacity of '$cluster_capacity'");
    }

    # ADDITIONAL VERIFICATION STEPS

    # VERIFY EXPECTED JOB START ORDER
    note("actual starting order = ".join(",",@$actual_start_order));
    if ($exp_start_order) {
        note("expected starting order = ".join(",",@$exp_start_order));
        note("Compare start order");
        is_deeply($exp_start_order,
                  $actual_start_order,
                  "Jobs are process in the expected order");
    }

    # TEST JOBS RUN IN PARALLEL
    # VERIFY THE CORRECT NUMBER OF CONCURRENT JOBS ARE ACHIEVED
    if (($jobs_info->{num_jobs} > 1) && ($min_concur_jobs)) {
        note("actual concurrent jobs instances = ".
             join(",",@$actual_concurrent_instances));
        # Number of concurrent jobs could be higher than the minimum number of
        # concurrent jobs if some jobs are not taking all the allotted capacity.
        ok(((grep {$_ >= $min_concur_jobs} @$actual_concurrent_instances) > 0),
           "Jobs should run in parallel with at least ".
           "'$min_concur_jobs' concurrent jobs: ".
           "concurrent instances: ".join(",",@$actual_concurrent_instances));
    }

    # VERIFY THE JOBS COMPLETED SUCCESSFULLY
    foreach my $job_index (0..$#$job_ids) {
        is($self->{test}->wait_for_job_to_succeed($job_ids->[$job_index], 5, 72, $verbose),
           0,
           "Job ".($job_index+1)." '$job_ids->[$job_index]' should completed successfully.");
    }
}


1;
