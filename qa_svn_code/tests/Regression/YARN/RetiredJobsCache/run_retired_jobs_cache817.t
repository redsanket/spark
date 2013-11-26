#!/usr/local/bin/perl

###############################################################################
# This tests MAPREDUCE-817: Add a cache for retired jobs with minimal job info
# and provide a way to access history file url
#
# https://issues.apache.org/jira/browse/MAPREDUCE-817
# http://twiki.corp.yahoo.com/view/Grid/AgileTestReport2949784#3_3_MAPREDUCE_817
#
# Tests run in approximately 2.25 minutes
###############################################################################

use strict;
use warnings;

use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Hadoop::HtmlTables;

use Test::More tests => 11;

$ENV{OWNER} = 'jnaisbit';

# Initialize the Test module
my $self   = __PACKAGE__->new;
my $Config = $self->config;

# Print the environment
note(explain($Config));

my $RETIRE_SECONDS = 10;
my $RETIRE_MS = $RETIRE_SECONDS * 1000;
my $SLEEP_SECONDS = 1;
my $SLEEP_MS = $SLEEP_SECONDS * 1000;
my $WAIT_FOR_RETIRE = ($RETIRE_SECONDS + $SLEEP_SECONDS) * 4;
my $WAIT_INTERVAL = 1; # How long to sleep between uri checks
my $KILL = 1;
my $FAIL = 2;
my $WAIT = 1;

# Checks whether the job_id is in the specified table
sub is_job_id_in_table {
    my ($self, $job_id, $table_name, $host_tables) = @_;
    foreach my $host (keys %$host_tables) {
        my $tables = $host_tables->{$host};
        my $rows = $tables->{$table_name}{rows};
        foreach my $row (@$rows) {
            # The job_id is a link and thus a hash-ref of job_id => link
            map {
                if (ref($_) eq 'HASH') {
                    return 1 if $job_id eq join('', keys(%$_));
                } else { # If the job failed, it may not be a link (depending on when it was killed)
                    return 1 if $job_id eq $_;
                }
            } @$row;
        }
    }
    return 0;
}

sub _print_table {
    my ($self, $host_tables, $table_name, $label) = @_;
    note($label);
    foreach my $host (keys %$host_tables) {
        my $tables = $host_tables->{$host};
        note(explain($tables->{$table_name}));
    }
}

# Runs the default job for this test script and returns the job_id
sub run_job {
    my ($self, $args, $job_cmd) = @_;
    my $hash = {
        args => $args,
        map_task => 1,
        map_sleep => $SLEEP_MS,
    };

    return $self->submit_sleep_job($hash) if $job_cmd == $KILL;

    my ($stdout, $stderr, $success, $exit_code, $job_id) = $self->run_sleep_job($hash);
    note("job_id: '$job_id' completed");
    return $job_id;
}

sub get_and_check_table {
    my ($self, $job_id, $wait) = @_;
    my $html_tables = Hadoop::HtmlTables->new($self);

    my $host_tables = $html_tables->get_jobtracker_html_tables;
    return $self->is_job_id_in_table($job_id, 'Retired Jobs', $host_tables) unless $wait;
    sleep($WAIT_INTERVAL);

    my $found;
    my $start_time = time;
    while (time - $start_time < $WAIT_FOR_RETIRE) {
        my $host_tables = $html_tables->get_jobtracker_html_tables;
        $found = $self->is_job_id_in_table($job_id, 'Retired Jobs', $host_tables);
        last if $found;
        sleep($WAIT_INTERVAL);
    }
    return $found;
}

sub test_jobtracker_table {
    my ($self, $job_cmd) = @_;
    $job_cmd = 0 unless defined($job_cmd);
    my $args = [ '-D', "mapred.jobtracker.retirejob.check=$RETIRE_MS" ];
    push(@$args, '-D', "mapred.child.java.opts=-youshoulddie") if $job_cmd == $FAIL;

    my $job_id = $self->run_job($args, $job_cmd);

    $self->kill_job($job_id) if $job_cmd == $KILL;

    my $job_in_table = $self->get_and_check_table($job_id);

    my $prefix = $job_cmd == $KILL ? 'killed ' : $job_cmd == $FAIL ? 'failed ' : '';
    my $msg_ok =  "${prefix}job ($job_id) is not in the 'Retired Jobs' table before it should be";
    ok(!$job_in_table, $msg_ok) unless $job_cmd == $FAIL;

    $job_in_table = $self->get_and_check_table($job_id, $WAIT);
    my $msg_nok =  "${prefix}job ($job_id) is in the 'Retired Jobs' table after the " .
        "specified mapred.jobtracker.retirejob.check ($RETIRE_SECONDS) seconds";
    ok($job_in_table, $msg_nok);
}


# Some table names have dynamic data, so check with regexes
sub check_table_exists {
    my ($self, $table_name, $tables) = @_;
    map { return 1 if ($_ =~ /$table_name/) } keys(%$tables);
    return 0;
}

# Runs a dummy job to make sure the jobtracker is up and running again
sub wait_for_jobtracker {
    my $self = shift;
    $self->run_sleep_job( {
        map_task => 1,
        reduce_task => 0,
        map_sleep => 0,
        reduce_sleep => 0,
    } );
}

# TC1: Set mapred.jobtracker.retirejob.check to value 'x'. Submit a job and let it complete
#   After time 'x', the job should be moved to retired jobs (DEFAULT is 10000)
$self->test_jobtracker_table;

# TC2: Set mapred.jobtracker.retirejob.check to value 'x'. Submit a job and kill it
#   After time 'x', the job should be moved to retired jobs
$self->test_jobtracker_table($KILL);

# TC3: Set mapred.jobtracker.retirejob.check to value 'x'. Submit a job and fail it
#   After time 'x', the job should be moved to retired jobs
$self->test_jobtracker_table($FAIL);

# TC4: Check the UI for all the sections
#   All the sections should be displayed correctly
#   FAILED: Not as expected:
#   When there are 0 jobs to be displayed in Completed/Failed section,
#   the entire section is not visible in the UI. This is different from
#   the the previous releases which used to display it. Also there is an
#   inconsistency since Retired/Running jobs show "none" even though
#   there are none to display. BUG:2962484

my $html_tables = Hadoop::HtmlTables->new($self);
my $host_tables = $html_tables->get_jobtracker_html_tables;

foreach my $host (keys %$host_tables) {
    my $tables = $host_tables->{$host};
    my @headers = ('Cluster Summary', 'Scheduling Information', 'Running Jobs', 'Retired Jobs');
    my @missing_headers = ('Completed Jobs', 'Failed Jobs'); # Missing due to bug 2962484
    foreach my $table_name (@headers) {
        ok($self->check_table_exists($table_name, $tables), "$table_name table displayed");
    }
    foreach my $table_name (@missing_headers) {
        ok($self->check_table_exists($table_name, $tables), "$table_name table displayed");
    }
}

# TC5: Create some dummy files in the "done" directory
#   The jobhistory.jsp page should display the job files correctly
#   FAILED:  Not as expected
#   ArrayIndexOutOfBoundsException when there are some dummy
#   files in DONE directory. BUG:2965036 is open for it
#   Automated-Failed and cannot verify the script

# Verify that the job history table works correctly

# We are unable to automate TC5 until bug 4438089 is resolved


