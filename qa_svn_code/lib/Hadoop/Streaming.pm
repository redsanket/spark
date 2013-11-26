#
# Copyright Yahoo! 2011
#

=pod

=head1 NAME

package Hadoop::Streaming

=head1 DESCRIPTION

This module contains basic functions for running various hadoop streaming jobs

=head1 FUNCTIONS

The following functions are implemented:

=cut

package Hadoop::Streaming;

use strict;
use warnings;
use FindBin qw($Bin $Script);

use Hadoop::Test; # This will change when more functions are broken out of Test.pm
use Hadoop::Config;
use Test::More;
use Text::Trim; 
use String::ShellQuote; # perl-String-ShellQuote

sub new {
    my ($class, $test) = @_;

    my $self = {
        test => $test,
        config => $test->config,
        lib_path => "$FindBin::Bin/../../../../lib/Hadoop",
    };
    bless $self, $class;
};

=head2 run_streaming_job($self, $files, $map_command, $reduce_command, @args)

Run a job using the hadoop-streaming jar.

=over 2

=item I<Parameters>:

   * $files: specify comma separated files to be copied to the map reduce cluster
   * $map_command: command to run on the mapper (e.g. 'env', 'cat')
   * $reduce_command: command to run on the reducer(e.g., 'env', 'NONE')
   * args (array reference)
   * verbose

=item I<Returns>:

   * string containing the text from the files in the output folder

=back

=cut
sub run_streaming_job {
    my ($self, $files, $map_command, $reduce_command, $input_file, $args_ref, $verbose) = @_;
    my $cur_time = time;
    my $temp_input_file = "input-${cur_time}-$$";
    my $output_path = "HadoopQEOutput-${cur_time}-$$";
    my @args = @$args_ref;

    # Write the dummy input file
    $self->{test}->run_hadoop_command( {
        command => 'fs',
        args => ['-put', $input_file, $temp_input_file],
    } );

    # Add the extra args to the args array_ref
    push(@args, '-files', "$files") if ($files);
    push(@args,
        '-input', $temp_input_file,
        '-output', $output_path,
        '-mapper', shell_quote($map_command),
        '-reducer', shell_quote($reduce_command),
    );

    # Run the streaming job
    my ($stream_stdout, $stream_stderr, $stream_success, $stream_exit_code) =
        $self->{test}->run_jar_command( {
            jar => $self->{config}->{HADOOP_STREAMING_JAR},
            args => \@args,
        } );

    my ($cat_stdout, $cat_stderr, $cat_success, $cat_exit_code) =
        $self->{test}->run_hadoop_command( {
            command => 'fs',
            args => ['-cat', "$output_path/*"],
        } );

    # Remove the input and output paths now
    $self->{test}->run_hadoop_command( {
        command => 'fs',
        args => ['-rmr', $output_path, $temp_input_file],
    } );

    if ($stream_exit_code != 0) {
        note("exit_code = $stream_exit_code\n");
        note("stdout = $stream_stdout\n");
        note("stderr = $stream_stderr\n");
        return;
    }

    $verbose = 1;
    note("streaming job output='$cat_stdout'") if $verbose;

    return $cat_stdout;
}

# Split key-value string into a hash
sub split_settings {
    my ($self, $delim, $text, $verbose) = @_;
    # handle text line without '=' by creating a default list ($_, '')
    my %hash = map { ($_ =~ m/=/) ? trim(split(/$delim/, $_, 2)) : (trim($_), '') } split(/\n/, $text);
    $verbose = 1;
    note("config hash = ", explain(\%hash)) if $verbose;
    return \%hash;
}

# Private function that adds -D to arguments
# Generates an array reference
sub _add_D_to_args {
    my ($self, $arg_hash) = @_;
    my @args;
    foreach my $arg (keys %$arg_hash) {
        push(@args, '-D', "$arg='$arg_hash->{$arg}'");
    }
    return \@args;
}

=head2 run_env_mapper($self, @args)

Run a job with a mapper that just runs 'env' to check settings.

=over 2

=item I<Parameters>:

   * Hash of args (key => value): These args will have the -D flag added to each value

=item I<Returns>:

   * $config_hash: Hash reference of all environment settings on the mapper

=back

=cut
sub run_env_mapper {
    my ($self, $arg_hash) = @_;

    # Make sure it doesn't run as an ubertask
    local $arg_hash->{'mapreduce.job.ubertask.enable'} = 'false';
    local $arg_hash->{'mapred.map.tasks'} = 1;
    local $arg_hash->{'mapred.reduce.tasks'} = 1;
    my $script='env.sh';
    my $files="$self->{lib_path}/$script#$script";
    my $output = $self->run_streaming_job("$files", "$script", 'NONE',
        "$self->{lib_path}/dummy_input", $self->_add_D_to_args($arg_hash));
    return (defined($output)) ? $self->split_settings('=', $output) : undef;
}


=head2 run_env_reducer($self, @args)

Run a job with a reducer that just runs 'env' to check settings.
** NOTE: this must also run a mapper, so ensure that mapper settings are valid!!

=over 2

=item I<Parameters>:

* Hash of args (key => value): These args will have the -D flag added to each value

=item I<Returns>:

   * $config_hash: Hash reference of all environment settings on the reducer

=back

=cut
sub run_env_reducer {
    my ($self, $arg_hash) = @_;

    # Make sure it doesn't run as an ubertask
    local $arg_hash->{'mapreduce.job.ubertask.enable'} = 'false';
    local $arg_hash->{'mapred.map.tasks'} = 1;
    local $arg_hash->{'mapred.reduce.tasks'} = 1;
    my $script='env.sh';
    my $files="$self->{lib_path}/$script#$script";
    my $output = $self->run_streaming_job("$files", 'cat', "$script",
        "$self->{lib_path}/dummy_input", $self->_add_D_to_args($arg_hash));
    return (defined($output)) ? $self->split_settings('=', $output) : undef;
}

1;
