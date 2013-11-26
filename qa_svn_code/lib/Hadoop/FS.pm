#
# Copyright Yahoo! 2011
#

=pod

=head1 NAME

package Hadoop::FS

=head1 DESCRIPTION

This module contains basic functions using hadoop fs

=head1 FUNCTIONS

The following functions are implemented:

=cut

package Hadoop::FS;

use strict;
use warnings;
use FindBin qw($Bin $Script);
use Test::More;
use Hadoop::Test; # This will change when more functions are broken out of Test.pm


# $user is optional and can be set/overridden with the set_user function
sub new {
    my ($class, $test, $user) = @_;

    my $self = {
        test => $test,
        config => $test->config,
        lib_path => "$FindBin::Bin/../../../../lib/Hadoop",
        tmpdirs => {},
    };
    $self->{user} = $user if defined($user);
    bless $self, $class;
};

# sub is defined to prevent AUTOLOAD to be called for DESTROY
sub DESTROY {
}

=head2 AUTOLOAD($self, @args)

Dynamically generates hadoop fs shell commands
A different user can be used by calling set_user first

=over 2

=item I<Parameters>:

   * Array of arguments will be passed along

=item I<Returns>:

   * Array containing stdout, stderr, success, and exit code as returned by run_hadoop_command

=back

=cut
our $AUTOLOAD;
sub AUTOLOAD {
    my $sub = $AUTOLOAD;
    $sub =~ s/.*:://;
    no strict 'refs';
    *$sub = sub {
        my $self = shift;
        my $arg_hash = {command => 'fs', args => ["-$sub", @_]};
        $arg_hash->{user} = $self->{user} if defined($self->{user});
        $self->{test}->run_hadoop_command($arg_hash)
    };
    goto &$sub;
}


=head2 set_user($self, $user)

Sets the user that commands will run as

=over 2

=item I<Parameters>:

   * String containing the user to run as or undef to reset the user

=item I<Returns>:

=back

=cut
sub set_user {
    my ($self, $user) = @_;
    if (defined($user)) {
        $self->{user} = $user;
    } else {
        delete($self->{user});
    }
}


=head2 ln($self, $target, $symlink)

Creates a symlink to the target. Currently this is not available through the hadoop fs command.

=over 2

=item I<Parameters>:

   * Target that the symlink should point to
   * The destination of the symlink

=item I<Returns>:

   * Array containing stdout, stderr, success, and exit code as returned by run_hadoop_command

=back

=cut
sub ln {
    my $self = shift;
    my $args = {command => 'jar', args => [$self->{lib_path} . '/Java/Symlink.jar', 'ln', @_]};
    $args->{user} = $self->{user} if defined($self->{user});
    $self->{test}->run_hadoop_command($args);
}

=head2 test_ip_change()

Test RPC Client properly handles IP change

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * Array containing stdout, stderr, success, and exit code as returned by run_hadoop_command

=back

=cut
sub test_ip_change {
    my $self = shift;
    my $args = {command => 'jar', 
                args => [$self->{lib_path} . '/Java/FileSystemTest.jar',                         
                         'FileSystemTest',
                         'test_ip_change', 
                         @_],
                mode => 'system'};
    $args->{user} = $self->{user} if defined($self->{user});
    my $exit_code = $self->{test}->run_hadoop_command($args);
    return $exit_code;
}

=head2 md5($self, $src)

Computes the md5 sum of the file by using cat and md5sum

=over 2

=item I<Parameters>:

   * The file to get the md5 sum

=item I<Returns>:

   * The md5 sum of the file or undef if error

=back

=cut
sub md5 {
    my $self = shift;
    my $args = {command => 'fs', args => ['-cat', @_]};
    $args->{user} = $self->{user} if defined($self->{user});
    my ($stdout, $stderr, $success, $exit_code) = $self->{test}->run_hadoop_command($args);

    my $md5;
    if ($exit_code == 0) {
        $md5 = `echo $stdout | md5sum` if ($exit_code == 0);
    }

    return ($md5, $stderr, $success, $exit_code);
}

=head2 get_tmpdir($self)

Creates a temporary folder name, adds it to the list kept track of in this object,
and returns the name

=over 2

=item I<Parameters>:

=item I<Returns>:

   * String containing the path of the newly-generated temporary folder

=back

=cut
sub get_tmpdir {
    my ($self) = @_;
    my $user = defined($self->{user}) ? $self->{user} : defined($ENV{USER}) ? $ENV{USER} : 'hadoop-fs';
    my $tmpdir = "/tmp/$user-temp-" . time . "-$$";
    $self->{tmpdirs}->{$tmpdir} = 1;
    return $tmpdir;
}


=head2 copy_files_to_hdfs($self, @files)

Creates a temporary folder and copies @files over to that temporary folder, returning the newly-created folder

=over 2

=item I<Parameters>:

   * Array of files to copy into the temporary folder on hdfs

=item I<Returns>:

   * String containing the path of the newly-created temporary folder

=back

=cut
sub copy_files_to_hdfs {
    my ($self, @files) = @_;
    my $test = $self->{test};
    my $user = defined($self->{user}) ? $self->{user} : defined($ENV{USER}) ? $ENV{USER} : 'hadoop-fs';
    my $tmpdir = "/tmp/$user-temp-" . time . "-$$";

    $self->mkdir($tmpdir);
    $self->{tmpdirs}->{$tmpdir} = 1;

    $self->copyFromLocal(@files, $tmpdir);
    return $tmpdir;
}


=head2 delete_folder($self, $folder)

Removes the specified folder from hdfs

=over 2

=item I<Parameters>:

   * String containing the path to the folder that will be deleted

=item I<Returns>:

=back

=cut
sub delete_folder {
    my ($self, $folder) = @_;
    delete $self->{tmpdirs}->{$folder};
    $self->rmr($folder);
}


=head2 cleanup_tmpdirs($self)

Removes the temporary folders that have been created by this object

=over 2

=item I<Parameters>:

=item I<Returns>:

=back

=cut
sub cleanup_tmpdirs {
    my $self = shift;
    $self->rmr(keys(%{$self->{tmpdirs}}));
}


1;
