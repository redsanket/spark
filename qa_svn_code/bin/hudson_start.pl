#!/home/y/bin/perl -w

########################################################################
# This is a wrapper script to
#  - retrieve build paramters from hudson UI page
#  - identify GW host for cluster
#  - scp source code to GW host
#  - kick off test suite from GW host
#  - copy the test results file back to hudson slave
########################################################################

use strict;
use Data::Dumper;
use Getopt::Long;
use File::Basename;

my %opts;

# Default values
$opts{igor_namespace} = "grid_re.clusters";
$opts{gw_dest_dir} = "/home/y/var/gridre/workspace";

# Get CLI options
GetOptions(
    "cluster=s" => \$opts{cluster},
    "cmd=s" => \$opts{cmd},
);

# Get cluster value from environment variable if it's not defined as
# a command line option
$opts{cluster} = $ENV{CLUSTER} if (!defined($opts{cluster}));
if (!defined($opts{cluster}))
{
    print "\n\nError: \"--cluster=<cluster>\" is a required parameter for running this script! exit now.....\n\n";
    exit (1);
}

if (!defined($opts{cmd}))
{
    print "\n\nError: \"--cmd=<cmd>\" is a required parameter for running this script! exit now.....\n\n";
    exit (1);
}


# Get all the other hudson related parameters;
$opts{workspace} = basename($ENV{WORKSPACE});
$opts{build_url} = $ENV{BUILD_URL};
$opts{hudson_url} = $ENV{HUDSON_URL};
$opts{job_name} = $ENV{JOB_NAME};
$opts{build_number} = $ENV{BUILD_NUMBER};
$opts{admin_host} = $ENV{ADMIN_HOST};

# retrieve GW host from igor definition for specified cluster
my $igor_gw_role = $opts{igor_namespace} . "." . $opts{cluster} . ".gateway" ;
my $gw_hostname = exec_cmd_w_output("/home/y/bin/igor fetch -members $igor_gw_role");

chomp($gw_hostname);
print "  ==> GW host for cluster '$opts{cluster}' is '$gw_hostname'\n";

# delete old artifacts if there is any
exec_cmd("rm -rf artifacts");

# delete old dir if there is any
exec_cmd("ssh $gw_hostname rm -rf $opts{gw_dest_dir}/$opts{workspace}");

# create a new dir for running test on GW host
exec_cmd("ssh $gw_hostname mkdir -p $opts{gw_dest_dir}/$opts{workspace}");


# scp the test script to GW
exec_cmd("tar cvfz qa_svn_code.tgz qa_svn_code");
exec_cmd("scp -r qa_svn_code.tgz $gw_hostname:$opts{gw_dest_dir}/$opts{workspace}");
exec_cmd("rm -f qa_svn_code.tgz");
exec_cmd("ssh $gw_hostname \"cd $opts{gw_dest_dir}/$opts{workspace} && tar xvfz qa_svn_code.tgz \"");
exec_cmd("ssh $gw_hostname \"cd $opts{gw_dest_dir}/$opts{workspace} && mv qa_svn_code/* . \"");

# kick off test suite from GW host
$opts{cmd} = "env TESTSUITEFILE=$ENV{TESTSUITEFILE} EMAIL_USER=$ENV{EMAIL_USER} CLUSTER=$opts{cluster} ADMIN_HOST=$opts{admin_host} WORKSPACE=$opts{gw_dest_dir}/$opts{workspace} " . $opts{cmd};
exec_cmd("ssh $gw_hostname \"cd $opts{gw_dest_dir}/$opts{workspace} && $opts{cmd} \"");

# copy the test results file back to hudson slave
exec_cmd("scp -r $gw_hostname:$opts{gw_dest_dir}/$opts{workspace}/artifacts . ");

print "TEST EXECUTION FINISHED!\n";

# wrapper function to execute system cmd
# and the return value is the cmd output 
sub exec_cmd_w_output {
    my $cmd = shift @_;
    print "  -- Issue cmd '$cmd'\n";
    my $output;
    $output = `$cmd`;
    if ($?)
    {
       print "Error in executing cmd '$cmd': $? $!\n";
       exit(1);
    }
    # $output = `$cmd` or die "Error in executing cmd '$cmd': $?\n";
    print "$output\n";
    return $output;
}

sub exec_cmd {
    my (@args) = @_;
    print "  -- Issue cmd '@args'\n";
    system(@args) == 0 or die "system @args failed: $? $!";
}

sub getTime {
    my ($minute, $hour, $day, $month, $year)= (localtime)[1..5];
    $year += 1900;
    $month++;
    return sprintf("%04d%02d%02d%02d%02d", $year,$month,$day,$hour,$minute);
}

