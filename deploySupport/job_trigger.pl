#!/usr/bin/perl -w

###################################################################
#
# $Id$
# You will need to install the following packages:
#   - yinst install perl_Yahoo_Backyard_SingleSignOn
#   - yinst install ypan/perl-XML-Simple
#
# This is a sample script that be used to trigger hudson jobs with 
# parameters from command line
# 
# Here is the example of how to use this script
# 1. Run as headless user hadoopqa
# $ ./job_trigger.pl --jobname RE-Deploy-Testing1 --paramenter PATH=abc 
#   CLUSTER=ankh --parameter REMOVEEXISTINGDATA="" --use_keydb 
#
# 2. Run as regular unix user:
# $ ./job_trigger.pl --jobname RE-Deploy-Testing1 --paramenter PATH=abc 
#   CLUSTER=ankh 
###################################################################

use Yahoo::Backyard::SingleSignOn;
use Getopt::Long;
use XML::Simple;
use Data::Dumper;
use strict;

my %opts;


# default value of hudson URL
$opts{deploy_hudson_server} = "http://hadoopre1.corp.sk1.yahoo.com:9999/yhudson";

GetOptions(
    "jobname=s"  => \$opts{jobname}, 
    "parameter=s@"   => \$opts{parameter},
    "use_keydb"     => \$opts{use_keydb},
    "deploy_hudson_server=s" => \$opts{deploy_hudson_server},
    "keydb_user" => \$opts{keydb_user},
    "keydb_password" => \$opts{keydb_password},
    "debug" => \$opts{debug},
    "h|?"       => \$opts{help},
);

print_usage() && exit(0) if $opts{help};


if (!defined($opts{jobname}))
{
    print "\n\nError: For required option \"--jobname\", please specify a hudson job name to start the automatic deployment.\nTo get help, please type \"$0 -?\"\n\n";
    exit(1);
}
 
print Dumper %opts if $opts{debug};

if ($opts{use_keydb})
{ 
    if ($ENV{USER} eq "hadoopqa")
    {
        # if we run as headless account "hadoopqa", 
	# by default we will user headless bouncer account from keydb file
        $opts{keydb_user} = "hadoopqa_bouncer.user";
	$opts{keydb_password} = "hadoopqa_bouncer.passwd";
    }

    if (!$opts{keydb_password} || !$opts{keydb_user})
    {
        print "\nError: please specify --keydb_user and --keydb_password if you would like to use keydb to sign in bouncer......\n";
	exit(1);
    }

    # reading user/password from keydb_pkg instead
    use ysecure;
    ycrKeyDbInit();
    $opts{bouncer_user} = ycrGetKey($opts{keydb_user})->value();
    $opts{bouncer_password} = ycrGetKey($opts{keydb_password})->value();
}


my $baseurl = $opts{deploy_hudson_server} . "/job/" . $opts{jobname}; 


###################################################################
# We are using bouncer headless account to automate this process
###################################################################

my $sso;

if ($opts{bouncer_user} && $opts{bouncer_password})
{
    $sso = Yahoo::Backyard::SingleSignOn->new(user=>$opts{bouncer_user},password=>$opts{bouncer_password});
} else {
    $sso = Yahoo::Backyard::SingleSignOn->new();
}
print "\n\nInvoke Hudson project '$opts{jobname}'\n";
print "URL for this project: $baseurl\n\n";

# Check if hudson job exists or not
$sso->get("$baseurl/api/xml");  

my $status = $sso->content;
if ($status =~ /HTTP\s+Status\s+404/) {
    print "It seems hudson job $opts{jobname} doesn't exist, server returns 404......\nare you sure the link '$baseurl' is corrent?\n\n";
    exit(1);
}


# parse XML response and get the crumb value
print "Try to get the crumb value now....\n";
my ($xs1, $doc, $crumb_response, $crumb);
$sso->get("$opts{deploy_hudson_server}/crumbIssuer/api/xml");
$crumb_response = $sso->content;
$xs1 = XML::Simple->new();
$doc = $xs1->XMLin($crumb_response);
$crumb = $doc->{crumb};

print "The crumb string is '$crumb'\n\n" if $opts{debug};

# start a new deployment run now
# this will be a POST request, and parameter will be crumb string
if ($opts{parameter}) {
    # trigger build  with parameters
    # construct form data
    my %form_data;
    $form_data{".crumb"} = $crumb;
    foreach (@{$opts{parameter}})
    {
        my @temp = split("=", $_);
        $form_data{$temp[0]} = $temp[1];
    }
    print Dumper %form_data if $opts{debug};
    $sso->post("$baseurl/buildWithParameters", \%form_data);
} else {
    # there is no build parameters
    $sso->post("$baseurl/build", {".crumb", "$crumb"});
}
print "\n\n";
print $sso->content if $opts{debug};
print "Start deployment job $opts{jobname} already\n\n";

# erase your backyard cookies written to disk...
$sso->logout();


# Print usage information for this script
sub print_usage
{
    print "Usage: $0 [options]
  
  -h|?
   Print usage.

";

}
