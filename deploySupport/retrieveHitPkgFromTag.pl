#!/usr/bin/perl -w

###########################################
# this sript is used to 
# - STEP 1. retrieve hadoop service pkg installation strings
#   from HIT_DEPLOYMENT_TAG, which basically is a dist tag.
#   we don't need to hardcoded pkg names since we are relying 
#   on the regex and pkg keywords
#
# - STEP 2. construct a shell script which will export all the env
#   variables based on the strings generated in step 1.
###########################################

use strict;

my $export_file = "exportHITpkgs.sh";
my $text = "";
my $tag = $ENV{HIT_DEPLOYMENT_TAG};
my @pkg_keywords = qw(distcp pig hive hcat oozie log_collector vaidya hdfsproxy zookeeper nova daq);

print "Error: cannot find '/home/y/bin/dist_tag' in this machine, please install dist_tools first!\n\n" && exit(1) if (!(-f "/home/y/bin/dist_tag"));

my $output = `/home/y/bin/dist_tag list $tag | cut -f1 -d " " | tr "\n" " "`;
print $output . "\n";
my @pkgs = split(/\s+/, $output);

# Constructing pkg string for hadoop core test pkg
foreach my $pkgname (@pkgs) 
{
    next if (!$pkgname);
    if ($pkgname =~ /hadoopcoretest\-\d+\.\d+/i) {
        $text .= "export HADOOPCORE_TEST_PKG=hadoopcoretest:$tag\n";
    }
}
	
# Constructing pkg string for hadoop service pkgs
foreach my $keyword (@pkg_keywords)
{
    my $tmpstr = "";
    my $tmpstr_test = "";
    foreach my $pkgname (@pkgs) 
    {
        next if (!$pkgname);


        if (($pkgname =~ /(.*$keyword.*?)\-\d+\.\d+/i) && ($pkgname !~ /test/))
        {
            $tmpstr .= $1 . ":" . $tag . " ";
        }

        if (($pkgname =~ /test/) && ($pkgname =~ /((.*$keyword.*?)\-\d+\.\d+)/i))
        {
            $tmpstr_test .= $2 . ":" . $tag . " ";
        }

    }

    $text .= "export ". uc($keyword) . "VERSION=\"". $tmpstr ."\"\n";

    if ($tmpstr_test) {
        $text .= "export ". uc($keyword) . "_TEST_PKG=\"". $tmpstr_test ."\"\n";
    }
}

# the naming convention for pkg "hit" is very straight-forward, and we don't need to rely on regex to get the pkg name
$text .= "export HITVERSION=hit:$tag\n";

# other hit related variables:
$text .= "export INSTALL_HIT_TEST_PACKAGES=true\n";
$text .= "export EXCLUDE_HIT_TESTS=none\n";
$text .= "export RUN_HIT_TESTS=true\n";

# spare config pkgs
# still need to figure out why we need these pkgs:
$text .= "export SPARECONFIGPACKAGES=\"datanode:$tag namenode:$tag hadoopviewfs:$tag -br test\"";

print $text;
my $fh;
open($fh , ">$export_file");

print $fh $text; 
close $fh;

print "\n==================================================\n";
print " Using HIT_DEPLOYMENT_TAG=$tag for deployment \n";
print "==================================================\n";
print $text;
print "\n==================================================\n";
