#!/usr/local/bin/perl

use strict;
use warnings;
         
use FindBin;
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::BalancerTest);
use Test::More tests => 25;
             
my $self = __PACKAGE__->new;
    
my $expected_output = "The cluster is balanced. Exiting...";
my $expected_output1 = "Usage: java Balancer";

#Waiting for 20 seconds before the tests are started as dfsadmin -safemode get  even though it returns "Safe mode is OFF"
#balancer still reports "Cannot create file/system/balancer.id. Name node is in safe mode."
#my $safemode_wait = 20;
#sleep($safemode_wait);

#Initialize kinit on NN before running any tests
$self->run_initialize_kinit();


#Test for start balancer with default value for threshold, policy, out of range value, negative value and string value

$self->run_balancer("Balancer_01: Test Start balancer and check if the cluster is balanced after the run",$expected_output);
$self->run_balancer("Balancer_02: Test Start balancer with policy as datanode",$expected_output,"policy","datanode");
$self->run_balancer("Balancer_03: Test Start balancer with policy as blockpool",$expected_output,"policy","blockpool");
$self->run_balancer("Balancer_04: Test Start balancer with threshold 10",$expected_output,"threshold", 10);
$self->run_balancer("Balancer_05: Test Start balancer with negative threshold value",$expected_output1,"threshold", -999);
$self->run_balancer("Balancer_06: Test Start balancer with out of range threshold value",$expected_output1,"threshold", 91999);
$self->run_balancer("Balancer_07: Test Start balancer with string value for threshold",$expected_output1,"threshold", "asdg");
$self->run_balancer("Balancer_08: Test Start balancer with no arguments for policy",$expected_output1,"policy");
$self->run_balancer("Balancer_09: Test Start balancer with policy as datanode and negative value for threshold",$expected_output1,"policy","datanode","threshold",-8988);
$self->run_balancer("Balancer_10: Test Start balancer with policy as datanode and out of range value for threshold",$expected_output1,"policy","datanode","threshold",10889);
$self->run_balancer("Balancer_11: Test Start balancer with policy as datanode and string value for threshold",$expected_output1,"policy","datanode","threshold","asdfghk");
$self->run_balancer("Balancer_12: Test Start balancer with policy as blockpool and negative value for threshold",$expected_output1,"policy","blockpool","threshold",-8988);
$self->run_balancer("Balancer_13: Test Start balancer with policy as blockpool and out of range value for threshold",$expected_output1,"policy","blockpool","threshold",10988);
$self->run_balancer("Balancer_14: Test Start balancer with policy as blockpool and string value for threshold",$expected_output1,"policy","blockpool","threshold","asdfghk");
$self->run_balancer("Balancer_15: Test Start balancer with policy as numeric value threshold value as 10",$expected_output1,"policy","9999","threshold",10);
$self->run_balancer("Balancer_16: Test Start balancer with policy as string value threshold value as 10",$expected_output1,"policy","asdsd*","threshold",10);
$self->run_balancer("Balancer_17: Test Start balancer with policy as negative value threshold value as 10",$expected_output1,"policy","-2345578","threshold",10);
$self->run_balancer("Balancer_18: Test Start balancer with no value for threshold",$expected_output1,"threshold");
$self->run_balancer("Balancer_19: Test Start balancer with no value for both policy and  threshold",$expected_output1,"policy","","threshold","");

#In order to run the below tests remove the comments, I have commented these tests as it takes longer time to run
#Test simulate load and run balancer with low threshold value
#note("Running test balancer with low threshold value of 5 percent\n");
#$self->load_cluster_with_data("Balancer_20: Test load cluster with data",5);
#$self->run_balancer("Balancer_21: Test run balancer with 5 percent threshold value",$expected_output,"threshold", 5);

#Test simulate load and run balancer with high threshold value
#note("Running test balancer with high threshold value of 95 percent\n");
#$self->load_cluster_with_data("Balancer_22: Test load cluster with data",5);
#$self->run_balancer("Balancer_23: Test run balancer with 95 percent threshold value",$expected_output,"threshold", 95);


#Test: Starting Balancer when the cluster is already balanced
$self->run_balancer("Balancer_24: Test Start balancer first time","The cluster is balanced. Exiting...");
$self->run_balancer("Balancer_25: Test Start balancer when the cluster is already balanced.","The cluster is balanced. Exiting...");

#Test: Starting Balancer when half the datanodes are not running
$self->run_balancer_with_half_DN("Balancer_26: Test Starting Balancer when half the datanodes are not running", $expected_output);

#Test: Starting Balancer and loading data when half the datanodes are not running
$self->run_balancer_with_half_DN_simulate_load("Balancer_27: Test Running the balancer and simultaneously simulating load on the cluster with half the data nodes not running", $expected_output);
