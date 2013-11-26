#
# Copyright Yahoo! 2011
#

=pod

=head1 NAME

package Hadoop::BalancerTest

=head1 DESCRIPTION

This module contains the test base class for Hadoop Balancer Tests. It used Hadoop::Test as the base.

=head1 FUNCTIONS

The following functions are implemented:

=cut

package Hadoop::BalancerTest;

use strict;
use warnings;

use Data::Dumper;
use base qw( Hadoop::Test);
use Test::More;

#Global constants
our $SLEEP_TIME = 960;

# Define Base Node
sub new {
    my ($class) = @_;
    my $self = Hadoop::Test->new;
    bless ($self,$class);
};

=head2 run_initialize_kinit_NN()

Initialize kinit on NAMENODE

=over 2

=item I<Parameters>:

   * None

=item I<Returns>:

   * None

=cut


sub run_initialize_kinit {
    my ($self) = @_;
    my $NAMENODE=$self->config->{NODES}->{NAMENODE}->{HOST1};
    my @nn = split('\\.',$NAMENODE);
    my @command = ("ssh  $NAMENODE kinit -kt /etc/grid-keytabs/$nn[0].dev.service.keytab hdfs/$NAMENODE\@DEV.YGRID.YAHOO.COM");
    note("command = @command\n");
    `@command`;
    is($?, 0, "Check Initialized kinit for hdfs user on $NAMENODE");

}

=head2 run_balancer($self, $test_desc, $expected_output, $param_type1, $param_type1_value1, $param_type2, $param_type2_value2)

Run the balancer job with the specified or default parameters

=over 2

=item I<Parameters>:

   * test_desc: Is the description of test
   * expected_output: output that is expected 
   * param_type1: policy
   * param_type1_value1: ploicy type value - datanode or blockpool, default is datanode
   * param_type2: threshold
   * param_type2_value2: percentage value for threshold , default is 10

=item I<Returns>:

   * None

=cut

sub run_balancer{

    my ($self,$test_desc, $expected_output, $type1, $type1_value, $type2, $type2_value) = @_;
    my $HADOOP_CONF_DIR=$self->config->{HADOOP_CONF_DIR};
    my $HADOOP_HDFS_HOME=$self->config->{HADOOP_HDFS_HOME};
    my $HADOOP_MAPRED_HOME=$self->config->{HADOOP_MAPRED_HOME};
    my $HADOOP_COMMON_HOME=$self->config->{HADOOP_COMMON_HOME};
    my $NAMENODE=$self->config->{NODES}->{NAMENODE}->{HOST1};
    my $JAVA_HOME="$ENV{HADOOP_QA_ROOT}/gs/java/jdk";
    my @command = ("ssh  $NAMENODE HADOOP_HDFS_HOME=$HADOOP_HDFS_HOME HADOOP_MAPRED_HOME=$HADOOP_MAPRED_HOME HADOOP_COMMON_HOME=$HADOOP_COMMON_HOME JAVA_HOME=$JAVA_HOME") ;
    push(@command, "$HADOOP_HDFS_HOME/bin/hdfs " ) ;
    push(@command, " --config ");
    push(@command, " $HADOOP_CONF_DIR ");
    push(@command, " balancer " ) ;
    push(@command, "-$type1") if ($type1);
    push(@command, "$type1_value") if ($type1_value);
    push(@command, "-$type2") if ($type2);
    push(@command, "$type2_value") if ($type2_value);
    push(@command, '2>&1');
    note("------------------------------------------------\n");
    note("Running test $test_desc\n");
    note("Command used: @command\n");
    my $result = `@command`;
    note("result in function : $result\n");
    like($result, qr/$expected_output/, "$test_desc");
}


=head2 run_balancer_with_half_DN($self, $test_desc, $expected_output)

Run the balancer job with default parameters when half the datanode is not running

=over 2

=item I<Parameters>:

   * test_desc: Is the description of test
   * expected_output: output that is expected 

=item I<Returns>:

   * None

=cut


# Test Start balancer when half the nodes are not running


sub run_balancer_with_half_DN{
    my ($self, $test_desc, $expected_output) = @_;

    #stop datanode
    stop_start_half_DN($self, "stop");

    #Wait for $SLEEP_TIME seconds  so that these stopped datanodes show up in the dead node list
    #note("wait $SLEEP_TIME seconds for the stopped datanodes to show up in the dead node list\n");
    #sleep($SLEEP_TIME); #### Marking DN as DEAD is not required

    # Run balancer
    run_balancer($self,$test_desc,$expected_output);  
    
    #Restore the cluster back - start the stopped nodes
    stop_start_half_DN($self, "start");

}

#Stop and start half data nodes
sub stop_start_half_DN {
    my ($self, $operation) = @_;
    my @list_dn = @{$self->config->{NODES}->{DATANODE}->{HOST}};
    my $num_dn = scalar(@list_dn);
    note("number of datanodes = $num_dn\n");
    note("List of datanodes = @list_dn\n");
    my $i;
    for ($i = 0 ; $i <= $num_dn/2 ; $i++)
    {
        $self->control_daemon($operation,
                              "datanode",
                              $list_dn[$i],
                              $self->config->{HADOOP_CONF_DIR});
    }
}



=head2 run_balancer_with_half_DN_simulate_load($self, $test_desc, $expected_output)

Run the balancer job with default parameters and simulate load when half the datanodes are not running

=over 2

=item I<Parameters>:

   * test_desc: Is the description of test
   * expected_output: output that is expected 

=item I<Returns>:

   * None

=cut

# Test Start balancer and simulate load when half the nodes are not running
sub run_balancer_with_half_DN_simulate_load{
    my ($self, $test_desc, $expected_output) = @_;
    my $HADOOP_MAPRED_HOME=$self->config->{HADOOP_MAPRED_HOME};
    my $HADOOP_CONF_DIR=$self->config->{HADOOP_CONF_DIR};
    my $HADOOP_COMMON_HOME=$self->config->{HADOOP_COMMON_HOME};
    my $HADOOP_EXAMPLES_JAR=$self->config->{HADOOP_EX_JAR};


    #stop datanode
    stop_start_half_DN($self, "stop");

    #Wait for $SLEEP_TIME seconds so that these stopped datanodes show up in the dead node list
    note("wait $SLEEP_TIME seconds for the stopped datanodes to show up in the dead node list\n");
    sleep($SLEEP_TIME); #### Marking DN as DEAD is not required

    # DEBUG
    # my ($overall_status, $status) = $self->get_cluster_status();
    # note("overall_status = '$overall_status'");
    # note("status = ", explain($status));
        
    #Run RandomWriter Job
    my $input = "input_" . time;
    #note("$HADOOP_EXAMPLES_JAR");
    my @command = ("$HADOOP_COMMON_HOME/bin/hadoop", "--config",
                   "$HADOOP_CONF_DIR", "jar", "$HADOOP_EXAMPLES_JAR",
                   "randomwriter", "-Dmapred.job.queue.name=grideng",
                   "$input");
    note("command : @command\n");
    `@command`;
    is($?,0,"Random writer job succesfull");

    #Run balancer
    run_balancer($self,$test_desc,$expected_output);

    #Restore the cluster back - start the stopped nodes
    stop_start_half_DN($self, "start");
    
    #remove data created by randomwriter
    my @command = ("$HADOOP_COMMON_HOME/bin/hadoop", "--config",
                   "$HADOOP_CONF_DIR", "dfs", "-rm", "-r", "-skipTrash",
                   "$input");
    note("remove data created by randomwriter\n");
    note("command : @command\n");
    `@command`;    
}


=head2 load_cluster_with_data($self, $test_desc, $num_times)

Load data to the cluster using randomwriter job

=over 2

=item I<Parameters>:

   * test_desc: Is the description of test
   * num_time: Number of times to run the randomwrite job

=item I<Returns>:

   * None

=cut


#Test to load data to the cluster - uses randomwriter job
sub load_cluster_with_data{
    my ($self, $test_desc, $num_times) = @_;
    my $HADOOP_MAPRED_HOME=$self->config->{HADOOP_MAPRED_HOME};
    my $HADOOP_CONF_DIR=$self->config->{HADOOP_CONF_DIR};
    my $HADOOP_COMMON_HOME=$self->config->{HADOOP_COMMON_HOME};


    #Run RandomWriter Job
    my $i;
    for ($i = 0 ; $i <=$num_times; $i++)
    {
         my $input = "input_" . time;
         $self->run_jar_command("hadoopqa", "$HADOOP_MAPRED_HOME/hadoop-examples.jar", "randomwriter -Dmapred.job.queue.name=grideng", "$input");
         is($?,0,"Random writer job succesfull");

    }
 

}


1;


