#!/usr/bin/perl
###############################################################################
# Date: Dec 16 2010 
# Driver to support 22 tests 
# Author - Arun Ramani
###############################################################################
# use module;
use XML::Simple;
# use IPC::Run;
# use strict;
use Data::Dumper;
my @actual_out=();
my $actual_out;
my $actual_stderr;
my @cmd=();
my $cmd="";
my $exp_stdout;
my $exp_stdout_regexp;
my $not_exp_stdout_regexp;
my $pass_flag=0;
my $count=0;
my $pass_count=0;
my $fail_count=0;
my $stdout_flag;
my $stdout_regexp_flag;
my $not_exp_stdout_regexp_flag;
my $jobID;
my @chk_output=();
my $cluster=$ARGV[0];
chomp $cluster;
my $NAMENODE;
my $JOBTRACKER;
if (@ARGV <=0){
  usage();
}

sub usage(){
  print "Usage: ./test.pl <Cluster name> \n";
  exit 1;
}
# Setting up the env variables
sub envSetUp{
  # print $cluster. "\n";
  $ENV{'HADOOP_COMMON_HOME'}="/grid/0/gs/gridre/yroot.$cluster/share/hadoopcommon"; 
  $ENV{'HADOOP_HDFS_HOME'}="/grid/0/gs/gridre/yroot.$cluster/share/hadoophdfs"; 
  $ENV{'HADOOP_MAPRED_HOME'}="/grid/0/gs/gridre/yroot.$cluster/share/hadoopmapred"; 
  $ENV{'HADOOP_CONF_DIR'}="/grid/0/gs/gridre/yroot.$cluster/conf/hadoop";
  # $ENV{'HADOOP_YARN_HOME'}="/grid/0/gs/gridre/yroot.$cluster/share/yarn";
}

sub getJTNN{
  # my $NAMENODE=`grep -A 2 "dfs.https.address" $ENV{'HADOOP_CONF_DIR'}/hdfs-site.xml |grep value |cut -f 2 -d '>'|cut -f 1 -d ':'`;
  $NAMENODE=`grep -A 2 dfs.namenode.rpc-address.gsbl90120 $ENV{'HADOOP_CONF_DIR'}/$cluster.namenodeconfigs.xml |grep "value" |cut -f 2 -d ">" |cut -f 1 -d ":"`;
  $JOBTRACKER=`grep -A 2 mapreduce.jobtracker.address.http.address $ENV{'HADOOP_CONF_DIR'}/mapred-site.xml |grep value |cut -f 2 -d '>' |cut -f 1 -d ':'`; 
  chomp $JOBTRACKER;
  chomp $NAMENODE;
  my $tt=`scp $JOBTRACKER:$ENV{'HADOOP_CONF_DIR'}/slaves /homes/hadoopqa/data/MR64`;
  my $TASKTRACKER=`tail -1 /homes/hadoopqa/data/MR64/slaves`;
  chomp $TASKTRACKER;
  # print "JOBTRACKER - $JOBTRACKER \n";
  # print "TASKTRACKER - $TASKTRACKER \n";
  my $JTSHORT=`grep -A 2 mapreduce.jobtracker.address.http.address $ENV{'HADOOP_CONF_DIR'}/mapred-site.xml |grep value |cut -f 2 -d '>' |cut -f 1 -d ':' |cut -f 1 -d '.'`;
  chomp $JTSHORT;
  # print $JTSHORT."\n";
  # print $NAMENODE;
  
}

sub execTests{
  # create object
  my $xml = new XML::Simple(KeyAttr=>[]);
  # my $xml = new XML::Simple();
  # read XML file
  # my $data = $xml->XMLin('MR157.xml', forcearray=>1, forcearray=>['group'], forcearray=>['test']);
  # my $data = $xml->XMLin('yarn.xml', forcearray=>['group']);
  my $data = $xml->XMLin('MR2046.xml', forcearray=>['group']);
  # print Dumper($data);
  # Splitting the command based on space and framing the command to execute
  foreach $data (@{$data->{group}}){
    $count++;
    foreach $data (@{$data->{test}}) {
      if($data->{cmd}){
        @cmd=split(' ', $data->{cmd});
        # Substituting the env variables with actual values
        foreach my $val (@cmd) {
          $val =~ s/\$HADOOP_COMMON_HOME/$ENV{'HADOOP_COMMON_HOME'}/;
          $val =~ s/\$HADOOP_MAPRED_HOME/$ENV{'HADOOP_MAPRED_HOME'}/; 
          $val =~ s/\$HADOOP_HDFS_HOME/$ENV{'HADOOP_HDFS_HOME'}/;
          $val =~ s/\$HADOOP_CONF_DIR/$ENV{'HADOOP_CONF_DIR'}/;
          # $val =~ s/\$HADOOP_YARN_HOME/$ENV{'HADOOP_YARN_HOME'}/;
          $val =~ s/\$JT/$JOBTRACKER/;
          $val =~ s/\$JT_SHORT/$JTSHORT/;
          $val =~ s/\$TT/$TASKTRACKER/;
          $val =~s/\$WORK_SPACE/$ENV{'WORKSPACE'}/;
          $val =~s/\$pwd/$ENV{'PWD'}/;
          $cmd = $cmd . $val. " ";
        }
        chomp $cmd;
        print $cmd ."\n";
        $actual_out = `$cmd 2>&1`;
        chomp $actual_out;
        $cmd="";
        print "ACTUAL OUTPUT IS \n $actual_out \n";
        if($data->{exp_stdout}){
          my @exp_stdout = split(',', $data->{exp_stdout});
          foreach my $exp_stdout (@exp_stdout){
            $exp_stdout =~ s/\$JT_SHORT/$JTSHORT/;
            $exp_stdout =~ s/\$JT/$JOBTRACKER/;
            $exp_stdout =~ s/\$NN/$NAMENODE/;
            for ($exp_stdout) {
              s/^\s+//;
            }
            chomp $exp_stdout;
            print "EXPECTED OUTPUT IS \n $exp_stdout \n";
            if($actual_out eq $exp_stdout) {
              $stdout_flag=0;
            } 
            elsif ($actual_out ne $exp_stdout){
              $stdout_flag=1;
              print "Expected stdout - $exp_stdout \n Actual out - $actual_out \n";
              last;
            }
          }
        }
        if($data->{exp_stdout_regexp}){
          my @exp_stdout_regexp = split(',', $data->{exp_stdout_regexp});
          foreach my $exp_stdout_regexp (@exp_stdout_regexp){
            $exp_stdout_regexp =~ s/\$JT_SHORT/$JTSHORT/;
            $exp_stdout_regexp =~ s/\$JT/$JOBTRACKER/;
            $exp_stdout_regexp =~ s/\$NN/$NAMENODE/;
            chomp $exp_stdout_regexp;
            print "EXPECTED REGEX IS \n $exp_stdout_regexp \n";
            for ($exp_stdout_regexp) {
              s/^\s+//;
            }
            # print "Expected regex " . $exp_stdout_regexp . "\n";
            # print "Actual out is " . $actual_out . "\n";
            if($actual_out=~ m/$exp_stdout_regexp/){
              $stdout_regexp_flag=0;
              # print "Expected output regexp " . $exp_stdout_regexp . "\n";
              # print "Actual output " . $actual_out . "\n";
            }
            else{
              $stdout_regexp_flag=1;
              print "Expected output regexp " . $exp_stdout_regexp . "\n";
              print "Actual output " . $actual_out . "\n";
              last;
            }
          }
        }
        if($data->{not_exp_stdout_regexp}){
          my @not_exp_stdout_regexp = split(',', $data->{not_exp_stdout_regexp});
          foreach my $not_exp_stdout_regexp (@not_exp_stdout_regexp) { 
            $not_exp_stdout_regexp =~ s/\$JT_SHORT/$JTSHORT/;
            $not_exp_stdout_regexp =~ s/\$JT/$JOBTRACKER/;
            $not_exp_stdout_regexp =~ s/\$NN/$NAMENODE/;
            chomp $not_exp_stdout_regexp;
            print "NOT EXPECTED TO BE PRESENT IN THE STDOUT \n $not_exp_stdout_regexp \n";
            for ($not_exp_stdout_regexp) {
              s/^\s+//;
            }
            if($actual_out=~ m/$not_exp_stdout_regexp/){
              $not_exp_stdout_regexp_flag=1;
              print $not_exp_stdout_regexp ." is not supposed to be found in ". $actual_out ."\n";
              last;
            } 
            else{
              $not_exp_stdout_regexp_flag=0;
            } 
         }
       }

       if(($stdout_flag ==1) || ($stdout_regexp_flag ==1) || ($not_exp_stdout_regexp_flag ==1)){
          # print "STDOUT FLAG - $stdout_flag \t STDOUT_REGEXP_FLAG - $stdout_regexp_flag \t NOT_EXP_STDOUT_REGEXP_FLAG - $not_exp_stdout_regexp_flag \n";
          print "TEST FAILED" . "\n";
          $fail_count++;
          $exp_stdout_regexp="";
          $exp_stdout="";
          $not_exp_stdout_regexp="";
        }
        else {
          print "TEST PASSED" . "\n";
          $pass_count++;
          $exp_stdout_regexp="";
          $exp_stdout="";
          $not_exp_stdout_regexp="";
        }
        $stdout_flag="";
        $stdout_regexp_flag="";
        $not_exp_stdout_regexp_flag="";
      } 
    } 
  }
}
envSetUp();
getJTNN();
execTests();
print "Total use cases executed - $count \n";
print "Test cases in $count usecase passed - $pass_count \n";
print "Test cases failed - $fail_count \n";
