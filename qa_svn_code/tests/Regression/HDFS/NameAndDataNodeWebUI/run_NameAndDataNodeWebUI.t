#!/usr/local/bin/perl

# This is an automated test for 
# - https://issues.apache.org/jira/browse/HDFS-1318
# - HDFS Namenode and Datanode WebUI information needs to be accessible
# - programmatically for scripts

use strict;
use warnings;

# Pull in the automated test framework perl modules.
use FindBin qw($Bin $Script);
use lib "$FindBin::Bin/../../../../lib";
use base qw(Hadoop::Test);
use Test::More tests => 29;

# Some of the JMX values are in JSON format.
use JSON -support_by_pp;
my $json = new JSON;

use IO::CaptureOutput qw/capture_exec/;

# Initialize test framework environment.
my $self = __PACKAGE__->new;
my $Config = $self->config;

my $workingNameNode = $Config->{NODES}->{NAMENODE}->{HOST1};

$ENV{OWNER} = 'ericp';

# Get value of $HDFS_USER (either 'hdfs' or 'hdfsqa') from the
# "HadoopConfiggeneric10nodeblue.TODO_HDFSUSER" yinst variable.
my $HDFS_USER = `yinst set -q -root $Config->{HADOOP_QA_ROOT}/gs/gridre/yroot.$Config->{CLUSTER} HadoopConfiggeneric10nodeblue.TODO_HDFSUSER`;
$HDFS_USER =~ s/^.*: (.*)$/$1/;
chomp($HDFS_USER);

# Display the environment.
use Data::Dumper;
print(Dumper($Config));
print "HDFS_USER => $HDFS_USER\n";

# Set JMX ports. NN will have 8002 and DN will have 8004.
my $nameNodeJmxPort=8002;
my $dataNodeJmxPort=8004;
my $dataNodeInfoPort=1004;

# The following will be added to $HADOOOP_CONF_DIR/hadoop-env.sh on NN and DN,
# respectively. These are the magic flags to turn on JMX.
my $nameNodeJmxOpts="export HADOOP_NAMENODE_OPTS=" . '\\"' . 
                   "-Dcom.sun.management.jmxremote " .
                   "-Dcom.sun.management.jmxremote.port=$nameNodeJmxPort " .
                   "-Dcom.sun.management.jmxremote.authenticate=false  " .
                   "-Dcom.sun.management.jmxremote.ssl=false " .
                   '\\' . "\$" . "HADOOP_NAMENODE_OPTS" . '\\"';

my $dataNodeJmxOpts="export HADOOP_DATANODE_OPTS=" . '\\"' . 
                   "-Dcom.sun.management.jmxremote " .
                   "-Dcom.sun.management.jmxremote.port=$dataNodeJmxPort " .
                   "-Dcom.sun.management.jmxremote.authenticate=false  " .
                   "-Dcom.sun.management.jmxremote.ssl=false " .
                   '\\' . "\$" . "HADOOP_DATANODE_OPTS" . '\\"';

# Only need to reconfigure and restart 1 datanode, so use the first one.
my $workingDataNode;

# Set up the test:
#   Copy the config directories for the namenode and 1 datanode, add the JMX
#   options to the daemons' configs, and restart the daemons on those nodes.
sub setUp_NameAndDataNodeWebUI{
  # Stop NN and copy its configs to a tmp directory.
  $self->control_daemon('stop',
                        'namenode',
                        $workingNameNode);
  $self->backup_namenode_conf();
  # Append the JMX magic config parms to NN's hadoop-env.sh.
  $self->append_string($nameNodeJmxOpts,
                       "$Config->{HADOOP_CONF_DIR}/hadoop-env.sh",
                       $workingNameNode);

  # Reset the value of the include file to be a temporary location and make sure
  # all data nodes are in it. This is so we can test the
  # NameNode:NamenodeInfo:DeadNodes attribute.
  my $infile = $self->{tmp_conf_dir} . "/ericsincludelist.txt";
  $self->set_xml_prop_value("dfs.hosts",
                            $infile,
                            "$Config->{HADOOP_CONF_DIR}/hdfs-site.xml",
                            $workingNameNode);

  my $datanodes = join("\n",@{$Config->{NODES}->{DATANODE}->{HOST}});

  # Append the all datanodes to the include list.
  $self->append_string($datanodes,
                       $infile,
                       $workingNameNode);

  # Restart NN with new configs
  $self->control_daemon('start','namenode',$workingNameNode);

  # Wait for changes to take effect.
  # Make sure the NameNode is not in safemode when we begin.
  waitUntilSafemodeOff();

  # Make sure we pick a live datanode to work with for these tests.
  my ($attr,$liveNodes) = $self->jmxGet('NameNode',
                                        'NameNodeInfo',
                                        'LiveNodes',
                                        $workingNameNode,
                                        $nameNodeJmxPort);
  my $dnhosts = $self->get_daemon_hosts('datanode');
  for my $h (@$dnhosts) {
    if ($liveNodes =~ qr/$h/) {
      $workingDataNode = $h;
    }
  }

  if (!defined($workingDataNode) || $workingDataNode eq "") {
    print "\n-----\nERROR: Could not find any live data nodes!!!\n-----\n";
    exit 1;
  }
  note("workingDataNode == $workingDataNode");

  # Stop DN and copy its configs to a tmp directory.
  # Must specify config dir because the first call to backup_conf_dir() (above)
  # caused the config dir global var to be changed. However, that conf dir
  # won't exist on the namenode yet, so specify the original when calling
  # control_daemon().
  $self->control_daemon('stop',
                        'datanode',
                        $workingDataNode,
                        $Config->{HADOOP_CONF_DIR});
  $self->backup_conf_dir([$workingDataNode]);
  # Append the JMX magic config parms to DN's hadoop-env.sh.
  $self->append_string($dataNodeJmxOpts,
                       "$Config->{HADOOP_CONF_DIR}/hadoop-env.sh",
                       $workingDataNode);
  # Also, add the dfs.datanode.info.port property to the DN's hdfs-site.xml
  # in order to test the "DataNode:DataNodeInfo:HttpPort" attribute.
  $self->set_xml_prop_value('dfs.datanode.info.port',
                            $dataNodeInfoPort,
                            "$Config->{HADOOP_CONF_DIR}/hdfs-site.xml",
                            $workingDataNode);
  # Restart DN with new configs
  $self->control_daemon('start','datanode',$workingDataNode);

  # Give datanode time to start back up.
  sleep 10;
}

# Cleanup and restart NN and the 1 DN that was reconfigured.
sub tearDown_NameAndDataNodeWebUI{
  $self->control_daemon('stop','namenode',$workingNameNode);
  $self->cleanup_namenode_conf();
  $self->control_daemon('start','namenode',$workingNameNode);

  $self->control_daemon('stop','datanode',$workingDataNode);
  $self->cleanup_conf_dir([$workingDataNode]);
  $self->control_daemon('start','datanode',$workingDataNode);

  # Wait for changes to take effect so that next test suite can start with a
  # clean slate.
  waitUntilSafemodeOff();
}

# Call jmxget and compare the results.
# If $contains is passed and == 1, use regex to determine if $expectedval
# contains $val. Otherwise, check if $val equals $expectedval.
sub testNodeAttrVal{
  my ($service, $class, $attribute, $expectedval, $contains) = @_;

  my ($node,$port);
  if ($service eq "NameNode") {
    $node = $workingNameNode;
    $port = $nameNodeJmxPort;
  } else {
    $node = $workingDataNode;
    $port = $dataNodeJmxPort;
  }

  my ($attr,$val) = $self->jmxGet($service,$class,$attribute,$node,$port);

  note("\n\nexptected attribute = $attribute");
  note("actual attribute = $attr");
  note("expected value = $expectedval");
  note("actual value = $val");

  if ($contains) {
    return 1 if (($attr eq $attribute)&&($val =~ qr/$expectedval/));
  } else {
    return 1 if (($attr eq $attribute)&&($val eq $expectedval));
  }
  return 0;
}

# After name node reset, wait until out of safe mode.
sub waitUntilSafemodeOff{
  my $timeout = 36; # 36 * 5 is 180 seconds or 3 minutes.
  my $done = 0;
  # Wait a few seconds to make sure namenode daemon has started.
  sleep 5;

  my $FS = $self->get_xml_prop_value(
                       "fs.defaultFS",
                       "$Config->{HADOOP_CONF_DIR}/core-site.xml",
                       "$workingNameNode");
  my $olduser = $ENV{USER};
  # Become HDFS user.
  $self->setup_kerberos($HDFS_USER);

  my @command;
  push (@command, "$Config->{HADOOP_HDFS_HOME}/bin/hdfs");
  push (@command, "--config $Config->{HADOOP_CONF_DIR}");
  push (@command, "dfsadmin -fs $FS -safemode");
  push (@command, "get");
  note("command = @command");

  print "Waiting for Namenode to restart after config change...";
  while (!$done && $timeout >= 0) {
    $timeout--;
    sleep 5;
    print ".";
    my ($stdout, $stderr, $success, $exit_code) = capture_exec( "@command" );
    $done = 1 if $stdout =~ qr/Safe mode is OFF/;
  }

  print "\n";

  # If it didn't work, force it.
  if (!$done) {
    print "\n-----\nWARN: Forcing $workingNameNode out of safe mode.\n-----\n";

    pop(@command);
    push (@command, "leave");
    note("command = @command");
    my ($stdout, $stderr, $success, $exit_code) = capture_exec( "@command" );
    print "\n";
  }

  # Become original user.
  $self->setup_kerberos($olduser);
}

sub NameAndDataNodeWebUI_01{
  my $passfail = testNodeAttrVal('DataNode',                   # Service
                  "DataNodeActivity-${workingDataNode}-1004",  # Class
                  'tag.Hostname',                              # Attribute
                  "$workingDataNode");                         # Expected Value
  ok ($passfail == 1, "Verify that can get DataNode from WebUI script interface.");
}

sub NameAndDataNodeWebUI_02{
  my $nn = $workingNameNode;
  $nn =~ /^(.*?)\./;
  my $nnshort = $1;
  my $fullnnrpcaddr = $self->get_xml_prop_value(
                       "dfs.namenode.rpc-address.$nnshort",
                       $Config->{HADOOP_CONF_DIR} . "/" . $Config->{CLUSTER} . ".namenodeconfigs.xml",
                       "$workingDataNode");

  $fullnnrpcaddr =~ /^.*:(.*)$/;
  my $nnrpc = $1;

  my $passfail = testNodeAttrVal('DataNode', # Service
                  'DataNodeInfo',            # Class
                  'RpcPort',                 # Attribute
                  "$nnrpc");                 # Expected Value
  ok ($passfail == 1, "Verify that DataNode has valid RPC port for NameNode.");
}

sub NameAndDataNodeWebUI_03{
  my $passfail = testNodeAttrVal('DataNode',  # Service
                  'DataNodeInfo',             # Class
                  'Version',                  # Attribute
                  $Config->{VERSION_LONG});   # Expected Value
  ok ($passfail == 1, "Verify Datanode knows correct version of Hadoop");
}

sub NameAndDataNodeWebUI_04{
  my ($attr,$val) = $self->jmxGet('DataNode','DataNodeInfo','NamenodeAddresses',$workingDataNode,$dataNodeJmxPort);
  like ($val , qr/$Config->{NODES}->{NAMENODE}->{HOST1}/ , "Verify Datanode has correct NameNode hostname");
}

sub NameAndDataNodeWebUI_05{
  my $vols = shift;
  my @volumeNames = (keys %$vols);
  ok (scalar(@volumeNames) > 0, "Live Datanode has volumes");
}

sub NameAndDataNodeWebUI_06{
  my $vols = shift;
  my @volumeNames = (keys %$vols);
  my $volAttrs = $vols->{$volumeNames[0]};
  like ($volAttrs->{'freeSpace'} , qr/^\d+$/, "Verify DataNode's volume's freeSpace field contains all digits");
}

sub NameAndDataNodeWebUI_07{
  my $vols = shift;
  my @volumeNames = (keys %$vols);
  my $volAttrs = $vols->{$volumeNames[0]};
  like ($volAttrs->{'usedSpace'} , qr/^\d+$/, "Verify DataNode's volume's usedSpace field contains all digits");
}

sub NameAndDataNodeWebUI_08{
  my $vols = shift;
  my @volumeNames = (keys %$vols);
  my $volAttrs = $vols->{$volumeNames[0]};
  like ($volAttrs->{'reservedSpace'} , qr/^\d+$/, "Verify DataNode's volume's reservedspace field contains all digits");
}

sub NameAndDataNodeWebUI_09{
  my $nn = $workingNameNode;
  $nn =~ /^(.*?)\./;
  my $nnshort = $1;
  my $dnhttp = $self->get_xml_prop_value(
               "dfs.datanode.info.port",
               $Config->{HADOOP_CONF_DIR} . "/" . "hdfs-site.xml",
               "$workingDataNode");

  my $passfail = testNodeAttrVal('DataNode', # Service
                  'DataNodeInfo',            # Class
                  'HttpPort',                # Attribute
                  "$dnhttp");                # Expected Value
  ok ($passfail == 1, "Verify that DataNode has expected info port .");
}

sub NameAndDataNodeWebUI_10{
  my $passfail = testNodeAttrVal(
                  'NameNode',                   # Service
                  "FSNamesystem",               # Class
                  'tag.Hostname',               # Attribute
                  $workingNameNode);     # Expected Value
  ok ($passfail == 1, "Verify that can get NameNode from WebUI script interface.");
}

sub NameAndDataNodeWebUI_11{
  my $passfail = testNodeAttrVal('NameNode',  # Service
                  'NameNodeInfo',             # Class
                  'Version',                  # Attribute
                  $Config->{VERSION_LONG});   # Expected Value
  ok ($passfail == 1, "Verify Namenode knows correct version of Hadoop");
}

sub NameAndDataNodeWebUI_12{
  my $datanodes = $Config->get_datanode;
  # Get number of live nodes.
  my (undef,$lns) = $self->jmxGet('NameNode',                 # Service
                           'FSNamesystemState',               # Class
                           'NumLiveDataNodes',                # Attribute
                           $workingNameNode,                  # host
                           $nameNodeJmxPort);                 # port
  # Get number of dead nodes.
  my (undef,$jmxResult) = $self->jmxGet('NameNode',
                                        'NameNodeInfo',
                                        'DeadNodes',
                                        $workingNameNode,
                                        $nameNodeJmxPort);
  my $deadnodes = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($jmxResult);
  my $dns = scalar(keys %$deadnodes);
  note("");
  note("expected # of nodes: " . scalar(@$datanodes));
  note("Num live nodes: $lns");
  note("Num dead nodes: $dns");
  my $actual = $lns + $dns;
  is ($actual, scalar(@$datanodes),
                         "Verify NameNode has correct number of datanodes");
}

# Count each datanode in the LiveDataNode list and compare it with the value
# returned from NumLiveDataNodes.
sub NameAndDataNodeWebUI_13{
  my $jmxnodes = shift;
  my $expectednumdns = 0;
  my $confnodes = $Config->{NODES}->{DATANODE}->{HOST};

  foreach my $n (keys %$jmxnodes) {
    if (grep { $_ eq $n} @$confnodes ) {
      $expectednumdns++;
    }
  }

  my ($attr,$numdn) = $self->jmxGet('NameNode','FSNamesystemState','NumLiveDataNodes',$workingNameNode,$nameNodeJmxPort);
  
  is ($expectednumdns, $numdn, "Verify each datanode is in the list of slaves");
}

sub NameAndDataNodeWebUI_14{
  my $liveNodes = shift;
  my @liveNodeNames = (keys %$liveNodes);
  my $liveNodeAttrs = $liveNodes->{$liveNodeNames[0]};
  like ($liveNodeAttrs->{'usedSpace'} , qr/^\d+$/, "Verify NameNode's LiveList's usedSpace field contains all digits");
}

sub NameAndDataNodeWebUI_15{
  my $liveNodes = shift;
  my @liveNodeNames = (keys %$liveNodes);
  my $liveNodeAttrs = $liveNodes->{$liveNodeNames[0]};
  like ($liveNodeAttrs->{'lastContact'} , qr/^\d+$/, "Verify NameNode's LiveList's lastContact field contains all digits");
}

sub NameAndDataNodeWebUI_16{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','BlockPoolUsedSpace',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  like ($val , qr/^\d+$/, "Verify NameNode's block pool used space attribute has all digits");
}

sub NameAndDataNodeWebUI_17{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','NonDfsUsedSpace',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  ok ((defined($val) && ($val ne "")), "Verify NameNode's NonDfsUsedSpace is non-null");
}

sub NameAndDataNodeWebUI_18{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','PercentBlockPoolUsed',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  ok ((defined($val) && ($val ne "")), "Verify NameNode's PercentBlockPoolUsed is non-null");
}

sub NameAndDataNodeWebUI_19{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','PercentUsed',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  ok ((defined($val) && ($val ne "")), "Verify NameNode's PercentUsed is non-null");
}

sub NameAndDataNodeWebUI_20{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','Used',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  like ($val , qr/^\d+$/, "Verify NameNode's used space attribute has all digits");
}

sub NameAndDataNodeWebUI_21{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','Free',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  like ($val , qr/^\d+$/, "Verify NameNode's Free space attribute has all digits");
}

sub NameAndDataNodeWebUI_22{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','Total',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  like ($val , qr/^\d+$/, "Verify NameNode's Total space attribute has all digits");
}

sub NameAndDataNodeWebUI_23{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','UpgradeFinalized',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  ok (($val eq 'true' || $val eq 'false'), "Verify NameNode's UpgradeFinalized attribute is either 'true' or 'false'");
}

sub NameAndDataNodeWebUI_24{
  my ($attr,$val) = $self->jmxGet('NameNode','NameNodeInfo','Threads',$workingNameNode,$nameNodeJmxPort);
  note("$attr=$val");
  like ($val , qr/^\d+$/, "Verify NameNode's Threads attribute has all digits");
}

# When nameNode is not in safemode, the Safemode attribute will be NULL.
sub NameAndDataNodeWebUI_25{
  my $reason = shift;
  my $passfail = testNodeAttrVal('NameNode',  # Service
                  'NameNodeInfo',             # Class
                  'Safemode',                 # Attribute
                  "");                        # Expected Value
  ok ($passfail == 1, $reason);
}

# Verify safemode is on.
sub NameAndDataNodeWebUI_26{
  my $olduser = $ENV{USER};
  # Become HDFS user.
  $self->setup_kerberos($HDFS_USER);

  # Enter safemode.
  my @command;
  push (@command, "$Config->{HADOOP_HDFS_HOME}/bin/hdfs");
  push (@command, "--config $Config->{HADOOP_CONF_DIR}");
  push (@command, "dfsadmin -safemode enter");

  my ($mbean, $stderr, $success, $exit_code) = capture_exec( "@command" );

  sleep(5);

  # Test to make sure it's in safemode.
  my $passfail = testNodeAttrVal('NameNode',  # Service
                  'NameNodeInfo',             # Class
                  'Safemode',                 # Attribute
                  'Safe mode is ON',          # Expected Value
                  1);                         # Contains instead of equals
  ok ($passfail == 1, "Verify NameNode's safe mode attribute has the string 'Safe mode is ON' when safe mode is on.");

  # Leave safemode.
  @command = ();
  push (@command, "$Config->{HADOOP_HDFS_HOME}/bin/hdfs");
  push (@command, "--config $Config->{HADOOP_CONF_DIR}");
  push (@command, "dfsadmin -safemode leave");
  ($mbean, $stderr, $success, $exit_code) = capture_exec( "@command" );

  # Become original user.
  $self->setup_kerberos($olduser);
}

# Verify NameNode's safe mode attribute is NULL when safemode is turned back
# off
sub NameAndDataNodeWebUI_27{
  NameAndDataNodeWebUI_25( "Verify NameNode's safe mode attribute is NULL when safemode is turned back off");
}

sub NameAndDataNodeWebUI_28{
  $self->control_daemon('stop','namenode',$workingNameNode);
  $self->control_daemon('stop','datanode',$workingDataNode);
  $self->control_daemon('start','namenode',$workingNameNode);

  # Wait for changes to take effect. Don't need to wait for namenode to leave
  # safemode for these changes to take effect.
  sleep 5;

  my $passfail = testNodeAttrVal('NameNode',  # Service
                  'NameNodeInfo',             # Class
                  'DeadNodes',                # Attribute
                  $workingDataNode,           # Expected Value
                  1);                         # Contains instead of equals

  ok ($passfail == 1, "Verify NameNode's dead node attribute contains the expected dead node.");

  $self->control_daemon('start','datanode',$workingDataNode);
  $self->control_daemon('stop','namenode',$workingNameNode);
  $self->control_daemon('start','namenode',$workingNameNode);

  # Wait for changes to take effect. Don't need to wait for namenode to leave
  # safemode for these changes to take effect.
  sleep 5;
}

# Verify decommissioned node.
sub NameAndDataNodeWebUI_29{
  my $exfile = $self->{tmp_conf_dir} . "/lonegunmen.txt";
  $self->control_daemon('stop','namenode',$workingNameNode);
  # Find the exclude file and change it to one we can write to.
  my $origexfile = $self->get_xml_prop_value(
                              "dfs.hosts.exclude",
                              "$Config->{HADOOP_CONF_DIR}/hdfs-site.xml",
                              $workingNameNode);
  $self->set_xml_prop_value("dfs.hosts.exclude",
                            $exfile,
                            "$Config->{HADOOP_CONF_DIR}/hdfs-site.xml",
                            $workingNameNode);

  # Append the affected datanode to the exclude list.
  $self->append_string($workingDataNode,
                       $exfile,
                       $workingNameNode);

  $self->control_daemon('start','namenode',$workingNameNode);

  # Wait for changes to take effect. Don't need to wait for namenode to leave
  # safemode for these changes to take effect.
  sleep 30;

  my ($attr,$jmxResult) = $self->jmxGet('NameNode','NameNodeInfo','LiveNodes',$workingNameNode,$nameNodeJmxPort);
  note("\n----------------------------\n" . $jmxResult . "\n-------------------------\n");
  my $livenodes = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($jmxResult);

  note("$workingDataNode: $livenodes->{$workingDataNode}->{'adminState'}\n------------\n");
  ok ("$livenodes->{$workingDataNode}->{'adminState'}" eq "Decommissioned", "Verify NameNodeInfo classifies ${workingDataNode} as decommissioned.");

  # Reset exclude file property.
  $self->control_daemon('stop','namenode',$workingNameNode);
  $self->set_xml_prop_value("dfs.hosts.exclude",
                            $origexfile,
                            "$Config->{HADOOP_CONF_DIR}/hdfs-site.xml",
                            $workingNameNode);
  $self->control_daemon('start','namenode',$workingNameNode);

  # Wait for changes to take effect. Don't need to wait for namenode to leave
  # safemode for these changes to take effect.
  sleep 5
}


#-- main --#

setUp_NameAndDataNodeWebUI();

NameAndDataNodeWebUI_01();
NameAndDataNodeWebUI_02();
NameAndDataNodeWebUI_03();
NameAndDataNodeWebUI_04();

# $volumes will be used as input for the next few tests:
my ($attr,$jmxResult) = $self->jmxGet('DataNode','DataNodeInfo','VolumeInfo',$workingDataNode,$dataNodeJmxPort);
my $volumes = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($jmxResult);

NameAndDataNodeWebUI_05($volumes);
NameAndDataNodeWebUI_06($volumes);
NameAndDataNodeWebUI_07($volumes);
NameAndDataNodeWebUI_08($volumes);

NameAndDataNodeWebUI_09();
NameAndDataNodeWebUI_10();
NameAndDataNodeWebUI_11();
NameAndDataNodeWebUI_12();

# $livenodes will be used as input for the next few tests:
($attr,$jmxResult) = $self->jmxGet('NameNode','NameNodeInfo','LiveNodes',$workingNameNode,$nameNodeJmxPort);
my $livenodes = $json->allow_nonref->utf8->relaxed->escape_slash->loose->allow_singlequote->allow_barekey->decode($jmxResult);

NameAndDataNodeWebUI_13($livenodes);
NameAndDataNodeWebUI_14($livenodes);
NameAndDataNodeWebUI_15($livenodes);

NameAndDataNodeWebUI_16();
NameAndDataNodeWebUI_17();
NameAndDataNodeWebUI_18();
NameAndDataNodeWebUI_19();
NameAndDataNodeWebUI_20();
NameAndDataNodeWebUI_21();
NameAndDataNodeWebUI_22();
NameAndDataNodeWebUI_23();
NameAndDataNodeWebUI_24();
NameAndDataNodeWebUI_25( "Verify NameNode's safe mode attribute is NULL when not in safe mode" );
NameAndDataNodeWebUI_26();
NameAndDataNodeWebUI_27();
NameAndDataNodeWebUI_28();
NameAndDataNodeWebUI_29();

tearDown_NameAndDataNodeWebUI();

