package Hadoop::Config;

use strict;
use warnings;
use FindBin qw($Bin $Script);

sub new {
    my ($class) = @_;
    my $self = {};
    bless($self, $class);

    # User MUST define the REQUIRED shell enviornment variable $CLUSTER.
    # It can be exported in the shell environment, passed in via the
    # command line, or via the Hudson configuration.
    $self->{CLUSTER}        = $ENV{'CLUSTER'}   || die('ERROR: Config $CLUSTER not defined');

    # Validate or defined HADOOP_QA_ROOT.
    $self->{HADOOP_QA_ROOT} = $ENV{'HADOOP_QA_ROOT'};
    my ($path, @paths);
    if ((!$self->{HADOOP_QA_ROOT}) || (!-d $self->{HADOOP_QA_ROOT})) {
        my @hadoop_qa_roots = ('/home', '/grid/0');
        foreach my $hadoop_qa_root (@hadoop_qa_roots) {
            $path="$hadoop_qa_root/gs/gridre/yroot.$self->{CLUSTER}/share/";
            push(@paths, $path);
            if (-d "$path") {
                $self->{HADOOP_QA_ROOT} = $hadoop_qa_root;
                last;
            }
        }
        die("ERROR: Cannot find Hadoop QA root in {".join(', ',@paths)."}!!!") unless ($self->{HADOOP_QA_ROOT});
        die("ERROR: Cannot find Hadoop QA root $self->{HADOOP_QA_ROOT}!!!") unless (-d $self->{HADOOP_QA_ROOT});
    }

    # $self->{WORKSPACE}      = $ENV{'WORKSPACE'} || "$Bin/../..";
    $self->{WORKSPACE}      = $ENV{'WORKSPACE'};
    $self->{HDFSQA_USER}    = $ENV{'HDFSQA_USER'} || 'hdfsqa';
    $self->{MAPREDQA_USER}  = $ENV{'MAPREDQA_USER'} || 'mapredqa';
    $self->{DEFAULT_USER}   = 'hadoopqa';
    $self->{QUEUE}          = ['default', 'grideng', 'grid-dev', 'gridops'];

    # Set the HADOOP_CONF_DIR
    $self->{HADOOP_CONF_DIR} =
        $ENV{'HADOOP_QA_CONF_DIR'} ||
        "$self->{HADOOP_QA_ROOT}/gs/gridre/yroot.$self->{CLUSTER}/conf/hadoop";
    $ENV{HADOOP_CONF_DIR}    = $self->{HADOOP_CONF_DIR};

    # HADOOP_HOME for 0.20 or HADOOP_COMMON_HOME for 0.23 must be set
    # (via $ENV{<>}) before any hadoop commands can be run. 
    # If neither home directory exist, we need to fail out. 
    # Here we need to first determine which one exists. 
    $self->{HADOOP_HOME}        = "$self->{HADOOP_QA_ROOT}/gs/gridre/yroot.$self->{CLUSTER}/share/hadoop-current";
    $self->{HADOOP_COMMON_HOME} = "$self->{HADOOP_QA_ROOT}/gs/gridre/yroot.$self->{CLUSTER}/share/hadoop";    

    if (-d $self->{HADOOP_HOME}) {
        $ENV{HADOOP_HOME}     = $self->{HADOOP_HOME};
    }
    elsif (-d $self->{HADOOP_COMMON_HOME}) {
        $ENV{HADOOP_COMMON_HOME} = $self->{HADOOP_COMMON_HOME};
    }
    else {
        die("ERROR: Cannot find HADOOP home in ".
            "{'$self->{HADOOP_HOME}', '$self->{HADOOP_COMMON_HOME}'}")
    }

    $self->{HADOOP_BIN}      = (-d $self->{HADOOP_HOME}) ? "$self->{HADOOP_HOME}/bin" : "$self->{HADOOP_COMMON_HOME}/bin";
    $self->{HADOOP}          = "$self->{HADOOP_BIN}/hadoop";

    $self->{VERSION_LONG}    = get_version_long($self);
    $self->{VERSION}         = get_version($self);

    # The following environment variables need to be defined for Hadoop
    if (($self->{VERSION} eq '0.20') || ($self->{VERSION} =~ '^1\.')) {
        delete($self->{HADOOP_COMMON_HOME});
        $self->{HADOOP_EX_JAR}   = "$self->{HADOOP_HOME}/hadoop-examples.jar";
        $self->{HADOOP_STREAMING_JAR}   = "$self->{HADOOP_HOME}/hadoop-streaming.jar";
    }
    elsif (($self->{VERSION} =~ '^0\.2[2-9]') || ($self->{VERSION} =~ '^2\.')) {
        delete($self->{HADOOP_HOME});

        $self->{HADOOP_HDFS_HOME}   = "$self->{HADOOP_COMMON_HOME}";
        $self->{HADOOP_HDFS_BIN}    = "$self->{HADOOP_HDFS_HOME}/bin";
        $self->{HDFS}               = "$self->{HADOOP_HDFS_BIN}/hdfs"; 

        $self->{HADOOP_MAPRED_HOME} = "$self->{HADOOP_COMMON_HOME}";
        $self->{HADOOP_MAPRED_BIN}  = "$self->{HADOOP_MAPRED_HOME}/bin";
        $self->{MAPRED}             = "$self->{HADOOP_MAPRED_BIN}/mapred"; 

        $self->{HADOOP_EX_JAR}      = "$self->{HADOOP_MAPRED_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar";
        $self->{HADOOP_TEST_JAR}    = "$self->{HADOOP_MAPRED_HOME}/share/hadoop/tools/lib/hadoop-mapreduce-test-*.jar";

        $self->{HADOOP_STREAMING_JAR} = "$self->{HADOOP_COMMON_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar";

        # Hadoop 23 YARN
        $self->{YARN_USED} = use_yarn($self);
        if ($self->{YARN_USED}) {
            $self->{YARN_CONF_DIR}        = $self->{HADOOP_CONF_DIR};
            $self->{HADOOP_PREFIX}        = "$self->{HADOOP_COMMON_HOME}";
            $self->{HADOOP_YARN_HOME}     = "$self->{HADOOP_COMMON_HOME}";
            $self->{JAVA_HOME}            = "$self->{HADOOP_QA_ROOT}/gs/java/jdk";
            $self->{YARN_MR_CLIENT}       = "$self->{HADOOP_COMMON_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*.jar";
            $self->{YARN_BIN}             = "$self->{HADOOP_YARN_HOME}/bin";
            $self->{YARN}                 = "$self->{YARN_BIN}/yarn";
            $self->{HADOOP_EX_JAR}        = "$self->{HADOOP_COMMON_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar";
            $self->{HADOOP_TEST_JAR}      = "$self->{HADOOP_COMMON_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-*-test*.jar";
            $self->{HADOOP_STREAMING_JAR} = "$self->{HADOOP_COMMON_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar";
            $self->{DEFAULT_QUEUE}        = 'root.default';
            $self->{QUEUE}                = ['default','a1','a2','b'];
        }
    }
    else {
        die("ERROR: Found unsupported Hadoop version '$self->{VERSION}', '$self->{VERSION_LONG}'");
    }

    # Need to export extra Hadoop variables needed for hadoop 0.23
    if (($self->{VERSION} =~ '^0\.2[2-9]') || ($self->{VERSION} =~ '^2\.')) {
        $ENV{HADOOP_HDFS_HOME}   = $self->{HADOOP_HDFS_HOME};
        $ENV{HADOOP_MAPRED_HOME} = $self->{HADOOP_MAPRED_HOME};
        if ($self->{YARN_USED}) {
            $ENV{YARN_CONF_DIR} = $self->{YARN_CONF_DIR};
            $ENV{HADOOP_YARN_HOME} = $self->{HADOOP_YARN_HOME};
            $ENV{JAVA_HOME}        = $self->{JAVA_HOME};
        }
    }

#    $self->{ADMIN_HOST} =
#        [ ( 'qeadm.qegrid.corp.gq1.y-cloud.net' ) ];
    $self->{ADMIN_HOST} =
        [ ( 'adm201.ygridvm.corp.ne1.yahoo.com' ) ];


    $self->{RESOURCEMANAGER_NAME} =
        ($self->{YARN_USED}) ? 'RESOURCEMANAGER' : 'JOBTRACKER';
    $self->{NODEMANAGER_NAME} =
        ($self->{YARN_USED}) ? 'NODEMANAGER' : 'TASKTRACKER';

    # Set the Job Tracker, Name Node, Data Nodes, Task Trackers
    $self->{NODES} = {};
    my $namenodes = get_namenode($self);

    $self->{NODES}->{NAMENODE}->{HOST}          = $namenodes;
    $self->{NODES}->{NAMENODE}->{HOST1}         = $namenodes->[0];
    $self->{NODES}->{NAMENODE}->{PORT}          = '8020';
    $self->{NODES}->{NAMENODE}->{URI}           = get_namenode_uri($self, $self->{NODES}->{NAMENODE}->{HOST1});
    $self->{NODES}->{SECONDARYNAMENODE}->{HOST} = get_secondary_namenode($self);
    $self->{NODES}->{SECONDARYNAMENODE}->{PORT} = get_secondary_namenode_port($self);
    $self->{NODES}->{SECONDARYNAMENODE}->{URI}  = get_secondary_namenode_uri($self, $self->{NODES}->{SECONDARYNAMENODE}->{HOST});

    # Determine the HADOOP_LOG_DIR
    # NOTE: The HADOOP_LOG_DIR can be in any location as defined in the config
    # file HADOOP_CONF_DIR/hadoop-env.sh on each node. Therefore, we cannot
    # assume a connection between HADOOP_HOME and HADOOP_LOG_DIR. 
    #
    # my $hadoop_qa_root_subdir =
    #     ($self->{HADOOP_QA_ROOT} eq '/grid/0') ? 'hadoop' : 'gs';
    # $self->{HADOOP_LOG_DIR} = $ENV{'HADOOP_LOG_DIR'} ||
    #     "$self->{HADOOP_QA_ROOT}/$hadoop_qa_root_subdir/var/log";

    # Validate or defined HADOOP_LOG_DIR. We will assume the log directory
    # defined for the namenode will also be used for the other nodes on the
    # cluster.
    $self->{HADOOP_LOG_DIR} = $ENV{'HADOOP_LOG_DIR'};
    my $namenode = $self->{NODES}->{NAMENODE}->{HOST1};

    if ((!$self->{HADOOP_LOG_DIR}) ||
        (!$self->ssh_file_exists($self->{HADOOP_LOG_DIR}, $namenode))) {
        $self->{HADOOP_LOG_DIR} =
            $self->get_hadoop_log_dir($namenode, $self->{HADOOP_CONF_DIR});
    }

    die("ERROR: Hadoop LOG directory is not defined and cannot be determined!!!") unless ($self->{HADOOP_LOG_DIR});
    die("ERROR: Cannot find Hadoop LOG directory $self->{HADOOP_LOG_DIR} on ".
        "namenode '$namenode'!!!")
        unless ($self->ssh_file_exists($self->{HADOOP_LOG_DIR}, $namenode));

    my ($sudoer, $comp);
    if ($self->{YARN_USED}) {

        $self->{NODES}->{RESOURCEMANAGER}->{HOST} = get_resourcemanager($self);
        $self->{NODES}->{RESOURCEMANAGER}->{URI}  = get_resourcemanager_uri($self);
        $self->{NODES}->{RESOURCEMANAGER}->{URI_JOBHISTORYSERVER} = get_jobhistoryserver_uri($self);

        $self->{NODES}->{DATANODE}->{HOST}        = get_datanode($self);
        $self->{NODES}->{NODEMANAGER}->{HOST}     = $self->{NODES}->{DATANODE}->{HOST};
        $self->{NODES}->{HISTORYSERVER}->{HOST}   = get_jobhistoryserver($self);

        $comp = 'NAMENODE';
        $sudoer = $self->get_sudoer($comp);
        $self->{NODES}->{$comp}->{LOG} =
            "$self->{HADOOP_LOG_DIR}/$sudoer/hadoop-$sudoer-".lc($comp)."-".
            "$self->{NODES}->{$comp}->{HOST1}.log";

        if ($self->{NODES}->{SECONDARYNAMENODE}->{HOST}) {
            $comp = 'SECONDARYNAMENODE';
            $sudoer = $self->get_sudoer($comp);
            $self->{NODES}->{$comp}->{LOG} =
                "$self->{HADOOP_LOG_DIR}/$sudoer/hadoop-$sudoer-".lc($comp)."-".
                "$self->{NODES}->{$comp}->{HOST}.log";
        }
        

        $comp = 'DATANODE';
        $sudoer = $self->get_sudoer($comp);
        $self->{NODES}->{$comp}->{LOG} = [];
        foreach my $host (@{$self->{NODES}->{$comp}->{HOST}}) {
            push(@{$self->{NODES}->{DATANODE}->{LOG}},
                 "$self->{HADOOP_LOG_DIR}/$sudoer/hadoop-$sudoer-".lc($comp)."-".
                 "$host.log");
        }

        $comp = 'NODEMANAGER';
        $sudoer = $self->get_sudoer($comp);
        $self->{NODES}->{$comp}->{LOG} = [];
        foreach my $host (@{$self->{NODES}->{$comp}->{HOST}}) {
            push(@{$self->{NODES}->{$comp}->{LOG}},
                "$self->{HADOOP_LOG_DIR}/$sudoer/yarn-$sudoer-".lc($comp)."-".
                 "$host.log");
        }

        $comp = 'RESOURCEMANAGER';
        $sudoer = $self->get_sudoer($comp);
        $self->{NODES}->{$comp}->{LOG} =
            "$self->{HADOOP_LOG_DIR}/$sudoer/yarn-$sudoer-".lc($comp)."-".
            "$self->{NODES}->{$comp}->{HOST}.log";
    }
    else {
        $self->{NODES}->{JOBTRACKER}->{HOST}  = get_jobtracker($self);
        $self->{NODES}->{JOBTRACKER}->{PORT}  = '50030';
        $self->{NODES}->{JOBTRACKER}->{URI}   = get_jobtracker_uri($self, $self->{NODES}->{JOBTRACKER}->{HOST});

        $self->{NODES}->{DATANODE}->{HOST}    = get_datanode($self);
        $self->{NODES}->{TASKTRACKER}->{HOST} = get_tasktracker($self);

        $comp = 'NAMENODE';
        $sudoer = $self->get_sudoer($comp);
        $self->{NODES}->{$comp}->{LOG} =
            "$self->{HADOOP_LOG_DIR}/$sudoer/hadoop-$sudoer-".lc($comp)."-".
            "$self->{NODES}->{$comp}->{HOST}.log";

        if ($self->{NODES}->{SECONDARYNAMENODE}->{HOST}) {
            $comp = 'SECONDARYNAMENODE';
            $sudoer = $self->get_sudoer($comp);
            $self->{NODES}->{$comp}->{LOG} =
                "$self->{HADOOP_LOG_DIR}/$sudoer/hadoop-$sudoer-".lc($comp)."-".
                "$self->{NODES}->{$comp}->{HOST}.log";
        }

        $comp = 'DATANODE';
        $sudoer = $self->get_sudoer($comp);
        $self->{NODES}->{$comp}->{LOG} = [];
        foreach my $host (@{$self->{NODES}->{$comp}->{HOST}}) {
            push(@{$self->{NODES}->{DATANODE}->{LOG}}, 
                 "$self->{HADOOP_LOG_DIR}/$sudoer/hadoop-$sudoer-".lc($comp)."-".
                 "$host.log");
        }

        $comp = 'TASKTRACKER';
        $sudoer = $self->get_sudoer($comp);
        $self->{NODES}->{$comp}->{LOG} = [];
        foreach my $host (@{$self->{NODES}->{$comp}->{HOST}}) {
            push(@{$self->{NODES}->{$comp}->{LOG}},
                 "$self->{HADOOP_LOG_DIR}/$sudoer/hadoop-$sudoer-".lc($comp)."-".
                 "$host.log");
        }

        $comp = 'JOBTRACKER';
        $sudoer = $self->get_sudoer($comp);
        $self->{NODES}->{$comp}->{LOG} =
            "$self->{HADOOP_LOG_DIR}/$sudoer/hadoop-$sudoer-".lc($comp)."-".
            "$self->{NODES}->{$comp}->{HOST}.log";
    };

    $self->{NODES}->{HOST} = get_cluster_nodes($self);

    # Other configuration files
    $self->{MAPRED_SITE_XML} =
        "$self->{HADOOP_CONF_DIR}/mapred-site.xml";
    $self->{YARN_SITE_XML} =
        "$self->{HADOOP_CONF_DIR}/yarn-site.xml";
    $self->{CAPACITY_SCHEDULER_XML} =
        "$self->{HADOOP_CONF_DIR}/capacity-scheduler.xml";
    $self->{LOCAL_CONF_DIR} = "$self->{HADOOP_QA_ROOT}/gs/conf/local";
    $self->{LOCAL_MAPRED_SITE_XML} =
        "$self->{HADOOP_QA_ROOT}/gs/conf/local/local-mapred-site.xml";
    $self->{LOCAL_CAPACITY_SCHEDULER_XML} =
        "$self->{HADOOP_QA_ROOT}/gs/conf/local/local-capacity-scheduler.xml";

    # DEPRECATED.  
    # $self->{URI}->{RESOURCEMANAGER_URI}  = get_resourcemanager_uri($self);
    # $self->{URI}->{JOBHISTORYSERVER_URI} = get_jobhistoryserver_uri($self);
    # $self->{URI}->{NAMENODE} = get_namenode_uri($self, $self->{NODES}->{NAMENODE}->{HOST});
    # $self->{URI}->{SECONDARYNAMENODE} = get_secondary_namenode_uri($self, $self->{NODES}->{SECONDARYNAMENODE}->{HOST});
    # $self->{NAMENODEPORT}          = $self->{NODES}->{NAMENODE}->{PORT};
    # $self->{SECONDARYNAMENODEPORT} = $self->{NODES}->{SECONDARYNAMENODE}->{PORT};
    # $self->{DATANODE}              = $self->{NODES}->{DATANODE}->{HOST};
    # $self->{NAMENODE}              = $self->{NODES}->{NAMENODE}->{HOST};
    # $self->{SECONDARYNAMENODE}     = $self->{NODES}->{SECONDARYNAMENODE}->{HOST};
    # if ($self->{YARN_USED}) {
    #     $self->{NODEMANAGER}         = $self->{NODES}->{NODEMANAGER}->{HOST};
    #     $self->{RESOURCEMANAGER}     = $self->{NODES}->{RESOURCEMANAGER}->{HOST};
    #     $self->{RESOURCEMANAGER_LOG} = $self->{NODES}->{RESOURCEMANAGER}->{LOG};
    # }
    # else {
    #     $self->{TASKTRACKER}    = $self->{NODES}->{TASKTRACKER}->{HOST};
    #     $self->{JOBTRACKER}     = $self->{NODES}->{JOBTRACKER}->{HOST};
    #     $self->{JOBTRACKER_LOG} = $self->{NODES}->{JOBTRACKER}->{LOG};
    #     $self->{JOBTRACKERPORT} = $self->{NODSE}->{JOBTRACKER}->{PORT};
    # }

    return $self;
}

sub get_sudoer {
    my ($self, $component) = @_;
    my $sudoer;

    $component = uc($component);
    if (($component =~ 'NAMENODE') || ($component eq 'DATANODE')) {
        $sudoer = 'hdfs';
        $sudoer .= 'qa' if ($self->{YARN_USED});
    } elsif (($component eq 'JOBTRACKER') || ($component eq 'TASKTRACKER')) {
        $sudoer = 'mapred';
    } elsif (($component eq 'RESOURCEMANAGER') || ($component eq 'NODEMANAGER')){
        $sudoer = 'mapredqa';
    }
    return $sudoer;
}

sub ssh_file_exists {
    my ($self, $file, $host) = @_;
    my $ssh  = "/usr/bin/ssh";
    my $test = "/usr/bin/test";
    system($ssh, $host, $test, "-e", $file);
    my $rc = $? >> 8;
    my $is_true = ($rc) ? 0 : 1;
    return $is_true;
}

sub use_yarn {
    my ($self) = @_;
    my $yarn_xml = "$self->{HADOOP_QA_ROOT}/gs/gridre/yroot.$self->{CLUSTER}/conf/hadoop/yarn-site.xml";
    return (-f $yarn_xml) ? 1 : 0;
}


sub get_cluster_nodes {
    my ($self, $verbose) = @_;
    my $cluster_nodes = [];
    push(@$cluster_nodes,  @{$self->{NODES}->{NAMENODE}->{HOST}});
    push(@$cluster_nodes,  $self->{NODES}->{SECONDARYNAMENODE}->{HOST});
    push(@$cluster_nodes,  @{$self->{NODES}->{DATANODE}->{HOST}});
    push(@$cluster_nodes,  $self->{NODES}->{$self->{RESOURCEMANAGER_NAME}}->{HOST});
    print("cluster nodes = ", join("\n",@$cluster_nodes),"\n") if $verbose;
    return $cluster_nodes;
}

sub get_version_long {
    my ($self, $verbose) = @_;
    my $result = `$self->{HADOOP} version`;
    print("version=$result\n") if $verbose;
    my $version = $2 if ($result =~ m/(Hadoop\s+)(.*)(\n)(.*.)/);
}

sub get_version {
    my ($self) = @_;
    my @versions = split('\.', $self->{VERSION_LONG});
    return "$versions[0].$versions[1]";
}

sub get_namenode_conf {
    my ($self) = @_;
    return (($self->{VERSION} eq '0.20') || ($self->{VERSION} =~ '^1\.')) ?
        "$self->{HADOOP_CONF_DIR}/hdfs-site.xml" :
        "$self->{HADOOP_CONF_DIR}/$self->{CLUSTER}.namenodeconfigs.xml";
}

sub get_secondary_namenode_conf {
    my ($self) = @_;
    return (($self->{VERSION} eq '0.20') || ($self->{VERSION} =~ '^1\.')) ?
        "$self->{HADOOP_CONF_DIR}/hdfs-site.xml" :
        "$self->{HADOOP_CONF_DIR}/$self->{CLUSTER}.namenodeconfigs.xml";
}

sub get_jobtracker_conf {
    my ($self) = @_;
    return "$self->{HADOOP_CONF_DIR}/mapred-site.xml";
}

# Replaces jobtracker for YARN
sub get_resourcemanager_conf {
    my ($self) = @_;
    return "$self->{HADOOP_CONF_DIR}/yarn-site.xml";
}

sub get_tasktracker_conf {
    my ($self) = @_;
    return "$self->{HADOOP_CONF_DIR}/mapred-site.xml";
}

# Replaces tasktracker for YARN
sub get_nodemanager_conf {
    my ($self) = @_;
    return "$self->{HADOOP_CONF_DIR}/yarn-site.xml";
}

sub parse_host {
    my ($self, $pattern, $config) = @_;
    my $result = `grep -A 2 '$pattern' $config`;
    my $host = $2 if ($result =~ m/(.*.<value>[http:\/\/]*)(.*.)(:.*)/);
    return $host;
}

sub parse_hosts {
    my ($self, $pattern, $config) = @_;
    my $result = `grep -A 2 '$pattern' $config`;
    my $host;
    my @hosts;
    foreach my $line (split('\n', $result)) {
        $host = $2 if ($line =~ m/(.*.<value>)(.*.)(:.*)/);
        push(@hosts, $host) if ($host);
        undef($host);
    }
    return \@hosts;
}

sub parse_property {
    my ($self, $pattern, $config) = @_;
    my $result = `grep -A 2 '$pattern' $config`;
    print("ERROR: grep -A 2 '$pattern' $config: \n") unless $result;
    my $value = $2 if ($result =~ m/(.*.<value>)(.*.)(<\/value>)/);
    return $value;
}

sub get_jobtracker {
    my ($self) = @_;
    my $pattern =
        (($self->{VERSION} eq '0.20') || ($self->{VERSION} =~ '^1\.')) ?
        '>mapred.job.tracker<' :
        'mapreduce.jobtracker.address.http.address';
    my $config = $self->get_jobtracker_conf();
    return $self->parse_host($pattern, $config);
}

# Replaces jobtracker for YARN
sub get_resourcemanager {
    my ($self) = @_;
    my $pattern = 'yarn.resourcemanager.resource-tracker.address';
    my $config = $self->get_resourcemanager_conf();
    return $self->parse_host($pattern, $config);
}

sub get_jobtracker_port {
    my ($self) = @_;
    return $self->{NODES}->{JOBTRACKER}->{PORT};
}

sub get_jobtracker_uri {
    my ($self, $host) = @_;
    return "http://$host:$self->{NODES}->{JOBTRACKER}->{PORT}/jobtracker.jsp";
}

sub get_resourcemanager_uri {
    my ($self) = @_;
    return "http://$self->{NODES}->{RESOURCEMANAGER}->{HOST}:8088";
}

sub get_jobhistoryserver {
    my ($self) = @_;
    my $pattern = 'yarn.log.server.url';
    my $config = $self->get_resourcemanager_conf();
    return $self->parse_host($pattern, $config);
}

sub get_jobhistoryserver_uri {
    my ($self) = @_;
    my $pattern = 'yarn.log.server.url';
    my $config = $self->get_resourcemanager_conf();
    return $self->parse_property($pattern, $config);
}

sub get_namenode {
    my ($self) = @_;
    my $pattern = 
        (($self->{VERSION} eq '0.20') || ($self->{VERSION} =~ '^1\.')) ?
        'dfs.https.address' : 'dfs.namenode.http-address';
    my $config = $self->get_namenode_conf();
    return $self->parse_hosts($pattern, $config);
}

sub get_namenode_uri {
    my ($self, $host) = @_;
    return "http://$host:50070/dfshealth.html";
}

## Get secondary namenode host
sub get_secondary_namenode {
    my ($self,$needport) = @_;
    my $pattern =
        (($self->{VERSION} eq '0.20') || ($self->{VERSION} =~ '^1\.')) ?
        'dfs.secondary.http.address' : 'dfs.namenode.secondary.http-address';
    my $config = $self->get_secondary_namenode_conf();

    my $result = `grep -A 2 '$pattern' $config`;
    my $host = $2 if ($result =~ m/(.*.<value>)(.*.):(.*) *<\/value>/);
    return $host;
}

## Get secondary namenode URL port
sub get_secondary_namenode_port {
    my ($self,$needport) = @_;
    my $pattern =
        (($self->{VERSION} eq '0.20') || ($self->{VERSION} =~ '^1\.')) ?
        'dfs.secondary.http.address' : 'dfs.namenode.secondary.http-address';
    my $config  = $self->get_namenode_conf();

    my $result = `grep -A 2 '$pattern' $config`;
    my $port = $3 if ($result =~ m/(.*.<value>)(.*.):(.*) *<\/value>/);
    return $port;
}

sub get_secondary_namenode_uri {
    my ($self, $host) = @_;
    return "http://$host:$self->{NODES}->{SECONDARYNAMENODE}->{PORT}/dfshealth.html";
}

sub get_hadoop_log_dir {
    my ($self, $host, $conf_dir) = @_;
    chomp(my $hadoop_log_dir = `ssh $host grep HADOOP_LOG_DIR $self->{HADOOP_CONF_DIR}/hadoop-env.sh`);
    $hadoop_log_dir = $2 if ($hadoop_log_dir =~ m/(.*.=)(.*.\/var\/log)(.*.)/);
    return $hadoop_log_dir;
}

sub get_datanode {
    my ($self, $verbose) = @_;
    my $slave_hosts = $self->get_slave_nodes($self->{NODES}->{NAMENODE}->{HOST1});
    my $exclude_hosts = $self->get_exclude_nodes($self->{NODES}->{RESOURCEMANAGER}->{HOST});
    print("slaves = ",join(',',@$slave_hosts),"\n") if $verbose;
    print("ex hosts = ",join(',',@$exclude_hosts),"\n") if $verbose;

    # Initialize the hash using a slice
    my %hash;
    @hash{@$exclude_hosts} = undef;
    @$slave_hosts = grep {not exists $hash{$_}} @$slave_hosts;
    print("dn = ",join(", ", @$slave_hosts),"\n") if $verbose;
    return $slave_hosts;
}

sub get_datanode_uri {
    my ($self, $host) = @_;
    return "http://$host:50060/tasktracker.jsp";
}

sub get_hosts_from_list {
    my ($self, $host, $file, $verbose) = @_;
    if ($verbose) {
        print("host = $host, file = $file \n");
        print("ssh $host cat $file\n");
    }
    chomp(my $hosts = `ssh $host cat $file 2> /dev/null`);
    print("hosts = '$hosts'\n") if $verbose;
    my @array = split("\n", $hosts);
    @array = grep { $_ } @array;
    return \@array;
}

sub get_slave_nodes {
    my ($self, $host) = @_;
    $self->{NODES}->{SLAVES} = 
        $self->get_hosts_from_list($host,
                                   "$self->{HADOOP_CONF_DIR}/slaves")
        unless $self->{NODES}->{SLAVES};
    return $self->{NODES}->{SLAVES};
}

sub get_exclude_nodes {
    my ($self, $host) = @_;

    $self->{NODES}->{EXCLUDE_NODES} =
        $self->get_hosts_from_list($self->{NODES}->{RESOURCEMANAGER}->{HOST},
                                   '/home/gs/conf/local/mapred.exclude')
        unless $self->{NODES}->{EXCLUDE_NODES};
    return $self->{NODES}->{EXCLUDE_NODES};
}

sub get_tasktracker {
    my ($self) = @_;
    return $self->get_slave_nodes($self->{NODES}->{JOBTRACKER}->{HOST});
}

# Replaces tasktracker for YARN
sub get_nodemanager {
    my ($self) = @_;
    return $self->get_datanode();
}

sub get_tasktracker_uri {
    my ($self, $host) = @_;
    return $self->get_datanode_uri($host);
}

sub get_tasktracker_short {
    my ($self) = @_;
    # my $JOBTRACKERSHORT=`grep -A 2 mapreduce.jobtracker.address.http.address $self->{HADOOP_CONF_DIR}/mapred-site.xml |grep value |cut -f 2 -d '>' |cut -f 1 -d ':' |cut -f 1 -d '.'`;
    # chomp $JOBTRACKERSHORT;
}

1;
