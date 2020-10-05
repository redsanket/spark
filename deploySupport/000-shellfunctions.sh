#
# Various forms of 'pdsh' invocation, with different expected timeout values.
#

# gridci-441, force pdsh to return non-zero on any node having an error
# without this a failure will allow pdsh to continue, the error will
# not be detected and cause other failures downstream
#
# NOTE: pdsh has a bug which will break a commandline if it has a trailing
# ';' char, see:
#       http://sourceforge.net/p/pdsh/mailman/message/290409/
#
FAST_WAIT_MIN=3
# 10 minutes was not enought for wait for exit safemode
SLOW_WAIT_MIN=15
FAST_WAIT_SEC=$((FAST_WAIT_MIN*60))
SLOW_WAIT_SEC=$((SLOW_WAIT_MIN*60))
PDSH="pdsh -S "
PDSH_FAST="$PDSH -u $FAST_WAIT_SEC -f 25 "
PDSH_SLOW="$PDSH -u $SLOW_WAIT_SEC -f 25 "

function transport_files_from_admin {
    local ADM_HOST=$1
    local ADM_PATH=$2
    local NODES=$3
    local NODE_PATH=$4
    local CHOWN_ATTR=$5

    # rsync files from admin node to the Jenkins worker node
    NODE_SHORT=`echo $NODE | cut -d'.' -f1`
    TMP_DIR="/tmp/${NODE_SHORT}.$$"
    mkdir -p $TMP_DIR
    ls -l $TMP_DIR
    echo "rsync --rsync-path 'sudo rsync' -avzq --timeout=300 --delete hadoopqa@${ADM_HOST}:${ADM_PATH} $TMP_DIR"
    rsync --rsync-path 'sudo rsync' -avzq --timeout=300 --delete hadoopqa@${ADM_HOST}:${ADM_PATH} $TMP_DIR
    RC=$?
    if [ $RC -ne 0 ]; then
        echo "Error: rsync of files from admin host $ADM_HOST failed!"
        exit $RC
    fi

    # Show no log and error status
    # CSV_NODES=`echo $NODE_LIST|tr '\n' ','`
    # echo "CSV_NODES=$CSV_NODES"
    # /usr/bin/pdcp -w $CSV_NODES $SOURCE $TARGET

    # rsync files from the Jenkins worker node to the target node
    echo "NODES=$NODES"
    for node in $NODES; do
        $SSH $node sudo mkdir -p $NODE_PATH
        echo "rsync --rsync-path 'sudo rsync' -avzq --timeout=300 --delete $TMP_DIR/* $node:$NODE_PATH"
        rsync --rsync-path 'sudo rsync' -avzq --timeout=300 --delete $TMP_DIR/* $node:$NODE_PATH
        RC=$?
        if [ $RC -ne 0 ]; then
            echo "Error: rsync of files from Jenkins worker node to node $node failed!"
            exit $RC
        fi
        echo "$SSH $node sudo chown -R $CHOWN_ATTR $NODE_PATH"
        $SSH $node sudo chown -R $CHOWN_ATTR $NODE_PATH
        RC=$?
        if [ $RC -ne 0 ]; then
            echo "Error: chown files on node $NODE failed!"
            exit $RC
        fi
    done
    rm -rf $TMP_DIR
}

remote_exec() {
    local remote_host=$1
    local cmd=$2
    local user=$3
    if [ -z $user ]; then
	set -x
	$SSH $remote_host "sudo bash -c \"$cmd\""
	set +x
    else
	set -x
	$SSH $remote_host "sudo -su $user bash -c \"$cmd\""
	set +x
    fi
}

exec_nn_hdfsuser() {
    local cmd=$1
    remote_exec "$NAMENODE_Primary" "$cmd" "$HDFSUSER"
}

exec_nn_root() {
    local cmd=$1
    remote_exec "$NAMENODE_Primary" "$cmd"
}

exec_jt_mapreduser() {
    local cmd=$1
    remote_exec "$jobtrackernode" "$cmd" "$MAPREDUSER"
}

# The set of fanout functions used to run from devadm102 as root.
# Now they will be run from the Jenkins workder node as hadoopqa.
# Therefore, we need to append them with "sudo bash -c \"$cmd\"".
# so they run as root on the target nodes.

# [ -n "$HOSTLIST" ] && $PDSH_FAST -w "$HOSTLIST" $*

fanout() {
    local cmd=$*
    # echo 'fanout: start on ' `date +%H:%M:%S`

    #[ -n "$HOSTLIST" ] && $PDSH_FAST -w "$HOSTLIST" "sudo bash -c \"$cmd\""

    if [ -n "$HOSTLIST" ]; then
        #echo "$PDSH_FAST -w \"$HOSTLIST\" \"sudo bash -c \\\"$cmd\\\"\""
        set -x
        $PDSH_FAST -w "$HOSTLIST" "sudo bash -c \"$cmd\""
        RC=$?
        set +x
        return $RC
    fi
    # echo 'fanout: end on ' `date +%H:%M:%S`
}
fanoutnogw() {
    # echo 'fanoutnogw: (not to gateway) start on ' `date +%H:%M:%S`
    [ -n "$HOSTLISTNOGW" ] && \
        set -x && $PDSH_FAST -w "$HOSTLISTNOGW" "sudo bash -c \"$*\"" && RC=$? && set +x
    # echo 'fanoutnogw: (not to gateway) end on ' `date +%H:%M:%S`
    return $RC
}

fanoutscp() {
    local SOURCE=$1
    local TARGET=$2
    local NODE_LIST=$3
    local CHOWN_ATTR=$4

    # host is comma separated list of hosts
    if [[ -z $NODE_LIST ]]; then
        echo "ERROR: Required host list argument is missing!!!"
        return 1
    else
        NODES=`echo $NODE_LIST|tr "," " "`
    fi
    num_nodes=`echo $NODES|wc -w`

    # rsync file from the Jenkins worker node to the target node
    for node in $NODES; do
        echo "rsync --rsync-path 'sudo rsync' -avzq --timeout=300 --delete $SOURCE $node:$TARGET"
        rsync --rsync-path 'sudo rsync' -avzq --timeout=300 --delete $SOURCE $node:$TARGET
        RC=$?
        if [ $RC -ne 0 ]; then
            echo "Error: rsync of files from Jenkins worker node to node $node failed!"
            exit $RC
        fi

        if [ -n "$CHOWN_ATTR" ]; then
            echo "$SSH $node sudo chown -R $CHOWN_ATTR $TARGET"
            $SSH $node sudo chown -R $CHOWN_ATTR $TARGET
            RC=$?
            if [ $RC -ne 0 ]; then
                echo "Error: chown files on node $NODE failed!"
                exit $RC
            fi
        fi
    done

    # Unable to use pdcp:
    # -l root switches user on the soruce host to root which then results in
    # error when it tries to ssh to the target hosts because of sshca.
    #
    # logfile="/tmp/pdcp.$$.log"
    # set -x
    # /usr/bin/pdcp -l root -w $NODE_LIST $SOURCE $TARGET 2> $logfile
    # set +x
    # # Error if file exists and has a size greater than zero
    # if [[ -s $logfile ]]; then
    #     cat "$logfile"
    #     rm $logfile
    #     echo "ERROR: pdcp failed!!!"
    #     exit 1
    # fi
    #
    # if [ -n "$CHOWN_ATTR" ]; then
    #     set -x
    #     pdsh -S -w $$NODE_LIST "sudo bash -c \"chown -R $CHOWN_ATTR $TARGET\""
    #     RC=$?
    #     set +x
    #     if [ $RC -ne 0 ]; then
    #         echo "ERROR: pdsh failed!!!"
    #         exit 1
    #     fi
    # fi
    # return
}


# parameter 1 - command to run
# parameter 2 - comma separated host names
# __HOSTNAME__ in the command arg will be replaced by a single hostname
# for each execution.
fanoutcmd() {
    command=$1
    if [[ -z $command ]]; then
        echo "ERROR: Required command argument is missing!!!"
        return 1
    fi

    # host is comma separated list of hosts
    hosts=$2
    if [[ -z $hosts ]]; then
        echo "ERROR: Required hosts argument is missing!!!"
        return 1
    else
        hosts=`echo $hosts|tr "," " "`
    fi

    num_hosts=`echo $hosts|wc -w`

    echo "fanout to [$num_hosts] hosts '"$hosts"':"
    echo "base command='$command'"

    count=1
    pids=""
    local hostname
    for hostname in $hosts; do
        cmd=`echo "$command" | sed s/__HOSTNAME__/$hostname/g`
        echo "Run command [$count/$num_hosts]: '$cmd &'"
        $cmd &
        pid=$!
        pids=$pids$pid" "
        count=$((count+1))
    done

    set -x
    wait $pids
    set +x
    return 0
}

initManifest() {
  [ -n "$MANIFEST" ] && cp /dev/null  $MANIFEST
}
recordManifest() {
  dt=`date +'%m.%d.%Y.%H.%M.%S (%s)'`
  [ -n "$MANIFEST" ] && (echo "# $dt"; echo "$*") >> $MANIFEST
  echo MANIFEST: "# $dt"; echo MANIFEST: "$*"
}
recordsuccess() {
	recordManifest "$* (success)"
}
recordfail() {
	recordManifest "$* (fail)"
}
recordpkginstall() {
	nm=$1
	ver=$2
	[ -n "$ver" ] && recordManifest  "pkgname=$nm" "pkgver=$ver"
}
fanout_workers_root() {
    local cmd=$1
    echo 'slavefanout: start on ' `date +%H:%M:%S`
    echo "$PDSH_FAST -w \"$SLAVELIST\" \"sudo bash -c \\\"$cmd\\\"\""
    $PDSH_FAST -w "$SLAVELIST" "sudo bash -c \"$cmd\""
    RC=$?
    echo 'slavefanout: end on ' `date +%H:%M:%S`
    return $RC
}
slownogwfanout() {
    echo 'slownogwfanout: (not to gateway) start on ' `date +%H:%M:%S`
    if [ -n "$HOSTLISTNOGW" ]; then
        set -x
        $PDSH_SLOW -w "$HOSTLISTNOGW" "sudo bash -c \"$*\""
        RC=$?
        set +x
        echo 'slownogwfanout: (not to gateway) end on ' `date +%H:%M:%S`
        # return $RC
    fi
}

slowfanout() {
	echo 'slowfanout: start on ' `date +%H:%M:%S`
	$PDSH_SLOW -w "$HOSTLIST" "sudo bash -c \"$*\""
	echo 'slowfanout: end on ' `date +%H:%M:%S`
}
export ALLNAMENODESLIST=`echo $ALLNAMENODES  | tr ' ' ,`
export ALLNAMENODESAndSecondariesList=`echo $ALLNAMENODESAndSecondaries  | tr ' ' ,`
fanoutNN() {
    local cmd=$1
    local user=$2
    echo 'fanoutNN: start on ' `date +%H:%M:%S`
    if [ -n "$user" ]; then
        set -x
	$PDSH_SLOW -w "$ALLNAMENODESLIST" "sudo -su $user bash -c \"$cmd\""
        RC=$?
        set +x
    else
        set -x
	$PDSH_SLOW -w "$ALLNAMENODESLIST" "sudo bash -c \"$cmd\""
        RC=$?
        set +x
    fi
    echo 'fanoutNN: end on ' `date +%H:%M:%S`
    return $RC
}

fanout_nn_hdfsuser() {
    local cmd=$1
    fanoutNN "$cmd" "$HDFSUSER"
}

fanout_nn_root() {
    local cmd=$1
    fanoutNN "$cmd"
}

fanoutSecondary() {
    local cmd=$1
    local user=$2
    echo 'fanoutSecondary: start on ' `date +%H:%M:%S`
    if [ -n "$user" ]; then
        set -x
	$PDSH_SLOW -w "$ALLSECONDARYNAMENODESLIST" "sudo -su $user bash -c \"$cmd\""
        RC=$?
        set +x
    else
        set -x
	$PDSH_SLOW -w "$ALLSECONDARYNAMENODESLIST" "sudo bash -c \"$cmd\""
        RC=$?
        set +x
    fi
    echo 'fanoutSecondary: end on ' `date +%H:%M:%S`
    return $RC
}

fanoutSecondary_hdfsuser() {
    local cmd=$1
    fanoutSecondary "$cmd" "$HDFSUSER"
}

fanoutSecondary_root() {
    fanoutSecondary "$cmd"
}

fanoutNNAndSecondary() {
    local cmd=$1
    local user=$2
    echo 'fanoutNNAndSecondary: start on ' `date +%H:%M:%S`
    if [ -n "$user" ]; then
	$PDSH_SLOW -w "$ALLNAMENODESAndSecondaries" "sudo -su $user bash -c \"$cmd\""
    else
	$PDSH_SLOW -w "$ALLNAMENODESAndSecondaries" "sudo bash -c \"$cmd\""
    fi
    echo 'fanoutNNAndSecondary: end on ' `date +%H:%M:%S`
}
fanoutHBASETestClient() {
        echo 'fanoutHBASETestClient: start on ' `date +%H:%M:%S`
        machname=`echo $HBASEMASTERNODE | cut -f1 -d:`
        echo $* > $scriptdir/hbase_test_client.cmds-to-run.$cluster.sh
        execCmd "scp $scriptdir/hbase_test_client.cmds-to-run.$cluster.sh ${machname}:/tmp/"
        execCmd "ssh $machname \"sudo bash -c \"sh /tmp/hbase_test_client.cmds-to-run.$cluster.sh\"\""
        st=$?
        echo "fanoutHBASETestClient status: st=$st"
        echo 'fanoutHBASETestClient: end on ' `date +%H:%M:%S`
        return $st
}
fanoutHBASEMASTER() {
        echo 'fanoutHBASEMASTER: start on ' `date +%H:%M:%S`
        machname=`echo $HBASEMASTERNODE | cut -f1 -d:`
        echo $* > $scriptdir/hbase_master.cmds-to-run.$cluster.sh
        execCmd "scp $scriptdir/hbase_master.cmds-to-run.$cluster.sh ${machname}:/tmp/"
        execCmd "ssh $machname \"sudo bash -c \"sh /tmp/hbase_master.cmds-to-run.$cluster.sh\"\""
        st=$?
        echo "fanoutHBASEMASTER status: st=$st"
        echo 'fanoutHBASEMASTER: end on ' `date +%H:%M:%S`
        return $st
}
fanoutHBASEZOOKEEPER() {
        echo 'fanoutHBASEZOOKEEPER: start on ' `date +%H:%M:%S`
        HBASEZOOKEEPERNODELIST=`echo $HBASEZOOKEEPERNODE| tr ' ' ,`
        $PDSH_SLOW -w "$HBASEZOOKEEPERNODELIST" "sudo bash -c \"$*\""
        st=$?
        echo 'fanoutHBASEZOOKEEPER: end on ' `date +%H:%M:%S`
        return $st
}
fanoutREGIONSERVER() {
        echo 'fanoutREGIONSERVER: start on ' `date +%H:%M:%S`
        REGIONSERVERLIST=`echo $REGIONSERVERNODES| tr ' ' ,`
        $PDSH_SLOW -w "$REGIONSERVERLIST" "sudo bash -c \"$*\""
        st=$?
        echo 'fanoutREGIONSERVER: end on ' `date +%H:%M:%S`
        return $st
}


fanoutHiveServer2() {
        echo 'fanoutHiveServer2: start on ' `date +%H:%M:%S`
        HIVE_SERVER2_LIST=`echo $hs2_nodes | tr ' ' ,`
        $PDSH_SLOW -w "$HIVE_SERVER2_LIST" "sudo bash -c \"$*\""
        st=$?
        echo 'fanoutHiveServer2: end on ' `date +%H:%M:%S`
        return $st
}

fanoutHiveClient() {
        echo 'fanoutHiveClient: start on ' `date +%H:%M:%S`
        HIVE_CLIENT_LIST=`echo $hive_client | tr ' ' ,`
        $PDSH_SLOW -w "$HIVE_CLIENT_LIST" "sudo bash -c \"$*\""
        st=$?
        echo 'fanoutHiveClient: end on ' `date +%H:%M:%S`
        return $st
}

fanoutHiveJdbcClient() {
        echo 'fanoutHiveJdbcClient: start on ' `date +%H:%M:%S`
        JDBC_CLIENT_LIST=`echo $jdbc_client | tr ' ' ,`
        $PDSH_SLOW -w "$JDBC_CLIENT_LIST" "sudo bash -c \"$*\""
        st=$?
        echo 'fanoutHiveJdbcClient: end on ' `date +%H:%M:%S`
        return $st
}

fanoutHiveMysql() {
        echo 'fanoutHiveMysql: start on ' `date +%H:%M:%S`
        HIVE_MYSQL_LIST=`echo $hive_mysql | tr ' ' ,`
        $PDSH_SLOW -w "$HIVE_MYSQL_LIST" "sudo bash -c \"$*\""
        st=$?
        echo 'fanoutHiveMysql: end on ' `date +%H:%M:%S`
        return $st
}

fanoutHcatServer() {
        echo 'fanoutHcatServer: start on ' `date +%H:%M:%S`
        HCAT_SERVER_LIST=`echo $hcat_server | tr ' ' ,`
        $PDSH_SLOW -w "$HCAT_SERVER_LIST" "sudo bash -c \"$*\""
        st=$?
        echo 'fanoutHcatServer: end on ' `date +%H:%M:%S`
        return $st
}

fanoutTez() {
   echo 'fanoutTez: start on ' `date +%H:%M:%S`
   if [ -z "$teznode" ]; then
      echo "ERROR: teznode not defined"
      return 1
   fi
   TEZ_NODE_LIST=`echo $teznode | tr ' ' ,`
   set -x
   $PDSH_SLOW -w "$TEZ_NODE_LIST" "sudo bash -c \"$*\""
   st=$?
   set +x
   echo 'fanoutTez: end on ' `date +%H:%M:%S`
   return $st
}

fanoutOneTez() {
   echo 'fanoutOneTez: start on ' `date +%H:%M:%S`
   TEZ_NODE_LIST=`echo $teznode | cut -f1 -d ' '`
   set -x
   $PDSH_SLOW -w "$TEZ_NODE_LIST" "sudo bash -c \"$*\""
   st=$?
   set +x
   echo 'fanoutOneTez: end on ' `date +%H:%M:%S`
   return $st
}

fanoutTezUI() {
  echo 'fanoutTezUI: start on ' `date +%H:%M:%S`
  if [ -z $jobtrackernode ]; then
     echo ERROR: Not define grid_re.clusters.$cluster.jobtracker
     return 1
  fi 
  TEZ_UI_NODE_LIST=`echo $jobtrackernode | tr ' ' ,`
  set -x
  $PDSH_SLOW -w "$TEZ_UI_NODE_LIST" "sudo bash -c \"$*\""
  st=$?
  set +x
  echo 'fanoutTez_UI: end on ' `date +%H:%M:%S`
  return $st
}

fanoutSpark() {
  echo 'fanoutSpark: start on ' `date +%H:%M:%S`
  if [ -z $gateway ]; then
     echo ERROR: Not define grid_re.clusters.$cluster.gateway
     return 1
  fi
  SPARK_NODE=`echo $gateway | tr ' ' ,`
  $PDSH_SLOW -w "$SPARK_NODE" "sudo bash -c \"$*\""
  st=$?
  echo 'fanoutSpark: end on ' `date +%H:%M:%S`
  return $st
}

fanoutSparkUI() {
  echo 'fanoutSparkUI: start on '`date +%H:%M:%S`
  if [ -z $jobtrackernode ]; then
     echo ERROR: Not define grid_re.clusters.$cluster.jobtracker
     return 1
  fi
  SPARK_UI_NODE_LIST=`echo $jobtrackernode | tr ' ' ,`
  $PDSH_SLOW -w "$SPARK_UI_NODE_LIST" "sudo bash -c \"$*\""
  st=$?
  echo 'fanoutSpark_UI: end on '`date +%H:%M:%S`
  return $st
}

fanoutGW() {
    local cmd=$1
    echo "$SSH $gateway \"sudo bash -c \\\"$cmd\\\"\""
    $SSH $gateway "sudo bash -c \"$cmd\""
    RC=$?
    return $RC
}

fanoutGW_old() {
   # echo fanoutGW: running "$@" 

   echo $* > $scriptdir/gw.cmds-to-run.$cluster.sh

   [ -n "$gateway" ]  && (
       machname=$gateway
       echo fanoutGW: running on ${gateway}:  "$@" 
       execCmd "scp $scriptdir/gw.cmds-to-run.$cluster.sh ${machname}:/tmp/"
       execCmd "ssh $machname \"sudo bash -c \"sh /tmp/gw.cmds-to-run.$cluster.sh\"\""

   )
#   st=0
   [ -n "$gateways" ]  && for g in $gateways; do
       machname=`echo $g | cut -f1 -d:`
       yrootname=`echo $g | cut -f2 -d:`
       [ -z "$yrootname" ] &&  yrootname=hadoop.${cluster}
       yrootname=${yrootname}
       
       # Check if yroot already exists or not,
       yroot_output=`ssh $machname /home/y/bin/yroot --set $yrootname`
       if [ -n "$yroot_output" ]; then
       	   echo fanoutGW: running on ${g}:  "$@" 
           execCmd "scp $scriptdir/gw.cmds-to-run.$cluster.sh ${machname}:/tmp/"
           st=$?
           execCmd "ssh ${machname} \"sudo /home/y/bin/yrootcp  /tmp/gw.cmds-to-run.$cluster.sh  ${yrootname}:/tmp/\""
           st=$?
           execCmd "ssh $machname \"sudo /home/y/bin/yroot  ${yrootname} --cmd \\\"sh -x  /tmp/gw.cmds-to-run.$cluster.sh \\\"\""
           st=$?
       else
	   echo "yroot $yrootname doesn't exist on $machname, skip fanoutGW on yroot..."
       fi
       echo "status: st=$st"
   done
   return $st
}
##     
## the above method and the following one are identical.
## the code is copied for one reason: we didn't want to accidently
## reevaluate $* in the each statements, causing it to be semantically
## different for quotes/escapes.
##
fanoutYRoots() {
   echo fanoutYRoots: running "$@" 

   echo $* > $scriptdir/gw.cmds-to-run.$cluster.sh

   echo fanoutYRoots: gateways = $gateways
   [ -n "$gateways" ]  && for g in $gateways; do
       machname=`echo $g | cut -f1 -d:`
       yrootname=`echo $g | cut -f2 -d:`
       [ -z "$yrootname" ] &&  yrootname=hadoop.${cluster}
       yrootname=${yrootname}

       # Check if yroot already exists or not,
       yroot_output=`ssh $machname /home/y/bin/yroot --set $yrootname`
       if [ -n "$yroot_output" ]; then
           echo fanoutYRoots: running on ${g}:  "$@" 
           execCmd "scp $scriptdir/gw.cmds-to-run.$cluster.sh ${machname}:/tmp/"
           [ $? -ne 0 ] && st=$?
           execCmd "ssh ${machname} \"sudo /home/y/bin/yrootcp  /tmp/gw.cmds-to-run.$cluster.sh  ${yrootname}:/tmp/\""
           [ $? -ne 0 ] && st=$?
           execCmd "ssh $machname \"sudo /home/y/bin/yroot  ${yrootname} --cmd \\\"sh /tmp/gw.cmds-to-run.$cluster.sh \\\"\""
           [ $? -ne 0 ] && st=$?
       else
	   echo "yroot $yrootname doesn't exist on $machname, skip fanoutGW on yroot..."
       fi
    done
    return $st
}

##
## wrapper function for executing cmd,
## exit when return status of executed cmd is not zero
##
function execCmd() {
   echo " -- Issue cmd '" $1 "' now..."
   $1
   status=$?
   [ "$status" -ne 0 ] && echo ">>>> Error! failed to execute cmd '" $1  "'<<<< " 
   return $status
   #[ "$status" -ne 0 ] && echo ">>>> Error! failed to execute cmd '" $1  "' " && exit $status
}

initManifest
