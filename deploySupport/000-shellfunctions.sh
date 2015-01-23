#
# Various forms of 'pdsh' invocation, with different expected timeout values.
#
fanout() {
	# echo 'fanout: start on ' `date +%H:%M:%S`
	[ -n "$HOSTLIST" ] && pdsh -w "$HOSTLIST" -u 180 -f 25 $*
	# echo 'fanout: end on ' `date +%H:%M:%S`
}
fanoutnogw() {
        # echo 'fanoutnogw: (not to gateway) start on ' `date +%H:%M:%S`
        [ -n "$HOSTLISTNOGW" ] && pdsh -w "$HOSTLISTNOGW" -u 180 -f 25 $*
        # echo 'fanoutnogw: (not to gateway) end on ' `date +%H:%M:%S`
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
slavefanout() {
	echo 'slavefanout: start on ' `date +%H:%M:%S`
	pdsh -w "$SLAVELIST" -u 180 -f 25 $*
	echo 'slavefanout: end on ' `date +%H:%M:%S`
}
slownogwfanout() {
        echo 'slownogwfanout: (not to gateway) start on ' `date +%H:%M:%S`
        [ -n "$HOSTLISTNOGW" ] && pdsh -w "$HOSTLISTNOGW" -u 600 -f 25 $*
        echo 'slownogwfanout: (not to gateway) end on ' `date +%H:%M:%S`
}

slowfanout() {
	echo 'slowfanout: start on ' `date +%H:%M:%S`
	pdsh -w "$HOSTLIST" -u 600 -f 25 $*
	echo 'slowfanout: end on ' `date +%H:%M:%S`
}
export ALLNAMENODESLIST=`echo $ALLNAMENODES  | tr ' ' ,`
export ALLNAMENODESAndSecondariesList=`echo $ALLNAMENODESAndSecondaries  | tr ' ' ,`
fanoutNN() {
	echo 'fanoutNN: start on ' `date +%H:%M:%S`
	pdsh -w "$ALLNAMENODESLIST" -u 600 -f 25 $*
	echo 'fanoutNN: end on ' `date +%H:%M:%S`
}
fanoutSecondary() {
	echo 'fanoutSecondary: start on ' `date +%H:%M:%S`
	pdsh -w "$ALLSECONDARYNAMENODESLIST" -u 600 -f 25 $*
	echo 'fanoutSecondary: end on ' `date +%H:%M:%S`
}
fanoutNNAndSecondary() {
	echo 'fanoutNNAndSecondary: start on ' `date +%H:%M:%S`
	pdsh -w "$ALLNAMENODESAndSecondaries" -u 600 -f 25 $*
	echo 'fanoutNNAndSecondary: end on ' `date +%H:%M:%S`
}
fanoutHBASETestClient() {
        echo 'fanoutHBASETestClient: start on ' `date +%H:%M:%S`
        machname=`echo $HBASEMASTERNODE | cut -f1 -d:`
        echo $* > $scripttmp/hbase_test_client.cmds-to-run.$cluster.sh
        execCmd "scp $scripttmp/hbase_test_client.cmds-to-run.$cluster.sh ${machname}:/tmp/"
        execCmd "ssh $machname sh /tmp/hbase_test_client.cmds-to-run.$cluster.sh"
        st=$?
        echo "fanoutHBASETestClient status: st=$st"
        echo 'fanoutHBASETestClient: end on ' `date +%H:%M:%S`
        return $st
}
fanoutHBASEMASTER() {
        echo 'fanoutHBASEMASTER: start on ' `date +%H:%M:%S`
        machname=`echo $HBASEMASTERNODE | cut -f1 -d:`
        echo $* > $scripttmp/hbase_master.cmds-to-run.$cluster.sh
        execCmd "scp $scripttmp/hbase_master.cmds-to-run.$cluster.sh ${machname}:/tmp/"
        execCmd "ssh $machname sh /tmp/hbase_master.cmds-to-run.$cluster.sh"
        st=$?
        echo "fanoutHBASEMASTER status: st=$st"
        echo 'fanoutHBASEMASTER: end on ' `date +%H:%M:%S`
        return $st
}
fanoutHBASEZOOKEEPER() {
        echo 'fanoutHBASEZOOKEEPER: start on ' `date +%H:%M:%S`
        HBASEZOOKEEPERNODELIST=`echo $HBASEZOOKEEPERNODE| tr ' ' ,`
        pdsh -w "$HBASEZOOKEEPERNODELIST" -u 600 -f 25 -S $*
        st=$?
        echo 'fanoutHBASEZOOKEEPER: end on ' `date +%H:%M:%S`
        return $st
}
fanoutREGIONSERVER() {
        echo 'fanoutREGIONSERVER: start on ' `date +%H:%M:%S`
        REGIONSERVERLIST=`echo $REGIONSERVERNODES| tr ' ' ,`
        pdsh -w "$REGIONSERVERLIST" -u 600 -f 25 -S $*
        st=$?
        echo 'fanoutREGIONSERVER: end on ' `date +%H:%M:%S`
        return $st
}


fanoutHiveServer2() {
        echo 'fanoutHiveServer2: start on ' `date +%H:%M:%S`
        HIVE_SERVER2_LIST=`echo $hs2_nodes | tr ' ' ,`
        pdsh -w "$HIVE_SERVER2_LIST" -u 600 -f 25 -S $*
        st=$?
        echo 'fanoutHiveServer2: end on ' `date +%H:%M:%S`
        return $st
}

fanoutHiveClient() {
        echo 'fanoutHiveClient: start on ' `date +%H:%M:%S`
        HIVE_CLIENT_LIST=`echo $hive_client | tr ' ' ,`
        pdsh -w "$HIVE_CLIENT_LIST" -u 600 -f 25 -S $*
        st=$?
        echo 'fanoutHiveClient: end on ' `date +%H:%M:%S`
        return $st
}

fanoutHiveJdbcClient() {
        echo 'fanoutHiveJdbcClient: start on ' `date +%H:%M:%S`
        JDBC_CLIENT_LIST=`echo $jdbc_client | tr ' ' ,`
        pdsh -w "$JDBC_CLIENT_LIST" -u 600 -f 25 -S $*
        st=$?
        echo 'fanoutHiveJdbcClient: end on ' `date +%H:%M:%S`
        return $st
}

fanoutHiveMysql() {
        echo 'fanoutHiveMysql: start on ' `date +%H:%M:%S`
        HIVE_MYSQL_LIST=`echo $hive_mysql | tr ' ' ,`
        pdsh -w "$HIVE_MYSQL_LIST" -u 600 -f 25 -S $*
        st=$?
        echo 'fanoutHiveMysql: end on ' `date +%H:%M:%S`
        return $st
}

fanoutHcatServer() {
        echo 'fanoutHcatServer: start on ' `date +%H:%M:%S`
        HCAT_SERVER_LIST=`echo $hcat_server | tr ' ' ,`
        pdsh -w "$HCAT_SERVER_LIST" -u 600 -f 25 -S $*
        st=$?
        echo 'fanoutHcatServer: end on ' `date +%H:%M:%S`
        return $st
}

fanoutTez() {
   echo 'fanoutTez: start on ' `date +%H:%M:%S`
   if [ -z $teznode ]; then
      echo ERROR: Not define grid_re.clusters.$cluster.tez
      return 1
   fi
   TEZ_NODE_LIST=`echo $teznode | tr ' ' ,`
   pdsh -w "$TEZ_NODE_LIST" -u 600 -f 25 -S $*
   st=$?
   echo 'fanoutTez: end on ' `date +%H:%M:%S`
   return $st
}

fanoutOneTez() {
   echo 'fanoutOneTez: start on ' `date +%H:%M:%S`
   TEZ_NODE_LIST=`echo $teznode | cut -f1 -d ' '`
   pdsh -w "$TEZ_NODE_LIST" -u 600 -f 25 -S $*
   st=$?
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
  pdsh -w "$TEZ_UI_NODE_LIST" -u 600 -f 25 -S $*
  st=$?
  echo 'fanoutTez_UI: end on ' `date +%H:%M:%S`
  return $st
}

fanoutGW() {
   # echo fanoutGW: running "$@" 

   echo $* > $scripttmp/gw.cmds-to-run.$cluster.sh

   [ -n "$gateway" ]  && (
       machname=$gateway
       echo fanoutGW: running on ${gateway}:  "$@" 
       execCmd "scp $scripttmp/gw.cmds-to-run.$cluster.sh ${machname}:/tmp/"
       execCmd "ssh $machname sh /tmp/gw.cmds-to-run.$cluster.sh"

   )
#   st=0
   [ -n "$gateways" ]  && for g in $gateways
   do
       machname=`echo $g | cut -f1 -d:`
       yrootname=`echo $g | cut -f2 -d:`
       [ -z "$yrootname" ] &&  yrootname=hadoop.${cluster}
       yrootname=${yrootname}
       
       # Check if yroot already exists or not,
       yroot_output=`ssh $machname /home/y/bin/yroot --set $yrootname`
       if [ -n "$yroot_output" ]; then
       	   echo fanoutGW: running on ${g}:  "$@" 
           execCmd "scp $scripttmp/gw.cmds-to-run.$cluster.sh ${machname}:/tmp/"
           st=$?
           execCmd "ssh ${machname} /home/y/bin/yrootcp  /tmp/gw.cmds-to-run.$cluster.sh  ${yrootname}:/tmp/"
           st=$?
           execCmd "ssh $machname /home/y/bin/yroot  ${yrootname} --cmd \"sh -x  /tmp/gw.cmds-to-run.$cluster.sh \""
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

   echo $* > $scripttmp/gw.cmds-to-run.$cluster.sh

   echo fanoutYRoots: gateways = $gateways
   [ -n "$gateways" ]  && for g in $gateways
   do
       machname=`echo $g | cut -f1 -d:`
       yrootname=`echo $g | cut -f2 -d:`
       [ -z "$yrootname" ] &&  yrootname=hadoop.${cluster}
       yrootname=${yrootname}

       # Check if yroot already exists or not,
       yroot_output=`ssh $machname /home/y/bin/yroot --set $yrootname`
       if [ -n "$yroot_output" ]; then
           echo fanoutYRoots: running on ${g}:  "$@" 
           execCmd "scp $scripttmp/gw.cmds-to-run.$cluster.sh ${machname}:/tmp/"
           [ $? -ne 0 ] && st=$?
           execCmd "ssh ${machname} /home/y/bin/yrootcp  /tmp/gw.cmds-to-run.$cluster.sh  ${yrootname}:/tmp/"
           [ $? -ne 0 ] && st=$?
           execCmd "ssh $machname /home/y/bin/yroot  ${yrootname} --cmd \"sh /tmp/gw.cmds-to-run.$cluster.sh \""
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

export PARTITIONHOME=/home
export GSHOME=$PARTITIONHOME/gs
export yroothome=$GSHOME/gridre/yroot.$cluster
export yrootHadoopCurrent=$yroothome/share/hadoop
export yrootHadoopMapred=$yroothome/share/hadoop
export yrootHadoopHdfs=$yroothome/share/hadoop
export yrootHadoopConf=$yroothome/conf/hadoop
export GRIDJDK_VERSION=$GRIDJDK_VERSION


initManifest
