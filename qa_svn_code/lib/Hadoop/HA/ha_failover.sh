## ha_failover.sh 20131121phw
#  failover the given cluster's namenodes as specfied, ha1->ha1 or ha2->ha1
#
# This needs to run on adm102 
#
# tweak things to try and speed up failover

# check inputs
if [ $# -ne 2 ]; then
  echo "Error: need the cluster name and the namenode to failover to"
  echo "Usage: ha_failover.sh <cluster_name> <namenode to make active, ha1/ha2>" 
  echo "Example to make argentum's HA2 Active and send HA1 to Standby:" 
  echo "   ha_failover.sh argentum ha2"
  exit 1
fi
CLUSTER=$1
NEW_ACTIVE_NN=$2

# make sure we're running on adm102 
LOCALSHORTHOST=`uname -n|cut -d '.' -f1`
if [ "adm102" != "$LOCALSHORTHOST" ]
then
  echo "ERROR: need to run this script on the admin box adm102"
  exit 1
fi

# get our cluster's namenode hosts
HA1=`/homes/patwhite/bin/getRoleMembers.sh $CLUSTER namenode`
HA2=`/homes/patwhite/bin/getRoleMembers.sh $CLUSTER namenode2`

# kinit as hdfs priv user
`ssh $HA1 /usr/kerberos/bin/kinit -k -t /homes/hdfsqa/etc/keytabs/hdfs.dev.headless.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM`
`ssh $HA2 /usr/kerberos/bin/kinit -k -t /homes/hdfsqa/etc/keytabs/hdfs.dev.headless.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM`

# check for eth0:0 network alias on ha1 and ha2
RESULT=`ssh $HA1 "if [ -f "/etc/sysconfig/network-scripts/ifcfg-eth0:0" ]; then echo exists; fi"`
if [ "$RESULT" = "exists" ]; then
  echo "Info, $HA1 eth0:0 file exists"
else
  echo "Error, $HA1 eth0:0 missing"
  exit 1
fi
RESULT=`ssh $HA2 "if [ -f "/etc/sysconfig/network-scripts/ifcfg-eth0:0" ]; then echo exists; fi"`
if [ "$RESULT" = "exists" ]; then
  echo "Info, $HA2 eth0:0 file exists"
else
  echo "Error, $HA2 eth0:0 missing"
  exit 1
fi

# check if HA is enabled on cluster under test
HA1_NNALIAS=`ssh $HA1 "grep -A1 dfs.nameservices  /home/gs/gridre/yroot.$CLUSTER/conf/hadoop/hdfs-ha.xml | grep flubber-alias104|cut -d '>' -f2|cut -d '<' -f1"`
if [ -z "$HA1_NNALIAS" ]; then
  echo "Error, HA is not enabled on $HA1"
  exit 1
fi
HA2_NNALIAS=`ssh $HA2 "grep -A1 dfs.nameservices  /home/gs/gridre/yroot.$CLUSTER/conf/hadoop/hdfs-ha.xml | grep flubber-alias104|cut -d '>' -f2|cut -d '<' -f1"`
if [ -z "$HA2_NNALIAS" ]; then
  echo "Error, HA is not enabled on $HA2"
  exit 1
fi
if [ "$HA1_NNALIAS" != "$HA2_NNALIAS" ]; then
  echo "Error, HA aliases for HA1 and HA2 do not match"
  exit 1
fi
echo "Info, cluster HA alias is: $HA1_NNALIAS"


##########
## Function check_ha_state 
##########
function check_ha_state {
  # don't have to go to each node for its state, just doing so to try functionality on both ha1 and ha2
  echo -n "ServiceID ha1 is: "
  RESULT=`ssh $HA1 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop haadmin -getServiceState ha1"`
  echo $RESULT 
  echo -n "ServiceID ha2 is: "
  RESULT=`ssh $HA2 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop haadmin -getServiceState ha2"`
  echo $RESULT 
}


##########
## Function failover_ha1_to_ha2
##########
function failover_ha1_to_ha2 {
  # start, check ha1 and ha2 service state, failover from ha1 to ha2, recheck service state
  check_ha_state

  # HA1 is Active right now so take its net interface down
  echo "Shutting down $HA1 interface eth0:0..."
echo "		Start switching at `date`"
  ssh $HA1 "ifdown eth0:0"
  # sleep 1
  ssh $HA1 "/sbin/ifconfig|grep eth0:0"
  if [ $? -ne "1" ];then
    echo "Error, eth0:0 is still up on $HA1" 
    exit 1
  fi
  echo "$HA1 eth0:0 is down"

  echo -n "Failing over from ha1 to ha2... "
  CMDRESULT=`ssh $HA1 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop haadmin -failover ha1 ha2"`
  CMDRESULT=$?
  echo $CMDRESULT
  if [ "$CMDRESULT" = "0" ]; then
    echo "success"
  elif [ "$CMDRESULT" = "255" ]; then
    echo "ha2 is already active"
  else
    echo "failed"
    echo "ERROR: failover attempt reported non-zero exit: $CMDRESULT"
    exit 1
  fi

  # confirm ha2 is now active
  CMDRESULT=`ssh $HA2 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop  haadmin  -getServiceState ha2 | grep active"`
  if [ "$CMDRESULT" != "active" ];then
    echo "Error, $HA2 is not Active"
    exit 1
  fi

  # HA2 is now Active, bring its net interface up
  echo "Bringing up $HA2 interface eth0:0..."
  ssh $HA2 "ifup eth0:0"
  #sleep 1
  ssh $HA2 "/sbin/ifconfig|grep eth0:0"
  if [ $? -ne "0" ];then
    echo "Error, eth0:0 is not up on $HA2" 
    exit 1
  fi
  echo "$HA2 eth0:0 is up"
echo "		Finished switching at `date`"

  check_ha_state

  # need to wait for safemode...
  COUNT=1
  while [ "$COUNT" -lt "10" ]; do
    # get safemode status, from the new ANN ha2
    SAFEMODE=`ssh $HA2 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop  dfsadmin -safemode get"`
    # if OFF go on, else gotta wait
    if [[ "$SAFEMODE" == *OFF ]] ;then
      echo "Safemode is off"
      COUNT=10
    else
      echo "Safemode is on, waiting 30 seconds, this is pass$COUNT..."
      sleep 30
      (( COUNT++ ))
    fi
    if [ "$COUNT" -ge "11" ]; then
      echo "Error, $HA2 should be out of safemode by now, it still is in safemode so stopping, please check $HA2"
      exit 1
    fi
  done
}


##########
## Function failover_ha2_to_ha1
##########
function failover_ha2_to_ha1 {
  # start, check ha1 and ha2 service state, failover from ha2 to ha1, recheck service state
  check_ha_state

  # HA2 is Active right now so take its net interface down
  echo "Shutting down $HA2 interface eth0:0..."
echo "		Start switching at `date`"
  ssh $HA2 "ifdown eth0:0"
  #sleep 1
  ssh $HA2 "/sbin/ifconfig|grep eth0:0"
  if [ $? -ne "1" ];then
    echo "Error, eth0:0 is still up on $HA2" 
    exit 1
  fi
  echo "$HA2 eth0:0 is down"

  echo "Failing over from ha2 to ha1..."
  CMDRESULT=`ssh $HA2 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop haadmin -failover ha2 ha1"`
  CMDRESULT=$?
  if [ "$CMDRESULT" = "0" ]; then
    echo "success"
  elif [ "$CMDRESULT" = "255" ]; then
    echo "ha1 is already active"
  else
    echo "failed"
    echo "ERROR: failover attempt reported non-zero exit: $CMDRESULT"
    exit 1
  fi
  # confirm ha1 is now active
  CMDRESULT=`ssh $HA2 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop  haadmin  -getServiceState ha1 | grep active"`
  CMDRESULT=$?
  if [ $? -ne "0" ];then
    echo "Error, $HA1 is not Active"
    exit 1
  fi

  # HA1 is now Active, bring its net interface up
  echo "Bringing up $HA1 interface eth0:0..."
  ssh $HA1 "ifup eth0:0"
  #sleep 1
  ssh $HA1 "/sbin/ifconfig|grep eth0:0"
  if [ $? -ne "0" ];then
    echo "Error, eth0:0 is not up on $HA1" 
    exit 1
  fi
  echo "$HA1 eth0:0 is up"
echo "		Finished switching at `date`"

  check_ha_state

  # need to wait for safemode...
  COUNT=1
  while [ "$COUNT" -lt "10" ]; do
    # get safemode status, from the new ANN ha2
    SAFEMODE=`ssh $HA1 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop  dfsadmin -safemode get"`
    # if OFF go on, else gotta wait
    if [[ "$SAFEMODE" == *OFF ]] ;then
      echo "Safemode is off"
      COUNT=10
    else
      echo "Safemode is on, waiting 30 seconds, this is pass$COUNT..."
      sleep 30
      (( COUNT++ ))
    fi
    if [ "$COUNT" -ge "11" ]; then
      echo "Error, $HA1 should be out of safemode by now, it still is in safemode so stopping, please check $HA1"
      exit 1
    fi
  done
}



##################
## Main entry
##################

# make sure ha1 is active and ha2 standby
if [ "$NEW_ACTIVE_NN" = "ha1" ]; then
  echo "Trying to failover ha2 to ha1..." 
  failover_ha2_to_ha1

  echo ""
  echo "Verify failover results are as expected..."

  # ha1 should be active now, if not already so
  ACTIVE=`ssh $HA1 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop haadmin -getServiceState ha1"`
  if [ "$ACTIVE" != "active" ]; then
    echo "FAIL, ha1 is not active"
  fi
  echo "ha1 $HA1 is Active"
  # and ha2 should be standby
  STANDBY=`ssh $HA2 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop haadmin -getServiceState ha2"`
  if [ "$STANDBY" != "standby" ]; then
    echo "FAIL, ha2 is not standby"
  fi
  echo "ha2 $HA2 is Standby"

elif [ "$NEW_ACTIVE_NN" = "ha2" ]; then
  echo "Trying to failover ha1 to ha2..." 
  failover_ha1_to_ha2

  echo ""
  echo "Verify failover results are as expected..."

  # ha2 should be active now, if not already so
  ACTIVE=`ssh $HA2 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop haadmin -getServiceState ha2"`
  if [ "$ACTIVE" != "active" ]; then
    echo "FAIL, ha2 is not active"
  fi
  echo "ha2 $HA2 is Active"
  # and ha2 should be standby
  STANDBY=`ssh $HA1 "/home/gs/gridre/yroot.$CLUSTER/share/hadoop/bin/hdfs --config /home/gs/gridre/yroot.$CLUSTER/conf/hadoop haadmin -getServiceState ha1"`
  if [ "$STANDBY" != "standby" ]; then
    echo "FAIL, ha1 is not standby"
  fi
  echo "ha1 $HA1 is Standby"

else
  echo "Error, HA failover target is neither ha1 nor ha2"
  exit 1
fi

