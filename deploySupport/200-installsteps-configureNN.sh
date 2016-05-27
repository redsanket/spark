#
# "run the yinst-sets to make the NN config directories have NN content."
#
#
# (1) [setup for later] We copy scripts for namenode startup, to all NNs.
# (2) we run "cfg-${scriptnames}.sh" (made by the packaging of the configs) on each NN.
# (2) We run a small perl(1) script to get the output and parse it.
#
# Inputs: $STARTNAMENODE	(boolean)
# Inputs: $REMOVEEXISTINGDATA	(boolean)
# Inputs: $NAMENODE_Primary (set by installgrid.sh)
# Inputs: $cluster
#
if [ "$CONFIGURENAMENODE" = true ]  && [ "$ENABLE_HA" = false ]
then
    echo == Copying namenode-start scripts to namenodes.
    fanoutcmd "scp $scripttmp/nameno*.sh $scripttmp/getclusterid.pl __HOSTNAME__:/tmp/" "$ALLNAMENODESLIST"

    echo == Running namenode-configure script
    fanoutNN "/bin/sh $yrootHadoopConf/cfg-${scriptnames}-namenode.sh " 
    if [ -z "$secondarynamenode" ]; then
       echo "no secondary name node to configure or start"
    else
       fanoutSecondary "sh $yrootHadoopConf/cfg-${scriptnames}-namenode.sh"
    fi
fi

if [ "$ENABLE_HA" = true ];then

  # HA network interface startup
  # keeping this block separate for clarity, HA support which allows ip-failover requires
  # having eth2:0 interfaces on both namenodes, and having one (and only one) active at a
  # time. This block verifies the grid_re.clusters.$cluster.namenode is up and that the
  # grid_re.clusters.$cluster.namenode2 is down.
  echo "Try to ifdown nn2..."
  fanoutSecondary "ifdown eth2:0"
  RESULT_STANDBY=`pdsh -w $ALLSECONDARYNAMENODESLIST "ifconfig | grep eth2:0"`
  #echo RESULT_STANDBY is: $RESULT_STANDBY
  if [ -z "$RESULT_STANDBY" ]; then
    echo "Standby namenode eth2:0 is down"
  else
    echo "ERROR: Standby namenode eth2:0 is not down"
  fi

  echo "Try to ifup nn..."
  fanoutNN "ifup eth2:0"
  RESULT_ACTIVE=`pdsh -w $ALLNAMENODESLIST "ifconfig | grep eth2:0"`
  echo RESULT_ACTIVE is: $RESULT_ACTIVE
  if [ -n "$RESULT_ACTIVE" ]; then
    echo "Active namenode eth2:0 is up"
  else
    echo "ERROR: Active namenode eth2:0 is not up"
  fi
fi
