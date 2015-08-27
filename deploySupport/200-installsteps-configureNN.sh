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
if [ "$CONFIGURENAMENODE" = true ]
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
