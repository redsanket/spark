##
## Here, we start a small test: if there's a ROLE in "roledb" for
## this cluster, assume that the member-list (machine-list) in that
## ROLE is trustworthy. Use it.
##
##

#
# initRoleList: cache away a list of IGOR roles for later searching.
# "dumpAllRoles.sh" is an API equivalent of
#            igor list -roles 'grid_re.clusters.*'
#

initRoleList()
{
    # If Hbase is not being installed, remove the hbase related roles which
    # are not needed. Otherwise, if these roles exists but are empty, they
    # will cause the deployment to fail.
    if [ -n "$HBASEVERSION" ]; then
        cat $role_list|grep -v regionserver|grep -v master|grep -v zookeeper > ${role_list}.new
        mv -f ${role_list}.new $role_list
    fi
}

#
# "roleExists  ankh"            -  does grid_re.clusters.ankh exist?
# "roleExists  ankh.namenode"   -  does grid_re.clusters.ankh.namenode exist?
#
# This is meant to be very fast - a grep(1) on a small file, which is really
# the cached output of "dumpAllRoles.sh" (which, itself, is the output of
# the API equivalent of `igor list -roles 'grid_re.clusters.*'`

# input:      $role_list - assumes someone's already set the cache-file name
# argument:   cluster-name or cluster-subrole-name
#
#
roleExists() {
    egrep -q "^grid_re.clusters.$1\$"  $role_list
}

# "setGridParameters  ankh"            -  set variables/files to install onto ankh
#
# input:    igor role names for cluster. ("grid_re.clusters.ankh" is such a role.
#                                        note that  "grid_re.clusters.ankh.namenode"
#                                        is used for details about ankh.)
# argument:   cluster-name
# output:     environment/shell variables
#				$namenode
#				$secondarynamenode
#                               $namenodehaalias
#				$jobtrackernode
#				$gateway
#				$hdfsproxynode
#				$hcatservernode
#				$daqnode
#				$oozienode
#				$teznode
#				$kmsnode
#				$zknode
#				$CLUSTER
#				$scriptnames
#				$confpkg
# output:     file contents
#        hostlist.$CLUSTER.txt  - all machines that will be updated with software
#        slaves.$CLUSTER.txt    - that list, with the specialty-machines removed
#                                 (no oozie/namenode (etc)/hdfsproxy/hcatserver/daq)
#
# Note: THE FAILURE CASE EXITS THE PROGRAM.
#

# dumpMembershipList.sh and dumpAllRoles.sh scripts is deprecated after GRIDCI-2332

setGridParameters() {
    # step 1: make sure that we have a list-of-roles to work from.
    initRoleList

    # step 2: pry basic information out of IGOR for this cluster:
    if roleExists $CLUSTER; then
        # step 2a: the machine-list itself
        echo cluster $CLUSTER exists according to rolesdb

        /usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway)" > $cluster_list

        # step 2b: use certain defaults if not provided
        export namenode=`tail -1 $cluster_list`
        export jobtrackernode=`tail -2 $cluster_list | sed -n 1p`
        export secondarynamenode=`tail -3 $cluster_list | sed -n 1p`
        ## export namenodehaalias=`tail -1 $cluster_list`
        export gateway=gwbl2003.blue.ygrid.yahoo.com
        export hdfsproxynode=
        export hcatservernode=
        export daqnode=
        export oozienode=
        export teznode=
        export kmsnode=
        export zknode=
        
        # step 2c: Look for specific overrides via IGOR roles
	#
	#
	#  Beginning of federated/striping support.
	#
	#
	#  Igor roles usage:
	#  use case 1
	#  	grid_re.clusters.ankh - if only this exists, it's assumed to be a list
	#  	                        of machines with NN/JT/2nd-NN, counting from
	#  	                        highest-numbered machine.
	#  use case 2
	#  	grid_re.clusters.ankh.namenode - if specified, this will be the default
	#  	                        namenode for the machines specified in grid_re.clusters.ankh
	#  	grid_re.clusters.ankh.namenode2 - Must also be given if .namenode is given
	#
        if roleExists $CLUSTER.namenode
        then
            if roleExists $CLUSTER.namenode2
            then
                /usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.namenode)" > $nns_list
                /usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.namenode2)" > $sns_list
                if roleExists $CLUSTER.namenode_alias
                then
                    /usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.namenode_alias)" > $nn_haalias_list
                else
                    touch $nn_haalias_list
                fi
                # Ignore Federation setting when HA is enabled. They can coexist if Federation
                # was properly supported in config and deployment. Currently the federation
                # config is generated during deployment using a script.
                if [ "$ENABLE_HA" = true ]; then
                    echo Using only one set of namenodes since HA is enabled.
                    head -n 1 $nns_list > $nn_list
                    head -n 1 $sns_list > $sn_list
                    #head -n 1 nnalias.$CLUSTER.txt > $nn_haalias_list
                    rm -f $nns_list $sns_list
                else
                    mv $nns_list $nn_list
                    mv $sns_list $sn_list
                fi
                export namenode=`cat $nn_list`
                export secondarynamenode=`cat $sn_list`
                export namenodehaalias=`cat $nn_haalias_list`
            else
                echo "Config error: namenode for $CLUSTER exists, but no .namenode2 role." && \
                    exit 1
            fi
        else
            echo $namenode > $nn_list
            echo $secondarynamenode > $sn_list
        fi
        cat $sn_list $nn_list > $all_nn_list

        if roleExists $CLUSTER.regionserver
        then
            /usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.regionserver)" > $hbase_region_list
        fi

        if roleExists $CLUSTER.master
        then
            /usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.master)" > $hbase_list
        fi

        if roleExists $CLUSTER.zookeeper
        then
            /usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.zookeeper)" > $hbase_zk_list
        fi

        roleExists $CLUSTER.jobtracker && \
            export jobtrackernode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.jobtracker)"`
        roleExists $CLUSTER.gateway && \
            export gateway=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.gateway)"`
        roleExists $CLUSTER.hdfsproxy && \
            export hdfsproxynode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.hdfsproxy)"`
        roleExists $CLUSTER.hcat&& \
            export hcatservernode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.hcat)"`
        roleExists $CLUSTER.hive&& \
            export hcatservernode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.hive)"`
        roleExists $CLUSTER.daq&& \
            export daqnode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.daq)"`
        roleExists $CLUSTER.oozie && \
            export oozienode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.oozie)"`
	if roleExists $CLUSTER; then
            roles="@grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway"
            roleExists $CLUSTER.hive  && roles+=",@grid_re.clusters.$CLUSTER.hive"
            roleExists $CLUSTER.oozie && roles+=",@grid_re.clusters.$CLUSTER.oozie"
            # teznode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER,@grid_re.clusters.$CLUSTER.gateway,@grid_re.clusters.$CLUSTER.hive,@grid_re.clusters.$CLUSTER.oozie)"`
            teznode=`/usr/local/bin/yinst range -ir "(${roles})"`
            if [[ -n $gateway ]]; then
                teznode+="\n$gateway"
            fi
            teznode=`echo -e "$teznode"|sort -u`
            export teznode=$teznode
	fi
        roleExists $CLUSTER.yroots && \
            export yroots=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.yroots)"`
        roleExists $CLUSTER.gateways && \
            export gateways=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.gateways)"`
        roleExists $CLUSTER.kms&& \
            # KMS node also hosts ZK service on flubber
        # only support single KMS node in Flubber
        export kmsnode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.kms)" | head -n 1`
        roleExists $CLUSTER.zookeeper && \
            export zookeepernodes=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.zookeeper)"`
        roleExists $CLUSTER.hit-yroots && \
            export hitnodes=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.hit-yroots)"`
        roleExists $CLUSTER.hs2 && \
            export hs2_nodes=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.hs2)"`
        roleExists $CLUSTER.server && \
            export hcat_server=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.server)"`
        roleExists $CLUSTER.jdbc && \
            export jdbc_client=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.jdbc)"`
        roleExists $CLUSTER.hs2.masters && \
            export hs2_masters=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.hs2_masters)"`
        roleExists $CLUSTER.hs2.slaves && \
            export hs2_slaves=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.hs2_slaves)"`
        roleExists $CLUSTER.client && \
            export hive_client=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.client)"`
        roleExists $CLUSTER.database && \
            export hive_mysql=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.$CLUSTER.database)"`

        echo "INSTALL_GW_IN_YROOT=$INSTALL_GW_IN_YROOT"
        if [ "$INSTALL_GW_IN_YROOT" == "true" ]; then
            if [ -n "$gateways" ]
            then
                echo " gateways/yroot info already defined in igor"
                echo " ===== gateways: $gateways"
            else
                export gateways=$gateway":"$CLUSTER
                echo " ===== gateways: $gateways"
            fi
        fi

        indent=30
        printf "%-${indent}s %s\n" "===  namenode" $namenode
        printf "%-${indent}s %s\n" "===  secondary namenodes" $secondarynamenode
        printf "%-${indent}s %s\n" "===  namenodehaalias" $namenodehaalias
        printf "%-${indent}s %s\n" "===  jobtrackernode" $jobtrackernode
        printf "%-${indent}s %s\n" "===  gateway" $gateway
        [ -n "$oozienode" ]      && printf "%-${indent}s %s\n" "===  oozienode" $oozienode
        [ -n "$kmsnode" ]        && printf "%-${indent}s %s\n" "===  kmsnode" $kmsnode
        [ -n "$zookeepernode" ]  && printf "%-${indent}s %s\n" "===  zookeepernode" $zookeepernode
        [ -n "$hdfsproxynode" ]  && printf "%-${indent}s %s\n" "===  hdfsproxynode" $hdfsproxynode
        [ -n "$hcatservernode" ] && printf "%-${indent}s %s\n" "===  hcatservernode" $hcatservernode
        [ -n "$daqnode" ]        && printf "%-${indent}s %s\n" "===  daqnode" $daqnode
        [ -n "$teznode" ]        && printf "%-${indent}s %s\n" "===  teznode" $teznode

	if [ -n "$yroots" ]; then
            export yroots="$yroots $hitnodes"
	else
            export yroots="$hitnodes"
	fi
	[ -n "$yroots" ]   && printf "%-${indent}s %s\n" "===  yroots" $yroots
	[ -n "$hitnodes" ] && printf "%-${indent}s %s\n" "===  hitnodes" $hitnodes

        # step 2d: Now, set up variables that the deploy-script needs.
	export gridname=$CLUSTER
	case $gridname in
            open*)
                export scriptnames=openstacklargedisk
                export cfgscriptnames=${scriptnames}
                export confpkg=HadoopConfig${scriptnames}
                export localconfpkg=hadooplocalconfigs_openstack_large
                ;;
            dense*)
                export scriptnames=generic10node12disk
                export cfgscriptnames=${scriptnames}blue
                export confpkg=HadoopConfig${scriptnames}blue
                export localnames=12disk
                export localconfpkg=hadooplocalconfigs
                ;;
            *)
                export scriptnames=generic10node
                export cfgscriptnames=${scriptnames}blue
                export confpkg=HadoopConfig${scriptnames}blue
                export localconfpkg=hadooplocalconfigs
                ;;
	esac

	# Just because stack component roles/nodes exist, it shouldn't
	# automatically be added for deployment in the hostlist.
	# They should only be added only if the stack component is being deployed.
	# We should also let users if stack component nodes are being added to the
	# hostlist from these stack component roles such as zookeeper, etc.
	stack_comp_nodes=''
	if [ -n "$GDMVERSION" ]; then
            if [ -n "$daqnode" ]; then
		echo "Adding daq node '$daqnode' to HOSTLIST"
		stack_comp_nodes+="$daqnode "
            fi
	fi
	if [ -n "$HBASEVERSION" ]; then
            if [ -n "$zookeepernodes" ]; then
		echo "Adding zookeeper nodes '$zookeepernodes' to HOSTLIST"
		stack_comp_nodes+="$zookeepernodes "
            fi
	fi
	# HCATVERSION is not being passed in, see TODO in installgrid.sh
	if [ -n "$HCATVERSION" ]; then
            if [ -n "$hcatservernode" ]; then
		echo "Adding hcat server node '$hcatservernode' to HOSTLIST"
		stack_comp_nodes+="$hcatservernode "
            fi
	fi
	# HDFSPROXYVERSION is not being passed in, see TODO in installgrid.sh
	if [ -n "$HDFSPROXYVERSION" ]; then
            if [ -n "$hdfsproxynode" ]; then
		echo "Adding hdfsproxy node '$hdfsproxynode' to HOSTLIST"
		stack_comp_nodes+="$hdfsproxynode "
            fi
	fi
	# OOZIEVERSION is not being passed in, see TODO in installgrid.sh
	if [ -n "$OOZIEVERSION" ]; then
            if [ -n "$oozienode" ]; then
		echo "Adding oozie node '$oozienode' and hive node $hcatservernode to HOSTLIST"
		stack_comp_nodes+="$oozienode $hcatservernode "
            fi
	fi

	# step 2e: Set up file-contents that the deploy-script needs.
	(
	    cat  $all_nn_list
	    echo $jobtrackernode
	    echo $gateway
	    [ -n "$stack_comp_nodes" ] && echo $stack_comp_nodes | tr ' ' '\n' | sed '/^s*$/d'
	    cat $cluster_list
	) | sort | uniq >  $cluster_list_tmp
	mv $cluster_list_tmp $cluster_list

	nmachines=`wc -l $cluster_list | cut -f1 -d' '`
	echo "**** $nmachines node cluster ****"

	# Construct space separated non slave node list to filter out nodes from
	# the host list
	nonslave_nodes="$daqnode $gateway $hcat_server \
                       $hive_client $hs2_masters $hs2_nodes $hs2_slaves \
                       $jobtrackernode $namenode $secondarynamenode \
                       $zookeepernodes "
	# use the hdfsproxy node as a Core worker
	# [ -n "$hdfsproxynode" ] && nonslave_nodes+=" $hdfsproxynode"

	# For Verizon case we need to exclude oozie and hive nodes from running Core
	# workers as long as Oozie is running on rhel6, check if oozienode's OS is
	# rhel7 and if Docker use is disabled (Oath case will also be rhel7 but will
	# enable Docker)
	OOZIE_ROLE_MEMBER_COUNT=`echo $oozienode | tr ' ' '\n' | wc -l`

	OOZIE_HIVE_OS_VER=`$SSH $jobtrackernode "cat /etc/redhat-release | cut -d' ' -f7"`

	if [[ "$OOZIE_HIVE_OS_VER" =~ ^7. ]] && [[ "$RHEL7_DOCKER_DISABLED" =~ "false" ]] && [[ "$IS_INTEGRATION_CLUSTER" =~ "true" ]]; then
            echo "Adding oozie, hdfsproxy and hive nodes to nonslave_nodes in order to only allow rhel7 Core workers because:"
            echo "OOZIE_HIVE_OS_VER is $OOZIE_HIVE_OS_VER, RHEL7_DOCKER_DISABLED is $RHEL7_DOCKER_DISABLED and IS_INTEGRATION_CLUSTER is $IS_INTEGRATION_CLUSTER"
            nonslave_nodes+=" $oozienode $hcatservernode $hdfsproxynode"
	else
            echo "NOT adding oozie, hdfsproxy and hive nodes to nonslave_nodes because:"
            echo "OOZIE_HIVE_OS_VER is $OOZIE_HIVE_OS_VER, RHEL7_DOCKER_DISABLED is $RHEL7_DOCKER_DISABLED and IS_INTEGRATION_CLUSTER is $IS_INTEGRATION_CLUSTER so NOT adding oozie, hdfsproxy and hive nodes to nonslave_nodes"
	fi

	# Construct out pipe separated nodes to filter out from the host list from
	# space separated non slave node list
	# First sed removes spaces from end of the variable
	# Second sed replaces one or more spaces with '|'
	re=`echo $nonslave_nodes|sed 's/ *$//'|sed 's/  */\|/g'`
	echo "re='$re'"

	# Filter out pipe separated nodes in $re from the host list
	egrep -v "$re" < $cluster_list > $worker_list


	# sort -o $worker_list $worker_list
	# step 2f: final dusting-off. Note that we have package-name hard-coded - a last bit of ugliness we should formalize and make go away.
	#
	#
	#
	echo "*** WARNING *** possible incompatibility for large clusters - see scripts - might hit kerberos issues if you are not using generic-10-node. (Nov 2 2010)"
	#
	#  note  -- note -- note -- the config-package that is not $confpkg, via some other default, will need to set two kerberos-values explicitly:
	#          confpkginstalloptions="$confpkginstalloptions -set $confpkg.TODO_KERBEROS_DOMAIN=DEV.YGRID.YAHOO.COM  -set $confpkg.TODO_KERBEROS_ZONE=dev"
	#
	#
	#
	#
	confpkginstalloptions="$confpkginstalloptions -set $confpkg.TODO_KERBEROS_DOMAIN=DEV.YGRID.YAHOO.COM  -set $confpkg.TODO_KERBEROS_ZONE=dev"

	return 0
    else
	echo cluster $CLUSTER does not exist according to rolesdb
	echo "**********"
	echo "**********" Cannot find igor ROLE for: $CLUSTER
	echo "**********"
	echo "**********"
	echo "**********   Look at http://twiki.corp.yahoo.com/view/Grid/HowToAddAClusterToThe022DeployMechanism%28s%29 for examples of creating the roles."
	echo "**********"
	exit 1
    fi
}

