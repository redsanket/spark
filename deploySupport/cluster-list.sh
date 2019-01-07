##
## Here, we start a small test: if there's a ROLE in "roledb" for
## this cluster, assume that the member-list (machine-list) in that
## ROLE is trustworthy. Use it.
##
##

banner() {
    echo "*********************************************************************************"
    echo "***" `TZ=PST8PDT date ; echo // ; TZ=  date `
    echo "***" $*
    echo "*********************************************************************************"
}
banner2() {
    echo "*********************************************************************************"
    echo "***" `TZ=PST8PDT date ; echo // ; TZ=  date `":"
    echo "***" $1
    echo "***" $2
    echo "*********************************************************************************"
}

#
# initRoleList: cache away a list of IGOR roles for later searching.
# "dumpAllRoles.sh" is an API equivalent of
#            igor list -roles 'grid_re.clusters.*'
#

initRoleList()
{
	export base=conf/hadoop/hadoopAutomation
	export roleList=$base/$cluster.rolelist.txt
        # If Hbase is not being installed, remove the hbase related roles which
        # are not needed. Otherwise, if these roles exists but are empty, they
        # will cause the deployment to fail.
        if [ -n "$HBASEVERSION" ]; then
            cat $roleList|grep -v regionserver|grep -v master|grep -v zookeeper > ${roleList}.new
            mv -f ${roleList}.new $roleList
        fi
}

#
# "roleExists  ankh"            -  does grid_re.clusters.ankh exist?
# "roleExists  ankh.namenode"   -  does grid_re.clusters.ankh.namenode exist?
#
# This is meant to be very fast - a grep(1) on a small file, which is really
# the cached output of "dumpAllRoles.sh" (which, itself, is the output of
# the API equivalent of `igor list -roles 'grid_re.clusters.*'`

# input:      $roleList - assumes someone's already set the cache-file name
# argument:   cluster-name or cluster-subrole-name
#
#
roleExists() {
  egrep -q "^grid_re.clusters.$1\$"  $roleList
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
#				$cluster
#				$scriptnames
#				$confpkg
# output:     file contents
#        hostlist.$cluster.txt  - all machines that will be updated with software
#        slaves.$cluster.txt    - that list, with the specialty-machines removed
#                                 (no oozie/namenode (etc)/hdfsproxy/hcatserver/daq)
#
# Note: THE FAILURE CASE EXITS THE PROGRAM.
#

# dumpMembershipList.sh and dumpAllRoles.sh scripts is deprecated after GRIDCI-2332

SSH_OPT="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
SSH="ssh $SSH_OPT"

setGridParameters() {
    export cluster=`echo $1 | tr   A-Z   a-z`

# step 1: make sure that we have a list-of-roles to work from.
    initRoleList

# step 2: pry basic information out of IGOR for this cluster:
    if roleExists $cluster
    then
        # step 2a: the machine-list itself
        echo cluster $cluster exists according to rolesdb

        /usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster},@grid_re.clusters.${cluster}.gateway)" > hostlist.$cluster.txt

        # step 2b: use certain defaults if not provided
        export namenode=`tail -1 hostlist.$cluster.txt`
        export jobtrackernode=`tail -2 hostlist.$cluster.txt | sed -n 1p`
        export secondarynamenode=`tail -3 hostlist.$cluster.txt | sed -n 1p`
        ## export namenodehaalias=`tail -1 hostlist.$cluster.txt`
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
        if roleExists $cluster.namenode
        then
              if roleExists $cluster.namenode2 
              then
                  /usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.namenode)" > nn.$cluster.txt
                  /usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.namenode2)" > sn.$cluster.txt
                  if roleExists $cluster.namenode_alias 
                  then
                    /usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.namenode_alias)" > namenodehaalias.$cluster.txt
                  else
                    touch namenodehaalias.$cluster.txt
                  fi 
                  # Ignore Federation setting when HA is enabled. They can coexist if Federation 
                  # was properly supported in config and deployment. Currently the federation
                  # config is generated during deployment using a script.
                  if [ "$ENABLE_HA" = true ]; then
                      echo Using only one set of namenodes since HA is enabled.
                      head -n 1 nn.$cluster.txt > namenodes.$cluster.txt
                      head -n 1 sn.$cluster.txt > secondarynamenodes.$cluster.txt
                      #head -n 1 nnalias.$cluster.txt > namenodehaalias.$cluster.txt
                      rm -f nn.$cluster.txt sn.$cluster.txt
                  else
                      mv nn.$cluster.txt namenodes.$cluster.txt
                      mv sn.$cluster.txt secondarynamenodes.$cluster.txt
                  fi
                  export namenode=`cat  namenodes.$cluster.txt`
                  export secondarynamenode=`cat secondarynamenodes.$cluster.txt`
                  export namenodehaalias=`cat namenodehaalias.$cluster.txt`
                  echo "DEBUG: namenodehaalias is: `cat namenodehaalias.$cluster.txt`"
              else
                  echo "Config error: namenode for $cluster exists, but no .namenode2 role." && \
                  exit 1
              fi
        else
              echo $namenode > namenodes.$cluster.txt
              echo $secondarynamenode > secondarynamenodes.$cluster.txt
        fi
        cat secondarynamenodes.$cluster.txt namenodes.$cluster.txt \
                                    > allnamenodes.$cluster.txt

        if roleExists $cluster.regionserver
        then
            /usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.regionserver)" > regionservernodes.$cluster.txt
        fi

        if roleExists $cluster.master
        then
            /usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.master)" > hbasemasternodes.$cluster.txt
        fi

        if roleExists $cluster.zookeeper
        then
            /usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.zookeeper)" > hbasezookeepernodes.$cluster.txt
        fi


        roleExists $cluster.jobtracker && \
             export jobtrackernode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.jobtracker)"`
        roleExists $cluster.gateway && \
             export gateway=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.gateway)"`
        roleExists $cluster.hdfsproxy && \
             export hdfsproxynode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.hdfsproxy)"`
        roleExists $cluster.hcat&& \
             export hcatservernode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.hcat)"`
        roleExists $cluster.hive&& \
             export hcatservernode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.hive)"`
        roleExists $cluster.daq&& \
             export daqnode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.daq)"`
        roleExists $cluster.oozie && \
             export oozienode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.oozie)"`
	if roleExists $cluster; then
            roles="@grid_re.clusters.${cluster},@grid_re.clusters.${cluster}.gateway"
            roleExists $cluster.hive  && roles+=",@grid_re.clusters.${cluster}.hive"
            roleExists $cluster.oozie && roles+=",@grid_re.clusters.${cluster}.oozie"
            # teznode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster},@grid_re.clusters.${cluster}.gateway,@grid_re.clusters.${cluster}.hive,@grid_re.clusters.${cluster}.oozie)"`
            teznode=`/usr/local/bin/yinst range -ir "(${roles})"`
            if [[ -n $gateway ]]; then
                teznode+="\n$gateway"
            fi
            teznode=`echo -e "$teznode"|sort -u`
            export teznode=$teznode
	fi
        roleExists $cluster.yroots && \
             export yroots=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.yroots)"`
        roleExists $cluster.gateways && \
             export gateways=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.gateways)"`
        roleExists $cluster.kms&& \
             # KMS node also hosts ZK service on flubber
             # only support single KMS node in Flubber
             export kmsnode=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.kms)" | head -n 1`
        roleExists $cluster.zookeeper && \
             export zookeepernodes=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.zookeeper)"`
        roleExists $cluster.hit-yroots && \
             export hitnodes=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.hit-yroots)"`
        roleExists $cluster.hs2 && \
             export hs2_nodes=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.hs2)"`
        roleExists $cluster.server && \
             export hcat_server=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.server)"`
        roleExists $cluster.jdbc && \
             export jdbc_client=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.jdbc)"`
        roleExists $cluster.hs2.masters && \
             export hs2_masters=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.hs2_masters)"`
        roleExists $cluster.hs2.slaves && \
             export hs2_slaves=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.hs2_slaves)"`
        roleExists $cluster.client && \
             export hive_client=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.client)"`
        roleExists $cluster.database && \
             export hive_mysql=`/usr/local/bin/yinst range -ir "(@grid_re.clusters.${cluster}.database)"`

       echo "=============INSTALL_GW_IN_YROOT=$INSTALL_GW_IN_YROOT ============"
       if [ "$INSTALL_GW_IN_YROOT" == "true" ]
       then
            if [ -n "$gateways" ]
            then
                echo " gateways/yroot info already defined in igor"
                echo " ===== gateways: $gateways"
            else
                export gateways=$gateway":"$cluster
                echo " ===== gateways: $gateways"
            fi
       fi

       
       echo "namenode='$namenode'"
       echo "secondarynamenode='$secondarynamenode'"
       echo "namenodehaalias='$namenodehaalias'"
       echo "jobtrackernode='$jobtrackernode'"
       echo "gateway='$gateway'"
       [ -n "$oozienode" ] && echo "oozienode='$oozienode'"
       [ -n "$teznode" ] && echo "teznode='$teznode'"
       [ -n "$kmsnode" ] && echo "kmsnode='$kmsnode'"
       [ -n "$zookeepernodes" ] && echo "zookeepernodes='$zookeepernodes'"
       [ -n "$hdfsproxynode" ] && echo "hdfsproxynode='$hdfsproxynode'"
       [ -n "$hcatservernode" ] && echo "hcatservernode='$hcatservernode'"
       [ -n "$daqnode" ] && echo "daqnode='$daqnode'"
       
       if [ -n "$yroots" ]; then
          export yroots="$yroots $hitnodes"
       else
          export yroots="$hitnodes"
       fi
       [ -n "$yroots" ] && echo "yroots='$yroots'"
       [ -n "$hitnodes" ] && echo "hitnodes='$hitnodes'"
      
        # step 2d: Now, set up variables that the deploy-script needs.
       export gridname=$cluster
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
               echo "NOT NOT oozie node '$oozienode' and hive node $hcatservernode to HOSTLIST"
               stack_comp_nodes+="$oozienode $hcatservernode "
           fi
       fi
       # step 2e: Set up file-contents that the deploy-script needs.
       (
			cat  allnamenodes.$cluster.txt
			echo $jobtrackernode 
			echo $gateway 
			[ -n "$stack_comp_nodes" ] && echo $stack_comp_nodes | tr ' ' '\n' | sed '/^s*$/d'
			cat hostlist.$cluster.txt
       ) | sort | uniq >  hostlist.$cluster.txt.1
       mv hostlist.$cluster.txt.1 hostlist.$cluster.txt
       
       nmachines=`wc -l hostlist.$cluster.txt | cut -f1 -d' '`
       echo "**** " ; echo "**** $nmachines node cluster" ; echo "**** "

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

       if [[ "$OOZIE_HIVE_OS_VER" =~ ^7. ]] && [[ "$RHEL7_DOCKER_DISABLED" =~ "false" ]]; then
           echo "INFO: OOZIE_HIVE_OS_VER is $OOZIE_HIVE_OS_VER and RHEL7_DOCKER_DISABLED is $RHEL7_DOCKER_DISABLED so adding oozie, hdfsproxy and hive nodes to nonslave_nodes in order to only allow rhel7 Core workers"
           nonslave_nodes+=" $oozienode $hcatservernode $hdfsproxynode"
       elsif [[ "IS_INTEGRATION_CLUSTER" =~ "false" ]]; then
           echo "INFO: IS_INTEGRATION_CLUSTER is $IS_INTEGRATION_CLUSTER, NOT adding oozie, hdfsproxy and hive nodes to nonslave_nodes so they can be hadoop workers"
       else
           echo "INFO: OOZIE_HIVE_OS_VER is $OOZIE_HIVE_OS_VER and RHEL7_DOCKER_DISABLED is $RHEL7_DOCKER_DISABLED so NOT adding oozie, hdfsproxy and hive nodes to nonslave_nodes"
       fi

       # Construct out pipe separated nodes to filter out from the host list from
       # space separated non slave node list
       # First sed removes spaces from end of the variable
       # Second sed replaces one or more spaces with '|'
       re=`echo $nonslave_nodes|sed 's/ *$//'|sed 's/  */\|/g'`
       echo "re='$re'"

       # Filter out pipe separated nodes in $re from the host list
       egrep -v "$re" < hostlist.$cluster.txt > slaves.$cluster.txt


       # sort -o slaves.$cluster.txt slaves.$cluster.txt
       # step 2f: final dusting-off. Note that we have package-name hard-coded - a last bit of ugliness we should formalize and make go away.
#
#
#
       echo "** ** ** possible incompatibility for large clusters - see scripts - might hit kerberos issues if you are not using generic-10-node. (Nov 2 2010)"
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
       echo cluster $cluster does not exist according to rolesdb
       echo "**********"
       echo "**********" Cannot find igor ROLE for: $cluster
       echo "**********"
       echo "**********"
       echo "**********   Look at http://twiki.corp.yahoo.com/view/Grid/HowToAddAClusterToThe022DeployMechanism%28s%29 for examples of creating the roles."
       echo "**********"
       exit 1
   fi
}

