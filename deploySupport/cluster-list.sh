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

export roleList=undefined
base=conf/hadoop/hadoopAutomation
initRoleList()
{
            if [ ! -e $roleList  -a  -x $base/dumpAllRoles.sh ] 
            then
            	echo  Creating $roleList...
                 $base/dumpAllRoles.sh  > $roleList
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
        initRoleList
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
#                               $teznode
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

export roleList=/tmp/$cluster.rolelist.txt
setroleList() {
	export roleList=$1
        if [[ -e $roleList ]]; then
            rm -f $roleList
        fi
	}
	
setGridParameters() {
    export cluster=`echo $1 | tr   A-Z   a-z`
	
	setroleList    /tmp/$cluster.rolelist.txt

# step 1: make sure that we have a list-of-roles to work from.
    initRoleList

# step 2: pry basic information out of IGOR for this cluster:
    if roleExists $cluster
    then
        # step 2a: the machine-list itself
        echo cluster $cluster exists according to rolesdb
        $base/dumpMembershipList.sh  $cluster > hostlist.$cluster.txt

        # step 2b: use certain defaults if not provided in IGOR
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
                  $base/dumpMembershipList.sh $cluster.namenode   | sort | uniq > nn.$cluster.txt
                  $base/dumpMembershipList.sh $cluster.namenode2  | sort | uniq > sn.$cluster.txt
                  $base/dumpMembershipList.sh $cluster.namenode_alias  | sort | uniq > namenodehaalias.$cluster.txt
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
            $base/dumpMembershipList.sh $cluster.regionserver | sort | uniq > regionservernodes.$cluster.txt
        fi

        if roleExists $cluster.master
        then
            $base/dumpMembershipList.sh $cluster.master | sort | uniq > hbasemasternodes.$cluster.txt
        fi

        if roleExists $cluster.zookeeper
        then
            $base/dumpMembershipList.sh $cluster.zookeeper | sort | uniq > hbasezookeepernodes.$cluster.txt
        fi


        roleExists $cluster.jobtracker && \
             export jobtrackernode=`$base/dumpMembershipList.sh  $cluster.jobtracker`
        roleExists $cluster.gateway && \
             export gateway=`$base/dumpMembershipList.sh  $cluster.gateway`
        roleExists $cluster.hdfsproxy && \
             export hdfsproxynode=`$base/dumpMembershipList.sh  $cluster.hdfsproxy`
        roleExists $cluster.hcat&& \
             export hcatservernode=`$base/dumpMembershipList.sh  $cluster.hcat`
        roleExists $cluster.daq&& \
             export daqnode=`$base/dumpMembershipList.sh  $cluster.daq`
        roleExists $cluster.oozie && \
             export oozienode=`$base/dumpMembershipList.sh  $cluster.oozie`
	if roleExists $cluster; then
            teznode=`$base/dumpMembershipList.sh $cluster`
            if [[ -n gateway ]]; then
                teznode+="\n$gateway"
            fi
            teznode=`echo -e "$teznode"|sort -u`
            export teznode=$teznode
	fi
        roleExists $cluster.yroots && \
             export yroots=`$base/dumpMembershipList.sh  $cluster.yroots`
        roleExists $cluster.gateways && \
             export gateways=`$base/dumpMembershipList.sh  $cluster.gateways`
        roleExists $cluster.zookeeper && \
             export zookeepernodes=`$base/dumpMembershipList.sh  $cluster.zookeeper`
        roleExists $cluster.hit-yroots && \
             export hitnodes=`$base/dumpMembershipList.sh  $cluster.hit-yroots`
        roleExists $cluster.hs2 && \
             export hs2_nodes=`$base/dumpMembershipList.sh  $cluster.hs2`
        roleExists $cluster.server && \
             export hcat_server=`$base/dumpMembershipList.sh  $cluster.server`
        roleExists $cluster.jdbc && \
             export jdbc_client=`$base/dumpMembershipList.sh  $cluster.jdbc`
        roleExists $cluster.hs2.masters && \
             export hs2_masters=`$base/dumpMembershipList.sh  $cluster.hs2_masters`
        roleExists $cluster.hs2.slaves && \
             export hs2_slaves=`$base/dumpMembershipList.sh  $cluster.hs2_slaves`
        roleExists $cluster.client && \
             export hive_client=`$base/dumpMembershipList.sh  $cluster.client`
        roleExists $cluster.database && \
             export hive_mysql=`$base/dumpMembershipList.sh  $cluster.database`

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
                    dense*)
                       export scriptnames=generic10node12disk
                       export localnames=12disk
                    ;;
                    *) export scriptnames=openstacklargedisk ;;
       esac
       export confpkg=HadoopConfig${scriptnames}
       export localconfpkg=hadooplocalconfigs_openstack_large

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
       if [ -n "$HCATVERSION" ]; then
           if [ -n "$hcatservernode" ]; then
               echo "Adding hcat server node '$hcatservernode' to HOSTLIST"
               stack_comp_nodes+="$hcatservernode "
           fi
       fi
       if [ -n "$HDFSPROXYVERSION" ]; then
           if [ -n "$hdfsproxynode" ]; then
               echo "Adding hdfsproxy node '$hdfsproxynode' to HOSTLIST"
               stack_comp_nodes+="$hdfsproxynode "
           fi
       fi
       if [ -n "$OOZIEVERSION" ]; then
           if [ -n "$oozienode" ]; then
               echo "Adding oozie node '$oozienode' to HOSTLIST"
               stack_comp_nodes+="$oozienode "
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
       nonslave_nodes="$daqnode $gateway $hcat_server $hcatservernode \
                       $hive_client $hs2_masters $hs2_nodes $hs2_slaves \
                       $jobtrackernode $namenode $secondarynamenode \
                       $zookeepernodes"
       [ -n "$hdfsproxynode" ] && nonslave_nodes+=" $hdfsproxynode"

       # hadooppf-8086, request to not run DN and NM on oozie nodes, so reverting
       # for oozie for two cases will happen, IntTest install or component install,
       # in both cases we exclude the oozie node(s), can have multiple members
       # for component install, need to convert spaces to | for correct exclusion
       #
       # 'if' check needs to deal with two cases for excluding oozienode, if integration
       # component install is selected (STACK_COMP_INSTALL_OOZIE is true) or if oozie
       # component is using the cluster (oozie role will have multiple members), then
       # we exclude these nodes from installing core processes, otherwise use these nodes
       # for core workers. Info, the oozie role will always have at least one member, in
       # first case, there is one member which is needed by the Config job to gen kerb
       # keytabs for oozie. In the second case, Oozie team needs to be able to set their
       # own oozie members and there will be multiple members, so we can't just see if
       # role is empty, we need to count members to know the downstream usage. 
       OOZIE_ROLE_MEMBER_COUNT=`echo $oozienode | tr ' ' '\n' | wc -l`
       if [ "$STACK_COMP_INSTALL_OOZIE" == true ] || [ $OOZIE_ROLE_MEMBER_COUNT -gt 1 ]; then
           nonslave_nodes+=" $oozienode"
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
