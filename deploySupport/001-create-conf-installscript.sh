set -x

t=/grid/0/tmp/deploy.$cluster.hdfsinfo
[ -e $t ] && rm -rf  $t
mkdir -p $t

cp namenodes.$cluster.txt /grid/0/tmp/
cp secondarynamenodes.$cluster.txt /grid/0/tmp/
cp namenodehaalias.$cluster.txt /grid/0/tmp/
cp ${base}/docker_fetch_image.py    /grid/0/tmp/
cp ${base}/processNameNodeEntries.py    /grid/0/tmp/
(
    # echo "scp  $ADMIN_HOST:/grid/0/tmp/processNameNodeEntries.py  /tmp/ "
    # echo "scp  $ADMIN_HOST:/grid/0/tmp/namenodes.$cluster.txt  /tmp/ "
    # echo "scp  $ADMIN_HOST:/grid/0/tmp/secondarynamenodes.$cluster.txt  /tmp/ "
    # echo "scp  $ADMIN_HOST:/tmp/namenodehaalias.$cluster.txt  /tmp/ "
    # echo "scp  $ADMIN_HOST:/grid/0/tmp/processNameNodeEntries.py  /tmp/ "
    echo 'if grep -q `hostname`  ' /tmp/namenodes.$cluster.txt 
    echo 'then'
    echo '	export  nn=`hostname`'
    echo 'else'
    echo '	export  nn=`head -1 '  /tmp/namenodes.$cluster.txt  '`'
    echo 'fi'
    echo 'if grep -q `hostname`  ' /tmp/secondarynamenodes.$cluster.txt
    echo 'then'
    echo '      export  nn2=`hostname`'
    echo 'else'
    echo '      export  nn2=`head -1 '  /tmp/secondarynamenodes.$cluster.txt  '`'
    echo 'fi'
    echo 'if grep -q `hostname`  ' /tmp/namenodehaalias.$cluster.txt
    echo 'then'
    echo '      export  nnalias=`hostname`'
    echo 'else'
    echo '      export  nnalias=`head -1 '  /tmp/namenodehaalias.$cluster.txt  '`'
    echo 'fi'
    # # # # # 3 January 2011: need to add variable for Secondary NN, above,
    # # # # # in order to set yinst-vars for secondary also. Otherwise,
    # # # # # there is no kerberos login file set for them and the
    # # # # # secondary NN will not start.
    # added empty fallback to include because this causes AM container launch failures on rhel7, since
    # this execs in docker container where this path does not exist
    if [ "$ENABLE_HA" = true ]; then
        echo export namenodeXML="'<xi:include href=\"${yroothome}/conf/hadoop/hdfs-ha.xml\" />'"
    else
        echo python /tmp/processNameNodeEntries.py -o /tmp/${cluster}.namenodeconfigs.xml   -1 /tmp/namenodes.$cluster.txt -2 /tmp/secondarynamenodes.$cluster.txt

        #
        # based on docker containers being used, use correct include directive 
        # rhel6 and rhel7 without docker (verizon) don't need the nn xml conf with fallback
        #

        echo 'OS_VER=`cat /etc/redhat-release | cut -d" " -f7` '
        echo 'if [[ "$OS_VER" =~ ^7. ]]; then '
            echo 'echo INFO: NN include OS is $OS_VER '
            echo export namenodeXML="'<xi:include href=\"${yroothome}/conf/hadoop/${cluster}.namenodeconfigs.xml\"><xi:fallback></xi:fallback></xi:include>'"

        echo 'elif [[ "$OS_VER" =~ ^6. ]]; then '
            echo 'echo NN include OS is $OS_VER '
            #PHWTEST  echo export namenodeXML="'<xi:include href=\"${yroothome}/conf/hadoop/${cluster}.namenodeconfigs.xml\"/>'"
            echo export namenodeXML="'<xi:include href=\"${yroothome}/conf/hadoop/${cluster}.namenodeconfigs.xml\"><xi:fallback></xi:fallback></xi:include>'"

        echo 'else '
            echo 'echo WARN: Unknown NN include OS $OS_VER! '
        echo 'fi '

    fi
    echo echo ====
    echo echo ====

    echo 'export shortname=`echo  $nn | cut -f1 -d.` '
    echo 'export shortnamenn2=`echo  $nn2 | cut -f1 -d.` '
    echo 'export nnaliasshortname=`echo  $nnalias | cut -f1 -d.` '
    shortjt=`expr  $jobtrackernode : '(' '\([^\.]*\)\..*$' ')'`

    clusternameopts="  -set $confpkg.TODO_HDFSCLUSTER_NAME=$cluster -set $confpkg.TODO_MAPREDCLUSTER_NAME=$cluster"


    echo "[ -x /usr/local/bin/yinst ] && export yinst=/usr/local/bin/yinst "
    echo "[ -x /usr/y/bin/yinst ] && export yinst=/usr/y/bin/yinst "
    echo 'if [ -n "$TARGET_YROOT" ] '
    echo 'then'
    echo '      export rootdirparam="-yroot $TARGET_YROOT   '  -set $confpkg.TODO_RUNMKDIRS=false   \"
    echo 'else'
    echo "   export rootdirparam=\"-root ${yroothome}  \"  "
    echo 'fi'
    # echo "echo ======= installing config-package using '$HADOOP_CONFIG_INSTALL_STRING'"
    echo GSHOME=$GSHOME $yinst install -downgrade -yes \$rootdirparam -br quarantine  \\
    echo "  " -set $confpkg.TODO_GSHOME=/home/gs \\
    echo "  " -set $confpkg.TODO_RMNODE_HOSTNAME=$jobtrackernode \\
    echo "  " -set $confpkg.TODO_NAMENODE_HOSTNAME=\$nn \\
    echo "  " -set $confpkg.TODO_DFS_WEB_KEYTAB=\$shortname.dev.service.keytab \\
    echo "  " -set $confpkg.TODO_SECONDARYNAMENODE_HOSTNAME=\$nn2 \\
    echo "  " -set $confpkg.TODO_MAPREDUSER=$MAPREDUSER \\
    echo "  " -set $confpkg.TODO_HDFSUSER=$HDFSUSER \\
    echo "  " -set $confpkg.TODO_RMNODE_SHORTHOSTNAME=$shortjt \\
    echo "  " -set $confpkg.TODO_NAMENODE_SHORTHOSTNAME=\$shortname \\
    echo "  " -set $confpkg.TODO_SECONDARYNAMENODE_SHORTHOSTNAME=\$shortnamenn2 \\
    echo "  " -set $confpkg.TODO_NAMENODE_EXTRAPROPS=\"\$namenodeXML\" \\
    echo "  " -set $confpkg.TODO_KERBEROS_ZONE=dev \\
    echo "  " -set $confpkg.TODO_YARN_LOG_DIR=/home/gs/var/log/mapredqa \\
    echo "  " -set $confpkg.TODO_COLOR=open \\
    echo "  " -set $confpkg.TODO_YGRID_CLUSTER=$cluster \\
    if [ "$ENABLE_HA" = true ]; then
        echo "  " -set $confpkg.TODO_DFS_HA_LOGICAL_NAME=\$nnaliasshortname \\
        echo "  " -set $confpkg.TODO_DFS_HA_SHARED_EDITS_DIR=file://$HOMEDIR/$HDFSUSER/ha_namedir/${cluster}_\$shortname/edits \\
        echo "  " -set $confpkg.TODO_DFS_HA_NAME_DIR_ANN=\"file:///grid/2/hadoop/var/hdfs/name,file://$HOMEDIR/$HDFSUSER/ha_namedir/${cluster}_\$shortname/name1\" \\
        echo "  " -set $confpkg.TODO_DFS_HA_NAME_DIR_SBN=\"file:///grid/2/hadoop/var/hdfs/name,file://$HOMEDIR/$HDFSUSER/ha_namedir/${cluster}_\$shortname/name2\" \\
        echo "  " -set $confpkg.TODO_DFS_HA_EDITS_DIR_ANN=\"file:///grid/2/hadoop/var/hdfs/name\" \\
        echo "  " -set $confpkg.TODO_DFS_HA_EDITS_DIR_SBN=\"file:///grid/2/hadoop/var/hdfs/name\" \\
        echo "  " -set $confpkg.TODO_FIRST_HANN_HOSTNAME=\$nn \\
        echo "  " -set $confpkg.TODO_SECOND_HANN_HOSTNAME=\$nn2 \\
        echo "  " -set $confpkg.TODO_FIRST_HANN_SHORTNAME=\$shortname \\
        echo "  " -set $confpkg.TODO_SECOND_HANN_SHORTNAME=\$shortnamenn2 \\
        #echo "    -set $confpkg.TODO_ENABLE_DFS_HA='<xi:include href=\"${yroothome}/conf/hadoop/hdfs-ha.xml\"/>' \\"
        echo "  " -set $confpkg.TODO_DFS_WEB_PRINCIPAL_INSTANCE=\$nnalias \\
    fi

    # The following is kept here to make old config work
    # REVERTING GRIDCI-549 as Hadoop 2.x & 3.x both use 8020 default port
    # It was defaulted to 8020 in Hadoop 2.x but in 3.x it was using 9820
    # GRIDCI-549 - defaultFS URL should not define the port number explicitly as
    # the value may change in the hadoop configuration. If it is not specified,
    # haddop fs will use the appropriate port number that's configured
    # internally. Also, depending on the protocol hdfs or webhdfs, the port
    # number may be different.
    if [ "$ENABLE_HA" = true ]; then
        echo "  " -set $confpkg.TODO_DFS_DEFAULT_FS=\$nnalias \\
        # not finding and active use of this var, disable for now
        # original work is in bugzilla at:
        #     http://bug.corp.yahoo.com/show_bug.cgi?id=6941236
        #     http://bug.corp.yahoo.com/show_bug.cgi?id=6941110
        # echo "  " -set $confpkg.TODO_QE_CLUSTER_NN_ADDR=\$nnalias \\
    else
       case "$HADOOPVERSION" in
       2.[0-3])
           echo "  " -set $confpkg.TODO_DFS_DEFAULT_FS=\$nn \\
           ;;
       2.[4-9])
           echo "  " -set $confpkg.TODO_DFS_DEFAULT_FS=hdfs://\$nn \\
           ;;
       3.[0-9])
	   echo "  " -set $confpkg.TODO_DFS_DEFAULT_FS=hdfs://\$nn \\
	   ;;
       *)
           echo "Invalid Hadoop version $HADOOPVERSION"
           exit 1
           ;;
       esac
    fi


    if [ "$USE_DEFAULT_QUEUE_CONFIG" = true ]; then
        echo "  " -set $confpkg.TODO_YARN_LOCAL_CAPACITY_SCHEDULER=/home/gs/gridre/yroot.${cluster}/conf/hadoop/EXAMPLE-local-capacity-scheduler.xml \\
    fi
    if [ "$HBASE_SHORTCIRCUIT" = true ] || [ "$HBASEVERSION" != "none" ]; then
        echo "  " -set $confpkg.TODO_DFS_CLIENT_READ_SHORTCIRCUIT=true \\
        echo "  " -set $confpkg.TODO_DFS_CLIENT_USE_LEGACY_BLOCKREADER_LOCAL=true \\
    fi
    echo "  " \$clusternameopts  \\
    echo "  " $HADOOP_CONFIG_INSTALL_STRING
    echo $yinst set \$rootdirparam  $confpkg.TODO_NAMENODE_EXTRAPROPS=\"\$namenodeXML\"
    echo cp  /tmp/${cluster}.namenodeconfigs.xml  ${yroothome}/conf/hadoop/

    # create the symlink for the bouncer public key
    echo "rm -f /home/gs/conf/bycookieauth"
    echo "ln -s /home/gs/gridre/yroot.${cluster}/conf/bycookieauth /home/gs/conf/bycookieauth"

    # Disable use of docker on rhel7 deployments
    # if docker use is disabled by checking this flag (enabled by default), change core conf to not use 
    # docker (Verizon rhel7 native). Requires Jenkins deploy job to have RHEL7_DOCKER_DISABLED setting!
    # This setting has no effect on rhel6 nodes/deploys.
    if [ "$RHEL7_DOCKER_DISABLED" = true ]
    then
        echo "echo ======= running yinst-set to disable Docker use and run tasks native, this has no effect on rhel6 nodes"
        echo "$yinst set -root ${yroothome} \\"
        echo "    $confpkg.TODO_YARN_NODEMANAGER_RUNTIME_LINUX_ALLOWED_RUNTIMES=default \\"
    else
        if [[ ${DOCKER_IMAGE_TAG_TO_USE} == 'rhel7' || ${DOCKER_IMAGE_TAG_TO_USE} == 'rhel6' ]]; then
            echo "get latest ${DOCKER_IMAGE_TAG_TO_USE}:current tag image"
            echo "$yinst set -root ${yroothome} \\"
            echo "    $confpkg.TODO_YARN_NODEMANAGER_RUNTIME_LINUX_DOCKER_IMAGE_NAME=docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/${DOCKER_IMAGE_TAG_TO_USE} \\"
            echo "    $confpkg.TODO_YARN_NODEMANAGER_RUNTIME_LINUX_DOCKER_ALLOWED_IMAGES=docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/${DOCKER_IMAGE_TAG_TO_USE} \\"
        else
            echo "get specific ${DOCKER_IMAGE_TAG_TO_USE} tag image"
            echo "$yinst set -root ${yroothome} \\"
            echo "    $confpkg.TODO_YARN_NODEMANAGER_RUNTIME_LINUX_DOCKER_IMAGE_NAME=docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/${DOCKER_IMAGE_TAG_TO_USE} \\"
            echo "    $confpkg.TODO_YARN_NODEMANAGER_RUNTIME_LINUX_DOCKER_ALLOWED_IMAGES=docker-registry.ops.yahoo.com:4443/hadoop/docker_configs/${DOCKER_IMAGE_TAG_TO_USE} \\"
        fi
    fi

    if [ "$HERRIOT_CONF_ENABLED" = true ]
    then
        echo "echo ======= running yinst-set to set Herriot config properties."
        echo "$yinst set -root ${yroothome} \\"
        echo "    $confpkg.TODO_ADMINPERMISSIONSGROUP=gridadmin,users \\"
        echo "    $confpkg.TODO_MAPRED_SITE_SPARE_PROPERTIES=' <property> <name>mapred.task.tracker.report.address</name> <!-- cluster variant --> <value>0.0.0.0:50030</value> <description>RPC connection from Herriot tests to a tasktracker</description> <final>true</final> </property>' \\"
        echo "    $confpkg.TODO_HADOOP_CONFIG_CONFIGLINE='<configuration xmlns:xi=\"http://www.w3.org/2001/XInclude\">    <xi:include href=\"${yroothome}/conf/hadoop/hadoop-policy-system-test.xml\"/>' "
    fi
) >  /grid/0/tmp/deploy.$cluster.confoptions.sh
