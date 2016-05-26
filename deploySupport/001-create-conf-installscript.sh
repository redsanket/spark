t=/grid/0/tmp/deploy.$cluster.hdfsinfo
[ -e $t ] && rm -rf  $t
mkdir -p $t

cp namenodes.$cluster.txt /grid/0/tmp/
cp secondarynamenodes.$cluster.txt /grid/0/tmp/
cp namenodehaalias.$cluster.txt /grid/0/tmp/
cp ${base}/processNameNodeEntries.py    /grid/0/tmp/
(
    # echo "scp  $ADMIN_HOST:/grid/0/tmp/processNameNodeEntries.py  /tmp/ "
    # echo "scp  $ADMIN_HOST:/grid/0/tmp/namenodes.$cluster.txt  /tmp/ "
    # echo "scp  $ADMIN_HOST:/grid/0/tmp/secondarynamenodes.$cluster.txt  /tmp/ "
    # echo "scp  $ADMIN_HOST::tmp/namenodehaalias.$cluster.txt  /tmp/ "
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
    if [ "$ENABLE_HA" = true ]; then
        echo export namenodeXML="'<xi:include href=\"${yroothome}/conf/hadoop/hdfs-ha.xml\" />'"
    else
        echo python /tmp/processNameNodeEntries.py -o /tmp/${cluster}.namenodeconfigs.xml   -1 /tmp/namenodes.$cluster.txt -2 /tmp/secondarynamenodes.$cluster.txt
        echo export namenodeXML="'<xi:include href=\"${yroothome}/conf/hadoop/${cluster}.namenodeconfigs.xml\"/>'"
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
    echo GSHOME=$GSHOME $yinst install -downgrade -yes \$rootdirparam  \\
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
    # GRIDCI-549 - defaultFS URL should not define the port number explicitly as
    # the value may change in the hadoop configuration. If it is not specified,
    # haddop fs will use the appropriate port number that's configured
    # internally. Also, depending on the protocol hdfs or webhdfs, the port
    # number may be different.
    if [ "$ENABLE_HA" = true ]; then
        echo "  " -set $confpkg.TODO_DFS_DEFAULT_FS=\$nnalias \\
        echo "  " -set $confpkg.TODO_QE_CLUSTER_NN_ADDR=\$nnalias \\
    else
      case "$HADOOPVERSION" in
      2.[0-3])
          echo "  " -set $confpkg.TODO_DFS_DEFAULT_FS=\$nn \\
          ;;
      2.[4-9])
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

    if [ "$HERRIOT_CONF_ENABLED" = true ]
    then
	echo "echo ======= running yinst-set to set Herriot config properties."
        echo "$yinst set -root ${yroothome} \\"
	echo "    $confpkg.TODO_ADMINPERMISSIONSGROUP=gridadmin,users \\"
	echo "    $confpkg.TODO_MAPRED_SITE_SPARE_PROPERTIES=' <property> <name>mapred.task.tracker.report.address</name> <!-- cluster variant --> <value>0.0.0.0:50030</value> <description>RPC connection from Herriot tests to a tasktracker</description> <final>true</final> </property>' \\"
	echo "     $confpkg.TODO_HADOOP_CONFIG_CONFIGLINE='<configuration xmlns:xi=\"http://www.w3.org/2001/XInclude\">    <xi:include href=\"${yroothome}/conf/hadoop/hadoop-policy-system-test.xml\"/>' "
    fi
) >  /grid/0/tmp/deploy.$cluster.confoptions.sh
