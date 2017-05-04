
if [ "$INSTALLNEWPACKAGES" = true ]
then
    echo == installing YINST packages.

    # For spark integration tests we want to deploy packages from the quarantine branch.
    # We separate out spark shuffle jar to explicitly specify the branch is spark is selected.
    spark_shuffle_cmd=""
    if [ "$SPARK_SHUFFLE_VERSION" != "none" ]; then
        HADOOP_INSTALL_STRING=`echo $HADOOP_INSTALL_STRING | sed "s/yspark_yarn_shuffle-$SPARK_SHUFFLE_VERSION//g"`
        spark_shuffle_cmd="$yinst install -yes -os rhel-6.x -root ${yroothome} yspark_yarn_shuffle-$SPARK_SHUFFLE_VERSION -br quarantine -same -live -downgrade"
    fi

    cmd="$yinst install -yes -os rhel-6.x -root ${yroothome}  $HADOOP_INSTALL_STRING -same -live -downgrade"
    
    slownogwfanout "/usr/bin/yum -y install openssl098e.x86_64 lzo lzo.i686 lzo.x86_64 compat-readline5.x86_64"
    slownogwfanout "$cmd"
    [[ "$SPARK_SHUFFLE_VERSION" != "none" ]] && slownogwfanout "$spark_shuffle_cmd"
    fanoutGW "/usr/bin/yum makecache"
    fanoutGW "/usr/bin/yum -y install lzo lzo.i686 lzo.x86_64 openssl098e.x86_64 compat-readline5.x86_64"
    fanoutGW "$cmd"
    [[ "$SPARK_SHUFFLE_VERSION" != "none" ]] && fanoutGW "$spark_shuffle_cmd"

    # GRIDCI-501
    # fanoutGW "$yinst set yjava_jdk.JAVA_HOME=/home/gs/java/jdk64/current"
    # fanoutGW "$yinst set yjava_vmwrapper.JAVACMD=/home/gs/java/jdk64/current/bin/java"

    # Because we create gateways from new virtual hosts
#    fanoutGW "$yinst install yhudson_slave"
#    fanoutGW "mkdir -p /home/y/var/builds"

#
# At this point, the packages are installed - except the configs.
#
#    f=YahooDNSToSwitchMapping-0.2.1111040716.jar
#    f=YahooDNSToSwitchMapping-0.22.0.1011272126.jar

    fanoutcmd "scp /grid/0/tmp/deploy.$cluster.confoptions.sh /grid/0/tmp/processNameNodeEntries.py /grid/0/tmp/namenodes.$cluster.txt /grid/0/tmp/secondarynamenodes.$cluster.txt /grid/0/tmp/processNameNodeEntries.py __HOSTNAME__:/tmp/" "$HOSTLIST"
    cmd="GSHOME=$GSHOME yroothome=$yroothome sh /tmp/deploy.$cluster.confoptions.sh && cp /tmp/deploy.$cluster.confoptions.sh  ${yroothome}/conf/hadoop/ "

#    echo ====== install workaround to get $f copied: Dec 22 2010 ;  \
#    [ -f ${yroothome}/share/hadoop/share/hadoop/hdfs/lib/$f ] || scp $ADMIN_HOST:/grid/0/tmp/$f  ${yroothome}/share/hadoop/share/hadoop/hdfs/lib/$f  "
    fanout "$cmd"
    fanoutGW "$cmd"

   # install addtional QA packages if there is any
   if [ "$QA_PACKAGES" != "none" ]
   then
        echo "====Install additional QA packages: $QA_PACKAGES"
        slowfanout "$yinst install -yes -os rhel-6.x -root ${yroothome}  $QA_PACKAGES -same -live"
        #fanoutGW "$yinst install -yes -root ${yroothome}  $QA_PACKAGES -same -live"
   fi
echo ......
echo ...... to run an exact imitation of this hadoop-config-install,
echo ...... run deploy.$cluster.confoptions.sh, which is in the config-dir.
echo ......
echo ......
fi

# make sure the permission on var and var/run is correct. the cfg-datanode-mkdirs.sh in old config packates have a bug.
#
# Need to remove the trailing ';' in the last 'fi', this causes pdsh -S to fail with:
#    bash: -c: line 0: syntax error near unexpected token `;;'
#    bash: -c: line 0: `<COMMAND_LINE> fi;;echo XXRETCODE:$?'
# For details on the pdsh bug see:
#  http://sourceforge.net/p/pdsh/mailman/message/290409/
#
fanout "if [ -d /home/gs/var ]; then chown root:root /home/gs/var; chmod 0755 /home/gs/var; fi; if [ -d /home/gs/var/run ]; then chown root /home/gs/var/run; chmod 0755 /home/gs/var/run; fi "
