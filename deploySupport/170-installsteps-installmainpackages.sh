
if [ "$INSTALLNEWPACKAGES" = true ]
then
    echo == installing YINST packages.

    slownogwfanout "/usr/bin/yum -y install openssl098e-0.9.8e-17.el6_2.2.x86_64 lzo lzo.i686 lzo.x86_64 compat-readline5-5.2-17.1.el6"
    slownogwfanout "$yinst install -yes -os rhel-6.x -root ${yroothome}  $HADOOP_INSTALL_STRING -same -live -downgrade"
    fanoutgw "/usr/bin/yum -y install openssl098e-0.9.8e-17.el6_2.2.x86_64 lzo lzo.i686 lzo.x86_64 compat-readline5.x86_64"
    fanoutGW "$yinst install -yes -os rhel-6.x -root ${yroothome}  $HADOOP_INSTALL_STRING -same -live -downgrade"
    
#
# At this point, the packages are installed - except the configs.
#
#    f=YahooDNSToSwitchMapping-0.2.1111040716.jar
#    f=YahooDNSToSwitchMapping-0.22.0.1011272126.jar
    cmd="scp $ADMIN_HOST:/grid/0/tmp/deploy.$cluster.confoptions.sh   /tmp/deploy.$cluster.confoptions.sh ; GSHOME=$GSHOME yroothome=$yroothome sh /tmp/deploy.$cluster.confoptions.sh && cp /tmp/deploy.$cluster.confoptions.sh  ${yroothome}/conf/hadoop/ "
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
