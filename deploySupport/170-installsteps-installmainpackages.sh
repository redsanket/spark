
if [ "$INSTALLNEWPACKAGES" = true ]
then
    echo == installing YINST packages.

    # For spark integration tests we want to deploy packages from the quarantine branch.
    # We separate out spark shuffle jar to explicitly specify the branch is spark is selected.
    spark_shuffle_cmd=""
    if [ "$SPARK_SHUFFLE_VERSION" != "none" ]; then
        HADOOP_INSTALL_STRING=`echo $HADOOP_INSTALL_STRING | sed "s/yspark_yarn_shuffle-$SPARK_SHUFFLE_VERSION//g"`

        # based on OS version, use correct install cmd
        #
        # NOTE: don't quote the os compare substr, parsing inserts escape which breaks the compare
        # gridci-3342, need quarantine on rhel7 in order to install with Core 3.x
        OS_VER=`cat /etc/redhat-release | cut -d' ' -f7`
        if [[ "$OS_VER" =~ ^7. ]]; then
            echo "INFO: OS is $OS_VER"
            spark_shuffle_cmd="$yinst install -br quarantine  -yes  -root ${yroothome} yspark_yarn_shuffle-$SPARK_SHUFFLE_VERSION -br quarantine -same -live -downgrade"

        elif [[ "$OS_VER" =~ ^6. ]]; then
            echo "OS is $OS_VER"
            #phw  spark_shuffle_cmd="$yinst install -yes -os rhel-6.x -root ${yroothome} yspark_yarn_shuffle-$SPARK_SHUFFLE_VERSION -br quarantine -same -live -downgrade"
            spark_shuffle_cmd="$yinst install -br quarantine -yes -root ${yroothome} yspark_yarn_shuffle-$SPARK_SHUFFLE_VERSION -br quarantine -same -live -downgrade"
  
        else
            echo "WARN: Unknown OS $OS_VER!"
            exit 1
        fi
    fi


    # based on OS version, use correct cmd
    #
    #
    # NOTE: don't quote the os compare substr, parsing inserts escape which breaks the compare
    # gridci-3342, need quarantine on rhel7 in order to install with Core 3.x
    OS_VER=`cat /etc/redhat-release | cut -d' ' -f7`
    if [[ "$OS_VER" =~ ^7. ]]; then
        echo "INFO: OS is $OS_VER"
        cmd="$yinst install  -br quarantine  -yes  -root ${yroothome}  $HADOOP_INSTALL_STRING -same -live -downgrade "

    elif [[ "$OS_VER" =~ ^6. ]]; then
        echo "OS is $OS_VER"
        #phw  cmd="$yinst install -br test -yes -os rhel-6.x -root ${yroothome}  $HADOOP_INSTALL_STRING -same -live -downgrade"
        cmd="$yinst install -br quarantine -yes -root ${yroothome}  $HADOOP_INSTALL_STRING -same -live -downgrade"

    else
        echo "WARN: Unknown OS $OS_VER!"
        exit 1
    fi

    
    # compat-readline should have come from Config job, removing compat-readline5.x86_64
    slownogwfanout "/usr/bin/yum -y install openssl098e.x86_64 lzo lzo.i686 lzo.x86_64"
    slownogwfanout "$cmd"
    [[ "$SPARK_SHUFFLE_VERSION" != "none" ]] && slownogwfanout "$spark_shuffle_cmd"
    fanoutGW "/usr/bin/yum makecache"
    # compat-readline should have come from Config job, removing compat-readline5.x86_64
    fanoutGW "/usr/bin/yum -y install lzo lzo.i686 lzo.x86_64 openssl098e.x86_64"
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
        #phw slowfanout "$yinst install -yes -os rhel-6.x -root ${yroothome}  $QA_PACKAGES -same -live"
        slowfanout "$yinst install -br test  -yes  -root ${yroothome}  $QA_PACKAGES -same -live "
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


##################################################################################
# KMS Support Helper - Add SSL Certificates to Each Node's JDK
#
# Encryption Zones (KMS) are rolling out to all prod clusters, KMS requires all 
# clients to support https connections to kms, since flubber has to use self-signed 
# certs these need to be added to a client's java jdk truststore, without it the
# https connection will fail on CA validation.
#
# Update: these certs are also needed by other services, at least Timeline service
# on the RM, hence removing check for KMS node and installing these certs explicitly.
#
# The certs have to be installed after the jdk is deployed but before the using
# service starts up (RM, NN, KMS), so placing these certs here instead of in the
# KMS installer (256-installsteps-KmsAndZookeeper.sh)
#
# Using cert from devadm102:/grid/3/dev/ygrid_certs_flubber/hadoop_kms.cert
#
##################################################################################
echo "INFO: Installing ssl certs for KMS and TimeLine on all nodes. Note that even"
echo "INFO: if KMS is not configured on cluster, certs are needed by TimeLine"

CERT_HOME="/etc/ssl/certs/prod/_open_ygrid_yahoo_com"
# the JDK_CACERTS are from the base community jdk, updated with internal cert
# changes, this is the truststore that is updated with our ssl certs
JDK_CACERTS="/home/gs/java/jdk/jre/lib/security/cacerts"
OPTS=" -storepass `sudo /home/y/bin/ykeykeygetkey jdk_keystore` -noprompt "
ALIAS="selfsigned"

fanout "sudo  /home/gs/java/jdk/bin/keytool -import $OPTS -alias $ALIAS  -file $CERT_HOME/hadoop_kms.cert -keystore  $JDK_CACERTS" 

# HTF uses a different jdk, the default install, so get it too
HTF_JDK_CACERTS="/home/y/libexec64/jdk64-1.8.0/jre/lib/security/cacerts"

# this will return nonzero if cert is already added, so added step 170 to teh
# installgrid exceptions, using +/1e here still fails the build 
echo "INFO: HTF uses default JDK on gateway so update it too" 
fanoutGW "sudo  /home/gs/java/jdk/bin/keytool -import $OPTS -alias $ALIAS  -file $CERT_HOME/hadoop_kms.cert -keystore  $HTF_JDK_CACERTS"


# gridci-3318, we need to pass the kms truststore to docker tasks since docker image
# JDK will not have this and we can't add it on the fly (would need to add to base 
# image) so instead we place our jks in a docker bind mount, which conf/hadoop is,
# and then tell pig tasks to use it via pig script 'set' cmds 
fanout "sudo cp $CERT_HOME/kms.jks /home/gs/conf/current/."


