# $Id$

echo ================= evaluating whether to install oozie server pkg
echo ================= oozienode = $oozienode
echo ================= cluster = $cluster
echo ================= OOZIEIGORTAG = $OOZIEIGORTAG
echo ================= evaluating whether to install oozie



regex="(oozie)"
if [[ $OOZIEIGORTAG =~ $regex ]]; then
  set -x
  if [ -n "$oozienode" ]
  then
    echo === Generate keytab files now
    repo_base="/etc/grid-keytabs"
    combined_service_keytab=$repo_base/$oozienode.oozie.dev.service.keytab

    # Check if keytab file already exist
    ssh $oozienode ls $combined_service_keytab

    if [ "$?" -ne 0 ]; then
        # keytab file doesn't exist, creating a new one now
        echo ""
        echo ==== keytab file $combined_service_keytab does not exist on $oozienode, creating a new one now...
        echo ""
        if [ -e $combined_service_keytab ]; then
            echo "KEYTAB $combined_service_keytab already exist... delete the old one now"
            /bin/rm -f $combined_service_keytab
        fi

        /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ank -randkey -maxrenewlife 7days HTTP/$oozienode" 
        /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ank -randkey -maxrenewlife 7days oozie/$oozienode" 
        /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ktadd -k $combined_service_keytab HTTP/$oozienode" 
        /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ktadd -k $combined_service_keytab oozie/$oozienode" 
        /bin/chmod 444 $combined_service_keytab
        /bin/chown root:root $combined_service_keytab

        if [ -e $combined_service_keytab ]; then
            echo ""
            echo "SUCCESS: Combined service keytab for host $oozienode generated: $combined_service_keytab"
            echo "Pushing $combined_service_keytab --> $oozienode"
            /usr/bin/rsync --timeout=5 -az $combined_service_keytab $oozienode:/etc/grid-keytabs/
            echo ""
        else
            echo "FAILED: Unable to generate combined service keytab for host $oozienode..."
            st=300
            exit $st
        fi
        
        # sleep for 5 minutes for propogation
        echo "sleep 5 minutes for propogation....."
        sleep 300
    else
        # keytab file already exist, skip creation
        echo "$combined_service_keytab already exists on $oozienode, skip combined keytab creation..."
    fi

    # install yoozie pkg on $oozieserver node
    echo === installing oozie server from igor tag=\"$OOZIEIGORTAG\" on host $oozienode
    ssh $oozienode /usr/local/bin/yinst restore -igor -igor_tag $OOZIEIGORTAG  -live -yes -quarantine --os rhel-6.x
    st=$?
    if [ "$st" -ne 0 ]
    then
         echo $st 
         echo "*****" OOZIE SERVER NOT INSTALLED "*****"
         exit $st
    fi

        echo === installing hbase/hbase_conf on $oozienode ===
        HBASE_PKG_VERSION=`ssh $HBASEMASTERNODE "/usr/local/bin/yinst ls hbase | cut -f 2 -d ' '"`
        HBASE_CONF_PKG_VERSION=`ssh $HBASEMASTERNODE "/usr/local/bin/yinst ls hbase_conf | cut -f 2 -d ' '"`
        HBASE_CONF_SETTINGS=`ssh $HBASEMASTERNODE /usr/local/bin/yinst set hbase_conf |awk '{print $1 $2}' | tr ":" "="`
        cmd="yinst install $HBASE_PKG_VERSION $HBASE_CONF_PKG_VERSION -br quarantine -live -br test -same "
        for i in $HBASE_CONF_SETTINGS; do
                cmd=" $cmd -set $i "
        done
        
        ssh $oozienode $cmd

    if [[ "$HADOOP_27" != "true" ]]; then
        # remove old files
        ssh $oozienode rm -rf /home/y/libexec/yjava_tomcat/lib/mapred-site.xml /home/y/libexec/yjava_tomcat/lib/core-site.xml /home/y/libexec/yjava_tomcat/lib/hdfs-site.xml /home/y/libexec/yjava_tomcat/lib/yarn-site.xml /home/y/libexec/yjava_tomcat/lib/hbase-site.xml
        # link core-site.xml
        ssh $oozienode ln -s $yroothome/conf/hadoop/core-site.xml /home/y/libexec/yjava_tomcat/lib/
        ssh $oozienode ln -s $yroothome/conf/hadoop/hdfs-site.xml /home/y/libexec/yjava_tomcat/lib/
        ssh $oozienode ln -s $yroothome/conf/hadoop/mapred-site.xml /home/y/libexec/yjava_tomcat/lib/
        ssh $oozienode ln -s $yroothome/conf/hadoop/yarn-site.xml /home/y/libexec/yjava_tomcat/lib/

        ssh $oozienode cp /home/y/libexec/hbase/conf/hbase-site.xml /home/y/libexec/yjava_tomcat/lib/

        ssh $oozienode rm -rf /home/y/lib/libhadoop.so.1.0.0 /home/y/lib/libhadoop.so.1 /home/y/lib/libhadoop.so /home/y/lib/libhadoop.a /home/y/lib/libhadoop.la
        ssh $oozienode ln -s $yroothome/share/hadoop/lib/native/Linux-i386-32/libhadoop.so.1  /home/y/lib/libhadoop.so.1.0.0
        ssh $oozienode ln -s $yroothome/share/hadoop/lib/native/Linux-i386-32/libhadoop.so.1  /home/y/lib/libhadoop.so.1
        ssh $oozienode ln -s $yroothome/share/hadoop/lib/native/Linux-i386-32/libhadoop.so.1  /home/y/lib/libhadoop.so

        ssh $oozienode ln -s $yroothome/share/hadoop/lib/native/Linux-i386-32/libhadoop.a  /home/y/lib/libhadoop.a
        ssh $oozienode ln -s $yroothome/share/hadoop/lib/native/Linux-i386-32/libhadoop.la /home/y/lib/libhadoop.la
    fi

    ssh $oozienode rm /home/y/lib/hadoop
    ssh $oozienode ln -s $yroothome/share/hadoop /home/y/lib/hadoop

    if [[ "$HADOOP_27" != "true" ]]; then
        # Copy the jce_policy jar files
        ssh $oozienode "rm -rf /home/y/share/yjava_jdk/java/jre/lib/security/*.jar"
        ssh $oozienode "mkdir -p /home/y/share/yjava_jdk/java/jre/lib/security/"
   
        jdkversion=`ssh $oozienode "yinst ls | egrep 'yjava_jdk-' | sed -e 's/yjava_jdk-//'" `
        if [[ $jdkversion =~ "(1.7.*)" ]]; then
            jcedir="policy_jdk1.7.0"
        else
            jcedir="policy_jdk1.6.0"
        fi
        scp /grid/0/gs/gridre/tmplib/${jcedir}/local_policy.jar $oozienode:/home/y/share/yjava_jdk/java/jre/lib/security/
        scp /grid/0/gs/gridre/tmplib/${jcedir}/US_export_policy.jar $oozienode:/home/y/share/yjava_jdk/java/jre/lib/security/

        ssh $oozienode "chown root:bin /home/y/share/yjava_jdk/java/jre/lib/security/*.jar"
    fi

    # restart oozie service
    ssh $oozienode /usr/local/bin/yinst restart yoozie
    
    # run a simple test to see if oozie sever is up
    ssh $oozienode "SUDO_USER=oozie kinit -kt $combined_service_keytab oozie/$oozienode@DEV.YGRID.YAHOO.COM"
    ssh $oozienode "SUDO_USER=oozie /home/y/var/yoozieclient/bin/oozie jobs -oozie http://$oozienode:4080/oozie -auth KERBEROS"
    st=$?
    if [ "$st" -ne 0 ];then
        echo $st
        echo "*****" OOZIE SERVER NOT INSTALLED PROPERLY "*****"
        exit $st
    else
        echo "*****" OOZIE SERVER IS UP AND RUNNING PROPERLY "*****"
    fi
  else
	echo ===  cannot find keyword oozie in igor tag, skip oozie server installation now.
  fi
else
    echo ========== Ignore oozie server installation now....
fi
