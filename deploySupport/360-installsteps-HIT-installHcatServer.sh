# $Id$

set +x

echo ================= evaluating whether to install hcat server pkg
echo ================= hcatnode = $hcatservernode
echo ================= cluster = $cluster
echo ================= HCATIGORTAG = $HCATIGORTAG
echo ================= gateway = $gateway
echo ================= evaluating whether to install hcat

if [ -n "$hcatservernode" ]; then

    # Generate keytab file for HCAT server
    repo_base="/etc/grid-keytabs"
    service_keytab=$repo_base/hcat.dev.service.keytab
    
    # Check if keytab file already exist
    ssh $hcatservernode ls $service_keytab

    if [ "$?" -ne 0 ]; then
        # keytab file doesn't exist, creating a new one now
	echo ""
	echo ==== keytab file $service_keytab does not exist on $hcatservernode, creating a new one now...
	echo ""
        if [ -e $service_keytab ]; then    
            echo "KEYTAB $service_keytab already exist... delete the old one now"
            /bin/rm -f $service_keytab
        fi
    
        /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ank -randkey -maxrenewlife 7days hcat/$hcatservernode"
        /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ktadd -k $service_keytab hcat/$hcatservernode"
        /bin/chmod 444 $service_keytab
        /bin/chown root:root $service_keytab
        if [ -e $service_keytab ]; then
            echo "" 
            echo "SUCCESS: hcat_hadoopqa service keytab for host $hcatservernode generated: $service_keytab"
            echo "Pushing $service_keytab --> $hcatservernode"
            /usr/bin/rsync --timeout=5 -az $service_keytab $hcatservernode:/etc/grid-keytabs/	
            echo "" 
        else
            echo "FAILED: Unable to generate combined service keytab for host $hcatservernode..."
        fi
	# sleep 5 minutes for propogation
	echo "sleep 5 minutes for propogation....."
        sleep 300
    else
        # keytab file already exist, skip creation
        echo "$service_keytab already exists on $hdfsproxynode, skip combined keytab creation..."
    fi
    # Update JAVA_HOME to make sure it's pointing to 64 bit
    realname=`ssh $hcatservernode /usr/bin/readlink $GSHOME/java/jdk`
    if [[ $realname =~ 32 ]]; then
        ssh $hcatservernode rm -rf $GSHOME/java/jdk
        ssh $hcatservernode ln -s $GSHOME/java/jdk64/current $GSHOME/java/jdk
    fi
  else
        echo ===  cannot find hcat server node defined in igor, skip hcat_server installation now.
  fi

if [[ $HCATIGORTAG != none ]]; then
    cmd="yinst restore -igor -igor_tag hcatalog.$HCATIGORTAG -live -yes -quarantine && \
  yinst restart hcat_server "
#  yinst install mysql_client-current mysql_server-current hcat_database_access_dev-test -br current -br test -br quarantine -downgrade -same -live -yes && \
#  yinst set hcat_server.database_connect_url=jdbc:mysql://\`hostname\`:3306/hivemetastoredb?createDatabaseIfNotExist=true mysql_config.read_only=off mysql_config.binlog_format=ROW && \
#  /home/y/bin/mysql -u hive -p\`/home/y/bin/keydbgetkey hive\` -e 'drop database if exists hivemetastoredb;' && \
#  yinst restart hcat_server "
    fanoutHcatServer "$cmd"
    hcat_server_pkg_version=`ssh $hcatservernode "/usr/local/bin/yinst ls hcat_server | cut -f 2 -d ' '"`
    if [ "$st" -eq 0 ] ; then
        recordManifest "$HCATIGORTAG"
        recordManifest "$hcat_server_pkg_version"
        echo "*****" Successful to restart HCAT SERVER "*****"
    else
        echo "*****" Failed to restart HCAT SERVER "*****"
        exit $st
    fi

else
    echo ========== Ignore hcat_server installation now....
fi
