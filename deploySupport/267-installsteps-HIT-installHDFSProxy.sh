# $Id$

set +x

echo ================= evaluating whether to install hdfsproxy
echo ================= hdfsproxynode = $hdfsproxynode
echo ================= cluster = $cluster
echo ================= HDFSPROXYCPVERSION = $HDFSPROXYVERSION
echo ================= gateway = $gateway
echo ================= evaluating whether to install hdfsproxy

if [ -n "$hdfsproxynode" ]; then
    case "$HDFSPROXYVERSION" in
        none)
            echo === not installing hdfsproxy at all.
            ;;
        *hdfsproxy*)
            echo === installing hdfsproxy version=\"$HDFSPROXYVERSION\"
            echo === cluster=$cluster
            echo === hdfsproxynode=$hdfsproxynode
            echo === Generate keytab files now
            shortname=`echo $hdfsproxynode | cut -d. -f1`
            repo_base="/etc/grid-keytabs"
            combined_service_keytab=$repo_base/$shortname.hdfsproxy.dev.service.keytab

            # Check if keytab file already exist
            ssh $hdfsproxynode ls $combined_service_keytab
            if [ "$?" -ne 0 ]; then
                # keytab file doesn't exist, creating a new one now
                echo ""
                echo ==== keytab file $combined_service_keytab does not exist on $hdfsproxynode, creating a new one now...
                echo ""
                if [ -e $combined_service_keytab ]; then
                    echo "KEYTAB $combined_service_keytab already exist... delete the old one now"
                    /bin/rm -f $combined_service_keytab
                fi

                /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ank -randkey -maxrenewlife 7days dfsproxy"
                /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ank -randkey -maxrenewlife 7days HTTP/$hdfsproxynode"
                /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ank -randkey -maxrenewlife 7days dfsproxy/$hdfsproxynode"
                /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ktadd -k $combined_service_keytab dfsproxy"
                /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ktadd -k $combined_service_keytab HTTP/$hdfsproxynode"
                /usr/kerberos/sbin/kadmin -p devkrbkeygen -k -t /etc/grid-keytabs/devkrbkeygen.keytab -q "ktadd -k $combined_service_keytab dfsproxy/$hdfsproxynode"
                /bin/chmod 444 $combined_service_keytab
                /bin/chown root:root $combined_service_keytab

                if [ -e $combined_service_keytab ]; then
                    echo ""
                    echo "SUCCESS: Combined service keytab for host $hdfsproxynode generated: $combined_service_keytab"
                    echo "Pushing $combined_service_keytab --> $hdfsproxynode"
                    /usr/bin/rsync --timeout=5 -az $combined_service_keytab $hdfsproxynode:/etc/grid-keytabs/
                    echo ""
                else
                    echo "FAILED: Unable to generate combined service keytab for host $hdfsproxynode..."
                fi
                # sleep for 5 minutes for propogation
                echo "sleep 5 minutes for propogation....."
                sleep 300
            else
                # keytab file already exist, skip creation
                echo "$combined_service_keytab already exists on $hdfsproxynode, skip combined keytab creation..."
            fi

            HIT_NN=$ALLNAMENODESLIST HIT_HDFS_PROXY=$hdfsproxynode YROOT=hit_hp WORKSPACE=`pwd` \
                HIT_HDFS_PROXY_VERSION=$HDFSPROXYVERSION \
                sh -x  ${base}/HIT-hdfsproxy-install.sh

            #echo " ==== Testing hdfsproxy installation now"
            #SUDO_USER=hadoopqa /usr/kerberos/bin/kinit -kt ~hadoopqa/hadoopqa.dev.headless.keytab hadoopqa@DEV.YGRID.YAHOO.COM
            #SUDO_USER=hadoopqa /usr/bin/curl -k --negotiate -u : "https://$hdfsproxynode:4443/fs/"

            st=$?
            if [ "$st" -ne 0 ];then
                echo $st
                echo "*****" HDFSPROXY NOT INSTALLED properly "*****"
                exit $st
            else
                echo "*****" HDFSPROXY is up and running properly now  "*****"
                recordManifest "$HDFSPROXYVERSION"
            fi
            ;;
        *)
            echo === "********** ignoring hdfsproxyversion=$HDFSPROXYVERSION"
        ;;
    esac
else
    echo ===  cannot find hdfsproxy node defined in igor, skip hdfsproxy installation now.
fi

