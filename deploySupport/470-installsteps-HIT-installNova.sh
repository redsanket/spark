set +x

echo ================= evaluating whether to install NOVA
echo ================= NOVAVERSION = $NOVAVERSION
echo ================= gateway = $gateway

case "$NOVAVERSION" in
    none)
        echo === not installing nova at all.
	;;
    *nova*)
        echo === installing nova version=\"$NOVAVERSION\"

        (
            echo "/usr/sbin/lgroupadd  -g 59002 novausrs "
            echo "/usr/local/bin/yinst install nova_dummy_keys nova_dummy_proxy_keys -br test -yes"
            echo "/usr/local/bin/yinst install $NOVAVERSION yapache_mod_jk-5.2.28.23 -br test"
            echo 'st=$?'
            echo 'if [ "$st" -ne 0 ]; then'
            echo ' echo $st'
            echo ' echo "*****" NOVA NOT INSTALLED "*****"'
            echo 'exit $st'
            echo 'fi'
            echo "/usr/local/bin/yinst stop yjava_tomcat yapache"
            echo "exit 0"
) > $scriptdir/$cluster.installnova.sh
        set -x
        fanoutYRoots "rsync -a $scriptaddr/$cluster.installnova.sh  /tmp/ && sh /tmp/$cluster.installnova.sh"
        st=$?
        set +x
        if [ "$st" -eq 0 ] ; then
            recordManifest "$NOVAVERSION"
        else
            echo "NOVA not installed properly, exiting ...."
            exit $st
        fi
	;;
    *)
        echo === "********** ignoring novaversion=$NOVAVERSION"
	;;
esac
