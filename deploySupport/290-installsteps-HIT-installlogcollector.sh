set +x

if [ -n "$LOG_COLLECTORVERSION" -a  "$LOG_COLLECTORVERSION" != none ]; then
    echo ================================================
    echo ===== cluster=$cluster
    echo ===== gateway=$gateway
    echo ===== LOG_COLLECTORVERSION=$LOG_COLLECTORVERSION
    echo ================================================
    [ -z "$yinst" ] && yinst=/usr/local/bin/yinst
    
    (
        echo $yinst install -root ${yroothome} $LOG_COLLECTORVERSION
        echo 'st=$?'
        echo '[ "$st" -ne 0 ] && echo $st &&echo "*****" log-collector NOT INSTALLED Properly "*****" && exit $st'
        echo "ls -la ${yroothome}/bin/log*"
    ) > $scripttmp/$cluster.logcollector.20.install.sh

    fanoutYRoots "rsync $scriptaddr/$cluster.logcollector.20.install.sh /tmp/ && sh /tmp/$cluster.logcollector.20.install.sh"
    st=$?
    if [ "$st" -eq 0 ] ; then
        recordManifest "$LOG_COLLECTORVERSION"
    else
        exit $st
    fi
else
    echo "****** Not installing log-collector."
fi
