set +x

echo ================= evaluating whether to install Vaidya
echo ================= VAIDYAVERSION = $VAIDYAVERSION
echo ================= gateway = $gateway

case "$VAIDYAVERSION" in
    none)
        echo === not installing vaidya at all.
	;;
    *vaidya*)
        echo === installing vaidya version=\"$VAIDYAVERSION\"
        echo cluster=$cluster
        echo gateway=$gateway

(
    echo cd ${yroothome}
    echo /usr/local/bin/yinst install -root ${yroothome}  $VAIDYAVERSION
    echo 'st=$?'
    echo '[ "$st" -ne 0 ] && echo $st &&echo "*****" VAIDYA NOT INSTALLED "*****" && exit $st'
    echo 'echo `hostname`:  you may need to add gateway-specific things to vaidya install. '

) > $scripttmp/$cluster.installvaidya.sh

        set -x
        fanoutYRoots "rsync -a $scriptaddr/$cluster.installvaidya.sh  /tmp/ && sh /tmp/$cluster.installvaidya.sh"
        st=$?
        set +x
        if [ "$st" -eq 0 ] ; then
            recordManifest "$VAIDYAVERSION"
        else
            exit $st
        fi
	;;
    *)
        echo === "********** ignoring vaidyaversion=$VAIDYAVERSION"
	;;
esac
