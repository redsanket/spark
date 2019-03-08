set +x

echo ================= evaluating whether to install starling
echo ================= STARLINGVERSION = $STARLINGVERSION
echo ================= gateway = $gateway

case "$STARLINGVERSION" in
    none)
        echo === not installing starling at all.
	;;
  *starling*)
        echo === installing starling version=\"$STARLINGVERSION\"
        echo cluster=$cluster
        echo gateway=$gateway
        echo yroothome=$yroothome

(
    echo cd $yroothome
    echo export INSTALLATION_ROOT=$yroothome
    echo /usr/local/bin/yinst install -root $yroothome   $STARLINGVERSION -br test -br nightly -same -live -yes\
    -set starling_proc.oozie_url=http://$oozienode:4080/oozie 
    echo 'st=$?'
    echo '[ "$st" -ne 0 ] && echo $st &&echo "*****" Failed to INSTALL STARLING "*****" && exit $st'

    echo "exit 0"

) > $scripttmp/$cluster.installstarling.sh

        fanoutYRoots "rsync -a $scriptaddr/$cluster.installstarling.sh  /tmp/ && sh /tmp/$cluster.installstarling.sh"
        if [ $? -eq 0 ] ; then
            recordManifest "$STARLINGVERSION "
        fi
	;;
    *)
        echo === "********** ignoring starlingversion=$STARLINGVERSION"
	;;
esac
