set +x
echo ================= evaluating whether to install pig
echo ================= PIGVERSION = $PIGVERSION
echo ================= gateway = $gateway

export PIG_HOME=

case "$PIGVERSION" in
    none)
        echo === not installing pig at all.
        ;;
    *pig*)
        echo === installing pig version=\"$PIGVERSION\"
        echo cluster=$cluster
        echo gateway=$gateway
(
    echo cd ${yroothome}
    echo /usr/local/bin/yinst install -root ${yroothome}   $PIGVERSION -br quarantine
    echo 'st=$?'
    echo '[ "$st" -ne 0 ] && echo $st &&echo "*****" PIG NOT INSTALLED "*****" && exit $st'
    echo 'echo `hostname`:  you may need to add gateway-specific things to pig install.  -- Dec 8 2010 note'

    echo "if [ -L ${yroothome}/share/pig ] "
    echo "then"
    echo '  realname=`readlink '${yroothome}'/share/pig`'
    echo '  t='${yroothome}'/share/`basename  $realname`'

    echo "  mkdir -p ${yroothome}/tmp/pigversions"
    echo '  rm -f '${yroothome}'/tmp/pigversions/current'
    echo '  ln -s $t '${yroothome}'/tmp/pigversions/current'
    echo "fi"
    echo "exit 0"

) > $scripttmp/$cluster.installpig.sh

        set -x
        fanoutYRoots "rsync -a $scriptaddr/$cluster.installpig.sh  /tmp/ && sh /tmp/$cluster.installpig.sh"
        st=$?
        set +x
        if [ "$st" -eq 0 ] ; then
            export PIG_HOME=${yroothome}/tmp/pigversions/current
            recordManifest "$PIGVERSION"
        else
            exit $st
        fi
        ;;
    *)
        echo === "********** ignoring pigversion=$PIGVERSION"
        ;;
esac
