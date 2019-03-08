# $Id$

set +x

echo ================= evaluating whether to install oozie
echo ================= oozienode = $oozienode
echo ================= cluster = $cluster
echo ================= OOZIEIGORTAG = $OOZIEIGORTAG
echo ================= gateway = $gateway
echo ================= evaluating whether to install oozie

if [ -n "$OOZIE_SERVER" ]; then
    echo ========== OOZIE_SERVER=$OOZIE_SERVER
    case "$OOZIE_SERVER" in
        default)
            echo ===== No need to update default igor setting
        ;;
        gsbl*)
            oozienode=$OOZIE_SERVER
        ;;
    esac
fi

if [ -n "$oozienode" ]; then
    case "$OOZIEIGORTAG" in
        none)
            echo === not installing oozie at all.
	    ;;
        *oozie*)
            echo === installing oozie igor tag=\"$OOZIEIGORTAG\"
            echo === cluster=$cluster
            echo === oozienode=$oozienode
            echo "======= using igor tag for oozie deployment= '$OOZIEIGORTAG'"

            OOZIEIGORTAG=$OOZIEIGORTAG oozienode=$oozienode cluster=$cluster \
                sh -x  ${base}/Oozie-install-igor.sh

            st=$?
            if [ "$st" -ne 0 ]; then
                echo $st
                echo "*****" oozie NOT INSTALLED properly "*****"
                exit $st
            else
                echo "*****" oozie is up and running properly now  "*****"
                recordManifest "$OOZIEIGORTAG"
            fi
            ;;
        *)
            echo === "********** ignoring oozie igor tag=$OOZIEIGORTAG"
	    ;;
    esac
else
    echo ===  cannot find oozie node defined in igor, skip oozie installation now.
fi

