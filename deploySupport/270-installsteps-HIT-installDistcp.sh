# $Id$

set +x

## ## for debugging only
## export cluster=ankh
## export gateway=gwbl2003.blue.ygrid.yahoo.com
## export DISTCPVERSION=distcp_latest
## export VERSION=0.1.1011250802
## export CLUSTER1=$cluster
## export CLUSTER2=alexandria
## for debugging only


echo ================= evaluating whether to install distcp
echo ================= cluster = $cluster
echo ================= DISTCPVERSION = $DISTCPVERSION
echo ================= gateway = $gateway
echo ================= CLUSTER1 = $CLUSTER1
echo ================= CLUSTER2 = $CLUSTER2
echo ================= VERSION = $VERSION


# install two distcp pkgs:
# -ygrid_hadoop_distcp
# and
# -ygrid_hadoop_distcp_sys 
case "$DISTCPVERSION" in
    none)
        echo === not installing distcp at all.
        ;;
    *distcp*)
        echo === installing distcp version=\"$DISTCPVERSION\"
        echo cluster=$cluster
        echo gateway=$gateway

(
    echo cd ${yroothome}
    echo /usr/local/bin/yinst install -root ${yroothome} $DISTCPVERSION
    echo 'st=$?'
    echo '[ "$st" -ne 0 ] && echo "*****" DISTCP NOT INSTALLED "*****" && exit $st'
    echo 'echo `hostname`:  you may need to add gateway-specific things to distcp install. '

    echo "if [ -L ${yroothome}/share/distcp ] "
    echo "then"
    echo '  realname=`readlink '${yroothome}'/share/distcp`'
    echo '  t='${yroothome}'/share/`basename  $realname`'
    echo "  mkdir -p ${yroothome}/tmp/distcpversions"
    echo '  rm -f '${yroothome}'/tmp/distcpversions/current'
    echo '  ln -s $t '${yroothome}'/tmp/distcpversions/current'
    echo "fi"
) > $scripttmp/$cluster.distcp.install.sh

        fanoutYRoots "rsync $scriptaddr/$cluster.distcp.install.sh /tmp/ && sh /tmp/$cluster.distcp.install.sh"
        st=$?
        if [ "$st" -eq 0 ] ; then
            recordManifest "$DISTCPVERSION"
        else
            exit $st
        fi
        ;;
    *)
        echo === "********** ignoring distcpversion=$DISTCPVERSION"
        ;;
esac
