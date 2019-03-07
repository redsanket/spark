set +x

if [ "$RUN_HIT_TESTS" = "true" ]; then
    dest=$cluster.$TIMESTAMP
    HITinstalledpkg=/tmp/HIT/HIT_installed_pkgs.txt
    HITmanifest=/tmp/HIT/manifest.txt
    (
        echo "if [ -e /tmp/HIT ]; then"
        echo "   echo ===== Find HIT results dir /tmp/HIT, rsync HIT test results back to $ADMIN_HOST now......"
        echo "   echo ##### Save HIT installed packages in GW to $HITinstalledpkg"
        echo "   $yinst ls -root ${yroothome} |grep - > $HITinstalledpkg "
        echo "   $yinst ls |grep - >> $HITinstalledpkg "
        echo "   cat $HITinstalledpkg | grep 'hadoop\|pig\|hive\|hdfsproxy\|oozie\|nova\|gdm\|hbase\|hcat' > $HITmanifest"
        echo "   rsync -av /tmp/HIT $ADMIN_HOST::tmp/$dest"
        echo "else"
        echo "  echo Cannot find /tmp/HIT dir for result files..."
        echo "fi"
    ) > $scripttmp/$cluster.hit.copyresults.sh
    fanoutYRoots "rsync $scriptaddr/$cluster.hit.copyresults.sh /tmp/ && sh /tmp/$cluster.hit.copyresults.sh "
    chown -R hadoopqa:users /grid/0/tmp/$dest/HIT
    cat /grid/0/tmp/$dest/HIT/HIT_installed_pkgs.txt
else
    echo ========= not install hit at all, skip results copy
fi
