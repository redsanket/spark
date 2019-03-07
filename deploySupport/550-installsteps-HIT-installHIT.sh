set +x

echo ================= HITVERSION = $HITVERSION
echo ================= gateway = $gateway
echo ================= gateways = $gateways
echo ================= yrootname = $yrootname

case "$HITVERSION" in
    none)
  	echo ========= not install hit at all
	;;
    *hit*)
        export INSTALLATION_ROOT=$yroothome
        export JAVA_HOME=$GSHOME/java/jdk
        export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
        export HADOOP_COMMON_HOME=$yrootHadoopCurrent
        export HADOOP_PREFIX=$yroothome/share/hadoop
        export HADOOP_MAPRED_HOME=$yrootHadoopMapred
        export HADOOP_HDFS_HOME=$yrootHadoopHdfs
        export HADOOP_HOME=${GSHOME}/hadoop/current
	env > $scripttmp/$cluster.hit.install.env
	echo ========= cluster = $cluster
(
   echo echo installing HIT on `date` with $HITVERSION
   echo /usr/local/bin/yinst install $HITVERSION -br quarantine -same -live
   echo 'st=$?'
   echo '[ "$st" -ne 0 ] && echo "*****" HIT NOT INSTALLED "*****" && exit $st'
   
) > $scripttmp/$cluster.hit.install.sh

        fanoutYRoots "
	rsync $scriptaddr/$cluster.hit.install.* /tmp/ &&
	sh /tmp/$cluster.hit.install.sh"
        st=$?
        if [ "$st" -eq 0 ] ; then
            recordManifest "$HITVERSION"

            manifest_file=/tmp/deployjobs/deploys.$CLUSTER/yroot.$DATESTRING/manifest.txt
            if [ -e $manifest_file ]; then
                echo "Find manifest file '$manifest' ..., copy inside the GW yroot now.."
                cp $manifest_file /grid/0/tmp/hit.manifest.txt
                echo "rsync -av $ADMIN_HOST::tmp/hit.manifest.txt /tmp/" > $scripttmp/$cluster.hit.copymanifest.sh
                fanoutYRoots "rsync $scriptaddr/$cluster.hit.copymanifest.sh /tmp/ && sh /tmp/$cluster.hit.copymanifest.sh "
            fi
        else
            echo "*****" Failed to install HIT "*****"
            exit $st
        fi
	;;
    *)
 	echo === "********** ignoring hitversion=$HITVERSION"
	;;
esac	
