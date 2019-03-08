set +x

echo "Running HIT ..."

case "$HITVERSION" in
    none)
  	echo ========= not install hit at all
	;;
    *hit*)
	echo ========= cluster = $cluster
(
   echo "JAVA_HOME=$GSHOME/java/jdk HADOOP_HOME=$GSHOME/hadoop/current HADOOP_CONF_DIR=${yroothome}/conf/hadoop SEND_LOG_TO_STDOUT=$SEND_LOG_TO_STDOUT NO_CERTIFICATION=$NO_CERTIFICATION sh /home/y/var/hit/bin/HIT.sh"
   echo 'st=$?'

   
   echo 'if [ "$st" -ne 0 ]; then echo $st; echo "*****" HIT.sh NOT running properly, exits with $st "*****" ; exit $st; fi'
) > $scripttmp/$cluster.hit.run.sh

        fanoutYRoots "
	rsync $scriptaddr/$cluster.hit.run.sh /tmp/ &&
	sh /tmp/$cluster.hit.run.sh "
        status=$?
        if [ $status -eq 0 ] ; then
            echo "==== HIT.sh running properly"
        else
            echo "==== HIT.sh NOT running properly, exit with $status...."
            exit $status
        fi
	;;
    *)
 	echo === "********** ignoring hitversion=$HITVERSION"
	;;
esac	
