set +x
case "$PIGVERSION" in
    none)
        echo === not installing pig at all.
	;;
  *pig*)
        echo === short-test of pig version=\"$PIGVERSION\"
        echo cluster=$cluster
        echo gateway=$gateway

        cp $ROOT_DIR/test-piginstall.sh  $scripttmp/$cluster.test-piginstall.sh
        fanoutYRoots "
    rsync -a $scriptaddr/$cluster.test-piginstall.sh /tmp/ && 
    GSHOME=$GSHOME yroothome=$yroothome sh -x /tmp/$cluster.test-piginstall.sh  -c $cluster -w -n $namenode 
    "
        st=$?
        if [ "$st" -ne 0 ] ; then
            echo "*****" Failed to run HIT pig test "*****"
            exit $st
        fi
	;;
  *)
        echo === "********** ignoring pigversion=$PIGVERSION"
	;;
esac
