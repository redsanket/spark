set +x
if [ "$REMOVEEXISTINGDATA" == "true" ]; then
    echo "== removing existing data on grid."
    for nnode in $namenode; do
        short_name=`echo $nnode| sed 's/\..*$//g'`
        SHARED_DIR=$HOMEDIR/$HDFSUSER/ha_namedir/${cluster}_${short_name}
        echo "Removing $SHARED_DIR."
        rm -rf $SHARED_DIR
    done
    set -x
    fanoutscp "$scriptdir/cleangrid.sh" "/tmp/cleangrid.sh" "$HOSTLIST"
    fanout "export GSHOME=$GSHOME && export HDFSUSER=$HDFSUSER && export MAPREDUSER=$MAPREDUSER &&  sh /tmp/cleangrid.sh && rm /tmp/cleangrid.sh"
    set +x
elif [ "$REMOVE_YARN_DATA" == "true" ]; then
    echo == removing existing yarn data on grid.
    # YINST root is where the deployment yinst package is installed on devadm102
    # ROOT_DIR vs scriptdir...
    set -x
    fanoutscp "$scriptdir/cleangrid.sh" "/tmp/cleangrid.sh" "$HOSTLIST"
    fanout "export GSHOME=$GSHOME && export HDFSUSER=$HDFSUSER && export MAPREDUSER=$MAPREDUSER && sh /tmp/cleangrid.sh yarn && rm /tmp/cleangrid.sh"
    set +x
else
    echo "== Not removing existing data on grid."
fi
