if [ "$REMOVEEXISTINGDATA" = true ]
then
    echo == removing existing data on grid.
    for nnode in $namenode
    do
        short_name=`echo $nnode| sed 's/\..*$//g'`
        SHARED_DIR=/homes/$HDFSUSER/ha_namedir/${cluster}_${short_name}
        echo "Removing $SHARED_DIR."
        rm -rf $SHARED_DIR
    done
    cp ${YINST_ROOT}/conf/hadoop/hadoopAutomation/cleangrid.sh  $scripttmp/cleangrid.sh
fanout "scp $ADMIN_HOST:/grid/0/tmp/scripts.deploy.$cluster/cleangrid.sh   /tmp/cleangrid.sh && export GSHOME=$GSHOME && export HDFSUSER=$HDFSUSER && export MAPREDUSER=$MAPREDUSER &&  sh /tmp/cleangrid.sh && rm /tmp/cleangrid.sh "

else
    echo == not removing existing data on grid.
    

fi
