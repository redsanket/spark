if [ "$REMOVEEXISTINGDATA" == "true" ]
then
    echo == removing existing data on grid.
    for nnode in $namenode
    do
        short_name=`echo $nnode| sed 's/\..*$//g'`
        SHARED_DIR=$HOMEDIR/$HDFSUSER/ha_namedir/${cluster}_${short_name}
        echo "Removing $SHARED_DIR."
        rm -rf $SHARED_DIR
    done
    cp ${YINST_ROOT}/conf/hadoop/hadoopAutomation/cleangrid.sh  $scripttmp/cleangrid.sh
    fanoutcmd "scp /grid/0/tmp/scripts.deploy.$cluster/cleangrid.sh __HOSTNAME__:/tmp/cleangrid.sh" "$HOSTLIST"
    fanout "export GSHOME=$GSHOME && export HDFSUSER=$HDFSUSER && export MAPREDUSER=$MAPREDUSER &&  sh /tmp/cleangrid.sh && rm /tmp/cleangrid.sh "

elif [ "$REMOVE_YARN_DATA" == "true" ]
    echo == removing existing yarn data on grid.
    cp ${YINST_ROOT}/conf/hadoop/hadoopAutomation/cleangrid.sh  $scripttmp/cleangrid.sh
    fanoutcmd "scp /grid/0/tmp/scripts.deploy.$cluster/cleangrid.sh __HOSTNAME__:/tmp/cleangrid.sh" "$HOSTLIST"
    fanout "export GSHOME=$GSHOME && export HDFSUSER=$HDFSUSER && export MAPREDUSER=$MAPREDUSER && sh /tmp/cleangrid.sh yarn && rm /tmp/cleangrid.sh "

else
    echo == not removing existing data on grid.
    

fi
