# $Id$

set +x

echo ================= evaluating whether to install GDM pkg
echo ================= daqnode = $daqnode
echo ================= cluster = $cluster
echo ================= GDMVERSION = $GDMVERSION
echo ================= gateway = $gateway
echo ================= ALLNAMENODESLIST = $ALLNAMENODESLIST
echo ================= evaluating whether to install GDM

regex="(gdmtestnode23(-|:)(\w+|\.)*)"
if [[ $GDMVERSION =~ $regex ]]; then
    gdm_pkg_version=${BASH_REMATCH[1]}
  
    gdm_pkg_name=gdmtestnode23
  
    if [ -n "$daqnode" ]; then
        echo === installing GDM version=\"$gdm_pkg_version\" on host $daqnode

        # install gdmtestnode23 pkg on $daqnode
    
        ssh $daqnode SUDO_USER=hadoopqa "sudo /usr/local/bin/yinst install $gdm_pkg_version -br current -same -live -yes -set $gdm_pkg_name.HADOOP_CONF_DIR=${yroothome}/conf/hadoop -set $gdm_pkg_name.HADOOP_HOME=${GSHOME}/hadoop/current -set $gdm_pkg_name.HIT_NN=\"$NAMENODE_Primary\" -set $gdm_pkg_name.HIT_JT=$jobtrackernode"
    
        st=$?
        if [ "$st" -eq 0 ] ; then
            tmpoutput=`ssh $daqnode grep CONSOLE_PORT /home/y/conf/HIT-env.conf`
            echo ==$tmpoutput==

#        if [[ $tmpoutput =~ "CONSOLE_PORT=(.*)" ]]; then
#            echo Find GDM_CONSOLE_PORT=${BASH_REMATCH[1]}
#            export GDM_CONSOLE_PORT=${BASH_REMATCH[1]}
#        else
#            echo cannot find CONSOLE_PORT from file /home/y/conf/HIT-env.conf on $daqnode
#        fi
            if [[ $tmpoutput =~ ^"export CONSOLE_PORT" ]]; then
                echo Find GDM_CONSOLE_PORT=`echo $tmpoutput | cut -d'=' -f2`
                export GDM_CONSOLE_PORT=`echo $tmpoutput | cut -d'=' -f2`
            else
                echo cannot find CONSOLE_PORT from file /home/y/conf/HIT-env.conf on $daqnode
            fi

            recordManifest "$gdm_pkg_version"
        else
            echo "*****" Failed to install $gdm_pkg_name "*****"
            exit $st
        fi
    else
	echo ===  cannot find DAQ node defined in igor, skip GDM installation now.
    fi
else
    echo ========== Cannot find out $gdm_pkg_name pkg from GDMVERSION=$GDMVERSION
    echo ========== Ignore GDM installation now....
fi
