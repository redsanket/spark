if [ "$INSTALLLOCALSAVE" = true ]
then
    cp slaves.$cluster.txt  /tmp/slaves.$cluster.txt
    [ -d $scripttmp ] || mkdir -p $scripttmp
    cp slaves.$cluster.txt  $scripttmp/slaves.$cluster.txt

    fanoutcmd "scp $scripttmp/slaves.$cluster.txt __HOSTNAME__:${GSHOME}/conf/local/slaves" "$HOSTLIST"
    fanoutcmd "scp $scripttmp/slaves.$cluster.txt __HOSTNAME__:${GSHOME}/gridre/yroot.$cluster/conf/hadoop/slaves.localcopy.txt" "$HOSTLIST"

    (
    echo "cd /tmp"
    echo 'mkdir /tmp/$$ && cd /tmp/$$ '
    echo "/usr/local/bin/yinst fetch  $LOCAL_CONFIG_INSTALL_STRING "
    echo "tar xzf $LOCAL_CONFIG_PKG_NAME-*.tgz"
    echo "cd share/localhadoopconfigs"
    echo  "[ -d ${GSHOME}/conf/local ] || mkdir -p ${GSHOME}/conf/local "
    echo "for f in *.xml"
    echo "do"
    echo  '   if [ '!' -f ${GSHOME}/conf/local/${f} ] '
    echo '    then '
    echo '        cp $f ${GSHOME}/conf/local/${f}'
    echo '    fi '
    echo "done"
    echo "for f in dfs.*"
    echo "do"
    echo  '   if [ '!' -f ${GSHOME}/conf/local/${f} ] '
    echo '    then '
    echo '        cp $f ${GSHOME}/conf/local/${f}'
    echo '    fi '
    echo "done"
    echo 'cd /tmp && rm -rf /tmp/$$'
    ) > $scripttmp/$cluster.cplocalfiles.sh

    fanoutcmd "scp $scripttmp/$cluster.cplocalfiles.sh __HOSTNAME__:/tmp/$cluster.cplocalfiles.sh" "$HOSTLIST"
    cmd="GSHOME=${GSHOME} sh -x /tmp/$cluster.cplocalfiles.sh"
    fanout "$cmd"
    fanoutGW "$cmd"

    if [ -n "$secondarynamenode" ]
    then
        set -x
        cmd="echo \"$secondarynamenode\" > ${GSHOME}/conf/local/masters"
        for i in $namenode; do ssh $i "$cmd"; done
        for j in $secondarynamenode; do ssh $j "$cmd"; done
        ssh $jobtrackernode "$cmd"
    fi
fi
